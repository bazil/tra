/*
 * Tra file system metadata database.
 *
 * The bulk of the database code is concerned with mapping
 * paths (lists of strings) to Stat structures.
 *
 * The available operations are:
 *	- dbgetstat: retrieve the Stat associated with path
 *	- dbputstat: set the Stat associated with path
 *	- dbgetkids: retrieve a list of children under path
 *
 * We use a few tricks to reduce space requirements.
 * 
 * The main one is that sync times are reduced as we walk down
 * the hierarchy.  For example, if A has sync time (x1 y2) and
 * A/B has sync time (x1 y3 z1), we'll only store (y3 z1) for
 * A/B.
 * 
 * It will may prove useful in the future to keep a list of
 * uid/gid/muid and system names and use pointers into that
 * list rather than use absolute strings.
 * 
 * The database is built on top of the storage layer defined in
 * storage.h.  Each directory is represented by an on-disk list
 * Map. The list will probably be too inefficient for enormous
 * directories, but so far it has not been a problem.  There is
 * a B+ Tree implementation in btree.c that may eventually be
 * used in place of list.c.
 * 
 * The storage layer provides atomic updates: until flush is
 * called, nothing is written to disk, and if we crash during
 * flush we get all the changes or none of them.  To augment
 * this, we keep a redo log of all mutating database operations
 * and replay it if necessary when opening the database.
 */

#include "tra.h"

int dbwalks, dbwalklooks, idtostrs, strtoids;

static int dbapplylog(Db*);
static void ghostbust(Db*, DMap*, Vtime*);

enum	/* ON-DISK: DON'T CHANGE */
{
	LogEnd,
	LogPutmeta,
	LogDelmeta,
	LogPutstat,
	LogXXXDelstat,

	LogSize = 1024*1024
};

/*
 * marshal/unmarshal stat structures.
 */
static int
strtoid(Db *db, char *s)
{
	int i, j;
	Datum k, v;
	uchar buf[4];

	if(s == nil){
		// fprint(2, "strtoid nil => 0\n");
		return 0;
	}

	s = atom(s);

	if(strcachebystr(&db->strcache, s, &i) >= 0){
		// fprint(2, "strtoid %s => %d\n", s, i);
		return i;
	}

strtoids++;
	k.a = s;
	k.n = strlen(s);
	v.a = buf;
	v.n = sizeof buf;
	if(db->strtoid->lookup(db->strtoid, &k, &v) >= 0){
		if(v.n != 2)
			panic("db: bad data in strtoid map");
		// fprint(2, "dbstrtoid %s %d\n", s, i);
		i = SHORT(buf);
		strcache(&db->strcache, s, i);
		return i;
	}

	k.a = buf;
	k.n = 2;
	j = rand()%65535;
	for(i=0; i<65535; i++){
		if(++j == 65536)
			j = 1;
		PSHORT(buf, j);
		v.a = nil;
		v.n = 0;
		if(db->idtostr->lookup(db->idtostr, &k, &v) < 0)
			break;
		free(v.a);
	}
	if(i==65535)
		panic("db: too many strings");

	// fprint(2, "dbstrtoid %s alloc %d\n", s, j);
	v.a = s;
	v.n = strlen(s);
	if(db->idtostr->insert(db->idtostr, &k, &v, DMapCreate) < 0)
		panic("db: cannot create new string");
	if(db->strtoid->insert(db->strtoid, &v, &k, DMapCreate) < 0)
		panic("db: cannot create new string");
	strcache(&db->strcache, s, j);
	return j;
}

static char*
idtostr(Db *db, int i)
{
	char *s;
	uchar buf[2];
	Datum k, v;

	if(i == 0)
		return nil;

	if((s = strcachebyid(&db->strcache, i)) != nil)
		return s;

idtostrs++;
	PSHORT(buf, i);
	k.a = buf;
	k.n = sizeof buf;
	v.a = nil;
	v.n = 0;
	if(db->idtostr->lookup(db->idtostr, &k, &v) < 0)
		return nil;
	s = atom(v.a);
	free(v.a);
	strcache(&db->strcache, s, i);
	return s;
}

static char*
dbreadbufstringatom(Db *db, Buf *b)
{
	int i;
	char *s;

	i = readbufc(b)<<8;
	i |= readbufc(b);
	if(i == 0)
		return nil;
	s = idtostr(db, i);
	if(s == nil)
		panic("db: bad string pointer %d", i);
	return atom(s);
}

static void
dbwritebufstring(Db *db, Buf *b, char *s)
{
	int i;

	i = strtoid(db, s);
	if(s)
		assert(i);

	writebufc(b, i>>8);
	writebufc(b, i);
}

static void
dbwritebufltime(Db *db, Buf *b, Ltime *t)
{
	assert(t->m != nil);

	writebufc(b, 1);
	writebufl(b, t->t);
	writebufl(b, t->wall);
	dbwritebufstring(db, b, t->m);
}

static void
dbreadbufltime(Db *db, Buf *b, Ltime *t)
{
	switch(readbufc(b)){
	case 0:
		b->p--;
		readbufltime(b, t);
		return;
	case 1:
		break;
	default:
		longjmp(b->jmp, BufData);
	}
	t->t = readbufl(b);
	t->wall = readbufl(b);
	t->m = dbreadbufstringatom(db, b);
	assert(t->m != nil);
}

static void
dbwritebufvtime(Db *db, Buf *b, Vtime *v)
{
	int i;

	writebufc(b, 0);
	if(v == nil){
		writebufl(b, -2);
		return;
	}
	writebufl(b, v->nl);
	for(i=0; i<v->nl; i++)
		dbwritebufltime(db, b, v->l+i);
}

static Vtime*
dbreadbufvtime(Db *db, Buf *b)
{
	int i, n;
	Vtime *v;

	if(readbufc(b) != 0)
		longjmp(b->jmp, 1);

	n = readbufl(b);
	if(n == -2)
		return nil;
	if(n < -2 || n > 65536)
		longjmp(b->jmp, 1);
	v = mkvtime();
	v->nl = n;
	if(v->nl > 0){
		v->l = emalloc(sizeof(v->l[0])*v->nl);
		for(i=0; i<v->nl; i++)
			dbreadbufltime(db, b, v->l+i);
	}
	setmalloctag(v, getcallerpc(&db));
	return v;
}

static Stat*
dbreadbufstat(Db *db, Buf *b)
{
	Stat *s;

	switch(readbufc(b)){
	case 0:
		b->p--;
		return readbufstat(b);
	case 1:
		break;
	default:
		longjmp(b->jmp, BufData);
	}

	if(readbufc(b) == 0)
		return nil;

	s = mkstat();
	s->state = readbufl(b);
	s->synctime = dbreadbufvtime(db, b);
	s->mtime = dbreadbufvtime(db, b);
	s->ctime = dbreadbufvtime(db, b);

	s->mode = readbufl(b);
	s->uid = dbreadbufstringatom(db, b);
	s->gid = dbreadbufstringatom(db, b);
	s->muid = dbreadbufstringatom(db, b);
	s->sysmtime = readbufl(b);

	s->length = readbufvl(b);
	memmove(s->sha1, readbufbytes(b, sizeof s->sha1), sizeof s->sha1);

	s->localsig = readbufdatum(b);
	s->localmode = readbufl(b);
	s->localuid = dbreadbufstringatom(db, b);
	s->localgid = dbreadbufstringatom(db, b);
	s->localmuid = dbreadbufstringatom(db, b);
	s->localsysmtime = readbufl(b);

	return s;
}

static void
dbwritebufstat(Db *db, Buf *b, Stat *s)
{
	writebufc(b, 1);

	if(s == nil){
		writebufc(b, 0);
		return;
	}

	writebufc(b, 1);
	writebufl(b, s->state);
	dbwritebufvtime(db, b, s->synctime);
	dbwritebufvtime(db, b, s->mtime);
	dbwritebufvtime(db, b, s->ctime);

	writebufl(b, s->mode);
	dbwritebufstring(db, b, s->uid);
	dbwritebufstring(db, b, s->gid);
	dbwritebufstring(db, b, s->muid);
	writebufl(b, s->sysmtime);

	writebufvl(b, s->length);
	writebufbytes(b, s->sha1, sizeof s->sha1);

	writebufdatum(b, s->localsig);
	writebufl(b, s->localmode);
	dbwritebufstring(db, b, s->localuid);
	dbwritebufstring(db, b, s->localgid);
	dbwritebufstring(db, b, s->localmuid);
	writebufl(b, s->localsysmtime);
}

static Stat*
dbparsestat(Db *db, uchar *a, int n)
{
	Buf b;
	Stat *s;

	b.p = a;
	b.ep = a+n;
	if(setjmp(b.jmp)){
		werrstr("bad dbparsestat format");
		return nil;
	}
	s = dbreadbufstat(db, &b);
	setmalloctag(s, getcallerpc(&db));
	return s;
}

static void
dbunparsestat(Db *db, Stat *s, Datum *d, int pad)
{
	int n;
	Buf b;

	memset(&b, 0, sizeof b);
	if(setjmp(b.jmp))
		sysfatal("malformed stat buffer in database");
	dbwritebufstat(db, &b, s);
	n = (intptr)b.p;
	b.p = emalloc(pad+n);
	d->a = b.p;
	d->n = pad+n;
	b.p += pad;
	b.ep = b.p + n;
	dbwritebufstat(db, &b, s);
	if(b.p != b.ep)
		panic("unparsestat");
}

/*
 * Write the database walking code once and not in three places.
 * Each particular call is probably overkill -- the caller doesn't need all
 * the DMap, Datum, and Stat structures along the way -- but this
 * makes everything else a bit easier.  It's only memory.
 */
typedef struct Dbwalk Dbwalk;
struct Dbwalk
{
	DMap *m;
	Datum v;
	Stat *s;
	u32int addr;
	int dirty;
};

/*
 * Allocate and fill a Dbwalk array for each walkable point in path.
 * N.B.  w[0] is root, so dbwalk returns n to indicate n+1 elements in *wp.
 */
static int
dbwalk(Db *db, char **e, int ne, Dbwalk **wp)
{
	int i, n;
	ulong addr;
	Datum k, v;
	Dbwalk *w;
	Stat *s;

dbwalks++;
	w = emalloc((ne+1)*sizeof(w[0]));
	w[0].m = db->root;
	w[0].s = copystat(db->rootstat);
	maxvtime(w[0].s->synctime, db->now);
	w[0].addr = db->root->addr;
	*wp = w;

	for(i=0; i<ne && w[i].m; i++){
		v.a = nil;
		v.n = 0;
		k.a = e[i];
		k.n = strlen(k.a);
dbwalklooks++;
		if(w[i].m->lookup(w[i].m, &k, &v) < 0)
			break;
		if(v.n < 4)
			panic("dbwalk: bad db data");
		addr = LONG((uchar*)v.a);
		s = dbparsestat(db, (uchar*)v.a+4, v.n-4);
		if(s == nil)
			panic("dbwalk: bad stat format");
		w[i+1].addr = addr;
		w[i+1].s = s;
		w[i+1].v = v;
		if(addr){
			w[i+1].m = dmapclist(db->listcache, db->s, addr, 0);
			if(w[i+1].m == nil)
				panic("dbwalk: bad list address");
			dbg(DbgDb, "list %p is %lux\n", w[i+1].m, addr);
		}else
			w[i+1].m = nil;
	}
	n = i;

	/* down-propagate sync times */
	maxvtime(w[0].s->synctime, db->now);
	for(i=1; i<=n; i++)
		maxvtime(w[i].s->synctime, w[i-1].s->synctime);

	return n;
}

/* 
 * Write out changed entries in the Dbwalk.
 */
static void
writedbwalk(Db *db, Dbwalk *w, char **e, int n)
{
	int i;
	Datum k, v;
	uchar *p;

	/*
	 * remove unnecessary sync information.
	 */
	for(i=n; i>0; i--)
		unmaxvtime(w[i].s->synctime, w[i-1].s->synctime);
	unmaxvtime(w[0].s->synctime, db->now);

	/*
	 * check whether any child ghosts can be reclaimed.
	 *
	 * it would be nicer if we could just drop the ghost
	 * when we writedbwalk on the path to the ghost during
	 * the file system scan, but at that point the ghost will
	 * always have a sync time bigger than the current
	 * directory, since it has been scanned and the directory
	 * has not.
	 */
	if(w[n].m != nil)
		ghostbust(db, w[n].m, w[n].s->synctime);

	/*
	 * the original database associated an empty
	 * list with every file and directory.  now we represent the
	 * empty list by not having one, a big space savings.
	 * look for old-style empty lists and remove them.
	 *
	 * this will also clean up directory lists as they become empty.
	 */
	if(n!=0 && w[n].m != nil){
		if(w[n].m->isempty(w[n].m)){
			dbg(DbgDb, "removing empty list %p at %ux for %s\n",
				w[n].m, w[n].m->addr, n ? e[n-1] : "<root>");
			w[n].m->free(w[n].m);
			w[n].m = nil;
			w[n].addr = 0;
			w[n].dirty = 1;
		}
	}

	/*
	 * write changes.
	 */
	if(w[0].dirty){
		freestat(db->rootstat);
		db->rootstat = copystat(w[0].s);
		db->rootstatdirty = 1;
	}
	for(i=1; i<=n; i++){
		if(!w[i].dirty)
			continue;
		dbunparsestat(db, w[i].s, &v, 4);
		p = v.a;
		PLONG(p, w[i].addr);
		k.a = e[i-1];
		k.n = strlen(e[i-1]);
		if(w[i-1].m->insert(w[i-1].m, &k, &v, DMapCreate|DMapReplace) < 0)
			panic("dmapinsert: %r");
		free(v.a);
	}
}

/*
 * free the dbwalk and its contents.  note that w[0].m is not ours to free.
 */
static void
freedbwalk(Dbwalk *w, int n)
{
	int i;

	for(i=0; i<=n; i++){
		if(i!=0 && w[i].m)
			w[i].m->close(w[i].m);
		freestat(w[i].s);
		free(w[i].v.a);
	}
	free(w);	
}

/*
 * create a list of kids from the map m. 
 */
static void
walkkids(void *v, Datum *key, Datum *val)
{
	struct { Db *db; Kid *k; int nk; int err; } *a;
	uchar *p;

	a = v;
	if(a->err)
		return;
	if(a->nk%16 == 0)
		a->k = erealloc(a->k, (a->nk+16)*sizeof(Kid));
	a->k[a->nk].name = emalloc(key->n+1);
	memmove(a->k[a->nk].name, key->a, key->n);
	if(val->n <= 4)
		panic("walkkids: bad db format");
	p = val->a;
	a->k[a->nk].addr = LONG(p);
	a->k[a->nk].stat = dbparsestat(a->db, (uchar*)val->a+4, val->n-4);
	if(a->k[a->nk].stat == nil)
		panic("walkkids: bad stat format");
	a->nk++;
}

static int
kidsinmap(Db *db, DMap *m, Kid **pk)
{
	struct { Db *db; Kid *k; int nk; int err; } a;

	memset(&a, 0, sizeof a);
	a.db = db;
	m->walk(m, walkkids, &a);
	*pk = a.k;
	return a.nk;
}

/*
 * look up the stat information for the given path.
 */
int
dbgetstat(Db *db, char **e, int ne, Stat **ps)
{
	int n;
	Dbwalk *w;

	n = dbwalk(db, e, ne, &w);

	if(n == ne){
		*ps = w[n].s;
		w[n].s = nil;
	}else
		/* invent ghost if necessary */
		*ps = mkghoststat(w[n].s->synctime);

	freedbwalk(w, n);
	return ne;
}

/*
 * rewrite the stat information for the given path.
 */
static int
_dbputstat(Db *db, char **e, int ne, Stat *s)
{
	int i, n;
	Dbwalk *w;

	dbg(DbgDb, "_dbputstat %s %$\n", ne ? e[ne-1] : "<root>", s);

	n = dbwalk(db, e, ne, &w);
	if(n == ne)
		freestat(w[n].s);

	/* look for weirdness in the map addrs */
	for(i=0; i<ne; i++)
		if((w[i].m==nil) ^ (w[i].addr==0))
			fprint(2, "dbputstat m %p addr %ux\n", w[i].m, w[i].addr);

	/* fill in links along the way */
	for(i=n; i<ne; i++){
		if(w[i].m == nil){
			w[i].m = dmapclist(db->listcache, db->s, 0, db->pagesize);
			w[i].addr = w[i].m->addr;
			w[i].dirty = 1;
		}
	}

	/* fill in stats along the way */
	for(i=n+1; i<ne; i++){
		w[i].s = mkghoststat(w[i-1].s->synctime);
		w[i].dirty = 1;
	}

	/* fill in final stat */
	w[ne].s = copystat(s);
	w[ne].dirty = 1;

	/* fill in modification times */
	for(i=0; i<ne; i++){
		if(!leqvtime(w[ne].s->mtime, w[i].s->mtime)){
			maxvtime(w[i].s->mtime, w[ne].s->mtime);
			w[i].dirty = 1;
		}
	}

	/*
	 * before the db rewrite which included the down-propagation
	 * of sync times in dbgetkids, the db scan wouldn't have maxed in
	 * the parent sync time with the kid sync time when updating 
	 * the local time entry.
	 *
	 * if we are reading a redo2 log from such an old minisync,
	 * using such a sync time unaltered will fail because the sync
	 * time is not >= the parent sync time.  make it so.
	 */
	if(ne>0 && !leqvtime(w[ne-1].s->synctime, w[ne].s->synctime)){
		static int first = 1;

		if(first){
			fprint(2, "database redo log is from pre-May 25 2002 trasrv; coercing sync times\n");
			first = 0;
		}
		maxvtime(w[ne].s->synctime, w[ne-1].s->synctime);
	}

	/* write everything back */
	writedbwalk(db, w, e, ne);
	freedbwalk(w, ne);
	return 0;
}

/*
 * remove all the ghosts with sync time <= vt from m, a list of children.
 * we don't need to recurse because the next file system scan will putstat
 * for every path in the system, so we'll get grandchildren the next time around. 
 *
 * also trim children's sync times.
 */
static void
ghostbust(Db *db, DMap *m, Vtime *vt)
{
	int i, n;
	uchar *p;
	Kid *k;
	Datum key, val;

	n = kidsinmap(db, m, &k);
	for(i=0; i<n; i++){
		// fprint(2, "%p %d %s...\n", m, i, k[i].name);
		if(k[i].stat->state == SNonexistent
		&& k[i].addr == 0
		&& leqvtime(k[i].stat->synctime, vt)){
			dbg(DbgGhost, "ghost for %s / %V removed; parent %V\n\t%$",
				k[i].name, k[i].stat->synctime, vt, k[i].stat);
			key.a = k[i].name;
			key.n = strlen(k[i].name);
			if(m->delete(m, &key) < 0)
				panic("ghostbust delete: %r");
			continue;
		}
		maxvtime(k[i].stat->synctime, vt);
		unmaxvtime(k[i].stat->synctime, vt);
		/* write back */
		key.a = k[i].name;
		key.n = strlen(k[i].name);
		dbunparsestat(db, k[i].stat, &val, 4);
		p = val.a;
		PLONG(p, k[i].addr);
		// fprint(2, "wb %d...", val.n);
		if(m->insert(m, &key, &val, DMapReplace) < 0)
			panic("ghostbust replace: %r");
		free(val.a);
	}
	freekids(k, n);
}

/* 
 * return the names and stat information for the children
 * of the named path.
 */
int
dbgetkids(Db *db, char **e, int ne, Kid **pk)
{
	int i, n, nk;
	Kid *k;
	Dbwalk *w;

	n = dbwalk(db, e, ne, &w);

	if(w[ne].m == nil){
		*pk = nil;
		nk = 0;
	}else{
		k = nil;
		nk = kidsinmap(db, w[ne].m, &k);
		/* down-propagate sync times */
		for(i=0; i<nk; i++)
			maxvtime(k[i].stat->synctime, w[ne].s->synctime);
		if(k)
			setmalloctag(k, getcallerpc(&db));
		*pk = k;
	}
	freedbwalk(w, n);
	return nk;
}

/*
 * metadata is just a key/value string map
 */
char*
dbgetmeta(Db *db, char *key)
{
	Datum k, v;

	k.a = key;
	k.n = strlen(key);
	v.a = nil;
	v.n = 0;

	if(db->meta->lookup(db->meta, &k, &v) < 0)
		return nil;
	return v.a;
}

static int
_dbputmeta(Db *db, char *key, char *val)
{
	Datum k, v;

	k.a = key;
	k.n = strlen(key);
	v.a = val;
	v.n = strlen(val);

	return db->meta->insert(db->meta, &k, &v, DMapCreate|DMapReplace);
}


/* * * * * * database redo log * * * * * */
/*
 * Log changes to the database so we can replay them if
 * we get killed before writing the real db back out to disk.
 */
static void
dbflushit(Db *db)
{
	int n;
	int x;
	uchar hdr[12];

	n = db->logbuf->p - db->logbase;
	PLONG(hdr, n);
	x = random();
	PLONG(hdr+4, x);
	x = time(0);
	PLONG(hdr+8, x);
	if(write(db->logfd, hdr, 12) != 12)
		sysfatal("writing db log: %r");
	if(write(db->logfd, db->logbase, n) != n)
		sysfatal("writing db log: %r");
	if(0 && fsync(db->logfd) < 0)
		sysfatal("syncing db log: %r");
	if(write(db->logfd, hdr, 12) != 12)
		sysfatal("writing db log: %r");
	if(0 && fsync(db->logfd) < 0)
		sysfatal("syncing db log: %r");
	db->logbuf->p = db->logbase;
}
	
void
logflush(Db *db)
{
	dbflushit(db);
}

static void
logit(Db *db, Buf *b)
{
	int n;

/*
	if(db->ignwr){
		free(b);
		return;
	}
*/

	n = b->ep - b->p;
	if(db->logbuf->ep - db->logbuf->p < n)
		dbflushit(db);
	if(db->logbuf->ep - db->logbuf->p < n)
		panic("dblogit");
//fprint(2, "LOG %.*H\n", b->ep - b->p, b->p);
	memmove(db->logbuf->p, b->p, n);
	free(b);
	db->logbuf->p += n;
	if(db->alwaysflush)
		dbflushit(db);
}

static int
_dbdelmeta(Db *db, char *key)
{
	Datum k;

	k.a = key;
	k.n = strlen(key);
	return db->meta->delete(db->meta, &k);
}

static Buf*
putstatbuf(char **e, int ne, Stat *st)
{
	int i;
	Buf *b, s;

	memset(&s, 0, sizeof s);
	if(setjmp(s.jmp))
		panic("putstatbuf");
	writebufc(&s, LogPutstat);
	writebufl(&s, ne);
	for(i=0; i<ne; i++)
		writebufstring(&s, e[i]);
	writebufstat(&s, st);

	b = mkbuf(nil, (intptr)s.p);
	writebufc(b, LogPutstat);
	writebufl(b, ne);
	for(i=0; i<ne; i++)
		writebufstring(b, e[i]);
	writebufstat(b, st);
	if(b->ep != b->p)
		panic("putstatbuf0");
	b->p = (uchar*)&b[1];
	return b;
}

int
dbputstat(Db *db, char **e, int ne, Stat *s)
{
	if(_dbputstat(db, e, ne, s) < 0)
		return -1;
	logit(db, putstatbuf(e, ne, s));
	return 0;
}

static Buf*
putmetabuf(char *key, char *val)
{
	Buf *b, s;

	memset(&s, 0, sizeof s);
	if(setjmp(s.jmp))
		panic("putmetabuf");
	writebufc(&s, LogPutmeta);
	writebufstring(&s, key);
	writebufstring(&s, val);

	b = mkbuf(nil, s.p-(uchar*)nil);
	writebufc(b, LogPutmeta);
	writebufstring(b, key);
	writebufstring(b, val);
	if(b->ep != b->p)
		panic("putmetabuf0");
	b->p = (uchar*)&b[1];
	return b;
}

int
dbputmeta(Db *db, char *key, char *val)
{
	if(_dbputmeta(db, key, val) < 0)
		return -1;
	logit(db, putmetabuf(key, val));
	return 0;
}

static Buf*
delmetabuf(char *key)
{
	Buf *b, s;

	memset(&s, 0, sizeof s);
	if(setjmp(s.jmp))
		panic("delmetabuf");
	writebufc(&s, LogDelmeta);
	writebufstring(&s, key);

	b = mkbuf(nil, (intptr)s.p);
	writebufc(b, LogDelmeta);
	writebufstring(b, key);
	if(b->ep != b->p)
		panic("delmetabuf0");
	b->p = (uchar*)&b[1];
	return b;
}

int
dbdelmeta(Db *db, char *key)
{
	if(_dbdelmeta(db, key) < 0)
		return -1;
	logit(db, delmetabuf(key));
	return 0;
}

static char*
logname(char *file)
{
	char *log;
	int len;

	log = malloc(strlen(file)+6+1);
	if(log == nil)
		return nil;
	strcpy(log, file);
	len = strlen(log);
	while(len > 1 && log[len-1] == '/')
		log[--len] = '\0';
	strcat(log, ".redo2");
	return log;
}

static int
dbresetlog(Db *db)
{
	if(seek(db->logfd, 0, 0) < 0
	|| write(db->logfd, "XXXXXXXXXXXX", 12) != 12
	|| ftruncate(db->logfd, 0) < 0)
		return -1;
	return 0;
}

static int
dbapplylog(Db *db)
{
	int i, n, ne;
	uchar hdr[12], hdr0[12];
	Buf *b;
	char *k, *v, **e;
	volatile int first, changes, me;
	Stat *s;

	changes = 0;
	b = db->logbuf;
	e = nil;
	me = 0;
	first = 1;
	for(;;){
		/*
		 * data errors here might be partially flushed data.
		 * the best we can do is just stop reading.
		 */
//fprint(2, "reading log...");
		if(readn(db->logfd, hdr, 12) != 12)
			break;
//fprint(2, "12...");
		n = LONG(hdr);
//fprint(2, "%d...", n);
		if(n == 0 || n > LogSize)	/* probably bad data */
			break;
		if(readn(db->logfd, db->logbase, n) != n)
			break;
//fprint(2, "12...");
		if(readn(db->logfd, hdr0, 12) != 12)
			break;
//fprint(2, "chk...");
		if(memcmp(hdr, hdr0, 12) != 0)
			break;

//fprint(2, "log...\n");

		/*
		 * this is a committed log record.  process it.
		 * errors here are in entirely written data and
		 * are thus cause for concern.
		 */
		b->p = db->logbase;
		b->ep = b->p + n;
		if(first){
			fprint(2, "database not closed properly; applying operation redo log\n");
			first = 0;
		}
		if(setjmp(b->jmp))
			sysfatal("bad format in redo2 log");
		while(b->p < b->ep){
			n = readbufc(b);
//fprint(2, "%d/%d...", b->p - db->logbase, n);
			switch(n){
			default:
				sysfatal("bad format in redo2 log");
				break;
			case LogPutmeta:
				k = readbufstring(b);
				v = readbufstring(b);
fprint(2, "log putmeta %s %s\n", k, v);
				_dbputmeta(db, k, v);
				changes = 1;
				break;
			case LogDelmeta:
				k = readbufstring(b);
fprint(2, "log delmeta %s\n", k);
				_dbdelmeta(db, k);
				changes = 1;
				break;
			case LogPutstat:
				ne = readbufl(b);
				if(ne > me){
					me = ne;
					e = erealloc(e, me*sizeof(char*));
				}
				for(i=0; i<ne; i++)
					e[i] = readbufstring(b);
				s = readbufstat(b);
if(0){
Path *p;
p=nil;
for(i=ne-1; i>=0; i--)
	p = mkpath(p, e[i]);
fprint(2, "log putstat %P %$\n", p, s);
freepath(p);
}
				_dbputstat(db, e, ne, s);
				freestat(s);
				changes = 1;
				break;
			}
		}
	}
//fprint(2, "out\n");
	if(changes)
		return flushdb(db);
	return 0;
}

/* * * * * * database open/close routines * * * * * */
static DMap*
namedmap(Db *db, char *s)
{
	char *q, *qq, buf[32];
	DMap *m;
	u32int a;

	if((q=dbgetmeta(db, s)) == nil){
		m = dmapclist(db->listcache, db->s, 0, db->pagesize);
		snprint(buf, sizeof buf, "%ud", m->addr);
		m->close(m);
		_dbputmeta(db, s, buf);
		q = estrdup(buf);
	}
	if((a = strtol(q, &qq, 10)) == 0 || *qq != '\0'){
		werrstr("bad %s address '%s'", s, q);
		fprint(2, "bad address %s\n", q);
		return nil;
	}
	if((m=dmapclist(db->listcache, db->s, a, 0)) == nil){
		werrstr("load %s: %r", s);
		fprint(2, "load %r\n");
		return nil;
	}
	// fprint(2, "namedmap %s %p\n", s, m);
	return m;
}

static Db*
genopendb(char *path, DStore *s, u32int addr, int pagesize)
{
	char err[ERRMAX];
	char *logpath;
	Db *db;
	DBlock *super;
	DMap *root, *meta;
	Listcache *lc;
	uchar *p;
	u32int a;
	int logfd;

	root = nil;
	meta = nil;
	lc = openlistcache();
	if(addr == 0){
		super = s->alloc(s, 20);
		if(super == nil)
			return nil;
		p = super->a;
		memmove(p, "DBHD", 4);
		p += 4;
		PLONG(p, pagesize);
		p += 4;
		root = dmapclist(lc, s, 0, pagesize);
		if(root==nil){
			super->free(super);
			closelistcache(lc);
			return nil;
		}
		PLONG(p, root->addr);
		p += 4;
		meta = dmapclist(lc, s, 0, pagesize);
		if(meta==nil){
			root->free(root);
			super->free(super);
			closelistcache(lc);
			return nil;
		}
		PLONG(p, meta->addr);
		p += 4;
		PLONG(p, 0);
		root->flush(root);
		meta->flush(meta);
	}else{
		if((super = s->read(s, addr)) == nil){
			closelistcache(lc);
			return nil;
		}
		if(super->n != 20 || memcmp(super->a, "DBHD", 4) != 0){
			super->close(super);
			werrstr("bad superblock");
			closelistcache(lc);
			return nil;
		}
	}
	db = emalloc(sizeof(Db));
	db->logfd = -1;
	db->s = s;
	db->addr = super->addr;
	p = super->a;
	p += 4;	/* DBHD */

	db->pagesize = LONG(p);
	p += 4;

	db->listcache = lc;

	a = LONG(p);
	p += 4;
	if(a == 0){
		strcpy(err, "bad root directory pointer");
	Err:
		if(addr == 0){
			super->free(super);
			root->free(root);
			meta->free(meta);
		}else
			super->close(super);
		close(db->logfd);
		closelistcache(db->listcache);
		free(db);
		werrstr("%s", err);
		return nil;
	}
	if((db->root = dmapclist(db->listcache, s, a, 0)) == nil)
		goto Err;

	a = LONG(p);
	p += 4;
	if(a == 0){
		strcpy(err, "bad meta db pointer");
	Err1:
		db->root->close(db->root);
		goto Err;
	}
	if((db->meta = dmapclist(db->listcache, s, a, 0)) == nil){
		rerrstr(err, sizeof err);
		goto Err1;
	}

	if((db->strtoid = namedmap(db, "strtoid")) == nil)
{
fprint(2, "no strtoid");
		goto Rerr2;
}
	if((db->idtostr = namedmap(db, "idtostr")) == nil)
{
fprint(2, "no idtostr");
		goto Rerr2;
}

	a = LONG(p);
	if(a == 0){
		db->rootstat = nil;
		db->rootstatblock = nil;
	}else{
		db->rootstatblock = s->read(s, a);	
		if(db->rootstatblock == nil){
fprint(2, "rootstat block %p\n", db->rootstatblock);
		Rerr2:
			rerrstr(err, sizeof err);
		Err2:
			fprint(2, "err %s\n", err);
			if(db->strtoid)
				db->strtoid->close(db->strtoid);
			if(db->idtostr)
				db->idtostr->close(db->idtostr);
			db->meta->close(db->meta);
			goto Err1;
		}
		if((db->rootstat = dbparsestat(db, db->rootstatblock->a, db->rootstatblock->n)) == nil){
			rerrstr(err, sizeof err);
			goto Err2;
		}
	}
	db->super = super;

	logpath = logname(path);
	if(logpath == nil){
		snprint(err, sizeof err, "logpath: %r");
		goto Err2;
	}
	if((logfd = open(logpath, ORDWR)) < 0){
		snprint(err, sizeof err, "open %s: %r", logpath);
		goto Err2;
	}
	db->logfd = logfd;
	db->logbuf = mkbuf(nil, LogSize);
	db->logbase = db->logbuf->p;
	if(dbapplylog(db) < 0){
		snprint(err, sizeof err, "dbapplylog: %r");
		goto Err2;
	}
	db->logbuf->p = db->logbase;
	db->logbuf->ep = db->logbase + LogSize;
	seek(logfd, 0, 0);
	write(logfd, "XXXXXXXXXXXX", 12);
	seek(logfd, 0, 0);
	ftruncate(logfd, 0);
	db->breakwrite = config("testdblog");
	return db;
}

Db*
createdb(char *path, int pagesize)
{
	char e[ERRMAX];
	int fd;
	Db *db;
	DStore *s;
	DBlock *b;

	s = createdstore(path, pagesize);
	if(s == nil)
		return nil;

	b = s->alloc(s, 8);
	if(b->addr != pagesize){
		werrstr("couldn't predict block address");
	Error:
		rerrstr(e, sizeof e);
		b->close(b);
		s->free(s);
		errstr(e, sizeof e);
		return nil;
	}

	if((fd = syscreateexcl(logname(path))) < 0)
		goto Error;
	close(fd);
	db = genopendb(path, s, 0, pagesize-s->hdrsize);
	if(db == nil)
		goto Error;
	memmove(b->a, "DBDB", 4);
	PLONG((uchar*)b->a+4, db->addr);
	flushdb(db);
	flushlistcache(db->listcache);
	return db;
}

Db*
opendb(char *path)
{
	Db *db;
	DBlock *b;
	DStore *s;
	u32int a;
	char e[ERRMAX];

	s = opendstore(path);
	if(s == nil)
		return nil;

	b = s->read(s, s->pagesize);
	if(b == nil){
		s->close(s);
		werrstr("couldn't read superblock");
		return nil;
	}

	if(b->n != 8 || memcmp(b->a, "DBDB", 4) != 0){
		werrstr("damaged superblock");
	Error:
		rerrstr(e, sizeof e);
		b->close(b);
		s->close(s);
		errstr(e, sizeof e);
		return nil;
	}

	a = LONG((uchar*)b->a+4);
	if(a == 0){
		werrstr("bad db address in superblock");
		goto Error;
	}
	free(b);

	db = genopendb(path, s, a, 0);
	if(db == nil)	
		goto Error;
	return db;
}

int
flushdb(Db *db)
{
	uchar *p;
	Datum sv;
	DBlock *b;

	if(db->meta->flush(db->meta) < 0)
		return -1;
	if(db->strtoid->flush(db->strtoid) < 0)
		return -1;
	if(db->idtostr->flush(db->idtostr) < 0)
		return -1;
	if(db->root->flush(db->root) < 0)
		return -1;
	if(db->rootstatdirty){
		dbunparsestat(db, db->rootstat, &sv, 0);
		b = db->rootstatblock;
		if(b && b->n == sv.n){
			memmove(b->a, sv.a, sv.n);
			b->flags |= DDirty;
		}else{
			if(b)
				b->free(b);
			b = db->s->alloc(db->s, sv.n);
			if(b == nil){
				free(sv.a);
				return -1;
			}
			memmove(b->a, sv.a, sv.n);
			free(sv.a);
			b->flags |= DDirty;
		}
		db->rootstatblock = b;
	}
	p = (uchar*)db->super->a + 4+4;
	PLONG(p, db->root->addr);
	p += 4;
	PLONG(p, db->meta->addr);
	p += 4;
	if(db->rootstatblock){
		PLONG(p, db->rootstatblock->addr);
	}else{
		PLONG(p, 0);
	}
	db->rootstatdirty = 0;
	db->super->flags |= DDirty;
	if(db->super->flush(db->super) < 0)
		return -1;
	if(db->s->flush(db->s) < 0)
		return -1;
	return 0;
}

int
closedb(Db *db)
{
	int r;

	if(db->breakwrite){
		free(db->logbuf);
		closelistcache(db->listcache);
		free(db);
		return 0;
	}

	r = flushdb(db);
if(r<0) {fprint(2, "cannot write: %r\n"); abort(); }
	freestat(db->rootstat);
	db->meta->close(db->meta);
	db->strtoid->close(db->strtoid);
	db->idtostr->close(db->idtostr);
	db->root->close(db->root);
	if(db->rootstatblock)
		db->rootstatblock->close(db->rootstatblock);
	db->super->close(db->super);
	closelistcache(db->listcache);
	db->s->close(db->s);
	dbresetlog(db);
	close(db->logfd);
	free(db->logbuf);
	free(db);
	return r;
}

int
freedb(Db *db)
{
	int r;

	flushdb(db);
	freestat(db->rootstat);
	db->meta->close(db->meta);
	db->strtoid->close(db->strtoid);
	db->idtostr->close(db->idtostr);
	db->root->close(db->root);
	db->rootstatblock->close(db->rootstatblock);
	db->super->close(db->super);
	closelistcache(db->listcache);
	r = db->s->free(db->s);
	dbresetlog(db);
	close(db->logfd);
	free(db);
	return r;
}

static void
metawalk(void *a, Datum *key, Datum *val)
{
	int fd;

	fd = (intptr)a;

	fprint(fd, "\t%.*s: %.*s\n",
		utfnlen(key->a, key->n), key->a,
		utfnlen(val->a, val->n), val->a);
}

typedef struct D D;
struct D
{
	Db *db;
	Path *p;
	Vtime *vt;
	int fd;
};

static void
dumpwalk(void *v, Datum *key, Datum *val)
{
	char *name;
	Stat *s;
	D a;
	DMap *m;
	u32int addr;

	a = *(D*)v;
	name = emallocnz(key->n+1);
	memmove(name, key->a, key->n);
	name[key->n] = '\0';
	a.p = mkpath(a.p, name);
	free(name);
	s = dbparsestat(a.db, (uchar*)val->a+4, val->n-4);
	addr = LONG((uchar*)val->a);
	fprint(a.fd, "%P\tlist=%ux\t\tdelta=%V", a.p, addr, s->synctime);
	s->synctime = maxvtime(s->synctime, a.vt);
	fprint(a.fd, " %$\n", s);
	a.vt = s->synctime;
	if(addr != 0){
		m = dmapclist(a.db->listcache, a.db->s, addr, 0);
		if(m != nil){
			m->walk(m, dumpwalk, &a);
			m->close(m);
		}else
			fprint(a.fd, "\t\t-no valid db for %P\n", a.p);
	}
	freestat(s);
	freepath(a.p);
}

void
dumpdb(Db *db, int fd)
{
	D a;
	Path *p;
	int now;
	char *name;

	fprint(fd, "meta:\n");
	/* intptr cast to placate 64-bit gcc */
	db->meta->walk(db->meta, metawalk, (void*)(intptr)fd);

	fprint(fd, "strtoid:\n");
	db->strtoid->walk(db->strtoid, metawalk, (void*)(intptr)fd);

	fprint(fd, "idtostr:\n");
	db->idtostr->walk(db->idtostr, metawalk, (void*)(intptr)fd);

	now = atoi(dbgetmeta(db, "now"));
	name = dbgetmeta(db, "sysname");

	a.vt = mkvtime1(name, now, time(0));
	db->rootstat->synctime = maxvtime(db->rootstat->synctime, a.vt);
	p = nil;
	fprint(fd, "%P\tlist=nil\t\tdelta='' %$\n", p, db->rootstat);
	a.db = db;
	a.p = p;
	a.fd = fd;
	a.vt = db->rootstat->synctime;
	db->root->walk(db->root, dumpwalk, &a);
}

int
dbignorewrites(Db *db)
{
//	db->ignwr = 1;
//	dstoreignorewrites(db->s);
	return 0;
}
