#include "tra.h"

/*
 * Find and fix inconsistencies in the Tra database.
 * Inconsistencies arise only from programmer errors,
 * not from crashes (database updates are atomic).
 *
 * Assuming the synchronization times on directories are
 * correct (there are no bugs found yet that break them), then
 * it is okay to drop entries from the database while fixing
 * it.  The next time the file system is scanned, new entries
 * will be created (for extant files) or not (for deleted
 * files).  Either way, the synchronization time will be
 * inherited from the directory and thus be less than or equal
 * to what it really should be.  Having old synchronization
 * times runs the risk of spurious conflicts but will not cause
 * lost file updates.
 *
 * In addition to finding and fixing database inconsistencies,
 * garbage collect any entries in the string table that aren't
 * needed anymore.  We could have ref counted the maps, but we
 * probably would have gotten a count wrong at some point and
 * had to do this anyway.  This garbage collection could be
 * done as part of a full-tree file system scan, but it's just
 * not a high priority event.  The table only lists strings
 * used as uid, gid, muid, and replica names.  It can hold
 * 65536 strings, but will get unbearably slow somewhere past
 * 1000 strings.  I doubt it will grow even to 100 strings.
 * I wouldn't even have implemented this except that I needed
 * a program to fix the broken lists.
 *
 * File space leaks (none are currently known) can be corrected
 * by running with -c to create a new copy of the database.
 */

char *knownproblems= 
	"\t-list insertions sometimes don't maintain sorted order.\n"
	"\t\t(fixed 25 May 2002)\n"
;

int nflag;
int verbose;
Db *db;
Db *wdb;

/*
 * marshal/unmarshal stat structures.
 */
static int
strtoid(Db *db, char *s)
{
	int i;
	Datum k, v;
	uchar buf[4];

	if(s == nil)
		return 0;

	k.a = s;
	k.n = strlen(s);
	v.a = buf;
	v.n = sizeof buf;
	if(db->strtoid->lookup(db->strtoid, &k, &v) >= 0){
		if(v.n != 2)
			panic("db: bad data in strtoid map");
		return SHORT(buf);
	}

	k.a = buf;
	k.n = 2;
	for(i=1;; i++){
		PSHORT(buf, i);
		v.a = nil;
		v.n = 0;
		if(db->idtostr->lookup(db->idtostr, &k, &v) < 0)
			break;
	}
	v.a = s;
	v.n = strlen(s);
	if(db->idtostr->insert(db->idtostr, &k, &v, DMapCreate) < 0)
		panic("db: cannot create new string");
	if(db->strtoid->insert(db->strtoid, &v, &k, DMapCreate) < 0)
		panic("db: cannot create new string");

	return i;
}

static char*
idtostr(Db *db, int i)
{
	char *s;
	uchar buf[2];
	Datum k, v;

	if(i == 0)
		return nil;

	PSHORT(buf, i);
	k.a = buf;
	k.n = sizeof buf;
	v.a = nil;
	v.n = 0;
	if(db->idtostr->lookup(db->idtostr, &k, &v) < 0)
		return nil;
	s = emalloc(v.n+1);
	v.a = s;
	if(db->idtostr->lookup(db->idtostr, &k, &v) < 0)
		panic("idtostr");
	return v.a;
}

static char*
dbreadbufstringdup(Db *db, Buf *b)
{
	int i;
	char *s;

	i = readbufc(b)<<8;
	i |= readbufc(b);
	if(i == 0)
		return nil;
	s = idtostr(db, i);
	if(s == nil){
		werrstr("db: bad string pointer %d", i);
		longjmp(b->jmp, BufData);
	}
	return s;
}

static void
dbwritebufstring(Db *db, Buf *b, char *s)
{
	int i;

	i = strtoid(db, s);
	writebufc(b, i>>8);
	writebufc(b, i);
}

static void
dbwritebufltime(Db *db, Buf *b, Ltime *t)
{
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
	t->m = dbreadbufstringdup(db, b);
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
	s->uid = dbreadbufstringdup(db, b);
	s->gid = dbreadbufstringdup(db, b);
	s->muid = dbreadbufstringdup(db, b);
	s->sysmtime = readbufl(b);

	s->length = readbufvl(b);
	memmove(s->sha1, readbufbytes(b, sizeof s->sha1), sizeof s->sha1);

	s->localsig = readbufdatum(b);
	s->localmode = readbufl(b);
	s->localuid = dbreadbufstringdup(db, b);
	s->localgid = dbreadbufstringdup(db, b);
	s->localmuid = dbreadbufstringdup(db, b);
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
		panic("malformed stat structure in database?");
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
 * create a list of kids from the map m, removing malformed entries.
 */
static void
walkkids(void *v, Datum *key, Datum *val)
{
	struct { Path *p; Db *db; Kid *k; int nk; int err; } *a;
	uchar *p;
	Path *kp;

	a = v;
	if(a->err)
		return;
	if(a->nk%16 == 0)
		a->k = erealloc(a->k, (a->nk+16)*sizeof(Kid));
	a->k[a->nk].name = emalloc(key->n+1);
	memmove(a->k[a->nk].name, key->a, key->n);
	if(val->n <= 4){
	Malformed:
		kp = mkpath(a->p, a->k[a->nk].name);
		print("%P: removing malformed database entry\n", kp);
		if(verbose)
			print("\tdata was %.*H\n", val->n, val->a);
		free(a->k[a->nk].name);
		return;
	}
	p = val->a;
	a->k[a->nk].addr = LONG(p);
	a->k[a->nk].stat = dbparsestat(a->db, (uchar*)val->a+4, val->n-4);
	if(a->k[a->nk].stat == nil)
		goto Malformed;
	a->nk++;
}

static int
kidsinmap(Db *db, Path *p, DMap *m, Kid **pk)
{
	struct { Path *p; Db *db; Kid *k; int nk; int err; } a;

	memset(&a, 0, sizeof a);
	a.db = db;
	a.p = p;
	m->walk(m, walkkids, &a);
	*pk = a.k;
	return a.nk;
}

int
kidnamecmp(const void *a, const void *b)
{
	Kid *ka, *kb;

	ka = (Kid*)a;
	kb = (Kid*)b;
	return strcmp(ka->name, kb->name);
}

DMap*
dbfixtree(Path *p, DMap *m)
{
	Kid *k;
	int bad, changed, i, j, nk;
	u32int naddr;
	DMap *km;
	Path *kp;
	uchar *up;
	Datum key, val;

	k = nil;
	nk = kidsinmap(db, p, m, &k);
	changed = 0;

	/*
	 * check for unsorted list
	 */
	bad = 0;
	for(i=0; i<nk-1; i++)
		if(strcmp(k[i].name, k[i+1].name) > 0){
			print("%P: missort: %s before %s\n", p, k[i].name, k[i+1].name);
			bad = 1;
		}
	if(bad){
		print("%P: directory listing is not sorted; sorting\n", p);
		qsort(k, nk, sizeof(k[0]), kidnamecmp);
		changed = 1;
	}

	/*
	 * check for duplicate entries
	 */
	for(i=0; i<nk-1; i++){
		if(strcmp(k[i].name, k[i+1].name) == 0){
			for(j=i; j<nk && strcmp(k[i].name, k[j].name)==0; j++)
				;
			kp = mkpath(p, k[i].name);
			print("%P: %d duplicate entries; deleting all\n", kp, j-i);
			freepath(kp);
			if(nk-j)
				memmove(&k[i], &k[j], (nk-j)*sizeof(k[0]));
			nk -= j-i;
			i--;
			changed = 1;
			continue;
		}
	}

	/*
	 * recurse to handle children
	 */
	for(i=0; i<nk; i++){
		if(k[i].addr == 0)
			continue;
		km = dmaplist(db->s, k[i].addr, 0);
		kp = mkpath(p, k[i].name);
		if(km == nil){
			print("%P: bad directory pointer; truncating directory\n", kp);
			k[i].addr = 0;
			changed = 1;
			freepath(kp);
			continue;
		}
		km = dbfixtree(kp, km);
		if(km){
			naddr = km->addr;
			km->close(km);
		}else
			naddr = 0;
		if(naddr != k[i].addr){
			changed = 1;
			k[i].addr = naddr;
		}
		freepath(kp);
	}

	/*
	 * rewrite the pages; insert in reverse order for linear time.
	 */
	if(changed || wdb != db){
		m->free(m);
		m = dmaplist(wdb->s, 0, db->pagesize);
		for(i=nk-1; i>=0; i--){
			key.a = k[i].name;
			key.n = strlen(k[i].name);
			dbunparsestat(wdb, k[i].stat, &val, 4);
			up = val.a;
			PLONG(up, k[i].addr);
			if(m->insert(m, &key, &val, DMapCreate) < 0){
				kp = mkpath(p, k[i].name);
				sysfatal("%P: cannot reinsert %P: %r", p, kp);
			}
		}
	}
	return m;
}

/*
 * arguably, we should be using something like these
 * Stab-based routines in db.c.  it's much simpler.
 */
typedef struct Stab Stab;
struct Stab
{
	char *s;
	int n;
	int mark;
};
Stab *stab;
int nstab;

void
strwalk(void *a, Datum *k, Datum *v)
{
	uchar *p;

	USED(a);

	if(nstab%16==0)
		stab = erealloc(stab, (nstab+16)*sizeof(stab[0]));

	stab[nstab].s = emalloc(k->n+1);
//	strcpy(stab[nstab].s, (char*)k->a);
	memmove(stab[nstab].s, (char*)k->a, k->n);
	if(v->n != 2){
		print("strtoid('%s'): ignoring bad id %.*H\n", stab[nstab].s, v->n, v->a);
		return;
	}
 
	p = v->a;
	stab[nstab].n = SHORT(p);
	nstab++;
}

void
idwalk(void *a, Datum *k, Datum *v)
{
	char *s;
	int i, j, n;
	uchar *p;

	USED(a);

	if(nstab%16==0)
		stab = erealloc(stab, (nstab+16)*sizeof(stab[0]));

	s = emalloc(v->n+1);
	memmove(s, (char*)v->a, v->n);
	//strcpy(s, (char*)v->a);

	if(k->n != 2){
		print("idtostr(%.*H)='%s': ignoring bad id\n", v->n, v->a, s);
		return;
	}
	p = k->a;
	n = SHORT(p);

	for(i=0; i<nstab; i++){
		if(stab[i].n == n){
			if(strcmp(s, stab[i].s) != 0){
				print("idtostr(%d)='%s' but strtoid('%s')=%d; giving up\n", n, s, stab[i].s, n);
				sysfatal("bad string table");
			}
			break;
		}
	}
	for(j=0; j<nstab; j++)
		if(stab[j].n != n && strcmp(s, stab[j].s) == 0)
			print("warning: idtostr(%d)='%s' but strtoid('%s')=%d\n", n, s, s, stab[j].n);
	if(i==nstab){
		/* didn't find entry s->n in strtoid */
		if(nstab%16==0)
			stab = erealloc(stab, (nstab+16)*sizeof(stab[0]));
		stab[i].s = s;
		stab[i].n = n;
		nstab++;
	}
}

int
stabscmp(const void *a, const void *b)
{
	return strcmp(((Stab*)a)->s, ((Stab*)b)->s);
}

int
stabncmp(const void *a, const void *b)
{
	return ((Stab*)a)->n - ((Stab*)b)->n;
}

void
checkstab(void)
{
	int i;
	char buf[32];
	uchar x[2];
	Datum k, v;

	if(db->strtoid == nil)	/* dbopen should not let this happen */
		panic("no strtoid");
	if(db->idtostr == nil)
		panic("no idtostr");

	db->strtoid->walk(db->strtoid, strwalk, nil);
	db->idtostr->walk(db->idtostr, idwalk, nil);

/*
	for(i=0; i<nstab; i++)
		if(stab[i].mark == 0)
			print("reclaim unused strtab entry: %d <-> %s\n", stab[i].n, stab[i].s);
*/

	wdb->strtoid->free(wdb->strtoid);
	wdb->idtostr->free(wdb->idtostr);

	wdb->strtoid = dmaplist(wdb->s, 0, wdb->pagesize);
	if(wdb->strtoid == nil)
		sysfatal("wbstab: allocating strtoid list: %r");
	snprint(buf, sizeof buf, "%ud", wdb->strtoid->addr);
	dbputmeta(wdb, "strtoid", buf);

	wdb->idtostr = dmaplist(wdb->s, 0, wdb->pagesize);
	if(wdb->idtostr == nil)
		sysfatal("wbstab: allocating strtoid list: %r");
	snprint(buf, sizeof buf, "%ud", wdb->idtostr->addr);
	dbputmeta(wdb, "idtostr", buf);

	qsort(stab, nstab, sizeof(stab[0]), stabscmp);
	for(i=nstab-1; i>=0; i--){
		k.a = stab[i].s;
		k.n = strlen(stab[i].s);
		v.a = x;
		PSHORT(x, stab[i].n);
		v.n = 2;
		if(wdb->strtoid->insert(wdb->strtoid, &k, &v, DMapCreate|DMapReplace) < 0)
			sysfatal("wbstab insert strtoid: %r");
	}

	qsort(stab, nstab, sizeof(stab[0]), stabncmp);
	for(i=nstab-1; i>=0; i--){
		v.a = stab[i].s;
		v.n = strlen(stab[i].s);
		k.a = x;
		PSHORT(x, stab[i].n);
		k.n = 2;
		if(wdb->strtoid->insert(wdb->idtostr, &k, &v, DMapCreate|DMapReplace) < 0)
			sysfatal("wbstab insert strtoid: %r");
	}
}

void
copymeta(void *a, Datum *k, Datum *v)
{
	DMap *dst;

	dst = a;
	if((k->n == 7 && memcmp(k->a, "idtostr", 7)==0) ||
	   memcmp(k->a, "strtoid", 7) == 0)
		return;
	if(dst->insert(dst, k, v, DMapCreate) < 0)
		sysfatal("meta insert %.*s failed", utfnlen((char*)k->a, k->n), (char*)k->a);
}	

void
usage(void)
{
	fprint(2, "usage: trafixdb [-c new.tradb] [-nv] dbfile\n");
	exits("usage");
}

void
main(int argc, char **argv)
{
	initfmt();

	ARGBEGIN{
	case 'V':
		traversion();
	case 'l':
		print("known inconsistencies that trafixdb fixes:\n");
		write(1, knownproblems, strlen(knownproblems));
		exits(nil);
	case 'n':
		nflag = 1;
		break;
	case 'v':
		verbose = 1;
		break;
	}ARGEND

	if(argc != 1 && argc != 2)
		usage();

	db = opendb(argv[0]);
	if(db == nil)
		sysfatal("opening db %q: %r", argv[0]);

	wdb = db;
	if(argc == 2){
		wdb = createdb(argv[1], db->pagesize+db->s->hdrsize);
		if(wdb == nil)
			sysfatal("creating db %q: %r", argv[1]);
		dbputstat(wdb, nil, 0, db->rootstat);

		db->meta->walk(db->meta, copymeta, wdb->meta);
	}

	if(nflag || db != wdb)
		dbignorewrites(db);

	checkstab();
	db->root = dbfixtree(nil, db->root);

	closedb(db);
	if(db != wdb)
		closedb(wdb);
	exits(nil);
}
