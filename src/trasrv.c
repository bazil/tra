#include "tra.h"

typedef struct Srv Srv;
struct Srv
{
	char *name;
	char *dbfile;
	char *root;
	Replica *r;
	Db *db;
	int closed;
	int readonly;
	Vtime *now;
};

Fid *fidhash[101];	/* just a hash table, not related to readhash/writehash */

Fid*
fidalloc(int internal)
{
	static int n;
	Fid *f;

	f = emalloc(sizeof(Fid));
	if(internal){
		f->fid = -1;
		f->next = nil;
	}else{
		f->fid = ++n;
		f->next = fidhash[f->fid%nelem(fidhash)];
		fidhash[f->fid%nelem(fidhash)] = f;
	}
	f->fd = -1;
	f->tpath = nil;
	f->hashlist = nil;
	f->hoff = 0;
	f->hsort = 0;
	f->rfid = nil;
	return f;
}

Fid*
findfid(int n)
{
	Fid *f;

	for(f=fidhash[n%nelem(fidhash)]; f; f=f->next)
		if(f->fid == n)
			return f;
	return nil;
}

void
freefid(Fid *f)
{
	Fid **l;

	for(l=&fidhash[f->fid%nelem(fidhash)]; *l; l=&(*l)->next)
		if(*l == f){
			*l = f->next;
			free(f);
			return;
		}
	free(f);
}

enum {
/*
	out16 = 1;
	for(i=0; i<16; i++)
		out16 = (out16 * 256) % 4093;
	out16 = 4093 - out16;
*/

	Out16 = 2998
};
/*
 * Define to force checking ``fast'' hash function against correct one.
 */
/* #define CHECK 1 */
static uint
splitblock(uchar *dat, uint n)
{
	uchar *bp, *ep, *p, *q;
	ulong v;
#ifdef CHECK
	uchar *goodp;
#endif

	ep = dat+n;
	bp = dat+2048;	/* minimum block size */
	if(bp >= ep)
		return n;

/*
	Old, slower version, for reference.
	When copying large trees, this is one
	of the bottlenecks (the other is _sha1block),
 */
#ifdef CHECK
	v = 0;
	for(p=dat; p<ep; p++){
		if(v==3453 && p>=bp)	// 3453: random
			break;
		v = (v*256+*p) % 4093;	// 4093: closest prime to 4096 (Blocksize/2) 
		if(p-dat >= 16)
			v = (v + p[-16]*Out16) % 4093;
	}
	goodp = p;
#endif

/*
	New faster version
*/
	v = 0;
	for(p=bp-16; p<bp; p++)
		v = (v*256+*p) % 4093;
	for(q=p-16; p<ep; p++, q++){
		if(v == 3453)
			break;
		v = (v*256 + *p + *q*Out16) % 4093;
	}

#ifdef CHECK
	assert(p == goodp);
#endif
	return p - dat;
}

Srv*
opensrv(char *dbfile)
{
	ulong inow;
	char buf[32], *sysname, *now, *p;
	Srv *srv;

	srv = emalloc(sizeof(*srv));
	srv->r = fd2replica(0, 1);
	srv->dbfile = estrdup(dbfile);
	srv->db = opendb(dbfile);
	if(srv->db == nil)
		sysfatal("cannot open db: %r");
	now = dbgetmeta(srv->db, "now");
	if(now == nil)
		sysfatal("cannot look up event counter in database: %r");
	sysname = dbgetmeta(srv->db, "sysname");
	srv->name = estrdup(sysname);
	if(sysname == nil)
		sysfatal("cannot look up system name in database: %r");
	inow = strtoul(now, &p, 0);
	free(now);
	if(*p != '\0')
		sysfatal("bad format for event counter: '%s'", now);
	inow++;
	srv->now = mkvtime1(sysname, inow, time(0));
	snprint(buf, sizeof buf, "%lud", inow);
	if(dbputmeta(srv->db, "now", buf) < 0)
		sysfatal("cannot write event counter back to database: %r");
	freevtime(srv->db->now);	/* old vtime for recovery */
	srv->db->now = copyvtime(srv->now);
	if(srv->db->rootstat)
		maxvtime(srv->db->rootstat->synctime, srv->now);

	/*
	 * The fact that the time has changed must go out to disk
	 * before we start advertising that time to other machines.
	 */
	logflush(srv->db);

	return srv;
}

char*
translate(Srv *srv, Path *p)
{
	return esmprint("%s%P", srv->root, p);
}

int
srvaddtime(Srv *srv, Path *p, Vtime *st, Vtime *mt)
{
	int n;
	Apath *ap;
	Stat *s, *g;

	ap = flattenpath(p);
	n = dbgetstat(srv->db, ap->e, ap->n, &s);

	/* add ghosts if necessary */
	if(n < ap->n){
		g = mkghoststat(s->synctime);
		freestat(s);
		for(n++; n < ap->n; n++)
			dbputstat(srv->db, ap->e, n, g);
		s = g;
	}

	s->synctime = maxvtime(s->synctime, st);
	if(mt)
		s->mtime = maxvtime(s->mtime, mt);
	dbputstat(srv->db, ap->e, ap->n, s);
	freestat(s);
	free(ap);
	return 0;
}

static Vtime*
copynow(Vtime *now, ulong mtime)
{
	now = copyvtime(now);
	if(mtime != 0)
		now->l[0].wall = mtime;
	return now;
}

static int
syskidscmp(const void *va, const void *vb)
{
	Sysstat *a, *b;

	a = *(Sysstat**)va;
	b = *(Sysstat**)vb;
	return strcmp(a->name, b->name);
}

static int
dbgetkidscmp(const void *va, const void *vb)
{
	Kid *a, *b;

	a = (Kid*)va;
	b = (Kid*)vb;
	return strcmp(a->name, b->name);
}

/*
 * BUG?: assumes db ops cannot fail. 
 */
static int
statupdate(Srv *srv, Path *p, Stat *os, Vtime *m, Sysstat *ss)
{
	int changed, i, j, nk, nks, ostate;
	char *tpath;
	Apath *ap;
	Kid *k;
	Path *kp;
	Stat *s;
	Sysstat **ks;

	s = os;
	ap = flattenpath(p);
	if(ignorepath(ap)){
		if(s == nil)
			return 0;
		if(!(s->state & SNonreplicated)){
			s->state |= SNonreplicated;
			dbputstat(srv->db, ap->e, ap->n, s);
		}
		free(ap);
		return 0;
	}
	if(s == nil){
		i = dbgetstat(srv->db, ap->e, ap->n, &s);
		if(i != ap->n)
			dbputstat(srv->db, ap->e, ap->n, s);	/* BUG: shouldn't be necessary */
	}
	if(s->state & SNonreplicated){
		s->state &= ~SNonreplicated;
		dbputstat(srv->db, ap->e, ap->n, s);
	}
/*
	if(leqvtime(srv->now, s->synctime)){
		free(ap);
		if(os==nil)
			freestat(s);
		return 0;
	}
*/
	tpath = translate(srv, p);
	ostate = s->state;
	changed = sysstat(tpath, s, 1, ss);
	if(changed){
//fprint(2, "%P: changed=%d; %d %d\n", p, changed, ostate, s->state);
		if(ostate==SNonexistent && s->state!=SNonexistent){	/* new file */
			freevtime(s->ctime);
			s->ctime = copynow(srv->now, s->sysmtime);
		}
		/*
		 * This is okay even for directories, because no one else
		 * in the world can have a sync time >= srv.now, so we don't
		 * violate any ordering invariants by losing the other vector
		 * entries.
		 */
		freevtime(s->mtime);
		s->mtime = copynow(srv->now, s->sysmtime);
//fprint(2, "%P: now %$\n", p, s);
		dbputstat(srv->db, ap->e, ap->n, s);
	}
/*
	s->synctime = maxvtime(s->synctime, srv->now);
*/

	nks = 0;
	ks = nil;
	if(s->state == SDir){
//fprint(2, "syskids dir %s\n", tpath);
		nks = syskids(tpath, &ks, ss);
		qsort(ks, nks, sizeof(ks[0]), syskidscmp);
		for(i=0; i<nks; i++){
			if(i) assert(strcmp(ks[i-1]->name, ks[i]->name) < 0);
			kp = mkpath(p, ks[i]->name);
			statupdate(srv, kp, nil, s->mtime, ks[i]);
			freepath(kp);
		}
	}

	k = nil;
	nk = dbgetkids(srv->db, ap->e, ap->n, &k);
	qsort(k, nk, sizeof(k[0]), dbgetkidscmp);
	j = 0;
	for(i=0; i<nk; i++){
		if(i) assert(strcmp(k[i-1].name, k[i].name) < 0);
		while(j<nks && strcmp(ks[j]->name, k[i].name) < 0)
			j++;
		if(j<nks && strcmp(ks[j]->name, k[i].name) == 0)
			continue;
		kp = mkpath(p, k[i].name);
		statupdate(srv, kp, k[i].stat, s->mtime, nil);
		freepath(kp);
	}
	freesysstatlist(ks, nks);
	freekids(k, nk);

	if(m)
		maxvtime(m, s->mtime);
//fprint(2, "%P: mtime now %V\n", p, m);
	if(os==nil)
		freestat(s);
	free(ap);
	free(tpath);
	return 1;
}

Stat*
srvstat(Srv *srv, Path *p)
{
	Apath *ap;
	Stat *s;
	Vtime *m;

	ap = flattenpath(p);

	dbgetstat(srv->db, ap->e, ap->n, &s);

	m = mkvtime();
	if(statupdate(srv, p, s, m, nil))
		logflush(srv->db);
	free(ap);
	freevtime(m);
	return s;
}

int
srvkids(Srv *srv, Path *p, Kid **pk)
{
	int nk;
	Apath *ap;

	freestat(srvstat(srv, p));

	ap = flattenpath(p);
	nk = dbgetkids(srv->db, ap->e, ap->n, pk);
	free(ap);
	return nk;
}

int
srvmkdir(Srv *srv, Path *p, Stat *t)
{
	char *tpath;
	Apath *ap;
	Stat *s;

	tpath = translate(srv, p);
	if(sysmkdir(tpath, t) < 0){
		free(tpath);
		fprint(2, "trasrv: sysmkdir: %r\n");
		return -1;
	}

	ap = flattenpath(p);
	dbgetstat(srv->db, ap->e, ap->n, &s);
	sysstat(tpath, s, 0, nil);
	syswstat(tpath, s, t);
	freevtime(s->ctime);
	s->ctime = copyvtime(t->ctime);
	if(s->mtime)
		s->mtime = maxvtime(s->mtime, t->mtime);
	else
		s->mtime = copyvtime(t->mtime);
	/*
	 * don't update synctime; sync will do that after the dir copy has completed
	 */
	dbputstat(srv->db, ap->e, ap->n, s);
	freestat(s);
	free(ap);
	free(tpath);
	return 0;
}

int
srvopen(Srv *srv, Path *p, int mode)
{
	Fid *fid;

	fid = fidalloc(0);
	fid->tpath = translate(srv, p);
	fid->ap = flattenpath(p);

	if(mode!='r' && mode!='w'){
		werrstr("bad open mode 0x%x", mode);
		freefid(fid);
		return -1;
	}

	if(sysopen(fid, fid->tpath, mode) < 0){
		free(fid->tpath);
		freefid(fid);
		return -1;
	}
	return fid->fid;
}

int
srvread(Srv *srv, int fidnum, void *a, int n)
{
	Fid *fid;

	USED(srv);
	if((fid = findfid(fidnum)) == nil){
		werrstr("unknown fid in read");
		return -1;
	}

	return sysread(fid, a, n);
}

int
srvseek(Srv *srv, int fidnum, vlong off)
{
	Fid *fid;

	USED(srv);
	if((fid = findfid(fidnum)) == nil){
		werrstr("unknown fid in read");
		return -1;
	}
	return sysseek(fid, off);
}

int
srvwrite(Srv *srv, int fidnum, void *a, int n)
{
	Fid *fid;

	USED(srv);
	if((fid = findfid(fidnum)) == nil){
		werrstr("unknown fid in write");
		return -1;
	}

	return syswrite(fid, a, n);
}

int
srvclose(Srv *srv, int fidnum)
{
	int n;
	Fid *fid;

	USED(srv);
	if((fid = findfid(fidnum)) == nil){
		werrstr("unknown fid in close");
		return -1;
	}

	if(fid->rfid){
		sysclose(fid->rfid);
		freefid(fid->rfid);
		fid->rfid = nil;
	}
	n = sysclose(fid);
	free(fid->ap);
	free(fid->tpath);
	free(fid->hashlist);
	freefid(fid);
	return n;
}

int
srvwstat(Srv *srv, Path *p, Stat *s)
{
	char *tpath;
	Apath *ap;
	Stat *os;

	tpath = translate(srv, p);
	ap = flattenpath(p);
	dbgetstat(srv->db, ap->e, ap->n, &os);

	if(syswstat(tpath, os, s) < 0)
		return -1;

	os->synctime = maxvtime(os->synctime, s->synctime);
	os->ctime = maxvtime(os->ctime, s->ctime);
	if(os->state == SDir)
		os->mtime = maxvtime(os->mtime, s->mtime);
	else
		os->mtime = s->mtime;
	dbputstat(srv->db, ap->e, ap->n, os);
	freestat(os);
	return 0;
}

int
srvcommit(Srv *srv, int fidnum, Stat *t)
{
	int n;
	Fid *fid;
	Stat *s;

	USED(srv);
	if((fid = findfid(fidnum)) == nil){
		werrstr("unknown fid in close");
		return -1;
	}

	if(fid->rfid){
		sysclose(fid->rfid);
		freefid(fid->rfid);
		fid->rfid = nil;
	}

	n = syscommit(fid);

	/*
	 * BUG: we need to package the rest of this 
	 * function somewhere that can be called from
	 * without commit, so that if the data contents
	 * are up-to-date but metadata has changed,
	 * we can update only the metadata.
	 */
	dbgetstat(srv->db, fid->ap->e, fid->ap->n, &s);
	s->state = t->state;
	s->ctime = maxvtime(s->ctime, t->ctime);
	freevtime(s->mtime);
	s->mtime = copyvtime(t->mtime);
	s->synctime = maxvtime(s->synctime, t->synctime);
	sysstat(fid->tpath, s, 1, nil);
	syswstat(fid->tpath, s, t);
	dbputstat(srv->db, fid->ap->e, fid->ap->n, s);
	free(fid->ap);
	free(fid->tpath);
	free(fid->hashlist);
	freestat(s);
	freefid(fid);
	return n;
}

int
srvremove(Srv *srv, Path *p, Stat *t)
{
	char *tpath;
	Sysstat **k;
	int i, nk;
	Apath *ap;
	Path *kp;
	Stat *s;

	ap = flattenpath(p);
	tpath = translate(srv, p);

	nk = syskids(tpath, &k, nil);
	for(i=0; i<nk; i++){
		kp = mkpath(p, k[i]->name);
		/*
		 * We can ignore error returns from
		 * srvremove.  The rmdir will fail below
		 * if the files aren't really gone.
		 */
		srvremove(srv, kp, t);
		freepath(kp);
	}
	freesysstatlist(k, nk);

	/*
	 * If the remove fails and the file is still there,
	 * don't record that it's gone.  There's a small
	 * race here, but only if the path really didn't
	 * exist when the remove failed and has been
	 * created since then.
	 */
	if(sysremove(tpath) < 0)
		if(sysaccess(tpath) >= 0)
			return -1;

	dbgetstat(srv->db, ap->e, ap->n, &s);
	s->state = SNonexistent;
	s->synctime = maxvtime(s->synctime, t->synctime);
	dbputstat(srv->db, ap->e, ap->n, s);
	free(tpath);
	freestat(s);
	return 0;
}

static Hashlist*
hashfile(Srv *srv, Fid *xfid, char *tpath)
{
	uchar *buf, *fbuf, dig[SHA1dlen];
	vlong off;
	int n, nbuf;
	Hashlist *hl;
	Fid *fid;

	USED(srv);
	buf = emallocnz(IOCHUNK);
	fbuf = buf;

	fid = fidalloc(1);
	fid->tpath = tpath;
	if(sysopen(fid, fid->tpath, 'r') < 0){
		freefid(fid);
		free(fbuf);
		return nil;
	}

	hl = mkhashlist();
	nbuf = 0;
	off = 0;
	while((n = sysread(fid, buf+nbuf, IOCHUNK-nbuf)) > 0){
		nbuf += n;
		while((n = splitblock(buf, nbuf)) < nbuf || n == IOCHUNK){
			sha1(buf, n, dig, nil);
			hl = addhash(hl, dig, off, n);
			if(n < nbuf)
				buf += n;
			nbuf -= n;
			off += n;
		}
		if(nbuf && buf != fbuf)
			memmove(fbuf, buf, nbuf);
		buf = fbuf;
	}
	if(n < 0){
		sysclose(fid);
		freefid(fid);
		free(hl);
		free(fbuf);
		return nil;
	}
	while(nbuf){
		n = splitblock(buf, nbuf);
		sha1(buf, n, dig, nil);
		hl = addhash(hl, dig, off, n);
		if(n < nbuf)
			buf += n;
		nbuf -= n;
		off += n;
	}
	xfid->rfid = fid;
	free(fbuf);
	return hl;
}

int
srvreadhash(Srv *srv, int fidnum, void *a, int n)
{
	int i, x;
	Fid *fid;
	uchar *p;

	USED(srv);
	if((fid = findfid(fidnum)) == nil){
		werrstr("unknown fid in readhash");
		return -1;
	}

	if(fid->hashlist == nil)
		if((fid->hashlist = hashfile(srv, fid, fid->tpath)) == nil)
			return -1;

	if(fid->hsort){
		/* XXX could sort by offset here, but not needed. */
		werrstr("cannot read hashes; not in file order");
		return -1;
	}

	p = a;
	n /= 2+SHA1dlen;
	if(n > fid->hashlist->nh - fid->hoff)
		n = fid->hashlist->nh - fid->hoff;
	for(i=0; i<n; i++){
		x = fid->hashlist->h[fid->hoff].n;
		PSHORT(p, x);
		p += 2;
		memmove(p, fid->hashlist->h[fid->hoff].sha1, SHA1dlen);
		p += SHA1dlen;
		fid->hoff++;
	}
	return p - (uchar*)a;
}

static int
hashcopy(Fid *fid, Hash *h)
{
	uchar *buf;
	int n, m, tot;

	if(fid->rfid == nil){
		werrstr("no rfid in hashcopy");
		return -1;
	}

	if(sysseek(fid->rfid, h->off) < 0)
		return -1;
	buf = emallocnz(IOCHUNK);
	n = h->n;
	tot = 0;
	while(tot < n){
		m = n - tot;
		if(m > IOCHUNK)	
			m = IOCHUNK;
		if((m = sysread(fid->rfid, buf, m)) <= 0){
			if(m == 0)
				werrstr("early eof in hashcopy");
			free(buf);
			return -1;
		}
		if(syswrite(fid, buf, m) != m){
			free(buf);
			return -1;
		}
		tot += m;
	}
	free(buf);
	return 0;
}

/*
 * For now, we use the hashes to create a temporary file
 * and then commit will copy the temporary file over the
 * current file, as it always does. 
 *
 * In the future, it is probably worth reworking this to
 * store the list of raw and referenced blocks and then
 * figure out how to edit the destination file in place.
 * I just don't have time to figure this out now.  It won't
 * require changing the wire formats, so it's easy to add later.
 */
int
srvwritehash(Srv *srv, int fidnum, void *a, int n)
{
	int i;
	Fid *fid;
	Hash *h;
	uchar *p;

	USED(srv);
	if(n%SHA1dlen){
		werrstr("unaligned count %d in writehash", n);
		return -1;
	}
	if((fid = findfid(fidnum)) == nil){
		werrstr("unknown fid in writehash");
		return -1;
	}
	if(fid->hashlist == nil){
		werrstr("cannot writehash; didn't read hashes");
		return -1;
	}
	if(!fid->hsort){
		if(fid->hashlist->nh)
			qsort(fid->hashlist->h, fid->hashlist->nh, sizeof(fid->hashlist->h[0]), hashcmp);
		fid->hsort = 1;
	}

	p = a;
	n /= SHA1dlen;
	for(i=0; i<n; i++){
		h = findhash(fid->hashlist, p);
		if(h == nil){
			werrstr("unknown hash %.*H", SHA1dlen, p);
			return -1;
		}
		if(hashcopy(fid, h) < 0)
			return -1;
		p += SHA1dlen;
	}
	return 0;
}

int
srvreadonly(Srv *srv, int ignwr)
{
	srv->readonly = 1;
	if(ignwr && dbignorewrites(srv->db) < 0)
		return -1;
	return 0;
}

int
srvhangup(Srv *srv)
{
	if(!srv->closed){
		srv->closed = 1;
		closedb(srv->db);
	}
	return 0;
}

char*
srvmeta(Srv *srv, char *k)
{
	return dbgetmeta(srv->db, k);
}

void
usage(void)
{
	fprint(2, "usage: trasrv [-i inc/exc] [-o opt] ... -a | dbfile root\n");
	exits("usage", 1);
}

void
freerpccontents(Rpc *r)
{
	freepath(r->p);
	freevtime(r->st);
	freevtime(r->mt);
	freekids(r->k, r->nk);
	freestat(r->s);
	free(r->str);
}

static char **cfg;
static int ncfg;

void
addcfg(char *s)
{
	if(ncfg%16 == 0)
		cfg = erealloc(cfg, (16+ncfg)*sizeof(cfg[0]));
	cfg[ncfg++] = estrdup(s);
}

int
config(char *s)
{
	int i;

	for(i=0; i<ncfg; i++)
		if(strcmp(cfg[i], s) == 0)
			return 1;
	return 0;
}

void
threadmain(int argc, char **argv)
{
	char *dbfile, *iefile, err[ERRMAX], *root;
	Buf *b;
	Rpc t, r;
	Srv *srv;
	Flate *inflate, *deflate;
	int fd, automatic;

	initfmt();
	automatic = 0;
	ARGBEGIN{
	default:
		usage();
	case 'a':
		automatic = 1;
		break;
	case 'D':
		debug |= dbglevel(EARGF(usage()));
		break;
	case 'V':
		traversion();
	case 'i':
		loadignore(EARGF(usage()));
		break;
	case 'o':
		addcfg(EARGF(usage()));
		break;
	}ARGEND

	if(dbgname == nil)
		dbgname = argv0;

	if(automatic){
		if(argc != 0)
			usage();
		dbfile = trapath("local.tradb");
		if(access(dbfile, 0) < 0)
			tramkdb(dbfile, nil, 8192, 1);
		root = getenv("HOME");
		if(root == nil)
			sysfatal("$HOME is not set");
		iefile = trapath("local.ie");
		if(access(iefile, 0) < 0){
			if((fd = create(iefile, OWRITE, 0666)) > 0){
				fprint(fd, "exclude .tra\n");
				close(fd);
			}
		}
		if(access(iefile, AREAD) >= 0)
			loadignore(iefile);
	}else{
		if(argc != 2)
			usage();
		dbfile = argv[0];
		root = argv[1];
	}

	nonotes();

	srv = opensrv(dbfile);
	fprint(2, "# %V\n", srv->now);
	srv->root = root;
	dbgname = srv->name;
	argv0 = dbgname;

	dbg(DbgRpc, "starting\n");
	if(banner(srv->r, nil) < 0)
		sysfatal("banner: %r");

	inflate = nil;
	deflate = nil;
	while((b = replread(srv->r)) != nil){
		memset(&t, 0, sizeof t);
		memset(&r, 0, sizeof r);
		if(convM2R(b, &t) < 0){
			memset(&t, 0, sizeof t);
			goto Error;
		}
		r.tag = t.tag;
		r.type = t.type+1;
		dbg(DbgRpc, "%R\n", &t);
		switch(t.type){
		default:
			werrstr("unknown RPC %x", t.type);
			goto Error;

		case Taddtime:
			if(srv->readonly){
			Readonly:
				werrstr("replica is read only");
				goto Error;
			}
			if(srvaddtime(srv, t.p, t.st, t.mt) < 0)
				goto Error;
			break;

		case Tclose:
			if(srvclose(srv, t.fd) < 0)
				goto Error;
			break;

		case Tcommit:
			if(srv->readonly)
				goto Readonly;
			if(srvcommit(srv, t.fd, t.s) < 0)
				goto Error;
			break;

		case Tdebug:
			debug = t.n;
			break;

		case Tflate:
			deflate = nil;
			if((inflate = inflateinit()) == nil || (deflate = deflateinit(t.n)) == nil){
				if(inflate)
					inflateclose(inflate);
				goto Error;
			}
			break;

		case Thangup:
			if(srvhangup(srv) < 0)
				goto Error;
			break;

		case Tkids:
			if((r.nk = srvkids(srv, t.p, &r.k)) < 0)
				goto Error;
			break;

		case Tmeta:
			if((r.str = srvmeta(srv, t.str)) == nil)
				goto Error;
			break;

		case Tmkdir:
			if(srv->readonly)
				goto Readonly;
			if(srvmkdir(srv, t.p, t.s) < 0)
				goto Error;
			break;

		case Topen:
			if(t.omode=='w' && srv->readonly)
				goto Readonly;
			if((r.fd = srvopen(srv, t.p, t.omode)) < 0)
				goto Error;
			break;

		case Tread:
			if(t.n >= 128*1024)
				sysfatal("bad count in Tread");
			r.a = emallocnz(t.n);
			if((r.n = srvread(srv, t.fd, r.a, t.n)) < 0)
				goto Error;
			break;
		case Treadhash:
			r.a = emallocnz(t.n);
			if((r.n = srvreadhash(srv, t.fd, r.a, t.n)) < 0)
				goto Error;
			break;
		case Treadonly:
			if(srvreadonly(srv, t.n) < 0)
				goto Error;
			break;

		case Tremove:
			if(srv->readonly)
				goto Readonly;
			if(srvremove(srv, t.p, t.s) < 0)
				goto Error;
			break;

		case Tseek:
			if(srvseek(srv, t.fd, t.vn) < 0)
				goto Error;
			break;

		case Tstat:
			if((r.s = srvstat(srv, t.p)) == nil)
				goto Error;
			break;

		case Twrite:
			if(srv->readonly)
				goto Readonly;
			if((r.n = srvwrite(srv, t.fd, t.a, t.n)) < 0)
				goto Error;
			break;

		case Twritehash:
			if(srv->readonly)
				goto Readonly;
			if((r.n = srvwritehash(srv, t.fd, t.a, t.n)) < 0)
				goto Error;
			break;

		case Terror:
			werrstr("cannot respond to Terror message");
		Error:
			rerrstr(err, sizeof err);
			r.err = err;
			r.type = Rerror;
			break;
		}
		free(b);
		b = convR2M(&r);
		dbg(DbgRpc, "%R (%ld bytes)\n", &r, b->ep-b->p);
		if(replwrite(srv->r, b) < 0)
			sysfatal("write response: %r");
		if(r.type==Rflate){
			dbg(DbgRpc, "trasrv flate %ld\n", t.n);
			srv->r->inflate = inflate;
			srv->r->deflate = deflate;
		}
		dbg(DbgRpc, "\twrote %ld\n", b->ep-b->p);
		free(b);
		if(t.type==Tread || t.type==Treadhash)
			free(r.a);
		freerpccontents(&t);
		freerpccontents(&r);
		if(t.type==Thangup)
			break;
	}

	srvhangup(srv);

if(0)
{
extern int dcs, dcentrycmps, dclistlookups, dclistadd1s, dclistinserts;
extern int dclookupavls, dcinsertavls, dcdeleteavls, lookupavls;
extern int cmaplookups, cmapinserts, dbwalks, dbwalklooks;
extern int strtoids, idtostrs;
fprint(2, "dc tot %d entrycmp %d listlookup %d listadd1 %d listinsert %d\n",
	dcs, dcentrycmps, dclistlookups, dclistadd1s, dclistinserts);
fprint(2, "\tavl lookup %d lookupcmp %d insertcmp %d deletecmp %d\n",
		lookupavls, dclookupavls, dcinsertavls, dcdeleteavls);
fprint(2, "\tcmaplookup %d cmapinsert %d dbwalks %d dbwalklooks %d\n", cmaplookups, cmapinserts, dbwalks, dbwalklooks);
fprint(2, "\tstrtoids %d idtostrs %d\n", strtoids, idtostrs);
}
	exits(nil, 0);
}

