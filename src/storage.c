#include <u.h>
#include <libc.h>
#include <bio.h>
#include "storage.h"

int		syscreateexcl(char*);

#define dodebug 0
#define DBG if(!dodebug){}else

typedef struct XDBlock XDBlock;
typedef struct XDStore XDStore;
typedef struct Dpage Dpage;

struct Dpage
{
	int		nref;
	u32int	flags;
	u32int	addr;
	uchar*	a;
	XDStore*	s;
	Dpage*	next;		/* in hash list */
};

struct XDStore
{
	DStore	ds;
	void*	magic;

	int		ignorewrites;
	char*	base;
	char*	redo;
	int		broken;
	int		fd;
	int		logfd;
	uint		lgpagesz;
	u32int	end;
	Dpage*	root;
	Dpage*	hash[256];
	XDBlock*	free[1];	/* unwarranted chumminess */
};

struct XDBlock
{
	DBlock	db;
	void*	magic;

	XDStore*	s;
	uchar*	pa;
	u32int	m;
	Dpage*	p;
	XDBlock*	next;	/* in free list */
};

enum
{
	LogMindat	= 4,		/* 16 bytes is smallest stored fragment size */
	MinPagesize	= 128,	/* can't use pagesz < 128 */

	HdrSize = 8
};

static	int		Bgbit32(Biobuf*, u32int*);
static	int		Bpbit32(Biobuf*, u32int);
static	uint		ahash(u32int, uint);
static	XDBlock*	allocdata(XDStore*, uint);
static	Dpage*	allocpage(XDStore*);
static 	int		applylog(XDStore*);
static	void		branddata(XDBlock*);
static	int		cleanpages(XDStore*);
static	Dpage*	evictpage(XDStore*, u32int);
static	Dpage*	findpage(XDStore*, u32int);
static	int		flush(XDStore*, int);
static	void		freedata(XDBlock*);
static	u32int	gbit32(uchar*);
static	int		isemptylog(XDStore*);
static	XDBlock*	loaddata(XDStore*, u32int);
static	Dpage*	loadpage(XDStore*, u32int);
static	int		dblog2(int);
static	XDBlock*	mkdata(XDStore*, Dpage*, uchar*, uint);
static	Dpage*	mkpage(XDStore*, u32int);
static	DStore*	openpathfd(char*, int);
static	void		pbit32(uchar*, u32int);
static	XDBlock*	popfree(XDStore*, int);
static	long		preadn(int, void*, long, vlong);
static	int		truncatelog(XDStore*);
static	int		unfreedata(XDStore*, u32int, int);
static	void		unloaddata(XDBlock*);
static	int		writelog(XDStore*);

/* * * * * * utility * * * * * */
static int
Bgbit32(Biobuf *b, u32int *p)
{
	uchar tmp[4];

	if(Bread(b, tmp, 4) != 4)
		return -1;
	*p = gbit32(tmp);
	return 0;
}

static int
Bpbit32(Biobuf *b, u32int v)
{
	uchar tmp[4];

	pbit32(tmp, v);
	if(Bwrite(b, tmp, 4) != 4)
		return -1;
	return 0;
}

#define PHIINV 0.61803398874989484820
static uint
ahash(u32int addr, uint nhash)
{
	return (uint)floor(nhash*fmod(addr*PHIINV, 1.0));
}

static u32int
gbit32(uchar *p)
{
	return p[0] | (p[1]<<8) | (p[2]<<16) | (p[3]<<24);
}

static int
dblog2(int v)
{
	int l;

	l = 0;
	v = (v<<1)-1;
	while(v > 1){
		l++;
		v>>=1;
	}
	return l;
}

static void
pbit32(uchar *p, u32int v)
{
	p[0] = v;
	p[1] = v>>8;
	p[2] = v>>16;
	p[3] = v>>24;
}

static long
preadn(int fd, void *vbuf, long size, vlong off)
{
	uchar *buf;
	long have, m;

	buf = vbuf;
	have = 0;
	while(have < size){
		m = pread(fd, buf+have, size-have, off+have);
		if(m < 0)
			return -1;
		if(m == 0)
			break;
		have += m;
	}
	return have;
}

/* * * * * * logging * * * * * */
static int
applylog(XDStore *ds)
{
	int np;
	uchar *a, *buf;
	u32int addr;
	Biobuf *b;
	Dpage *p;

	buf = malloc(ds->ds.pagesize);
	if(buf == nil)
		return -1;
	b = malloc(sizeof(Biobuf));
	if(b == nil)
		goto Error;
	if(seek(ds->logfd, 0, 0) != 0){
	Error:
		free(buf);
		free(b);
		return -1;
	}
	Binit(b, ds->logfd, OREAD);
	if(Bgbit32(b, &addr) < 0)
		goto Error;
	if(addr != gbit32((uchar*)"log\n")){
		werrstr("malformed log");
		goto Error;
	}
	np = 0;
	for(;;){
		if(Bgbit32(b, &addr) < 0)
			goto Error;
		if(addr == ~(u32int)0)
			break;
DBG print("apply log page %ud\n", addr);
		p = findpage(ds, addr);
		if(p)
			a = p->a;
		else
			a = buf;
		if(Bread(b, a, ds->ds.pagesize) != ds->ds.pagesize)
			goto Error;
		if(pwrite(ds->fd, a, ds->ds.pagesize, addr) != ds->ds.pagesize)
{
abort();
			goto Error;
}
		np++;
	}
	fprint(2, "applied changes to %d pages\n", np);
	free(b);
	free(buf);
	return 0 && fsync(ds->fd);
}

static int
cleanpages(XDStore *ds)
{
	int i;
	Dpage *p;

	ds->root->flags &= ~DDirty;
	for(i=0; i<nelem(ds->hash); i++)
		for(p=ds->hash[i]; p; p=p->next)
			p->flags &= ~DDirty;
	return 0;
}

static int
isemptylog(XDStore *ds)
{
	char tmp[4];
	vlong ret;

	ret = seek(ds->logfd, 0, 2);
	if(ret < 0)
		return -1;
	if(ret < 4)
		return 1;
	if(preadn(ds->logfd, tmp, 4, 0) != 4)
		return -1;
	if(memcmp(tmp, "log\n", 4) != 0)
		return 1;
	return 0;
}

static int
serializeroot(XDStore *s)
{
	int i;
	uchar *p;
	Dpage *root;

	root = s->root;
	p = memchr(root->a, '\n', s->ds.pagesize);
	if(p == nil){
		werrstr("root data went bad");
		return -1;
	}
	p++;
	for(i=LogMindat; i<=s->lgpagesz; i++){
		if(s->free[i])
			pbit32(p, s->free[i]->db.addr);
		else
			pbit32(p, 0);
		p += 4;
	}
	root->flags |= DDirty;
	return 0;
}

static int
truncatelog(XDStore *ds)
{
	pwrite(ds->logfd, "XXX\n", 4, 0);
	ftruncate(ds->logfd, 0);	/* ignore if fails */
	return 0;
}

static int
writelogpage(Biobuf *b, Dpage *p)
{
DBG print("log page %ud\n", p->addr);
	if(Bpbit32(b, p->addr) < 0)
		return -1;
	if(Bwrite(b, p->a, p->s->ds.pagesize) != p->s->ds.pagesize)
		return -1;
	return 0;
}

static int
ppaddrcmp(const void *va, const void *vb)
{
	Dpage *a, *b;

	a = *(Dpage**)va;
	b = *(Dpage**)vb;
	if(a->addr < b->addr)
		return -1;
	if(a->addr > b->addr)
		return 1;
	return 0;
}

static int
dirtylist(XDStore *ds, Dpage ***ppp)
{
	int i, n, m;
	Dpage *p, **pp;

	n = 0;
	for(i=0; i<nelem(ds->hash); i++)
		for(p=ds->hash[i]; p; p=p->next)
			if(p->flags&DDirty)
				n++;
	if(n == 0){
		*ppp = nil;
		return 0;
	}

	pp = malloc(n*sizeof(pp[0]));
	if(pp == nil)
		return -1;

	m = 0;
	for(i=0; i<nelem(ds->hash); i++)
		for(p=ds->hash[i]; p; p=p->next)
			if(p->flags&DDirty)
				pp[m++] = p;
	assert(m == n);

	qsort(pp, n, sizeof(pp[0]), ppaddrcmp);
	*ppp = pp;
	return n;
}

static int
writelog(XDStore *ds)
{
	int i, np;
	Biobuf *b;
	Dpage **pp;

	if(seek(ds->logfd, 0, 0) != 0)
		return -1;

	b = malloc(sizeof(Biobuf));
	if(b == nil)
		return -1;

	Binit(b, ds->logfd, OWRITE);
	if(Bwrite(b, "LOG\n", 4) != 4){
	Err:
		Bterm(b);
		free(b);
		return -1;
	}
	if(serializeroot(ds) < 0)
		goto Err;

	np = dirtylist(ds, &pp);
	if(np == -1)
		goto Err;
	for(i=0; i<np; i++)
		if(writelogpage(b, pp[i]) < 0)
			goto Err;
	free(pp);

	if(Bpbit32(b, ~(u32int)0) < 0)
		goto Err;
	if(Bflush(b) < 0)
		goto Err;
	Bterm(b);
	free(b);
	if(0 && fsync(ds->logfd) < 0)
		return -1;
	if(pwrite(ds->logfd, "log\n", 4, 0) != 4)
		return -1;
	if(0 && fsync(ds->logfd) < 0)
		return -1;
	return 0;
}

static int
writepages(XDStore *ds)
{
	int i, np;
	Dpage **pp, *p;

	np = dirtylist(ds, &pp);
	if(np == -1)
		return -1;
	for(i=0; i<np; i++){
		p = pp[i];
	//	fprint(2, "%lux...", p->addr);
		if(pwrite(ds->fd, p->a, ds->ds.pagesize, p->addr) != ds->ds.pagesize){
			abort();
			return -1;
		}
	}

	if(0 && fsync(ds->fd) < 0)
		return -1;
	return 0;
}

/* * * * * * page management * * * * * */
static Dpage*
allocpage(XDStore *s)
{
	u32int addr;
	Dpage *p;

	addr = s->end;
	s->end += s->ds.pagesize;

//print("allocpage %ud\n", addr);
	if((p = evictpage(s, addr)) == nil
	&& (p = mkpage(s, addr)) == nil)
		return nil;

	memset(p->a, 0, s->ds.pagesize);
	p->flags = DDirty;
DBG print("allocpage %ud\n", p->addr);
	return p;
}

static Dpage*
evictpage(XDStore *s, u32int addr)
{
	USED(s);
	USED(addr);
	return nil;
}

static Dpage*
findpage(XDStore *s, u32int addr)
{
	uint h;
	Dpage *p;

	assert(addr%s->ds.pagesize == 0);
	h = ahash(addr, nelem(s->hash));
	assert(h < nelem(s->hash));
//print("look for %ud on %ud\n", addr, h);
	for(p=s->hash[h]; p; p=p->next){
		if(p->addr == addr){
			p->nref++;
			return p;
		}
	}
	return nil;
}

static Dpage*
loadpage(XDStore *s, u32int addr)
{
	Dpage *p;

	if((p = findpage(s, addr)) != nil)
		return p;

	if((p = evictpage(s, addr)) == nil
	&& (p = mkpage(s, addr)) == nil){
		werrstr("mkpage: %r");
		return nil;
	}

	if(preadn(s->fd, p->a, s->ds.pagesize, addr) != s->ds.pagesize){
		werrstr("pread @%ud: %r", addr);
		p->addr = ~(u32int)0;		/* mark unused */
		p->nref--;
		return nil;
	}
	return p;
}

static Dpage*
mkpage(XDStore *s, u32int addr)
{
	uint h;
	Dpage *p;

	p = malloc(sizeof(Dpage)+s->ds.pagesize);
	if(p == nil)
		return nil;
	memset(p, 0, sizeof(Dpage));
	p->a = (uchar*)&p[1];
	p->nref = 1;
	p->addr = addr;
	p->s = s;
	h = ahash(addr, nelem(s->hash));
	p->next = s->hash[h];
	s->hash[h] = p;
//print("mk %ud linked to %d\n", addr, h);
	return p;
}

/* * * * * * data management * * * * * */
static XDBlock*
allocdata(XDStore *s, uint n)
{
	int i, j;
	XDBlock *d, *d1;
	Dpage *p;

	/* find or create a bigger block */
	i = dblog2(n+HdrSize);
	if(i < LogMindat)
		i = LogMindat;
	if(i > s->lgpagesz){
		werrstr("block too big");
		return nil;
	}

	for(j=i; s->free[j]==nil && j<s->lgpagesz; j++)
		;
DBG print("alloc %ud log %d j %d\n", n, i, j);
	if(s->free[j]==nil){
//print("A\n");
		p = allocpage(s);
		if(p == nil)
			return nil;
DBG print("new page %ud\n", p->addr);
		d = mkdata(s, p, p->a, s->ds.pagesize);
		if(d == nil){
			p->nref--;
			return nil;
		}
	}else{
//print("B\n");
		d = popfree(s, j);
		if(d == nil)
			print("popfree nil: %r\n");
		p = d->p;
		assert(p != nil);
	}

//print("have block %ud size %ud\n", d->db.addr, d->m);
	/*
	 * we have to mark d as allocated (d->db.n != ~0)
	 * otherwise freeing blocks next to d will reclaim d
	 * out from under us.
	 */
	d->db.n = n;
	d->m = 1<<i;
	branddata(d);

	/* chop the block in half until it's just about right */
	for(j--; j>=i; j--){
//print("chop off %d (lgpagesz=%d)\n", 1<<j, s->lgpagesz);
		d1 = mkdata(s, p, d->pa+(1<<j), 1<<j);
		if(d1 == nil){
			d->m = 1<<(j+1);
			branddata(d);
			freedata(d);
			return nil;
		}
		branddata(d1);
		freedata(d1);
	}
//print("allocdata addr %ud p->a %p a %p\n", d->db.addr, d->p->a, d->pa);
//print("%.2ux %.2ux %.2ux %.2ux %.2ux %.2ux %.2ux %.2ux\n",
//	d->pa[0], d->pa[1], d->pa[2], d->pa[3],
//	d->pa[4], d->pa[5], d->pa[6], d->pa[7]);
	return d;
}

static void
branddata(XDBlock *d)
{
	d->db.flags |= DDirty;
	d->p->flags |= DDirty;
	pbit32(d->pa, d->m);
	pbit32(d->pa+4, d->db.n);
}

static void
freedata(XDBlock *d)
{
	int i;
	u32int paddr;
	uchar *ba, *pa;
	XDBlock *f;

//print("free %ud size %ud\n", d->db.addr, d->m);
//print("slots before:\n"); for(i=LogMindat; i<=d->s->lgpagesz; i++) print("%d %ud\n", 1<<i, d->s->free[i] ? d->s->free[i]->addr : 0);
	i = dblog2(d->m);
	assert(LogMindat <= i && i <= d->s->lgpagesz);
	d->db.n = ~(u32int)0;
	branddata(d);
//print("free %p\n", d->pa);
//print("%.2ux %.2ux %.2ux %.2ux %.2ux %.2ux %.2ux %.2ux\n",
//	d->pa[0], d->pa[1], d->pa[2], d->pa[3],
//	d->pa[4], d->pa[5], d->pa[6], d->pa[7]);

	for(; i < d->s->lgpagesz; i++){
		/* try to merge with buddy */
		paddr = d->db.addr&~(2*d->m-1);
		if(paddr == d->db.addr){
			pa = d->pa;
			ba = pa+d->m;
		}else{
			pa = d->pa - d->m;
			ba = pa;
		}
		if(gbit32(ba+4) != ~(u32int)0 || gbit32(ba) != d->m)
			break;
		if(unfreedata(d->s, paddr+(ba-pa), i) < 0)
			break;
DBG print("merged %ud and %lud in slot %d\n", d->db.addr, paddr+(ba-pa), i);
		d->db.addr = paddr;
assert(pa == d->p->a+paddr-d->p->addr);
		d->pa = pa;
		d->db.a = d->pa+HdrSize;
		d->m *= 2;
	}

	branddata(d);
	f = d->s->free[i];
	pbit32((uchar*)d->db.a+4, 0);
	if(f){
		pbit32(d->db.a, f->db.addr);
		pbit32((uchar*)f->db.a+4, d->db.addr);
		((Dpage*)f->p)->flags |= DDirty;
	}else
		pbit32(d->db.a, 0);
DBG print("added %ud to slot %d next %ud (%ud)\n", d->db.addr, i, f ? f->db.addr: 0, gbit32(d->db.a));
	d->next = f;
	d->s->free[i] = d;
//print("slots after:\n"); for(i=LogMindat; i<=d->s->lgpagesz; i++) print("%d %ud\n", 1<<i, d->s->free[i] ? d->s->free[i]->addr : 0);
}

static XDBlock*
loaddata(XDStore *s, u32int addr)
{
	XDBlock *d;
	Dpage *p;

	p = loadpage(s, addr&~(s->ds.pagesize-1));
	if(p == nil)
		return nil;
	d = mkdata(s, p, p->a+(addr&(s->ds.pagesize-1)), 0);
	if(d == nil){
		p->nref--;
		return nil;
	}
//print("load %ud p->addr %ud off %ud pa %p ",
//	addr, p->addr, (addr&(s->ds.pagesize-1)), p->a);
//print("%.2ux %.2ux %.2ux %.2ux %.2ux %.2ux %.2ux %.2ux\n",
//	d->pa[0], d->pa[1], d->pa[2], d->pa[3],
//	d->pa[4], d->pa[5], d->pa[6], d->pa[7]);
	d->m = gbit32(d->pa);
	d->db.n = gbit32(d->pa+4);
	return d;
}

static XDBlock*
mkdata(XDStore *s, Dpage *p, uchar *a, uint m)
{
	XDBlock *d;

	d = mallocz(sizeof(XDBlock), 1);
	if(d == nil)
		return nil;
	d->db.addr = p->addr + (a - p->a);
	d->db.a = a+HdrSize;
	d->m = m;
	d->db.n = 0;
	d->s = s;
	d->pa = a;
	d->p = p;
//print("mkdata %p %d\n", d->pa, m);
//print("%.2ux %.2ux %.2ux %.2ux %.2ux %.2ux %.2ux %.2ux\n",
//	d->pa[0], d->pa[1], d->pa[2], d->pa[3],
//	d->pa[4], d->pa[5], d->pa[6], d->pa[7]);
	return d;
}

static XDBlock*
popfree(XDStore *s, int slot)
{
	u32int na;
	XDBlock *d, *nd;

	d = s->free[slot];
	if(d == nil){
		werrstr("empty slot %d", slot);
		return nil;
	}
	if(d->db.n != ~(u32int)0){
		fprint(2, "slot %d allocated page on the free list (oops)\n", slot);
		abort();
	}

	if(d->p == nil){
		d->p = loadpage(s, d->db.addr&~(s->ds.pagesize-1));
		if(d->p == nil){
			werrstr("loadpage %ud: %r", d->db.addr&~(s->ds.pagesize-1));
			return nil;
		}
		d->pa = d->p->a + (d->db.addr&(s->ds.pagesize-1));
		d->db.a = d->pa+HdrSize;
	}
	if(d->next){
DBG print("used slot %d got %ud next %ud\n", slot, d->db.addr, d->next->db.addr);
		s->free[slot] = d->next;
		return d;
	}
	na = gbit32(d->db.a);
	if(na == 0){
DBG print("used slot %d got %ud next 0\n", slot, d->db.addr);
		s->free[slot] = nil;
		return d;
	}
	nd = loaddata(s, na);
	if(nd == nil){	/* if we return d we'll leak the rest of the free chain */
DBG print("used slot %d got %ud next %ud bad: %r\n", slot, d->db.addr, na);
		werrstr("loaddata %ud: %r", na);
		return nil;
	}
	pbit32((uchar*)nd->db.a+4, 0);
	((Dpage*)nd->p)->flags |= DDirty;
	s->free[slot] = nd;
DBG print("used slot %d got %ud next %ud (loaded)\n", slot, d->db.addr, nd->db.addr);
	return d;
}

static void
unloaddata(XDBlock *d)
{
DBG if(d->db.addr == 28288) print("\t%ud -> %ud, %ud\n", d->db.addr, gbit32(d->db.a), gbit32((uchar*)d->db.a+4));
	if(d->db.flags&DDirty)
		d->p->flags |= DDirty;
	d->p->nref--;
	d->p = (Dpage*)0xBBBBBBBB;
//print("unloaddata %p\n", d->pa);
//print("%.2ux %.2ux %.2ux %.2ux %.2ux %.2ux %.2ux %.2ux\n",
//	d->pa[0], d->pa[1], d->pa[2], d->pa[3],
//	d->pa[4], d->pa[5], d->pa[6], d->pa[7]);
	free(d);
}

static int
unfreedata(XDStore *s, u32int addr, int slot)
{
	u32int naddr, paddr;
	XDBlock *d, **l, *pd, *nd;

	d = loaddata(s, addr);
	if(d == nil)
		return -1;

	/*
	 * unlink d from the on-disk doubly-linked list.
	 */
	nd = nil;
	naddr = gbit32(d->db.a);
	if(naddr != 0){
		nd = loaddata(s, naddr);
		if(nd == nil){
			unloaddata(d);
			return -1;
		}
	}
	pd = nil;
	paddr = gbit32((uchar*)d->db.a+4);
	if(paddr != 0){
		pd = loaddata(s, paddr);
		if(pd == nil){
			unloaddata(d);
			unloaddata(nd);
			return -1;
		}
	}
	if(pd){
		pbit32(pd->db.a, naddr);
		pd->db.flags |= DDirty;
		unloaddata(pd);
	}
	if(nd){
		pbit32((uchar*)nd->db.a+4, paddr);
		nd->db.flags |= DDirty;
		unloaddata(nd);
	}
DBG print("unfreedata %ud prev %ud next %ud\n", d->db.addr, paddr, naddr);
	unloaddata(d);

	/*
	 * unlink addr from the in-memory singly-linked list
	 */
	for(l=&s->free[slot]; *l; l=&(*l)->next){
		d = *l;
		if(d->db.addr == addr){
DBG print("unlinked %ud from in-memory chain\n", d->db.addr);
			*l = d->next;
			unloaddata(d);
			break;
		}
	}
	return 0;
}

/* * * * * * external interface * * * * * */
static XDStore*
ds2xds(DStore *ds)
{
	XDStore *s;

	s = (XDStore*)ds;
	if(s->magic != (void*)ds2xds)
		abort();
	return s;
}

static XDBlock*
db2xdb(DBlock *db)
{
	XDBlock *b;

	b = (XDBlock*)db;
	if(b->magic != (void*)db2xdb)
		abort();
	return b;
}

static int
flush(XDStore *s, int closing)
{
	if(s->broken){
		werrstr("store is broken");
		return -1;
	}

	if(s->ignorewrites)
		return 0;

if(!closing)return 0;

	if(writelog(s) < 0
	|| writepages(s) < 0
	|| truncatelog(s) < 0
	|| cleanpages(s) < 0){
		s->broken = 1;
		return -1;
	}
	return 0;
}

static int
xdstoreclose(XDStore *s)
{
	int i, j;
	Dpage *p, *pnext;
	XDBlock *d, *dnext;

	if(s == nil)
		return -1;

	i = flush(s, 1);
	for(j=0; j<nelem(s->hash); j++){
		for(p=s->hash[j]; p; p=pnext){
			pnext = p->next;
			memset(p->a, 0xBB, s->ds.pagesize);
			free(p);
		}
		s->hash[j] = (Dpage*)0xBBBBBBBB;
	}
	for(j=0; j<=s->lgpagesz; j++){
		for(d=s->free[j]; d; d=dnext){
			dnext = d->next;
			memset(d, 0xBB, sizeof(XDBlock));
			free(d);
		}
		s->free[j] = (XDBlock*)0xBBBBBBBB;
	}
	s->root = (Dpage*)0xBBBBBBBB;
	close(s->fd);
	close(s->logfd);
	free(s->base);
	free(s->redo);
	free(s);
	return i;
}

static int
dstorefree(DStore *ds)
{
	int r;
	char *base, *redo;
	XDStore *s;

	s = ds2xds(ds);
	base = s->base;
	redo = s->redo;
	s->base = nil;
	s->redo = nil;
	xdstoreclose(s);
	r = remove(base);
	r |= remove(redo);
	free(base);
	free(redo);
	return r;
}

static int
dstoreclose(DStore *ds)
{
	return xdstoreclose(ds2xds(ds));
}

static int
dstoreflush(DStore *ds)
{
	return flush(ds2xds(ds), 0);
}

static int
dblockfree(DBlock *db)
{
	XDBlock *d;

	d = db2xdb(db);

DBG print("dfree %p (addr %ud size %ud p 0x%p)\n", d, d ? d->db.addr : 0, d ? d->db.n : 0, d ? d->p : 0);
	freedata(d);
	return 0;
}

static int
dblockclose(DBlock *db)
{
	XDBlock *d;

	d = db2xdb(db);

DBG print("ddrop %p (addr %ud size %ud)\n", d, d ? d->db.addr : 0, d ? d->db.n : 0);
	unloaddata(d);
	return 0;
}

static int
dblockflush(DBlock *db)
{
	XDBlock *d;

	d = db2xdb(db);
	if(d->db.flags&DDirty){
		d->p->flags |= DDirty;
		d->db.flags &= ~DDirty;
	}
	return 0;
}

DBlock*
dstoreread(DStore *ds, u32int addr)
{
	XDBlock *ret;
	XDStore *s;

	s = ds2xds(ds);

	ret = loaddata(s, addr);
DBG print("dread %ud = 0x%p (size %ud p 0x%p)\n", addr, ret, ret ? ret->db.n : 0, ret ? ret->p : 0);
	if(ret == nil)
		return nil;
	ret->db.close = dblockclose;
	ret->db.free = dblockfree;
	ret->db.flush = dblockflush;
	ret->magic = db2xdb;
	setmalloctag(ret, getcallerpc(&ds));
	return &ret->db;
}

DBlock*
dstorealloc(DStore *ds, uint size)
{
	XDBlock *ret;
	XDStore *s;

	s = ds2xds(ds);

	ret = allocdata(s, size);
DBG print("dalloc %ud = 0x%p (addr %ud size %ud)\n", size, ret, ret ? ret->db.addr : 0, ret ? ret->db.n : 0);
	if(ret == nil)
		return nil;
	ret->db.close = dblockclose;
	ret->db.free = dblockfree;
	ret->db.flush = dblockflush;
	ret->magic = db2xdb;
	setmalloctag(ret, getcallerpc(&ds));
	return &ret->db;
}

static char*
logname(char *file)
{
	char *log;
	int len;

	log = malloc(strlen(file)+5+1);
	if(log == nil)
		return nil;
	strcpy(log, file);
	len = strlen(log);
	while(len > 1 && log[len-1] == '/')
		log[--len] = '\0';
	strcat(log, ".redo");
	return log;
}

static DStore*
openpathfd(char *path, int fd)
{
	char *logpath, tmp[MinPagesize];
	uchar *p;
	int i, lg, logfd, pagesz;
	u32int addr;
	vlong off;
	XDBlock *d;
	Dpage *root;
	XDStore *s;

	s = nil;
	logfd = -1;
	if(preadn(fd, tmp, sizeof tmp, 0) != sizeof tmp){
	Error:
		xdstoreclose(s);
		if(logfd >= 0)
			close(logfd);
		close(fd);
		return nil;
	}

	if(memcmp(tmp, "dstore ", 7) != 0){
		werrstr("not a dstore file");
		goto Error;
	}

	pagesz = atoi(tmp+7);
	if((pagesz&(pagesz-1)) || pagesz < 128){
		werrstr("corrupt dstore file (bad page size)");
		goto Error;
	}
	lg = dblog2(pagesz);

	logpath = logname(path);
	if(logpath == nil)
		goto Error;
	if((logfd = open(logpath, ORDWR)) < 0){
		werrstr("open %s: %r", logpath);
		free(logpath);
		goto Error;
	}
	s = malloc(sizeof(XDStore)+((lg+1)*sizeof(XDBlock*)));
	if(s == nil){
		free(logpath);
		goto Error;
	}
	memset(s, 0, sizeof(XDStore));
	s->redo = logpath;
	s->base = strdup(path);
	if(s->base == nil)
		goto Error;
	s->fd = fd;
	s->logfd = logfd;
	s->ds.pagesize = pagesz;
	s->lgpagesz = lg;
	off = seek(fd, 0, 2);
	if(off < 0)
		goto Error;
	if(off%pagesz){
		werrstr("corrupt dstore file (partial end page)");
		goto Error;
	}
	s->end = off;

	if((root = loadpage(s, 0)) == nil)
		goto Error;
	p = memchr(root->a, '\n', pagesz);
	if(p == nil){
		werrstr("corrupt dstore file (bad header)");
		goto Error;
	}

	for(i=0; i<=lg; i++)
		s->free[i] = nil;

	p++;
	for(i=LogMindat; i<=lg; i++){
		if((addr = gbit32(p)) != 0){
			d = loaddata(s, addr);
			if(d == nil)
				goto Error;
			s->free[i] = d;
		}
		p += 4;
	}

	if(!isemptylog(s)){
		fprint(2, "database shut down during block writes; applying block redo log\n");
		if(applylog(s) < 0
		|| truncatelog(s) < 0)
			goto Error;
	}
	s->root = root;
	s->ds.hdrsize = HdrSize;
	s->ds.flush = dstoreflush;
	s->ds.close = dstoreclose;
	s->ds.alloc = dstorealloc;
	s->ds.read = dstoreread;
	s->ds.free = dstorefree;
	s->magic = ds2xds;
	return &s->ds;
}

DStore*
opendstore(char *path)
{
	int fd;

	if((fd = open(path, ORDWR|OLOCK)) < 0)
		return nil;
	return openpathfd(path, fd);
}

DStore*
createdstore(char *path, uint pagesz)
{
	int fd, logfd;
	char *log;
	uchar *buf;

	if((pagesz&(pagesz-1)) || pagesz < 128){
		werrstr("bad page size (need power of two >= 128)");
		return nil;
	}

	buf = mallocz(pagesz, 1);
	if(buf == nil)
		return nil;
	if((fd = syscreateexcl(path)) < 0){
		free(buf);
		return nil;
	}
	sprint((char*)buf, "dstore %ud\n", pagesz);
	if(pwrite(fd, buf, pagesz, 0) != pagesz){
	Error:
		free(buf);
		close(fd);
		remove(path);
		return nil;
	}
	free(buf);
	buf = nil;

	log = logname(path);
	if(log == nil)
		goto Error;
	logfd = syscreateexcl(log);
	free(log);
	if(logfd < 0)
		goto Error;
	close(logfd);
	return openpathfd(path, fd);
}

int dcs;
int
datumcmp(Datum *p, Datum *q)
{
	int r;
	uint n;

dcs++;
	n = p->n;
	if(n > q->n)
		n = q->n;
	r = memcmp(p->a, q->a, n);
	if(r < 0)
		return -1;
	if(r > 0)
		return 1;
	if(p->n < q->n)
		return -1;
	if(p->n > q->n)
		return 1;
	return 0;
}

int
dstoreignorewrites(DStore *s)
{
	ds2xds(s)->ignorewrites = 1;
	return 0;
}

