#include "tra.h"
#include "avl.h"

/*
 * Avoid unsightly O(n^2) behavior in on-disk list insertions by keeping 
 * the in-memory version in a balanced tree.  This means reading
 * the entire list from disk even to do a lookup.  We cache lists even
 * after they have been closed to avoid the hit all the time. 
 */

typedef struct CMap CMap;
typedef struct Entry Entry;

enum
{
	MaxUnused = 16,	/* somewhat arbitrary */
};

struct Listcache
{
	CMap **c;
	int nc;
	int mc;
	int now;	/* lru time */
};

struct Entry 
{
	Avl avl;
	Datum k;	/* k.a is malloced data */
	Datum v;	/* v.a is malloced data */
};

struct CMap
{
	DMap cmap;	/* cached interface we present MUST BE FIRST */
	DMap *ucmap;	/* uncached interface we eventually write to */
	Avltree *tree;	/* tree holding records */
	int nopen;		/* number of clients holding this open */
	DStore *s;		/* identity */
	u32int addr;
	int used;
	int dirty;
	Listcache *lc;
};

static Avl*
e2a(Entry *e)
{
	return (Avl*)e;
}

static CMap*
map2clist(DMap *m)
{
	return (CMap*)m;
}

Listcache*
openlistcache(void)
{
	return emalloc(sizeof(Listcache));
}

static int
cmapinsert(DMap *m, Datum *k, Datum *v, int flag)
{
	Avl *a;
	CMap *c;
	Entry ek, *e, *ep;

	dbg(DbgCache, "cinsert %.*s %.*H\n", (int)utfnlen((char*)k->a, k->n), (char*)k->a, v->n, v->a);

	c = map2clist(m);
	ek.k = *k;
	if((a = lookupavl(c->tree, e2a(&ek))) != nil){
		if(!(flag&DMapReplace)){
			werrstr("key already exists");
			return -1;
		}
		c->dirty = 1;
		e = (Entry*)a;
		free(e->v.a);
		e->v.a = emalloc(v->n);
		e->v.n = v->n;
		memmove(e->v.a, v->a, v->n);
		return 0;
	}else{
		if(!(flag&DMapCreate)){
			werrstr("key not found");
			return -1;
		}
		c->dirty = 1;
		e = emalloc(sizeof(Entry));
		e->k.a = emalloc(k->n);
		e->k.n = k->n;
		memmove(e->k.a, k->a, k->n);
		e->v.a = emalloc(v->n);
		e->v.n = v->n;
		memmove(e->v.a, v->a, v->n);
		ep = nil;
		insertavl(c->tree, e2a(e), (Avl**)(void*)&ep);
		if(ep != nil)
			panic("cmapinsert");
		return 0;
	}	
}

static int
cmaplookup(DMap *m, Datum *k, Datum *v)
{
	Avl *a;
	CMap *c;
	Entry ek, *e;
	int n;

	c = map2clist(m);
	ek.k = *k;
	if((a = lookupavl(c->tree, e2a(&ek))) == nil){
		werrstr("key not found");
		return -1;
	}
	e = (Entry*)a;
	n = e->v.n;
	if(n > v->n)
		n = v->n;
	if(n > 0)
		memmove(v->a, e->v.a, n);
	v->n = e->v.n;
	return 0;
}

static int
cmapdelete(DMap *m, Datum *k)
{
	Avl *a;
	CMap *c;
	Entry ek, *e;

	c = map2clist(m);
	ek.k = *k;
	a = nil;
	deleteavl(c->tree, e2a(&ek), &a);
	if(a == nil){
		werrstr("key not found");
		return -1;
	}
	c->dirty = 1;
	e = (Entry*)a;
	free(e->k.a);
	free(e->v.a);
	free(e);
	return 0;	
}

static int
cmapdeleteall(DMap *m)
{
	Avl *a;
	Avlwalk *w;
	CMap *c;
	Entry *e, *ep;
	c = map2clist(m);
	c->dirty = 1;
	w = avlwalk(c->tree);
	while((a = avlnext(w)) != nil){
		ep = nil;
		deleteavl(c->tree, a, (Avl**)(void*)&ep);
		e = (Entry*)a;
		if(ep != e)
			panic("cmapdeleteall %p %p", e, ep);
		free(e->k.a);
		free(e->v.a);
		free(e);
	}
	endwalk(w);
	return 0;
}

static int
cmapwalk(DMap *m, void (*fn)(void*, Datum*, Datum*), void *arg)
{
	Entry *e;
	Avlwalk *w;
	CMap *c;

	c = map2clist(m);
	w = avlwalk(c->tree);
	while((e = (Entry*)avlnext(w)) != nil)
		(*fn)(arg, &e->k, &e->v);
	endwalk(w);
	return 0;
}

static int
cmapflush(DMap *m)
{
	Avlwalk *w;
	CMap *c;
	Entry *e;

	c = map2clist(m);
	if(!c->dirty)
		return 0;
	c->ucmap->deleteall(c->ucmap);
	w = avlwalk(c->tree);
	while((e = (Entry*)avlnext(w)) != nil)
		if(c->ucmap->insert(c->ucmap, &e->k, &e->v, DMapCreate) < 0)
			panic("wb insert failed during cmapflush: %r");
	endwalk(w);
	c->ucmap->flush(c->ucmap);
	c->dirty = 0;
	return 0;
}

static int
cmapclose(DMap *m)
{
	CMap *c;

	c = map2clist(m);
	c->nopen--;
	return 0;
}

static int
cmapfree(DMap *m)
{
	int i;
	CMap *c;
	Listcache *lc;

	c = map2clist(m);
	cmapdeleteall(m);
	c->ucmap->free(c->ucmap);
	freeavltree(c->tree);
	lc = c->lc;

	for(i=0; i<lc->nc; i++)
		if(lc->c[i] == c)
			break;
	if(i==lc->nc)
		panic("couldn't find cached map in list to free it");
	lc->c[i] = lc->c[--lc->nc];
	free(c);
	return 0;
}

static int
cmapisempty(DMap *m)
{
	CMap *c;

	c = map2clist(m);
	return isemptyavl(c->tree);
}

static void
cmapdump(DMap *m, int fd)
{
	USED(m);
	fprint(fd, "cmapdump not implemented\n");
}

static void
cmapfill(void *a, Datum *k, Datum *v)
{
	CMap *c;
	Entry *e, *ep;

	c = a;
	e = emalloc(sizeof(Entry));
	e->k.a = emalloc(k->n);
	e->k.n = k->n;
	memmove(e->k.a, k->a, k->n);
	e->v.a = emalloc(v->n);
	e->v.n = v->n;
	memmove(e->v.a, v->a, v->n);
	ep = nil;
	insertavl(c->tree, e2a(e), (Avl**)(void*)&ep);
	if(ep){
		free(ep->k.a);
		free(ep->v.a);
		free(ep);
	}
}

static int
entrycmp(Avl *a, Avl *b)
{
	Entry *ea, *eb;

	ea = (Entry*)a;
	eb = (Entry*)b;

	/* keep avl sorted backwards so list insertion during flush is fast */
	return -datumcmp(&ea->k, &eb->k);
}

CMap*
newcache(Listcache *lc, DStore *s, u32int addr, uint size)
{
	CMap *c;
	DMap *uc;

	uc = dmaplist(s, addr, size);
	if(uc == nil)
		return nil;

	c = emalloc(sizeof(*c));
	dbg(DbgCache, "newcache addr %ux %ux m %p\n", addr, uc->addr, c);
	c->cmap.addr = uc->addr;
	c->cmap.insert = cmapinsert;
	c->cmap.lookup = cmaplookup;
	c->cmap.delete = cmapdelete;
	c->cmap.deleteall = cmapdeleteall;
	c->cmap.walk = cmapwalk;
	c->cmap.close = cmapclose;
	c->cmap.free = cmapfree;
	c->cmap.flush = cmapflush;
	c->cmap.isempty = cmapisempty;
	c->cmap.dump = cmapdump;
	c->lc = lc;
	c->ucmap = uc;
	c->addr = uc->addr;
	c->s = s;

	c->tree = mkavltree(entrycmp);
	if(uc->walk(uc, cmapfill, c) < 0){
		cmapfree(&c->cmap);
		uc->close(uc);
		return nil;
	}

	return c;
}

void
dumpcache(CMap *c)
{
	dbg(DbgCache, "dumpcache addr %ux %ux m %p\n", c->addr, c->cmap.addr, c);
	if(cmapflush(&c->cmap) < 0)
		panic("couldn't flush cache to evict entry");
	cmapdeleteall(&c->cmap);
	c->ucmap->close(c->ucmap);
	freeavltree(c->tree);
	free(c);
}

DMap*
dmapclist(Listcache *lc, DStore *s, u32int addr, uint size)
{
	int i, unused;
	CMap **clast, *c;

	unused = 0;
	clast = nil;
	for(i=0; i<lc->nc; i++){
		if(lc->c[i]->s == s && lc->c[i]->addr == addr){
			lc->c[i]->used =  lc->now++;
			lc->c[i]->nopen++;
			return &lc->c[i]->cmap;
		}
		if(lc->c[i]->nopen == 0){
			unused++;
			if(clast==nil || (*clast)->used > lc->c[i]->used)
				clast = &lc->c[i];
		}
	}

	c = newcache(lc, s, addr, size);
	if(c == nil)
		return nil;

	c->nopen++;
	if(unused >= MaxUnused){
		dumpcache(*clast);
		*clast = lc->c[--lc->nc];
	}

	if(lc->nc >= lc->mc){
		lc->mc = lc->nc+16;
		lc->c = erealloc(lc->c, lc->mc*sizeof(lc->c[0]));
	}

	lc->c[lc->nc++] = c;

	dbg(DbgCache, "dmapclist %ux => %ux %ux %ux\n", addr, c->addr, c->cmap.addr, c->ucmap->addr);
	return &c->cmap;
}

void
flushlistcache(Listcache *lc)
{
	int i;

	dbg(DbgCache, "flushlistcache called from %lux\n", getcallerpc(&lc));
	for(i=0; i<lc->nc; i++)
		cmapflush(&lc->c[i]->cmap);
}

void
closelistcache(Listcache *lc)
{
	int i;

	dbg(DbgCache, "closelistcache called from %lux\n", getcallerpc(&lc));
	for(i=0; i<lc->nc; i++)
		dumpcache(lc->c[i]);
	free(lc);
}
