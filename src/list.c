#include "tra.h"

/*
 * Maintain an on-disk (key, value) mapping by
 * keeping a list of (key, value) pairs sorted by key.
 * Keys are strings, values are arbitrary data.
 * 
 * The list may be spread over any number of disk
 * blocks kept in a doubly-linked list.  The disk blocks
 * have the form:
 *	"DIR\0"	(4 bytes)
 *	ptr-to-prev-block	(4 bytes)
 *	ptr-to-next-block	(4 bytes)
 *	number of pairs in this block (2 bytes)
 *	<pairs>
 *
 * Each pair has the form:
 *	namelen	(2 bytes)
 *	name	namelen bytes (includes NUL)
 *	length	(2 bytes)
 *	data		length bytes
 *
 * Blocks are split and joined whenever possible to 
 * avoid pathologic cases.
 */

typedef struct DListpage DListpage;
typedef struct DListent DListent;
typedef struct DListhdr DListhdr;
typedef struct DList DList;

struct DListhdr
{
	uchar *prevp;
	ulong prev;
	uchar *nextp;
	ulong next;
	uchar *np;
	int n;
};

struct DListent
{
	uchar *bp;
	Datum key;
	Datum val;
	int sz;
};

struct DListpage
{
	ulong addr;
	DList *list;
	DBlock *dat;
	DListhdr hdr;
	DListent *de;
	uchar *end;
	int free;
};

struct DList
{
	DMap m;
	u32int magic;

	int pagesize;
	DStore *s;
	u32int firstblock;
	uchar *firstblockp;
	DBlock *hdr;
};

enum
{
	DListHdrSize = 4+4+4+2
};
#define BLOCKSIZE(nlen, blen)	(2+(nlen)+2+(blen))

DList*
map2list(DMap *map)
{
	DList *l;

	l = (DList*)map;
	if(l->magic != (u32int)map2list)
		abort();
	return l;
}

static int
parselisthdr(uchar **pp, uchar *ep, DListhdr *hdr, int errok)
{
	uchar *p;

	p = *pp;
	if(p+DListHdrSize > ep){
		if(!errok)
			abort();
		werrstr("hdr too small (need %d have %d)", DListHdrSize, (int)(ep-p));
		return -1;
	}
	if(memcmp(p, "DIR", 4) != 0){
		if(!errok)
			abort();
		werrstr("bad magic %.2ux %.2ux %.2ux %.2ux", p[0], p[1], p[2], p[3]);
		return -1;
	}
	p += 4;
	hdr->prevp = p;
	hdr->prev = LONG(p);
	p += 4;
	hdr->nextp = p;
	hdr->next = LONG(p);
	p += 4;
	hdr->np = p;
	hdr->n = SHORT(p);
	p += 2;
	*pp = p;
	return 0;
}

static int
parselistent(uchar **pp, uchar *ep, DListent *de, int errok)
{
	uchar *p;

	p = *pp;
	de->bp = p;
	if(p+2 > ep){
		if(!errok)
			abort();
		return -1;
	}
	de->key.n = SHORT(p);
	p += 2;
	de->key.a = p;
	if(p+de->key.n > ep-2){
		if(!errok)
			abort();
		return -1;
	}
	p += de->key.n;
	de->val.n = SHORT(p);
	p += 2;
	de->val.a = p;
	p += de->val.n;
	de->sz = BLOCKSIZE(de->key.n, de->val.n);
	*pp = p;
	return 0;
}

static DListpage*
openlistpage(DList *list, ulong addr)
{
	int i;
	uchar *p, *ep;
	DBlock *dat;
	DListpage *dir;
	DListhdr hdr;

	if((dat = list->s->read(list->s, addr)) == nil){
		werrstr("could not read directory block %lux", addr);
		return nil;
	}
	if(dat->n != list->pagesize){
		dat->close(dat);
		werrstr("bad block size in directory block");
		return nil;
	}
	p = dat->a;
	ep = p+dat->n;
	if(parselisthdr(&p, ep, &hdr, 1) < 0){
		dat->close(dat);
		werrstr("malformed directory header: %r");
		return nil;
	}
	dir = malloc(sizeof(DListpage)+hdr.n*sizeof(DListent));
	if(dir == nil){
		dat->close(dat);
		return nil;
	}
	dir->addr = addr;
	dir->de = (DListent*)&dir[1];
	dir->hdr = hdr;
	dir->dat = dat;
	dir->list = list;
	dir->free = dir->dat->n - DListHdrSize;
	for(i=0; i<hdr.n; i++){
		if(parselistent(&p, ep, &dir->de[i], 1) < 0){
			dat->close(dat);
			free(dir);
			werrstr("malformed directory entry");
			return nil;
		}
		dir->free -= dir->de[i].sz;
	}
	dir->end = p;
	return dir;
}

static DListpage*
reparsepage(DListpage *dir)
{
	int i;
	uchar *p, *ep;

	p = dir->dat->a;
	ep = p+dir->dat->n;

	parselisthdr(&p, ep, &dir->hdr, 0);
	dir = realloc(dir, sizeof(DListpage)+dir->hdr.n*sizeof(DListent));
	if(dir == nil)
		return nil;
	dir->de = (DListent*)&dir[1];
	dir->free = dir->dat->n - DListHdrSize;
	for(i=0; i<dir->hdr.n; i++){
		parselistent(&p, ep, &dir->de[i], 0);
		dir->free -= dir->de[i].sz;
	}
	dir->end = p;
	return dir;
}

static void
closelistpage(DListpage *dir)
{
	if(dir == nil)
		return;
	dir->dat->close(dir->dat);
	free(dir);
}

static void
freelistpage(DListpage *dir)
{
	if(dir == nil)
		return;
	dir->dat->free(dir->dat);
	free(dir);
}

static int
listlookup(DMap *map, Datum *key, Datum *val)
{
	int i, n;
	ulong addr, next;
	DListpage *dir;
	DList *list;

	list = map2list(map);
	for(addr=list->firstblock; addr; addr=next){
		dir = openlistpage(list, addr);
		if(dir == nil)
			return -1;
		for(i=0; i<dir->hdr.n; i++){
			switch(datumcmp(&dir->de[i].key, key)){
			case 0:
				n = dir->de[i].val.n;
				if(n > val->n)
					n = val->n;
				if(n > 0)
					memmove(val->a, dir->de[i].val.a, n);
				val->n = dir->de[i].val.n;
				closelistpage(dir);
				return 0;

			case 1:
				werrstr("key not found");
				return -1;
			}
		}
		closelistpage(dir);
		next = dir->hdr.next;
	}
	werrstr("key not found");
	return -1;
}

static DListpage*
mklistpage(DList *list)
{
	ulong addr;
	DBlock *dat;
	DListpage *dir;

	dat = list->s->alloc(list->s, list->pagesize);
	if(dat == nil)
		return nil;
	memset(dat->a, 0, dat->n);
	strcpy(dat->a, "DIR");
	dat->flags |= DDirty;
	addr = dat->addr;
	dat->close(dat);

	dir = openlistpage(list, addr);
	if(dir == nil)
		panic("mklistpage: %r");
	return dir;
}

static void
listadd1(DListpage *dir, Datum *key, Datum *val)
{
	int i, sz;
	uchar *p;

	sz = BLOCKSIZE(key->n, val->n);
	if(sz > dir->free)
		abort();
	for(i=0; i<dir->hdr.n; i++)
		if(datumcmp(&dir->de[i].key, key) > 0)
			break;
	if(i<dir->hdr.n){
		memmove(dir->de[i].bp+sz, dir->de[i].bp, dir->end - dir->de[i].bp);
		p = dir->de[i].bp;
	}else
		p = dir->end;
	PSHORT(p, key->n);
	p += 2;
	memmove(p, key->a, key->n);
	p += key->n;
	PSHORT(p, val->n);
	p += 2;
	memmove(p, val->a, val->n);
	dir->hdr.n++;
	PSHORT(dir->hdr.np, dir->hdr.n);
	dir->dat->flags |= DDirty;
}

static int
listinsert(DMap *map, Datum *key, Datum *val, int action)
{
	int i, insert, n, sz, first;
	uchar *p;
	ulong addr;
	DListpage *dir, *ndir, *pdir, *dir0;
	DList *list;

	if(action == 0){
		werrstr("no action specified");
		return -1;
	}

	list = map2list(map);
	// fprint(2, "listinsert %s %p first %ux...", (char*)key->a, list, list->firstblock);
	if(list->firstblock == 0){
		dir = mklistpage(list);
		if(dir == nil)
			return -1;
		if(!(action&DMapCreate)){
			closelistpage(dir);
			werrstr("key not found");
			return -1;
		}
		listadd1(dir, key, val);
		list->firstblock = dir->dat->addr;
		PLONG(list->firstblockp, list->firstblock);
		list->hdr->flags |= DDirty;
		closelistpage(dir);
		return 0;
	}

	sz = BLOCKSIZE(key->n, val->n);
	addr = list->firstblock;
	for(;;){
		// fprint(2, "@%lux...", addr);
		dir = openlistpage(list, addr);
		if(dir == nil){
			// fprint(2, "key not found\n");
			werrstr("bad list");
			return -1;
		}
		// fprint(2, "prev %ux next %ux...", dir->hdr.prev, dir->hdr.next);
		for(i=0; i<dir->hdr.n; i++){
			// fprint(2, "%s...", (char*)dir->de[i].key.a);
			switch(datumcmp(&dir->de[i].key, key)){
			case 0:
				if(!(action&DMapReplace)){
					closelistpage(dir);
					// fprint(2, "key already exists\n");
					werrstr("key already exists");
					return -1;
				}
				if(sz - dir->de[i].sz <= dir->free){
					if(i < dir->hdr.n-1)
						memmove(dir->de[i].bp+sz, dir->de[i+1].bp, dir->end - dir->de[i+1].bp);
					PSHORT((uchar*)dir->de[i].val.a - 2, val->n);
					memmove(dir->de[i].val.a, val->a, val->n);
					dir->dat->flags |= DDirty;
					closelistpage(dir);
					// fprint(2, "done\n");
					return 0;
				}
				/* can't just change data on page; remove key and reinsert */
				if(dir->hdr.n <= 1)
					abort();
				if(i < dir->hdr.n-1)
					memmove(dir->de[i].bp, dir->de[i+1].bp, dir->end - dir->de[i+1].bp);
				dir->hdr.n--;
				PSHORT(dir->hdr.np, dir->hdr.n);
				dir->dat->flags |= DDirty;
				dir = reparsepage(dir);
				if(dir == nil){
					// fprint(2, "reparsepage: %r\n");
					return -1;
				}
				goto FoundInsertPoint;

			case 1:
				if(!(action&DMapCreate)){
					closelistpage(dir);
					// fprint(2, "key not found\n");
					werrstr("key not found");
					return -1;
				}
				goto FoundInsertPoint;
			}
		}
		if(dir->hdr.next == 0)
			goto FoundInsertPoint;
		addr = dir->hdr.next;
		closelistpage(dir);
	}

FoundInsertPoint:
	first = 1;
	insert = i;
	// fprint(2, "insert=%d...", insert);

	/* can fit in current block? */
	if(dir->free >= sz){
		listadd1(dir, key, val);
		closelistpage(dir);
		// fprint(2, "fits, inserted\n");
		return 0;
	}

	/* push records to our left */
	if(dir->hdr.prev==0)
		pdir = nil;
	else{
		if((pdir=openlistpage(list, dir->hdr.prev)) == nil){
			closelistpage(dir);
			// fprint(2, "openlistpage: %r\n");
			return -1;
		}
	}
PushLeft:
	if(pdir){
		n = 0;
		for(i=0; i<dir->hdr.n && n<sz && i<insert; i++){
			if(n+dir->de[i].sz > pdir->free)
				break;
			n += dir->de[i].sz;
		}
		if(i>0){
			p = (uchar*)dir->dat->a + DListHdrSize;
			memmove(pdir->end, p, n);
			memmove(p, p+n, dir->end - (p+n));
			pdir->hdr.n += i;
			PSHORT(pdir->hdr.np, pdir->hdr.n);
			dir->hdr.n -= i;
			PSHORT(dir->hdr.np, dir->hdr.n);
			dir->dat->flags |= DDirty;
			pdir->dat->flags |= DDirty;
			dir = reparsepage(dir);
			pdir = reparsepage(pdir);
			if(dir == nil || pdir == nil){
				closelistpage(pdir);
				closelistpage(dir);
				// fprint(2, "reparsepage: %r\n");
				return -1;
			}
			if(n >= sz){	/* we made room in our block */
				listadd1(dir, key, val);
				closelistpage(pdir);
				closelistpage(dir);
				// fprint(2, "made room\n");
				return 0;
			}
			// fprint(2, "pushed %d left; insert=%d...", insert);
			insert -= i;
		}
		if(insert==0 && pdir->free >= sz){	/* we can use the prev block */
			listadd1(pdir, key, val);
			closelistpage(pdir);
			closelistpage(dir);
			// fprint(2, "used prev\n");
			return 0;
		}
	}
	if(!first)	/* can't get here second time around */
		abort();

	/* push records to our right */
	if(dir->hdr.next==0)
		ndir = nil;
	else{
		if((ndir=openlistpage(list, dir->hdr.next)) == nil){
			closelistpage(dir);
			closelistpage(pdir);
			// fprint(2, "openlistpage: %r\n");
			return -1;
		}
	}
	if(ndir){
		n = 0;
		for(i=dir->hdr.n; i>0 && i>insert; i--){
			if(n+dir->de[i-1].sz > ndir->free)
				break;
			n += dir->de[i-1].sz;
		}
		if(i<dir->hdr.n){
			p = (uchar*)ndir->dat->a + DListHdrSize;
			memmove(p+n, p, ndir->end - p);
			memmove(p, dir->de[i].bp, n);
			ndir->hdr.n += (dir->hdr.n - i);
			PSHORT(ndir->hdr.np, ndir->hdr.n);
			ndir->dat->flags |= DDirty;
			ndir = reparsepage(ndir);
			if(ndir == nil){
				// fprint(2, "reparse: %r\n");
				closelistpage(pdir);
				closelistpage(dir);
				return -1;
			}
			dir->hdr.n = i;
			PSHORT(dir->hdr.np, dir->hdr.n);
			dir->dat->flags |= DDirty;
			dir = reparsepage(dir);
			if(dir == nil){
				// fprint(2, "reparse: %r\n");
				closelistpage(pdir);
				closelistpage(ndir);
				return -1;
			}
			if(n >= sz){	/* we made room in our block */
				listadd1(dir, key, val);
				closelistpage(pdir);
				closelistpage(ndir);
				closelistpage(dir);
				// fprint(2, "made room\n");
				return 0;
			}
		}
		if(insert==dir->hdr.n && ndir->free >= sz){	/* we can use the next block */
			listadd1(ndir, key, val);
			closelistpage(pdir);
			closelistpage(ndir);
			closelistpage(dir);
			// fprint(2, "use next\n");
			return 0;
		}
	}

	/* split into two blocks: create new previous page and push left again */
	first = 0;
	closelistpage(ndir);

	// fprint(2, "split\n");
	dir0 = mklistpage(list);
	if(dir->addr == list->firstblock){
		list->firstblock = dir0->addr;
		PLONG(list->firstblockp, list->firstblock);
		list->hdr->flags |= DDirty;
	}

	if(pdir){
		pdir->hdr.next = dir0->addr;
		PLONG(pdir->hdr.nextp, pdir->hdr.next);
		pdir->dat->flags |= DDirty;
		dir0->hdr.prev = pdir->addr;
		PLONG(dir0->hdr.prevp, dir0->hdr.prev);
		dir0->dat->flags |= DDirty;
		closelistpage(pdir);
	}

	dir->hdr.prev = dir0->addr;
	PLONG(dir->hdr.prevp, dir->hdr.prev);
	dir->dat->flags |= DDirty;
	dir0->hdr.next = dir->addr;
	PLONG(dir0->hdr.nextp, dir0->hdr.next);

	pdir = dir0;
	goto PushLeft;
}

static int
listdelete(DMap *map, Datum *key)
{
	int i, sz;
	uchar *p, *np;
	ulong addr, next;
	DListpage *dir, *pdir, *ndir;
	DList *list;

	list = map2list(map);
	for(addr=list->firstblock; addr; addr=next){
		dir = openlistpage(list, addr);
		if(dir == nil)
			return -1;
		for(i=0; i<dir->hdr.n; i++)
			if(datumcmp(&dir->de[i].key, key) == 0)
				goto Found;
		next = dir->hdr.next;
		closelistpage(dir);
	}
	werrstr("directory entry not found");
	return -1;

Found:
	if(i<dir->hdr.n-1)
		memmove(dir->de[i].bp, dir->de[i+1].bp, dir->end - dir->de[i+1].bp);
	dir->hdr.n--;
	PSHORT(dir->hdr.np, dir->hdr.n);
	dir->dat->flags |= DDirty;
	dir = reparsepage(dir);
	if(dir == nil)
		return -1;

	if(dir->hdr.prev==0)
		pdir = nil;
	else{
		if((pdir=openlistpage(list, dir->hdr.prev)) == nil){
			closelistpage(dir);
			return -1;
		}
	}
	if(dir->hdr.next==0)
		ndir = nil;
	else{
		if((ndir=openlistpage(list, dir->hdr.next)) == nil){
			closelistpage(dir);
			closelistpage(pdir);
			return -1;
		}
	}

	p = (uchar*)dir->dat->a + DListHdrSize;
	sz = dir->end - p;
	if(dir->hdr.n != 0){
		if(pdir && sz <= pdir->free){
			memmove(pdir->end, p, sz);
			pdir->hdr.n += dir->hdr.n;
			PSHORT(pdir->hdr.np, pdir->hdr.n);
			dir->hdr.n = 0;
			pdir->dat->flags |= DDirty;
		}else if(ndir && sz <= ndir->free){
			np = (uchar*)ndir->dat->a + DListHdrSize;
			memmove(np+sz, np, ndir->end - np);
			memmove(np, p, sz);
			ndir->hdr.n += dir->hdr.n;
			PSHORT(ndir->hdr.np, ndir->hdr.n);
			dir->hdr.n = 0;
			ndir->dat->flags |= DDirty;
		}
	}

	if(dir->hdr.n == 0){
		if(pdir){
			pdir->hdr.next = dir->hdr.next;
			PLONG(pdir->hdr.nextp, pdir->hdr.next);
			pdir->dat->flags |= DDirty;
		}else{
			if(dir->dat->addr != list->firstblock)
				abort();
			list->firstblock = dir->hdr.next;
			PLONG(list->firstblockp, list->firstblock);
			list->hdr->flags |= DDirty;
		}
		if(ndir){
			ndir->hdr.prev = dir->hdr.prev;
			PLONG(ndir->hdr.prevp, ndir->hdr.prev);
			ndir->dat->flags |= DDirty;
		}
		freelistpage(dir);
	}else
		closelistpage(dir);

	if(ndir)
		closelistpage(ndir);
	if(pdir)
		closelistpage(pdir);
	return 0;
}

static int
listdeleteall(DMap *map)
{
	DList *list;
	DListpage *dir;
	u32int addr, next;

	list = map2list(map);
	for(addr=list->firstblock; addr; addr=next){
		dir = openlistpage(list, addr);
		if(dir == nil)
			break;
		next = dir->hdr.next;
		freelistpage(dir);
	}
	list->firstblock = 0;
	PLONG(list->firstblockp, list->firstblock);
	list->hdr->flags |= DDirty;
	return 0;
}

static int
listwalk(DMap *map, void (*fn)(void*, Datum*, Datum*), void *arg)
{
	DListpage *dir;
	DList *list;
	int i;
	ulong a, next;

	list = map2list(map);
	// fprint(2, "walk %p...", map);
	for(a=list->firstblock; a; a=next){
		dir = openlistpage(list, a);
		if(dir == nil){
			/*
			 * maybe it would be better to return -1
			 * here, but very few callers check it, and
			 * it's not clear what to do anyway.
			 * we haven't faced the problem of what
			 * to do if the database goes bad under us.
			 */
			panic("listwalk: openlistpage: %r");
			return -1;
		}
		// fprint(2, "@%lux...", a);
		next = dir->hdr.next;
		for(i=0; i<dir->hdr.n; i++){
			// fprint(2, "%s...", (char*)dir->de[i].key.a);
			(*fn)(arg, &dir->de[i].key, &dir->de[i].val);
		}
		closelistpage(dir);
	}
	// fprint(2, "\n");
	return 0;
}

static void
listdump(DMap *map, int fd)
{
	int i;
	DListpage *dir;
	DList *list;
	ulong a, next;

	list = map2list(map);
	fprint(fd, "===\n");
	for(a=list->firstblock; a; a=next){
		fprint(fd, "--- %lux\n", a);
		dir = openlistpage(list, a);
		if(dir == nil){
			// fprint(fd, "?cannot load: %r\n");
			return;
		}
		fprint(fd, "[prev %lux next %lux n %d free %d]\n", dir->hdr.prev, dir->hdr.next, dir->hdr.n, dir->free);
		next = dir->hdr.next;
		for(i=0; i<dir->hdr.n; i++)
			fprint(fd, "\t%.*s: %.*s\n", 
				utfnlen((char*)dir->de[i].key.a, dir->de[i].key.n), (char*)dir->de[i].key.a,
				utfnlen((char*)dir->de[i].val.a, dir->de[i].val.n), (char*)dir->de[i].val.a);
		closelistpage(dir);
	}
	fprint(fd, "===\n");
}

static int
listclose(DMap *map)
{
	int r;
	DList *list;

	list = map2list(map);
	r = list->hdr->close(list->hdr);
	free(list);
	return r;
}

static int
listflush(DMap *map)
{
	DList *list;

	list = map2list(map);
	return list->hdr->flush(list->hdr);
}

static int
listfree(DMap *map)
{
	DListpage *dir;
	DList *list;
	ulong a, next;

	list = map2list(map);
	for(a=list->firstblock; a; a=next){
		dir = openlistpage(list, a);
		if(dir == nil)
			return -1;
		next = dir->hdr.next;
		freelistpage(dir);
	}
	list->hdr->free(list->hdr);
	free(list);
	return 0;
}

static int
listisempty(DMap *map)
{	
	DList *list;

	list = map2list(map);
	return list->firstblock==0;
}

DMap*
dmaplist(DStore *s, u32int addr, uint pagesize)
{
	DList *l;
	DBlock *hdr;
	uchar *p;

	l = malloc(sizeof(DList));
	if(l == nil)
		return nil;

	if(addr == 0){
		if(pagesize == 0)
			panic("cannot allocate list with page size 0");
		hdr = s->alloc(s, 12);
		if(hdr == nil)
			return nil;
		p = hdr->a;
		memmove(p, "LHDR", 4);
		p += 4;
		PLONG(p, 0);
		p += 4;
		PLONG(p, pagesize);
		hdr->flags |= DDirty;
	}else{
		hdr = s->read(s, addr);
		if(hdr == nil)
			return nil;
		if(hdr->n != 12 || memcmp(hdr->a, "LHDR", 4) != 0){
			if(hdr->n != 12)
				werrstr("bad list header at 0x%ux; size %ud expected 12", addr, hdr->n);
			else
				werrstr("bad list header at 0x%ux: magic %.8ux", addr, *(u32int*)hdr->a);
			hdr->close(hdr);
			return nil;
		}
	}

	l->s = s;
	l->m.lookup = listlookup;
	l->m.insert = listinsert;
	l->m.delete = listdelete;
	l->m.deleteall = listdeleteall;
	l->m.walk = listwalk;
	l->m.dump = listdump;
	l->m.close = listclose;
	l->m.free = listfree;
	l->m.flush = listflush;
	l->m.addr = hdr->addr;
	l->m.isempty = listisempty;
	l->magic = (u32int)map2list;
	p = hdr->a;
	p += 4;
	l->firstblockp = p;
	l->firstblock = LONG(p);
	p += 4;
	l->pagesize = LONG(p);
	l->hdr = hdr;
	return &l->m;
}

