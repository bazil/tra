#include "tra.h"

/*
 * To ease comparisons we keep Vtime entries
 * sorted by machine name. 
 * 
 * As a clumsy hack, if v->nl == -1, v is the 
 * top element in the partial order (i.e., infinity).
 */
Vtime*
_mkvtime(int m)
{
	Vtime *v;

	v = emalloc(sizeof *v);
	memset(v, 0, sizeof *v);
	setmalloctag(v, getcallerpc(&m));
	return v;
}

static void
freevltime(Ltime *l, int nl)
{
	int i;

	for(i=0; i<nl; i++){
		free(l[i].m);
		l[i].m = (char*)0xDeadbeef;
	}
	free(l);
}

void
freevtime(Vtime *v)
{
	if(v){
		freevltime(v->l, v->nl);
		v->l = (Ltime*)0xDeadbeef;
		free(v);
	}
}

Vtime*
mkvtime1(char *m, ulong t, ulong wall)
{
	Vtime *v;

	v = emalloc(sizeof *v);
	v->l = emalloc(sizeof(v->l[0]));
	v->nl = 1;
	v->l[0].m = estrdup(m);
	v->l[0].t = t;
	v->l[0].wall = wall;
	setmalloctag(v, getcallerpc(&m));
	return v;
}

Vtime*
_infvtime(int m)
{
	Vtime *v;

	v = emalloc(sizeof(*v));
	v->nl = -1;
	setmalloctag(v, getcallerpc(&m));
	return v;
}

int
isinfvtime(Vtime *a)
{
	return a && a->nl == -1;
}

Vtime*
copyvtime(Vtime *a)
{
	int i;
	Vtime *b;

	if(isinfvtime(a))
		b = infvtime();
	else{
		b = mkvtime();
		b->l = emalloc(a->nl*sizeof(b->l[0]));
		b->nl = a->nl;
		for(i=0; i<b->nl; i++){
			b->l[i].m = estrdup(a->l[i].m);
			b->l[i].t = a->l[i].t;
			b->l[i].wall = a->l[i].wall;
		}
	}
	setmalloctag(b, getcallerpc(&a));
	return b;
}

/*
static int
ltimecmp(const void *va, const void *vb)
{
	Ltime *a, *b;

	a = va;
	b = vb;
	return strcmp(a->m, b->m);
}
*/

/* treating a as a set of ltimes, see if any of them are <= b */
int
intersectvtime(Vtime *a, Vtime *b)
{
	int i, j;

	j=0;
	for(i=0; i<a->nl; i++){
		while(j < b->nl && strcmp(a->l[i].m, b->l[j].m) > 0)
			j++;

		if(j < b->nl && strcmp(a->l[i].m, b->l[j].m) == 0)
			if(a->l[i].t <= b->l[j].t)
				return 1;
	}
	return 0;
}

int
leqvtime(Vtime *a, Vtime *b)
{
	int i, j;

	if(isinfvtime(b))
		return 1;
	if(isinfvtime(a))
		return 0;

	j=0;
	for(i=0; i<a->nl; i++){
		while(j < b->nl && strcmp(a->l[i].m, b->l[j].m) != 0)
			j++;
		if(j==b->nl)	/* entry missing from b */
			return 0;
		if(a->l[i].t > b->l[j].t)	/* entry present but smaller */
			return 0;
	}
	return 1;
}

Vtime*
maxvtime(Vtime *a, Vtime *b)
{
	int i, j, k, kk;
	Ltime *c;

	if(isinfvtime(a))
		return a;
	if(isinfvtime(b)){
		freevltime(a->l, a->nl);
		a->l = nil;
		a->nl = -1;
		return a;
	}

	/* count number of distinct elements in vector */
	j=0;
	k=0;
	for(i=0; i<a->nl; i++){
		while(j < b->nl && strcmp(a->l[i].m, b->l[j].m) > 0)
			j++, k++;

		if(j < b->nl && strcmp(a->l[i].m, b->l[j].m) == 0)
			j++;
		k++;
	}
	k += b->nl - j;
	kk=k;

	c = emalloc(k*sizeof(c[0]));
	j=0;
	k=0;
	for(i=0; i<a->nl; i++){
		while(j < b->nl && strcmp(a->l[i].m, b->l[j].m) > 0){
			c[k].m = estrdup(b->l[j].m);
			c[k].t = b->l[j].t;
			c[k].wall = b->l[j].wall;
			j++, k++;
		}

		c[k].m = estrdup(a->l[i].m);
		c[k].t = a->l[i].t;
		c[k].wall = a->l[i].wall;
		if(j < b->nl && strcmp(a->l[i].m, b->l[j].m) == 0){
			if(c[k].t < b->l[j].t){
				c[k].t = b->l[j].t;
				c[k].wall = b->l[j].wall;
			}
			j++;
		}
		k++;
	}
	for(; j<b->nl; j++){
		c[k].m = estrdup(b->l[j].m);
		c[k].t = b->l[j].t;
		c[k].wall = b->l[j].wall;
		k++;
	}
	assert(kk==k);

	freevltime(a->l, a->nl);
	a->l = c;
	a->nl = k;
	return a;
}

/* return smallest vtime c such that max(c, b) == a */
Vtime*
unmaxvtime(Vtime *a, Vtime *b)
{
	int ri, wi, j;

	if(!leqvtime(b, a))
		panic("unmaxvtime %V %V", a, b);

	/* b is infinity => a is infinity => use 0 */
	if(isinfvtime(b)){
		a->nl = 0;
		return a;
	}

	/* a is infinity, b is not => use infinity */
	if(isinfvtime(a))
		return a;

	j=0;
	wi=0;
	for(ri=0; ri<a->nl; ri++){
		while(j < b->nl && strcmp(a->l[ri].m, b->l[j].m) != 0)
			j++;
		if(j==b->nl || a->l[ri].t > b->l[j].t){
			/* entry missing from b, or b is smaller */
			/* we need to include it */
			if(wi != ri)
				a->l[wi] = a->l[ri];
			wi++;
		}else{
			/* b will take care of this entry, drop it */
			free(a->l[ri].m);
			a->l[ri].m = nil;
		}
	}
	a->nl = wi;
	return a;
}

Vtime*
minvtime(Vtime *a, Vtime *b)
{
	int i, j, k, kk;
	Ltime *c;
	Vtime *x;

	if(isinfvtime(b))
		return a;
	if(isinfvtime(a)){
		x = copyvtime(b);
		freevltime(a->l, a->nl);
		*a = *x;
		memset(x, 0, sizeof *x);
		freevtime(x);
		return a;
	}

	/* count number of common elements in vector */
	j=0;
	k=0;
	for(i=0; i<a->nl; i++){
		while(j < b->nl && strcmp(a->l[i].m, b->l[j].m) > 0)
			j++;
		if(j < b->nl && strcmp(a->l[i].m, b->l[j].m) == 0)
			j++, k++;
	}
	kk=k;

	c = emalloc(k*sizeof(c[0]));
	j=0;
	k=0;
	for(i=0; i<a->nl; i++){
		while(j < b->nl && strcmp(a->l[i].m, b->l[j].m) > 0)
			j++;
		if(j < b->nl && strcmp(a->l[i].m, b->l[j].m) == 0){
			c[k].m = estrdup(a->l[i].m);
			c[k].t = a->l[i].t;
			c[k].wall = a->l[i].wall;
			if(c[k].t > b->l[j].t){
				c[k].t = b->l[j].t;
				c[k].wall = b->l[j].wall;
			}
			j++;
			k++;
		}
	}
	assert(kk==k);

	freevltime(a->l, a->nl);
	a->l = c;
	a->nl = k;
	return a;
}

static void
writebufltime(Buf *b, Ltime *t)
{
	/* N.B. Version #1 is used by the database routines. */
	writebufc(b, 0);
	writebufl(b, t->t);
	writebufl(b, t->wall);
	writebufstring(b, t->m);
}

void
readbufltime(Buf *b, Ltime *t)
{
	if(readbufc(b) != 0)
		longjmp(b->jmp, BufData);
	t->t = readbufl(b);
	t->wall = readbufl(b);
	t->m = readbufstringdup(b);
}

void
writebufvtime(Buf *b, Vtime *v)
{
	int i;

	writebufc(b, 0);
	if(v == nil){
		writebufl(b, -2);
		return;
	}
	writebufl(b, v->nl);
	for(i=0; i<v->nl; i++)
		writebufltime(b, v->l+i);
}

Vtime*
readbufvtime(Buf *b)
{
	int i, n;
	Vtime *v;

	if(readbufc(b) != 0)
		longjmp(b->jmp, BufData);

	n = readbufl(b);
	if(n == -2)
		return nil;
	if(n < -2 || n > 65536)
		longjmp(b->jmp, BufData);
	v = mkvtime();
	v->nl = n;
	if(v->nl > 0){
		v->l = emalloc(sizeof(v->l[0])*v->nl);
		for(i=0; i<v->nl; i++)
			readbufltime(b, v->l+i);
	}
	setmalloctag(v, getcallerpc(&b));
	return v;
}

int
vtimefmt(Fmt *fmt)
{
	int i;
	Vtime *v;

	v = va_arg(fmt->args, Vtime*);
	if(v == nil)
		return fmtstrcpy(fmt, "<nil.vtime>");
	if(v->nl == -1)
		return fmtstrcpy(fmt, "Inf");
	if(v->nl==0)
		return fmtstrcpy(fmt, "''");
	for(i=0; i<v->nl; i++){
		if(i)
			fmtstrcpy(fmt, ",");
		fmtprint(fmt, "%s:%lud/%lud", v->l[i].m, v->l[i].t, v->l[i].wall);
	}
	return 0;
}
