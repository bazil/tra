#include "tra.h"

/* BUG: keep a free list here? */
void
freepath(Path *p)
{
	if(p==nil)
		return;
	if(--p->ref == 0){
		freepath(p->up);
		free(p->s);
		memset(p, 0xAA, sizeof(Path));
		free(p);
	}
}

Path*
mkpath(Path *up, char *s)
{
	Path *p;

	p = emalloc(sizeof(Path));
	p->up = up;
	if(up)
		up->ref++;
	p->s = estrdup(s);
	p->ref = 1;
	setmalloctag(p, getcallerpc(&up));
	return p;
}

static void
_writebufpath(Buf *b, Path *p, int depth)
{
	if(p){
		_writebufpath(b, p->up, depth+1);
		writebufstring(b, p->s);
	}else
		writebufl(b, depth);
}

void
writebufpath(Buf *b, Path *p)
{
	writebufc(b, 0);
	_writebufpath(b, p, 0);
}

Path*
readbufpath(Buf *b)
{
	int i, n;
	Path *p;
	char *s;

	if(readbufc(b) != 0)
		longjmp(b->jmp, BufData);

	p = nil;
	n = readbufl(b);
	for(i=0; i<n; i++){
		if(p)
			p->ref--;
		p = mkpath(p, readbufstring(b));
	}
	setmalloctag(p, getcallerpc(&b));
	return p;
}

static int
_pathfmt(Fmt *fmt, Path *p)
{
	if(p == nil)
		return fmtstrcpy(fmt, "/");
	_pathfmt(fmt, p->up);
	if(p->up != nil)
		fmtstrcpy(fmt, "/");
	return fmtstrcpy(fmt, p->s);
}

int
pathfmt(Fmt *fmt)
{
	Path *p;

	p = va_arg(fmt->args, Path*);
	return _pathfmt(fmt, p);
}

Apath*
flattenpath(Path *p)
{
	char *s;
	int n, ns;
	Apath *ap;
	Path *q;

	n = 0;
	ns = 0;
	for(q=p; q; q=q->up){
		ns += strlen(q->s)+1;
		n++;
	}

	ap = emalloc(sizeof(Apath)+n*sizeof(char*)+ns);
	ap->e = (char**)&ap[1];
	ap->n = n;
	s = (char*)&ap->e[n];
	for(q=p; q; q=q->up){
		strcpy(s, q->s);
		ap->e[--n] = s;
		s += strlen(s)+1;
	}
	setmalloctag(ap, getcallerpc(&p));
	return ap;
}

Apath*
mkapath(char *s)
{
	int n;
	char *t;
	Apath *ap;

	n = 1;
	for(t=s; *t; t++)
		if(*t == '/')
			n++;

	ap = emalloc(sizeof(Apath)+sizeof(char*)*n);
	ap->e = (char**)&ap[1];
	ap->n = 0;
	for(t=s; *t; ){
		while(*t == '/')
			*t++ = '\0';
		if(*t == '\0')
			break;
		ap->e[ap->n++] = t;
		while(*t != '\0' && *t != '/')
			t++;
	}
	assert(ap->n <= n);
	setmalloctag(ap, getcallerpc(&s));
	return ap;
}

