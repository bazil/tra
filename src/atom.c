#include "tra.h"

/*
 *	Custom allocators to avoid malloc overheads on small objects.
 * 	We never free these.  (See below.)
 */
typedef struct Stringtab	Stringtab;
struct Stringtab {
	Stringtab *link;
	char *str;
};
static Stringtab*
taballoc(void)
{
	static Stringtab *t;
	static uint nt;

	if(nt == 0){
		t = emalloc(64*sizeof(Stringtab));
		nt = 64;
	}
	nt--;
	return t++;
}

static char*
xstrdup(char *s)
{
	char *r;
	int len;
	static char *t;
	static int nt;

	len = strlen(s)+1;
	if(len >= 8192)
		sysfatal("strdup big string");

	if(nt < len){
		t = emalloc(8192);
		nt = 8192;
	}
	r = t;
	t += len;
	nt -= len;
	strcpy(r, s);
	return r;
}

/*
 *	Return a uniquely allocated copy of a string.
 *	Don't free these -- they stay in the table for the 
 *	next caller who wants that particular string.
 *	String comparison can be done with pointer comparison 
 *	if you know both strings are atoms.
 */
static Stringtab *stab[1024];

static uint
hash(char *s)
{
	uint h;
	uchar *p;

	h = 0;
	for(p=(uchar*)s; *p; p++)
		h = h*37 + *p;
	return h;
}

char*
atom(char *str)
{
	uint h;
	Stringtab *tab;

	if(str == nil)
		return nil;

	h = hash(str) % nelem(stab);
	for(tab=stab[h]; tab; tab=tab->link)
		if(strcmp(str, tab->str) == 0)
			return tab->str;

	tab = taballoc();
	tab->str = xstrdup(str);
	tab->link = stab[h];
	stab[h] = tab;
	return tab->str;
}
