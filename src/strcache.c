#include "tra.h"

void
flushstrcache(Strcache *c)
{
	memset(c->cache, 0, sizeof c->cache);
	c->n = 0;
}

static void
addcache1(Strcache *c, char *s, int id, int h)
{
	int i;

	if(++c->n >= nelem(c->cache)/2)
		flushstrcache(c);
	for(i=0; i<nelem(c->cache); i++){
		if(++h == nelem(c->cache))
			h = 0;
		if(c->cache[h].s == nil)
			break;
	}
	c->cache[h].id = id;
	c->cache[h].s = s;
}
		
void
strcache(Strcache *c, char *s, int id)
{
	addcache1(c, s, id, (ulong)s%nelem(c->cache));
	addcache1(c, s, id, id%nelem(c->cache));
}

int
strcachebystr(Strcache *c, char *s, int *id)
{
	int h;

	if(s == nil)
		return -1;

	h = (ulong)s%nelem(c->cache);
	for(;;){
		if(++h == nelem(c->cache))
			h = 0;
		if(c->cache[h].s == nil)
			return -1;
		if(c->cache[h].s == s){
			*id = c->cache[h].id;
			return 0;
		}
	}
}

char*
strcachebyid(Strcache *c, int id)
{
	int h;

	h = id%nelem(c->cache);
	for(;;){
		if(++h == nelem(c->cache))
			h = 0;
		if(c->cache[h].s == nil)
			return nil;
		if(c->cache[h].id == id)
			return c->cache[h].s;
	}
}
