#include "tra.h"

void
sysstatnotedelete(Stat *s)
{
	s->state = SNonexistent;
	s->uid = nil;
	s->gid = nil;
	s->muid = nil;
	freevtime(s->mtime);
	s->mtime = mkvtime();
	freevtime(s->ctime);
	s->ctime = mkvtime();
	free(s->localsig.a);
	memset(&s->localsig, 0, sizeof(s->localsig));
	s->localuid = nil;
	s->localgid = nil;
	s->localmuid = nil;
	s->mode = ~0;
}

