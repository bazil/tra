#include "tra.h"

void
sysstatnotedelete(Stat *s)
{
	s->state = SNonexistent;
	free(s->uid);
	s->uid = nil;
	free(s->gid);
	s->gid = nil;
	free(s->muid);
	s->muid = nil;
	freevtime(s->mtime);
	s->mtime = mkvtime();
	freevtime(s->ctime);
	s->ctime = mkvtime();
	free(s->localsig.a);
	memset(&s->localsig, 0, sizeof(s->localsig));
	free(s->localuid);
	s->localuid = nil;
	free(s->localgid);
	s->localgid = nil;
	free(s->localmuid);
	s->localmuid = nil;
	s->mode = ~0;
}

