#include "tra.h"

void
syncthread0(void *a)
{
	Queue *q;
	Syncpath *s;

	threadsetname("syncthread");
	startclient();
	q = a;
	for(;;){
		s = qrecv(q);
		assert(s->state == SyncStart);
		syncstat(s);
		if(s->state == SyncError){
		Err:
			syncfinish(s);
			continue;
		}
		synctriage(s);
		if(s->state == SyncError)
			goto Err;
		if(s->triage != DoNothing)
			tralog("%P %s%s: [%$] [%$]", s->p, s->conflict ? "conflict " : "",
				workstr(s, s->triage), s->a.s, s->b.s);
		qsend(s->sync->triageq, s);
	}		
}

void
syncthread(void *a)
{
	int i;

	for(i=0; i<SyncThreads; i++)
		spawn(syncthread0, a);
}

char*
stripdot(char *s)
{
	static char buf[64];
	char *p;

	strecpy(buf, buf+sizeof buf, s);
	p = strrchr(buf, '.');
	if(p)
		*p = 0;
	return buf;
}

char*
rsysname(Replica *r)
{
	return stripdot(r->sysname);
}

char*
workstr(Syncpath *s, int action)
{
	static char buf[128];

	switch(action){
	default:
		sprint(buf, "<unexpected action %d>", s->action);
		return buf;
	case DoNothing:
	case DoNothing1:
		return "nothing";
	case DoCopyBtoA:
		snprint(buf, sizeof buf, "copy to %s", rsysname(s->sync->ra));
		return buf;
	case DoCopyAtoB:
		snprint(buf, sizeof buf, "copy to %s", rsysname(s->sync->rb));
		return buf;
	case DoCreateA:
		snprint(buf, sizeof buf, "create on %s", rsysname(s->sync->ra));
		return buf;
	case DoCreateB:
		snprint(buf, sizeof buf, "create on %s", rsysname(s->sync->rb));
		return buf;
	case DoRemoveA:
		snprint(buf, sizeof buf, "remove from %s", rsysname(s->sync->ra));
		return buf;
	case DoRemoveB:
		snprint(buf, sizeof buf, "remove from %s", rsysname(s->sync->rb));
		return buf;
	case DoKids:
	case DoKids1:
		return "examine children";
	}
}

