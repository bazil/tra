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
		qsend(s->sync->triageq, s);
	}		
}

void
syncthread(void *a)
{
	int i;

	for(i=0; i<32; i++)
		spawn(syncthread0, a);
}
