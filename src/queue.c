#include "tra.h"

Queue*
mkstack(char *name, int max)
{
	Queue *q;

	q = mkqueue(name, max);
	q->isstack = 1;
	return q;
}

Queue*
mkqueue(char *name, int max)
{
	Queue *q;

	q = emalloc(sizeof(Queue));
	q->name = name;
	q->max = max;
	q->recv.l = &q->lk;
	q->send.l = &q->lk;
	return q;
}

Syncpath*
qrecv(Queue *q)
{
	Syncpath *s;

	qlock(&q->lk);
	while(q->s == nil){
		threadstate("qrecv %s", q->name);
		rsleep(&q->recv);
		threadstate("");
	}
	s = q->s;
	q->s = s->nextq;
	q->n--;
	rwakeup(&q->send);
	qunlock(&q->lk);
	return s;
}

void
qsend(Queue *q, Syncpath *s)
{
	qlock(&q->lk);
	while(q->max && q->n >= q->max){
		threadstate("qsend %s", q->name);
		rsleep(&q->send);
		threadstate("");
	}
	if(q->isstack){
		s->nextq = q->s;
		q->s = s;
	}else{
		s->nextq = nil;
		if(q->s == nil)
			q->s = s;
		else
			*(q->es) = s;
		q->es = &s->nextq;
		s->nextq = nil;
	}
	rwakeup(&q->recv);
	if(++q->n > q->m)
		q->m = q->n;
	qunlock(&q->lk);
}
