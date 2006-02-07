#include "tra.h"
#include <thread.h>

/*
 * It's important to run the stats in parallel, because the
 * most common case where we don't have s->a.s and s->b.s
 * is the root, and statting the root takes a long time (it has
 * to check the whole tree for changes).
 */
typedef struct Statarg Statarg;
struct Statarg
{
	Replica *repl;
	Stat *s;
	Path *p;
	Channel *c;
	char *err;
};
static void
statthread(void *a)
{
	Statarg *sa;

	sa = a;
	threadstate("statthread %P", sa->p);
	startclient();
	sa->s = rpcstat(sa->repl, sa->p);
	if(sa->s == nil)
		sa->err = rpcerror();
	endclient();
	sendp(sa->c, sa);
}
void
syncstat(Syncpath *s)
{
	Channel *c;
	Statarg *sa;

	dbg(DbgSync, "%P triage\n", s->p);
	if(s->state != SyncStart){
		fprint(2, "%P: state is not Start\n", s->p);
		return;
	}
	s->state = SyncStat;

	if(s->a.s && s->b.s)
		return;

	sa = emalloc(2*sizeof(Statarg));
	c = chan(Statarg*);
	if(s->a.s == nil){
		sa[0].repl = s->sync->ra;
		sa[0].p = s->p;
		sa[0].c = c;
		spawn(statthread, &sa[0]);
	}
	if(s->b.s == nil){
		sa[1].repl = s->sync->rb;
		sa[1].p = s->p;
		sa[1].c = c;
		spawn(statthread, &sa[1]);
	}
	threadstate("syncstat wait");
	if(s->a.s == nil)
		recvp(c);
	threadstate("syncstat wait");
	if(s->b.s == nil)
		recvp(c);
	chanfree(c);
	if(s->a.s == nil){
		s->a.s = sa[0].s;
		if(s->a.s == nil){
			s->err = sa[0].err;
			s->state = SyncError;
		}
	}
	if(s->b.s == nil){
		s->b.s = sa[1].s;
		if(s->b.s == nil){
			if(s->err == nil){
				s->err = sa[1].err;
				s->state = SyncError;
			}else
				free(sa[1].err);
		}
	}
	free(sa);
	if(s->state == SyncError && s->err == nil)
		s->err = estrdup("unspecified error in syncstat");
}

