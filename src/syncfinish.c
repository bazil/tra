#include "tra.h"

void
syncfinish(Syncpath *s)
{
	int iscomplete;
	Vtime *m;

	/*
	 * We need to figure out whether a and b are now
	 * up to date with respect to the other.  This state is
	 * stored in s->a.complete and s->b.complete.
	 * If a or b was complete or incomplete without 
	 * doing any work, these are already set.  Else they are -1.
	 * 
	 * If the sync failed, we map -1 to 0.
	 * If the sync succeeded, we map -1 to 1.
	 * 
	 * If the sync was one-way only, then the value
	 * of s->a.complete is incorrect but will not be used.
	 */
	iscomplete = (s->state == SyncDone);
	if(nop){
		s->a.complete = 0;
		s->b.complete = 0;
	}
	dbg(DbgSync, "syncfinish %P %d %d %d\n", s->p, iscomplete, s->a.complete, s->b.complete);
	if(s->a.complete == -1)
		s->a.complete = iscomplete;
	if(s->b.complete == -1)
		s->b.complete = iscomplete;

	dbg(DbgSync, "syncfinish %P %d %d\n", s->p, s->a.complete, s->b.complete);
	/*
	 * Update the sync time on the now synced systems.
	 * For a directory, propagate the mtime too.  
	 *
	 * BUG: I think that when we create a directory tree
	 * (meaning one of the ->state's is not SDir),
	 * we might not set the mtime properly.  Check this.
	 */
	if(s->b.complete){
		m = nil;
		/* BUG: why is this both instead of just one? */
		if(s->a.s->state==SDir && s->b.s->state==SDir)
			m = s->a.s->mtime;
		if(rpcaddtime(s->sync->rb, s->p, s->a.s->synctime, m) < 0){
			s->state = SyncError;
			s->err = rpcerror();
			s->b.complete = 0;
			s->a.complete = 0;
			goto Err;
		}
	}
	if(!s->sync->oneway && s->a.complete){
		m = nil;
		if(s->b.s->state==SDir && s->a.s->state==SDir)
			m = s->b.s->mtime;
		if(rpcaddtime(s->sync->ra, s->p, s->b.s->synctime, m) < 0){
			s->state = SyncError;
			s->err = rpcerror();
			s->a.complete = 0;
			goto Err;
		}
	}

Err:
	qsend(s->sync->finishq, s);
}

void
synccleanup(Syncpath *s)
{
	Syncpath *up;
	int i;

	/* 
	 * Update the parent counts.  Once all the parent's
	 * kids finish successfully, the parent finishes 
	 * successfully.
	 */
	up = s->parent;
	if(up){
		if(!s->a.complete)
			up->a.complete = 0;
		if(!s->b.complete)
			up->b.complete = 0;
		if(--up->npend == 0){
			up->state = SyncDone;
			syncfinish(up);
		}
	}

	/*
	 * Free our own kids, if there are any.
	 */
	for(i=0; i<s->nkid; i++){
		freestat(s->kid[i].a.s);
		freestat(s->kid[i].b.s);
		freepath(s->kid[i].p);
		free(s->kid[i].err);
	}
	free(s->kid);
	s->kid = nil;
}
