#include "tra.h"

void
synctriage(Syncpath *s)
{
	Stat *sa, *sb;

	dbg(DbgSync, "%P triage %$ %$\n", s->p, s->a.s, s->b.s);
	if(s->state != SyncStat){
		fprint(2, "%P: state is not Stat\n", s->p);
		return;
	}
	s->state = SyncTriage;

	sa = s->a.s;
	sb = s->b.s;

	s->a.complete = -1;
	s->b.complete = -1;

	coverage();
	if((sa->state&SNonreplicated) || (sb->state&SNonreplicated)){
		/*
		 * One or both replicas don't care about this file right now.
		 * Record that it is incomplete w.r.t. the other.
		 */
		coverage();
		s->a.complete = !(sa->state&SNonreplicated);
		s->b.complete = !(sb->state&SNonreplicated);
		s->triage = DoNothing;
		return;
	}

	if((sa->state == SNonexistent) && (sb->state == SNonexistent)){
		/*
		 * File doesn't exist on either.  Great.
		 */
		coverage();
		s->a.complete = 1;
		s->b.complete = 1;
		s->triage = DoNothing;
		return;
	}

	if(sa->state == SNonexistent){
		/*
		 * File exists on b but not a.
		 */
		if(intersectvtime(sb->ctime, sa->synctime)){
			/* a knew about b's copy but deleted it ... */
			coverage();
			if(leqvtime(sb->mtime, sa->synctime)){
				/* ... and that copy was same or newer */
				coverage();
				s->a.complete = 1;
				s->triage = DoRemoveB;
				return;
			}else{
				/* ... but that copy is now out-of-date */
				coverage();
				s->conflict = 1;
				s->triage = DoRemoveB;
				return;
			}
		}else{
			/* a has never seen this file */
			coverage();
			s->b.complete = 1;
			s->triage = DoCreateA;
			return;
		}
	}

	/* just a copy of the above, reversed.  not happy about this */
	if(sb->state == SNonexistent){
		/*
		 * File exists on a but not b.
		 */
		if(intersectvtime(sa->ctime, sb->synctime)){
			/* b knew about a's copy but deleted it ... */
			coverage();
			if(leqvtime(sa->mtime, sb->synctime)){
				/* ... and that copy was same or newer */
				coverage();
				s->b.complete = 1;
				s->triage = DoRemoveA;
				return;
			}else{
				/* ... but that copy is now out-of-date */
				coverage();
				s->conflict = 1;
				s->triage = DoRemoveA;
				return;
			}
		}else{
			/* b has never seen this file */
			coverage();
			s->b.complete = 1;
			s->triage = DoCreateB;
			return;
		}
	}

	if(leqvtime(sa->mtime, sb->synctime)){
		/* b knows all about a ... */
		coverage();
		s->b.complete = 1;
	}
	if(leqvtime(sb->mtime, sa->synctime)){
		/* a knows all about b ... */
		coverage();
		s->a.complete = 1;
	}
	if(s->a.complete == 1 && s->b.complete == 1){
		/* both are up to date */
		coverage();
		s->triage = DoNothing;
		return;
	}
	if(sa->state == SDir && sb->state == SDir){
		/* at least one is out-of-date */
		coverage();
		s->triage = DoKids;
		return;
	}
	if(sa->state==SFile && sb->state==SFile
	&& memcmp(sa->sha1, sb->sha1, SHA1dlen) == 0){
		s->a.complete = 1;
		s->b.complete = 1;
		coverage();
		s->triage = DoNothing;
		return;
	}

	if(s->a.complete == 1){
		/* a trumps b */
		coverage();
		s->triage = DoCopyAtoB;
		return;
	}
	if(s->b.complete == 1){
		/* vice versa */
		coverage();
		s->triage = DoCopyBtoA;
		return;
	}

	/* neither knows of the other: conflict */
	s->conflict = 1;
	s->triage = DoCopyBtoA;
	return;
}

