/*
 * file system synchronizer
 */

#include "tra.h"
#include <thread.h>
#undef exits
#define exits(string, number) threadexitsall(string)

int printstats;
int res;
int interactive;
int verbose;
int nop;
int nconflict;
int expectconflicts;
int superquiet;

void resolvethread(void*);
void dumpsyncpath(Syncpath*);
void printwork(Syncpath*);
void printconflict(Syncpath*);
void printfinished(Syncpath*);

void
usage(void)
{
	fprint(2, "usage: tra [-1abmnvV] [-z n] replica-a replica-b [path...]\n");
	exits("usage", 1);
}

void
threadmain(int argc, char **argv)
{
	int flate, i, nfinish, npending, nsuccess, oneway;
	Sync *sync;
	Syncpath *s;

	flate = 0;
	oneway = 0;
	initfmt();
	threadsetname("main");

	ARGBEGIN{
	default:
		usage();
		break;
	case 'C':
		expectconflicts = 1;
		break;
	case 'D':
		debug |= dbglevel(EARGF(usage()));
		break;
	case 'V':
		traversion();
	case '1':
		oneway = 1;
		break;
	case 'a':
	case 'b':
		res = ARGC();
		break;
	case 'i':
		interactive = 1;
		break;
	case 'm':
		res = 'm';
		break;
	case 'n':
		nop = 1;
		break;
	case 'q':
		superquiet = 1;
		verbose = 0;
		break;
	case 's':
		printstats = 1;
		break;
	case 'v':
		superquiet = 0;
		verbose++;
		break;
	case 'z':
		flate = atoi(EARGF(usage()));
		break;
	}ARGEND

	if(argc < 2)
		usage();

	if(interactive)
		sysfatal("interactive mode not implemented");

	startclient();
	sync = emalloc(sizeof(Sync));
	sync->oneway = oneway;

	sync->ra = dialreplica(argv[0]);
	sync->rb = dialreplica(argv[1]);

	if(strcmp(sync->ra->sysname, sync->rb->sysname) == 0)
		sysfatal("%s and %s are the same replica", sync->ra->name, sync->rb->name);

	if(flate){
		if(rpcflate(sync->ra, flate) < 0)
			sysfatal("compressing %s: %r", sync->ra->name);
		if(rpcflate(sync->rb, flate) < 0)
			sysfatal("compressing %s: %r", sync->rb->name);
	}

	tralog("# starting tra%s %s %s", nop ? " -n" : (oneway ? " -1" : ""), 
		argv[0], argv[1]);

	if((oneway || nop) && rpcreadonly(sync->ra, 1) < 0)
		sysfatal("setting %s to readonly: %r", argv[0]);
	if(nop && rpcreadonly(sync->rb, 1) < 0)
		sysfatal("setting %s to readonly: %r", argv[1]);

	sync->syncq = mkstack("syncq", 0);
	sync->workq = mkqueue("workq", 0);
	sync->finishq = mkqueue("finishq", 0);
	sync->triageq = mkqueue("triageq", 0);

	spawn(syncthread, sync->syncq);
	spawn(workthread, sync->workq);
	spawn(resolvethread, sync);

	argc -= 2;
	argv += 2;
	if(argc == 0){
		s = emalloc(sizeof(Syncpath));
		s->sync = sync;
		s->p = nil;	/* root */
		qsend(sync->syncq, s);
		npending = 1;
	}else{
		for(i=0; i<argc; i++){
			s = emalloc(sizeof(Syncpath));
			s->sync = sync;
			s->p = strtopath(argv[i]);
			qsend(sync->syncq, s);
		}
		npending = argc;
	}

	nsuccess = 0;
	nfinish = 0;
	while(nfinish < npending){
		s = qrecv(sync->finishq);
		printfinished(s);
		if(s->parent){
			/* has a parent, just being given to us as a courtesy */
			if(verbose)
			if(verbose>1 || s->triage != DoNothing || s->action != DoNothing){
				print("SUBFINISH ");
				dumpsyncpath(s);
			}
		}else{
			nfinish++;
			if(s->state == SyncDone)
				nsuccess++;
			if(verbose){
				print("FINISH ");
				dumpsyncpath(s);
			}
		}
		synccleanup(s);
	}

	rpchangup(sync->ra);
	rpchangup(sync->rb);

	if(printstats){
		print("queue highwaters sync=%d work=%d finish=%d triage=%d\n",
			sync->syncq->m, sync->workq->m, sync->finishq->m,
			sync->triageq->m);
		print("rpc in %d out %d zin %d zout %d\n",
			inrpctot, outrpctot, inzrpctot, outzrpctot);
	}

	if(expectconflicts != (nsuccess < npending)){
		fprint(2, "expect %d, got %d<%d = %d\n", expectconflicts, nsuccess, npending, nsuccess<npending);
		if(expectconflicts)
			exits("expected conflicts, got none", 2);
		else
			exits("unresolved conflicts", 2);
	}
	exits(nil, 0);
}

void
dumpsyncpath(Syncpath *s)
{
	print("%P %s %s %s%s\n", s->p,
		syncpathstate(s->state),
		syncpathaction(s->triage),
		syncpathaction(s->action),
		s->conflict ? " conflict" : "");
}

void
resolvethread(void *v)
{
	Sync *sync;
	Syncpath *s;

	threadsetname("resolvethread");

	startclient();
	sync = v;
	for(;;){
		s = qrecv(sync->triageq);
		if(s->conflict){
			if(res=='a')
				s->action = s->triage|1;
			else if(res == 'b')
				s->action = s->triage&~1;
			else if(res == 'm' && s->a.s->mtime && s->b.s->mtime
			&& s->a.s->mtime != s->b.s->mtime
			&& s->a.s->state==SFile && s->b.s->state==SFile){
				if(s->a.s->mtime > s->b.s->mtime)
					s->action = s->triage|1;
				else
					s->action = s->triage&~1;
				tralog("mtime resolve %P -> %s", s->p, workstr(s, s->action));
			}else{
				nconflict++;
				printconflict(s);
				if(nop)
					qsend(s->sync->finishq, s);
				else
					syncfinish(s);
				continue;
			}
		}else
			s->action = s->triage;
		if(s->action != DoKids && s->sync->oneway && !(s->action&1))
			s->action = DoNothing;
		printwork(s);
		if(nop && s->action != DoKids)
			qsend(s->sync->finishq, s);
		else
			qsend(s->sync->workq, s);
	}
}

enum
{
	Verblen = 8,
};

char*
conflictstr(Syncpath *s)
{
	switch(s->triage&~1){
	case DoNothing:
	case DoKids:
		return "no conflict";
	case DoCopyBtoA:
		return "update/update";
	case DoCreateA:
		return "remove/update";
	case DoRemoveA:
		return "update/remove";
	default:
		return "unknown conflict";
	}
}

static char*
thetime(long t)
{
	static char buf[32];

	strcpy(buf, sysctime(t));
	buf[strlen(buf)-1] = '\0';
	return buf;
}

void
printmtime(int fd, Replica *r, Stat *s)
{
	int i;
	char *by;
	char *muid;
	Vtime *m;

	m = s->mtime;
	switch(m->nl){
	case 1:
		muid = s->muid;
		if(muid && muid[0])
			by = " by ";
		else{
			muid = "";
			by = "";
		}
		fprint(fd, "\t%s: last %s %s on %s%s%s\n",
			rsysname(r), s->sysmtime ? "modified" : "noticed modification",
			thetime(s->sysmtime ? s->sysmtime : m->l[0].wall), stripdot(m->l[0].m), by, muid);
		break;
	default:
		if(m->nl < 1){
			fprint(2, "\t%s: unexpected mtime %V\n", r->name, m);
			break;
		}
		fprint(fd, "\t%s: file is resolution of modifications\n", r->name);
		for(i=0; i<m->nl; i++)
			fprint(fd, "\t\tnoticed at %s on %s\n", thetime(m->l[i].wall), stripdot(m->l[i].m));
		fprint(fd, "\t\tfile's system mtime is %s\n", thetime(s->sysmtime));
		break;
	}
}
	
void
printconflict(Syncpath *s)
{
	print("%P: %s conflict\n", s->p, conflictstr(s));
	printmtime(1, s->sync->ra, s->a.s);
	printmtime(1, s->sync->rb, s->b.s);
}

int
quiet(Syncpath *s)
{
	if(superquiet)
		return 1;
	return (((s->action&~1)==DoNothing || (s->action&~1)==DoKids)) && verbose == 0;
}


void
printwork(Syncpath *s)
{
	if(quiet(s))
		return;
	print("%P: will %s\n", s->p, workstr(s, s->action));
}

void
printfinished(Syncpath *s)
{
	switch(s->state){
	case SyncDone:
		if(quiet(s))
			return;
		print("%P: did %s\n", s->p, workstr(s, s->action));
		break;
	case SyncError:
		print("error syncing %P: %s\n", s->p, s->err);
		break;
	case SyncTriage:
		/* no-op mode */
		if(nop)
			break;
		/* unresolved conflict; do nothing */
		if(s->conflict)
			break;
		break;
	default:
		print("%P: unexpected state %s\n", s->p, syncpathstate(s->state));
		break;
	}
}

