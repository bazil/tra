#include "tra.h"

/*
 * Carry out the work specified in the syncpath structures we get.
 */

static void	copy(Syncpath*, Replica*, Stat*, Replica*, Stat*);
static void	copyfile(Syncpath*, Replica*, Stat*, Replica*, Stat*);
static void	copytree(Syncpath*, Replica*, Stat*, Replica*, Stat*);
static int		copybytes(Replica*, int, Replica*, int, char*, vlong*, vlong, vlong);
static void	rm(Syncpath*, Replica*, Stat*);
static void	work(Syncpath*);
static void	workthread0(void*);

/*
 * Dispatch work to the slave threads.
 * This used to dispatch only file copies, handling the rest itself. 
 * I don't see why, so I cut that out.  I'm sure it will come back.
 */
void
workthread(void *a)
{
	int i;
	Syncpath *s;
	Queue *q;
	Queue *subq;

	threadsetname("workthread");
	q = a;
	subq = mkqueue("subq", 32);
	for(i=0; i<32; i++)
		spawn(workthread0, subq);

	while((s = qrecv(q)) != nil)
		qsend(subq, s);
}

/* one slave thread */
static void
workthread0(void *a)
{
	Queue *q;

	threadsetname("workslave0");
	q = a;
	startclient();
	for(;;)
		work(qrecv(q));
}

/* do the prescribed work on a path */
static void
work(Syncpath *s)
{
	if(s->state != SyncTriage){
		fprint(2, "%P: state is not Triage\n", s->p);
		return;
	}
	s->state = SyncAct;

	switch(s->action){
	default:
		fprint(2, "%P: unexpected action %d\n", s->p, s->action);
		s->state = SyncError;
		break;

	case DoNothing:
	case DoNothing1:
		break;

	case DoCopyBtoA:
	case DoCreateA:
		copy(s, s->sync->rb, s->b.s, s->sync->ra, s->a.s);
		break;

	case DoCopyAtoB:
	case DoCreateB:
		copy(s, s->sync->ra, s->a.s, s->sync->rb, s->b.s);
		break;

	case DoRemoveB:
		rm(s, s->sync->rb, s->a.s);
		break;

	case DoRemoveA:
		rm(s, s->sync->ra, s->b.s);
		break;

	case DoKids:
	case DoKids1:	
		s->state = SyncKids;
		break;
	}

	/* if state has not changed, we succeeded */
	if(s->state == SyncAct)
		s->state = SyncDone;

	/*
	 * if state is SyncKids, queue the kids.
	 * if that succeeds, then either it's already
	 * been finished or will finish once the kids are done.
	 */
	if(s->state == SyncKids && synckids(s) == 0)
		return;

	syncfinish(s);
}

static void
copy(Syncpath *s, Replica *rf, Stat *sf, Replica *rt, Stat *st)
{
	if(sf->state == SDir)
		copytree(s, rf, sf, rt, st);
	else
		copyfile(s, rf, sf, rt, st);
}

static void
copytree(Syncpath *s, Replica *rf, Stat *sf, Replica *rt, Stat *st)
{
	USED(rf);

	/* if both from and to are directories, we should be syncing */
	assert(st->state != SDir);

	/* remove any extant file */
	if(st->state == SFile && rpcremove(rt, s->p, sf) < 0)
		goto Err;

	if(rpcmkdir(rt, s->p, sf) < 0)
		goto Err;

	s->state = SyncKids;
	return;

Err:
	s->state = SyncError;
	s->err = rpcerror();
}

static void
copyfile(Syncpath *s, Replica *rf, Stat *sf, Replica *rt, Stat *st)
{
	char *buf, *e;
	int fdf, fdt, i, nc, nh;
	Hash *hf;
	Hashlist *hlf, *hlt;
	vlong roff;

	buf = nil;
	fdf = -1;
	fdt = -1;
	hlf = nil;
	hlt = nil;
	e = nil;

	/* remove any extant directory */
	if(st->state == SDir && rpcremove(rt, s->p, sf) < 0)
		goto Err;

	if((fdf = rpcopen(rf, s->p, 'r')) < 0)
		goto Err;
	if((fdt = rpcopen(rt, s->p, 'w')) < 0)
		goto Err;

	/*
	 * Try LBFS-style smart copy, but fall back on byte
 	 * copy for new sections.  
	 *
	 * TODO: run rpchashfile calls in parallel?
	 * TODO: overlap reads and writes in copybytes?
	 */
	buf = emalloc(IOCHUNK);
	if((hlf = rpchashfile(rf, fdf)) == nil){
		hlf = mkhashlist();
		hlf = addhash(hlf, sf->sha1, 0, sf->length);
	}
	hlt = rpchashfile(rt, fdt);
	if(hlt)
		qsort(hlt->h, hlt->nh, sizeof(hlt->h[0]), hashcmp);

	hf = nil;
	roff = 0;	/* read offset in hf */
	nh = 0;	/* size of pending hash copies */
	nc = 0;	/* size of pending byte copies */

	for(i=0; i<hlf->nh; i++){
		hf = &hlf->h[i];
		if(findhash(hlt, hf->sha1) != nil){
			/* do pending byte copies */
			if(nc && copybytes(rf, fdf, rt, fdt, buf, &roff, hf->off-nc, nc) < 0)
				goto Err;
			nc = 0;
			/* do pending hash copies if buffer is full */
			if(nh+SHA1dlen > IOCHUNK){
				if(rpcwritehash(rt, fdt, buf, nh) < 0)
					goto Err;
				nh = 0;
			}
			/* queue hash copy */
			memmove(buf+nh, hf->sha1, SHA1dlen);
			nh += SHA1dlen;
		}else{
			/* do pending hash copies */
			if(nh && rpcwritehash(rt, fdt, buf, nh) < 0)
				goto Err;
			nh = 0;
			/* queue byte copy */
			nc += hf->n;
		}
	}

	/* do pending hash copies */
	if(nh && rpcwritehash(rt, fdt, buf, nh) < 0)
		goto Err;
	/* do pending byte copies */
	if(nc && copybytes(rf, fdf, rt, fdt, buf, &roff, hf->off+hf->n-nc, nc) < 0)
		goto Err;

	/* we're done */

	if(rpcclose(rf, fdf) < 0)
		goto Err;
	fdf = -1;
	if(rpccommit(rt, fdt, sf) < 0)
		goto Err;
	fdt = -1;

Out:
	free(buf);
	free(hlf);
	free(hlt);
	free(e);
	if(fdf >= 0)
		rpcclose(rf, fdf);
	if(fdt >= 0)
		rpcclose(rt, fdt);
	return;

Err:
	s->err = rpcerror();
	s->state = SyncError;
	goto Out;
}


static int
copybytes(Replica *rr, int rfd, Replica *wr, int wfd,
	char *buf, vlong *roff, vlong off, vlong n)
{
	vlong tot;
	int m;

	if(*roff != off){
		if(rpcseek(rr, rfd, off) < 0)
			return -1;
		*roff = off;
	}
	for(tot=0; tot<n; tot+=m){
		if(n-tot > IOCHUNK)
			m = IOCHUNK;
		else
			m = n-tot;
//		fprint(2, "copybytes: tot=%lld n=%lld m=%d\n", tot, n, m);
		if((m = rpcread(rr, rfd, buf, m)) < 0
		|| rpcwrite(wr, wfd, buf, m) != m){
			*roff = -1;
			return -1;
		}
	}
	*roff += tot;
	return 0;
}

/* named rm because remove is a library function on some systems */
static void
rm(Syncpath *s, Replica *rt, Stat *sf)
{
	if(rpcremove(rt, s->p, sf) < 0){
		s->state = SyncError;
		s->err = rpcerror();
	}
}

