#include "tra.h"

typedef struct Kids Kids;
struct Kids
{
	Replica *repl;
	Path *p;
	Kid *k;
	int nk;
	Channel *c;
	char *err;
};
static void
kidthread(void *a)
{
	Kids *k;

	k = a;
	threadsetname("kidthread");
	startclient();
	k->nk = rpckids(k->repl, k->p, &k->k);
	if(k->nk < 0)
		k->err = rpcerror();
	endclient();
	threadstate("kidthread end");
	send(k->c, k);
	threadstate("DONE %p", k->c);
}
static int
mergekids(Syncpath *s, Kid *a, int na, Kid *b, int nb, int n)
{	
	int i, j, k, c;
	Syncpath *w;

	if(n){
		s->nkid = n;
		s->kid = emalloc(n*sizeof(Syncpath));
		w = s->kid;
	}else
		w = nil;

	for(i=j=k=0; i<na || j<nb; k++){
		if(w){
			w->sync = s->sync;
			w->state = SyncStart;
			w->parent = s;
		}
		if(i>=na)
			goto UseT;
		if(j>=nb)
			goto UseF;
		c = strcmp(a[i].name, b[j].name);
		if(c < 0){
		UseF:
			if(w){
				w->p = mkpath(s->p, a[i].name);
				w->a.s = a[i].stat;
				a[i].stat = nil;
				w++;
			}
			i++;
			continue;
		}
		if(c == 0){
			if(w){
				w->p = mkpath(s->p, a[i].name);
				w->a.s = a[i].stat;
				a[i].stat = nil;
				w->b.s = b[j].stat;
				b[j].stat = nil;
				w++;
			}
			i++;
			j++;
			continue;
		}
		if(c > 0){
		UseT:
			if(w){
				w->p = mkpath(s->p, b[j].name);
				w->b.s = b[j].stat;
				b[j].stat = nil;
				w++;
			}
			j++;
			continue;
		}
		abort();	/* not reached */
	}
	return k;
}
static int
kidnamecmp(const void *a, const void *b)
{
	Kid *ka, *kb;

	ka = (Kid*)a;
	kb = (Kid*)b;
	return strcmp(ka->name, kb->name);
}

int
synckids(Syncpath *s)
{
	int i, ret, n;
	Channel *c;
	Kids *ak, *bk;

	c = chan(Kids*);
	ak = emalloc(sizeof(*ak));
	ak->repl = s->sync->ra;
	ak->p = s->p;
	ak->c = c;
	spawn(kidthread, ak);

	bk = emalloc(sizeof(*bk));
	bk->repl = s->sync->rb;
	bk->p = s->p;
	bk->c = c;
	spawn(kidthread, bk);

	threadstate("synckids wait %p", c);
	recvp(c);
	threadstate("synckids wait %p", c);
	recvp(c);
	chanfree(c);

	ret = -1;
	if(ak->err){
		s->err = ak->err;
		ak->err = 0;
		s->state = SyncError;
		goto End;
	}
	if(bk->err){
		s->err = bk->err;
		bk->err = 0;
		s->state = SyncError;
		goto End;
	}

	ret = 0;
	if(ak->nk)
		qsort(ak->k, ak->nk, sizeof(ak->k[0]), kidnamecmp);
	if(bk->nk)
		qsort(bk->k, bk->nk, sizeof(bk->k[0]), kidnamecmp);
	n = mergekids(s, ak->k, ak->nk, bk->k, bk->nk, 0);
	if(n == 0){
		s->state = SyncDone;
		syncfinish(s);
		goto End;
	}
	mergekids(s, ak->k, ak->nk, bk->k, bk->nk, n);
	s->npend = s->nkid;
	for(i=0; i<n; i++)
		qsend(s->sync->syncq, &s->kid[i]);

End:
	if(ak->err)
		free(ak->err);
	if(bk->err)
		free(bk->err);
	freekids(ak->k, ak->nk);
	freekids(bk->k, bk->nk);
	free(ak);
	free(bk);
	return ret;
}

