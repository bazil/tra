#include "tra.h"

#define curclient *threaddata()

struct Client
{
	Client *next;	/* in free list */
	Buf *b;
	Rendez r;
	Rpc *rpc;
	int done;
	int sleeping;
	char err[ERRMAX];
};

static int gettag(Replica*, Client*);
static void puttag(Replica*, Client*, int);

int
clientrpc(Replica *repl, Rpc *r)
{
	int i, tag;
	uchar *x;
	Buf *b;
	Client *cli, *cli2;
	Rpc nr;

	cli = curclient;

	qlock(&repl->lk);
	r->tag = gettag(repl, cli);
	cli->r.l = &repl->lk;
	cli->b = nil;
	b = convR2M(r);
	if(b == nil)
		abort();

	dbg(DbgRpc, "-> %p %R\n", repl, r);
	threadstate("clientrpc %R", r);
	qunlock(&repl->lk);

	if(replwrite(repl, b) < 0){
		rerrstr(cli->err, sizeof cli->err);
		threadstate("");
		free(b);
		return -1;
	}
	free(b);

	qlock(&repl->lk);
	/* wait for the muxer to give us our packet */
	cli->sleeping = 1;
	repl->nsleep++;
	while(repl->muxer && cli->b==nil){
		threadstate("clientrpc muxwait %R", r);
		rsleep(&cli->r);
		threadstate("");
	}
	repl->nsleep--;
	cli->sleeping = 0;

	/* if not done, there's no muxer: start muxing */
	if(cli->b == nil){
		if(repl->muxer)
			abort();
		repl->muxer = 1;
		while(!cli->b){
			qunlock(&repl->lk);
			/*
			 * XXX There must be a better way to do this.
			 * We want to wait to flush the output buffer
			 * until all the other threads finish generating
			 * requests.  There are at most two mux readers -- one for
			 * each trasrv -- hence the test here.
			 */
		//	while(yield() >= 2)
		//		;
			threadstate("replread %R", r);
			if((b = replread(repl)) == nil){
				rerrstr(cli->err, sizeof cli->err);
				qlock(&repl->lk);
				break;
			}
			x = b->p+2;
			tag = LONG(x);
			qlock(&repl->lk);
			if(tag >= nelem(repl->wait) || (cli2 = repl->wait[tag]) == nil
			|| cli2->b != nil){
				fprint(2, "rpcmux: unexpected packet tag %d\n", tag);
				free(b);
				continue;
			}
			cli2->b = b;
			rwakeup(&cli2->r);
		}
		repl->muxer = 0;
		/* if there is anyone else sleeping, wake them to mux */
		if(repl->nsleep){
			for(i=0; i<nelem(repl->wait); i++)
				if(repl->wait[i] != nil && repl->wait[i]->sleeping)
					break;
			if(i==nelem(repl->wait))
				fprint(2, "tra: nsleep botch\n");
			else
				rwakeup(&repl->wait[i]->r);
		}
	}

	b = cli->b;
	cli->b = nil;
	puttag(repl, cli, r->tag);
	qunlock(&repl->lk);
	threadstate("");

	if(b == nil){
		rerrstr(cli->err, sizeof cli->err);
		return -1;
	}

	if(b == nil){
		rerrstr(cli->err, sizeof cli->err);
		return -1;
	}

	if(convM2R(b, &nr) < 0){
		rerrstr(cli->err, sizeof cli->err);
		free(b);
		return -1;
	}

	dbg(DbgRpc, "<- %p %R\n", repl, &nr);
	if(nr.type==Rerror){
		utfecpy(cli->err, cli->err+sizeof cli->err, nr.err);
		free(b);
		return -1;
	}
	if(nr.type != r->type+1){
		snprint(cli->err, sizeof cli->err, "bad tag %d expected %d", nr.type, r->type+1);
		free(b);
		return -1;
	}
	if(r->type==Tread || r->type==Treadhash){
		memmove(r->a, nr.a, nr.n);
		nr.a = r->a;
	}
	free(b);
	*r = nr;
	return 0;
}
	
static int 
gettag(Replica *repl, Client *cli)
{
	int i;

Again:
	while(repl->ntag == nelem(repl->wait))
		rsleep(&repl->tagrend);
	for(i=0; i<nelem(repl->wait); i++)
		if(repl->wait[i] == 0){
			repl->ntag++;
			repl->wait[i] = cli;
			return i;
		}
	fprint(2, "gettag: ntag botch\n");
	goto Again;
}

static void
puttag(Replica *repl, Client *cli, int tag)
{
	assert(repl->wait[tag] == cli);
	repl->wait[tag] = nil;
	repl->ntag--;
	rwakeup(&repl->tagrend);
}

static Client *freelist;

static Client*
allocclient(void)
{
	int i;
	Client *cli;

	if(freelist == nil){
		freelist = emalloc(sizeof(Client)*64);
		for(i=0; i<64-1; i++)
			freelist[i].next = &freelist[i+1];
		freelist[i].next = nil;
	}
	cli = freelist;
	freelist = cli->next;
	return cli;
}

static void
freeclient(Client *cli)
{
	cli->next = freelist;
	freelist = cli;
}

void
startclient(void)
{
	curclient = allocclient();
}

void
endclient(void)
{
	Client *cli;

	cli = curclient;
	curclient = nil;
	freeclient(cli);
}

char*
rpcerror(void)
{
	Client *cli;

	cli = curclient;
	return estrdup(cli->err);
}
