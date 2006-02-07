#include "tra.h"

int
clientrpc(Replica *repl, Rpc *r)
{
	Buf *b, *bb;
	Rpc nr;

	b = convR2M(r);
	if(b == nil)
		abort();
	/* printed by replmuxsettag */
	/* dbg(DbgRpc, "-> %s %R\n", repl->sysname, r); */
	b->aux = r;
	r->repl = repl;
	threadstate("clientrpc %R", r);
	bb = muxrpc(&repl->mux, b);
	threadstate("");
	free(b);
	if(bb == nil)
		return -1;
	if(convM2R(bb, &nr) < 0){
		free(bb);
		return -1;
	}
	dbg(DbgRpc, "<- %s %R\n", repl->name, &nr);
	if(nr.type == Rerror){
		werrstr("%s", nr.err);
		free(bb);
		return -1;
	}
	if(nr.type != r->type+1){
		werrstr("bad type %d expected %d", nr.type, r->type+1);
		free(bb);
		return -1;
	}
	if(r->type==Tread || r->type==Treadhash){
		memmove(r->a, nr.a, nr.n);
		nr.a = r->a;
	}
	free(bb);
	*r = nr;
	return 0;
}

void
startclient(void)
{
}

void
endclient(void)
{
}

char*
rpcerror(void)
{
	char buf[ERRMAX];
	
	rerrstr(buf, sizeof buf);
	return estrdup(buf);
}
