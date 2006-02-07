#include "tra.h"
#include <thread.h>
/* BUG: the sysfatals here and in _dialreplica are antisocial */
Replica*
dialreplica(char *name)
{
	Replica *r;

	r = _dialreplica(name);
	r->name = estrdup(name);
	if(banner(r, name) < 0)
		sysfatal("%s banner: %r", name);
	if((r->sysname = rpcmeta(r, "sysname")) == nil)
		sysfatal("%s sysname: %r", name);
	return r;
}

static int
replmuxgettag(Mux *mux, void *v)
{
	uchar *p;
	Buf *b;
	
	USED(mux);
	b = v;
	if(b->p+6 > b->ep)
		return -1;
	p = b->p+2;
	return (p[0]<<24)|(p[1]<<16)|(p[2]<<8)|p[3];	
}

static int
replmuxsettag(Mux *mux, void *v, uint tag)
{
	uchar *p;
	Buf *b;
	Rpc *r;
	
	USED(mux);
	b = v;
	if(b->p+6 > b->ep)
		return -1;
	p = b->p+2;
	p[0] = (tag>>24)&0xFF;
	p[1] = (tag>>16)&0xFF;
	p[2] = (tag>>8)&0xFF;
	p[3] = tag&0xFF;
	if(b->aux){
		r = b->aux;
		r->tag = tag;
		dbg(DbgRpc, "-> %s %R\n", r->repl->name, r);
	}
	return 0;
}

static void*
replmuxrecv(Mux *mux)
{
	void *v;
	Replica *r;

	r = mux->aux;
	qlock(&r->rlock);
	threadidle();
	v = replread(r);
	qunlock(&r->rlock);
	return v;
}

static int
replmuxsend(Mux *mux, void *v)
{
	int x;
	Replica *r;

	r = mux->aux;
	qlock(&r->wlock);
	x = replwrite(r, v);
	qunlock(&r->wlock);
	replflush(r);
	return x;
}

#undef send
#undef recv
void
replmuxinit(Replica *repl)
{	
	muxinit(&repl->mux);
	repl->mux.mintag = 0;
	repl->mux.maxtag = 255;
	repl->mux.send = replmuxsend;
	repl->mux.recv = replmuxrecv;
	repl->mux.gettag = replmuxgettag;
	repl->mux.settag = replmuxsettag;
	repl->mux.aux = repl;
}

