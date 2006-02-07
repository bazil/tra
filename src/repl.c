#include "tra.h"
#undef send	/* oops */
#undef recv

static uchar rbuf[100];
Buf*
replread(Replica *r)
{
	uchar hdr[4];
	int n, nn;
	Buf *b, *bb;

	if(r->err){
	Error:
		werrstr("%s", r->err);
		return nil;
	}

//fprint(2, "treadn %d\n", 4);
	if(!tcanread(r->rfd))
		replflush(r);

	if(treadn(r->rfd, hdr, 4) != 4){
		r->err = "eof reading input";
		goto Error;
	}

	n = LONG(hdr);
	if(n < 6){
		r->err = "short rpc packet";
		goto Error;
	}

	if(n > MAXPACKET){
		memmove(rbuf, hdr, 4);
		n = tread(r->rfd, rbuf+4, sizeof rbuf-5);
		rbuf[n] = 0;
		fprint(2, "%s: IMPLAUSIBLE %s\n", argv0, rbuf); 
		r->err = "implausible rpc packet";
sysfatal("bad rpc"); //XXX
		goto Error;
	}

	b = mkbuf(nil, n);
	if(treadn(r->rfd, b->p, n) != n){
		r->err = "eof reading input";
		goto Error;
	}

	// dbg(DbgRpc, "replread %.*H\n", (int)(b->ep-b->p), b->p);
	if(r->inflate){
		inzrpctot += b->ep - b->p;
		nn = readbufl(b);
		if(nn > MAXPACKET){
			r->err = "implausible rpc packet";
			goto Error;
		}
		bb = mkbuf(nil, nn);
		// dbg(DbgRpc, "inflate %.*H\n", (int)(b->ep-b->p), b->p);
		if(inflateblock(r->inflate, bb->p, nn, b->p, b->ep-b->p) != nn){
			r->err = "error decompressing block";
			goto Error;
		}
		free(b);
		b = bb;
	}
	inrpctot += b->ep - b->p;
	return b;
}

int
replwrite(Replica *r, Buf *b)
{
	int n, nn;
	uchar hdr[8];
	Buf *bb;

	n = b->ep - b->p;
	bb = nil;
	outrpctot += n;
	if(r->deflate){
		// dbg(DbgRpc, "deflate %.*H\n", (int)(b->ep-b->p), b->p);
		bb = mkbuf(nil, n+128);
		writebufl(bb, n);
		nn = deflateblock(r->deflate, bb->p, bb->ep-bb->p, b->p, n);
		if(nn < 0){
			r->err = "error compressing block";
			return -1;
		}
		bb->ep = bb->p+nn;
		bb->p -= 4;
		b = bb;
		// dbg(DbgRpc, " => %.*H\n", (int)(b->ep-b->p), b->p);
		n = b->ep - b->p;
		outzrpctot += n;
	}
	PLONG(hdr, n);
	if(twrite(r->wfd, hdr, 4) != 4
	|| twrite(r->wfd, b->p, n) != n){
		free(bb);
fprint(2, "write error\n");
sysfatal("write error");	// XXX
		r->err = "write error";
		return -1;
	}
	free(bb);
	return 0;
}

int
replflush(Replica *r)
{
	qlock(&r->wlock);
//Xfprint(2, "%s: replflush %p start\n", argv0, r);
	dbg(DbgRpc, "replflush\n");
	if(twflush(r->wfd) < 0){
		r->err = "write error";
		qunlock(&r->wlock);
		return -1;
	}
//Xfprint(2, "%s: replflush %p end\n", argv0, r);
	qunlock(&r->wlock);
	return 0;
}

void
replclose(Replica *r)
{
	tclose(r->rfd);
	if(r->rfd != r->wfd)
		tclose(r->wfd);
	free(r);
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
	
	USED(mux);
	b = v;
	if(b->p+6 > b->ep)
		return -1;
	p = b->p+2;
	p[0] = (tag>>24)&0xFF;
	p[1] = (tag>>16)&0xFF;
	p[2] = (tag>>8)&0xFF;
	p[3] = tag&0xFF;
	return 0;
}

static void*
replmuxrecv(Mux *mux)
{
	void *v;
	Replica *r;

	r = mux->aux;
	qlock(&r->rlock);
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
	return x;
}

Replica*
fd2replica(int fd0, int fd1)
{
	Replica *repl;

	repl = emalloc(sizeof(Replica));
	repl->rfd = topen(fd0, OREAD);
	repl->wfd = topen(fd1, OWRITE);
	
	muxinit(&repl->mux);
	repl->mux.mintag = 0;
	repl->mux.maxtag = 255;
	repl->mux.send = replmuxsend;
	repl->mux.recv = replmuxrecv;
	repl->mux.gettag = replmuxgettag;
	repl->mux.settag = replmuxsettag;
	repl->mux.aux = repl;

	return repl;
}

/* BUG: the sysfatals here and in _dialreplica are antisocial */
Replica*
dialreplica(char *name)
{
	Replica *r;

	r = _dialreplica(name);
	if(banner(r, name) < 0)
		sysfatal("%s banner: %r", name);
	r->name = estrdup(name);
	if((r->sysname = rpcmeta(r, "sysname")) == nil)
		sysfatal("%s sysname: %r", name);
	return r;
}

