#include "tra.h"

int
rpcaddtime(Replica *repl, Path *p, Vtime *st, Vtime *mt)
{
	Rpc r;

	memset(&r, 0, sizeof r);
	r.type = Taddtime;
	r.p = p;
	r.st = st;
	r.mt = mt;
	if(clientrpc(repl, &r) < 0)
		return -1;
	return 0;
}

int
rpcclose(Replica *repl, int fd)
{
	Rpc r;

	memset(&r, 0, sizeof r);
	r.type = Tclose;
	r.fd = fd;
	if(clientrpc(repl, &r) < 0)
		return -1;
	return 0;
}

int
rpccommit(Replica *repl, int fd, Stat *s)
{
	Rpc r;

	memset(&r, 0, sizeof r);
	r.type = Tcommit;
	r.fd = fd;
	r.s = s;
	if(clientrpc(repl, &r) < 0)
		return -1;
	return 0;
}

int
rpcdebug(Replica *repl, int debug)
{
	Rpc r;

	memset(&r, 0, sizeof r);
	r.type = Tdebug;
	r.n = debug;
	return clientrpc(repl, &r);
}

int
rpcflate(Replica *repl, int flate)
{
	Rpc r;
	Flate *inflate, *deflate;

	if((inflate = inflateinit()) == nil || (deflate = deflateinit(flate)) == nil){
		if(inflate)
			inflateclose(inflate);
		return -1;
	}

	memset(&r, 0, sizeof r);
	r.type = Tflate;
	r.n = flate;
	if(clientrpc(repl, &r) < 0){
		inflateclose(inflate);
		deflateclose(deflate);
		return -1;
	}
	dbg(DbgRpc, "%p flate=%d\n", repl, flate);
	repl->inflate = inflate;
	repl->deflate = deflate;
	return 0;
}

int
rpchangup(Replica *repl)
{
	Rpc r;

	memset(&r, 0, sizeof r);
	r.type = Thangup;
	if(clientrpc(repl, &r) < 0)
		return -1;
	return 0;
}

Hashlist*
rpchashfile(Replica *repl, int fd)
{
	Hashlist *hl;
	int i, n, nh;
	uchar *buf;
	vlong off;

	hl = mkhashlist();
	off = 0;
	nh = 0;
	buf = emallocnz(IOCHUNK);
	while((n = rpcreadhash(repl, fd, buf, IOCHUNK)) > 0){
		if(n % (2+SHA1dlen)){
			werrstr("got bad readhash count %d", n);
			free(hl);
			free(buf);
			return nil;
		}
		for(i=0; i<n; i+=2+SHA1dlen){
			hl = addhash(hl, buf+i+2, off, SHORT(buf+i));
			nh++;
			off += SHORT(buf+i);
		}
	}
	free(buf);
	if(n < 0){
		free(hl);
		return nil;
	}
	assert(hl->nh == nh);
	return hl;
}

int
rpckids(Replica *repl, Path *p, Kid **pkid)
{
	Rpc r;

	memset(&r, 0, sizeof r);
	r.type = Tkids;
	r.p = p;
	if(clientrpc(repl, &r) < 0)
		return -1;
	*pkid = r.k;
	return r.nk;
}

char*
rpcmeta(Replica *repl, char *s)
{
	Rpc r;

	memset(&r, 0, sizeof r);
	r.type = Tmeta;
	r.str = s;
	if(clientrpc(repl, &r) < 0)
		return nil;
	return r.str;
}
int
rpcmkdir(Replica *repl, Path *p, Stat *s)
{
	Rpc r;

	memset(&r, 0, sizeof r);
	r.type = Tmkdir;
	r.p = p;
	r.s = s;
	return clientrpc(repl, &r);
}

int
rpcopen(Replica *repl, Path *p, char omode)
{
	Rpc r;

	memset(&r, 0, sizeof r);
	r.type = Topen;
	r.p = p;
	r.omode = omode;
	
	if(clientrpc(repl, &r) < 0)
		return -1;
	return r.fd;
}
	
long
rpcread(Replica *repl, int fd, void *a, long n)
{
	Rpc r;

	memset(&r, 0, sizeof r);
	r.type = Tread;
	r.fd = fd;
	r.a = a;
	r.n = n;
	if(clientrpc(repl, &r) < 0)
		return -1;
	return r.n;
}

long
rpcreadhash(Replica *repl, int fd, void *a, long n)
{
	Rpc r;

	memset(&r, 0, sizeof r);
	r.type = Treadhash;
	r.fd = fd;
	r.a = a;
	r.n = n;
	if(clientrpc(repl, &r) < 0)
		return -1;
	return r.n;
}

long
rpcreadn(Replica *repl, int fd, void *a, long n)
{
	long tot, m;

	for(tot=0; tot<n; tot+=m){
		m = rpcread(repl, fd, (uchar*)a+tot, n-tot);
		if(m < 0)
			break;
		if(m == 0){
			werrstr("early eof");
			break;
		}
	}
	return tot;
}

int
rpcreadonly(Replica *repl, int ignwr)
{
	Rpc r;

	memset(&r, 0, sizeof r);
	r.type = Treadonly;
	r.n = ignwr;
	if(clientrpc(repl, &r) < 0)
		return -1;
	return 0;
}

int
rpcremove(Replica *repl, Path *p, Stat *s)
{
	Rpc r;

	memset(&r, 0, sizeof r);
	r.type = Tremove;
	r.p = p;
	r.s = s;
	return clientrpc(repl, &r);
}

int
rpcseek(Replica *repl, int fd, vlong off)
{
	Rpc r;

	memset(&r, 0, sizeof r);
	r.type = Tseek;
	r.fd = fd;
	r.vn = off;
	return clientrpc(repl, &r);
}

Stat*
rpcstat(Replica *repl, Path *p)
{
	Rpc r;

	memset(&r, 0, sizeof r);
	r.type = Tstat;
	r.p = p;
	if(clientrpc(repl, &r) < 0)
		return nil;
	return r.s;
}

long
rpcwrite(Replica *repl, int fd, void *a, long n)
{
	Rpc r;

	memset(&r, 0, sizeof r);
	r.type = Twrite;
	r.fd = fd;
	r.a = a;
	r.n = n;
	if(clientrpc(repl, &r) < 0)
		return -1;
	return r.n;
}

long
rpcwritehash(Replica *repl, int fd, void *a, long n)
{
	Rpc r;

	memset(&r, 0, sizeof r);
	r.type = Twritehash;
	r.fd = fd;
	r.a = a;
	r.n = n;
	if(clientrpc(repl, &r) < 0)
		return -1;
	return r.n;
}

int
rpcwstat(Replica *repl, Path *p, Stat *s)
{
	Rpc r;

	memset(&r, 0, sizeof r);
	r.type = Twstat;
	r.p = p;
	r.s = s;
	return clientrpc(repl, &r);
}

