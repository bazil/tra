#include "tra.h"

int
convM2R(Buf *b, Rpc *r)
{
	int i;

	r->type = -1;
	switch(i=setjmp(b->jmp)){
	case 0:
		break;
	case BufSpace:
		werrstr("not enough room in buffer (type %d)", r->type);
		return -1;
	case BufData:
		werrstr("bad data format in buffer");
		return -1;
	default:
		werrstr("unexpected buffer error %d", i);
		return -1;
	}
	memset(r, 0, sizeof *r);
	// dbg(DbgRpc, "convM2R %.*H\n", (int)(b->ep-b->p), b->p);
	if(readbufc(b) != 0)
		longjmp(b->jmp, BufData);
	r->type = readbufc(b);
	r->tag = readbufl(b);
	switch(r->type){
	default:
		werrstr("bad RPC type %d", r->type);
		return 0;
	case Taddtime:
		r->p = readbufpath(b);
		r->st = readbufvtime(b);
		r->mt = readbufvtime(b);
		break;
	case Raddtime:
		break;
	case Tclose:
		r->fd = readbufl(b);
		break;
	case Rclose:
		break;
	case Tcommit:
		r->fd = readbufl(b);
		r->s = readbufstat(b);
		break;
	case Rcommit:
		break;
	case Thangup:
		break;
	case Rhangup:
		break;
	case Tkids:
		r->p = readbufpath(b);
		break;
	case Rkids:
		r->nk = readbufl(b);
		if(r->nk){
			r->k = emalloc(r->nk*sizeof(Kid));
			for(i=0; i<r->nk; i++){
				r->k[i].name = readbufstringdup(b);
				r->k[i].stat = readbufstat(b);
			}
		}
		break;
	case Tmkdir:
		r->p = readbufpath(b);
		r->s = readbufstat(b);
		break;
	case Rmkdir:
		break;
	case Topen:
		r->p = readbufpath(b);
		r->omode = readbufc(b);
		break;
	case Ropen:
		r->fd = readbufl(b);
		break;
	case Tread:
		r->fd = readbufl(b);
		r->n = readbufl(b);
		break;		
	case Rread:
		r->n = readbufl(b);
		r->a = readbufbytes(b, r->n);
		break;
	case Treadonly:
		r->n = readbufl(b);
		break;
	case Rreadonly:
		break;
	case Tremove:
		r->p = readbufpath(b);
		r->s = readbufstat(b);
		break;
	case Rremove:
		break;
	case Tstat:
		r->p = readbufpath(b);
		break;
	case Rstat:
		r->s = readbufstat(b);
		break;
	case Twrite:
		r->fd = readbufl(b);
		r->n = readbufl(b);
		r->a = readbufbytes(b, r->n);
		break;
	case Rwrite:
		r->n = readbufl(b);
		break;
	case Twstat:
		r->p = readbufpath(b);
		r->s = readbufstat(b);
		break;
	case Rwstat:
		break;
	case Rerror:
		r->err = readbufstring(b);
		break;
	case Tdebug:
		r->n = readbufl(b);
		break;
	case Rdebug:
		break;
	case Tflate:
		r->n = readbufl(b);
		break;
	case Rflate:
		break;
	case Tmeta:
		r->str = readbufstringdup(b);
		break;
	case Rmeta:
		r->str = readbufstringdup(b);
		break;
	case Treadhash:
		r->fd = readbufl(b);
		r->n = readbufl(b);
		break;
	case Rreadhash:
		r->n = readbufl(b);
		r->a = readbufbytes(b, r->n);
		break;
	case Twritehash:
		r->fd = readbufl(b);
		r->n = readbufl(b);
		r->a = readbufbytes(b, r->n);
		break;
	case Rwritehash:
		r->n = readbufl(b);
		break;
	case Tseek:
		r->fd = readbufl(b);
		r->vn = readbufvl(b);
		break;
	case Rseek:
		break;
	}
	return 0;
}

void
freekids(Kid *k, int nk)
{
	int i;

	for(i=0; i<nk; i++){
		free(k[i].name);
		freestat(k[i].stat);
	}
	free(k);
}

static int
_convR2M(Rpc *r, Buf *b)
{
	int i;

	/*
	 * N.B.  If you change this format (which is highly discouraged),
	 * you need to change the code in replthread, which knows the
	 * offset of the tag in the packet.
 	 */
	writebufc(b, 0);
	writebufc(b, r->type);
	writebufl(b, r->tag);
	switch(r->type){
	case Taddtime:
		writebufpath(b, r->p);
		writebufvtime(b, r->st);
		writebufvtime(b, r->mt);
		break;
	case Raddtime:
		break;
	case Tclose:
		writebufl(b, r->fd);
		break;
	case Rclose:
		break;
	case Tcommit:
		writebufl(b, r->fd);
		writebufstat(b, r->s);
		break;
	case Rcommit:
		break;
	case Thangup:
		break;
	case Rhangup:
		break;
	case Tkids:
		writebufpath(b, r->p);
	case Rkids:
		writebufl(b, r->nk);
		for(i=0; i<r->nk; i++){
			writebufstring(b, r->k[i].name);
			writebufstat(b, r->k[i].stat);
		}
		break;
	case Tmkdir:
		writebufpath(b, r->p);
		writebufstat(b, r->s);
		break;
	case Rmkdir:
		break;
	case Topen:
		writebufpath(b, r->p);
		writebufc(b, r->omode);
		writebufstat(b, r->s);
		break;
	case Ropen:
		writebufl(b, r->fd);
		break;
	case Tread:
		writebufl(b, r->fd);
		writebufl(b, r->n);
		break;
	case Rread:
		writebufl(b, r->n);
		writebufbytes(b, r->a, r->n);
		break;
	case Treadonly:
		writebufl(b, r->n);
		break;
	case Rreadonly:
		break;
	case Tremove:
		writebufpath(b, r->p);
		writebufstat(b, r->s);
		break;
	case Rremove:
		break;
	case Tstat:
		writebufpath(b, r->p);
		break;
	case Rstat:
		writebufstat(b, r->s);
		break;
	case Twrite:
		writebufl(b, r->fd);
		writebufl(b, r->n);
		writebufbytes(b, r->a, r->n);
		break;
	case Rwrite:
		writebufl(b, r->n);
		break;
	case Twstat:
		writebufpath(b, r->p);
		writebufstat(b, r->s);
	case Rwstat:
		break;
	case Rerror:
		writebufstring(b, r->err);
		break;
	case Tdebug:
		writebufl(b, r->n);
		break;
	case Rdebug:
		break;
	case Tflate:
		writebufl(b, r->n);
		break;
	case Rflate:
		break;
	case Tmeta:
		writebufstring(b, r->str);
		break;
	case Rmeta:
		writebufstring(b, r->str);
		break;
	case Treadhash:
		writebufl(b, r->fd);
		writebufl(b, r->n);
		break;
	case Rreadhash:
		writebufl(b, r->n);
		writebufbytes(b, r->a, r->n);
		break;
	case Twritehash:
		writebufl(b, r->fd);
		writebufl(b, r->n);
		writebufbytes(b, r->a, r->n);
		break;
	case Rwritehash:
		writebufl(b, r->n);
		break;
	case Tseek:
		writebufl(b, r->fd);
		writebufvl(b, r->vn);
		break;
	case Rseek:
		break;
	}
	return 0;
}

Buf*
mkbuf(void *data, int ndata)
{
	Buf *b;

	b = emalloc(sizeof(Buf)+ndata);
	b->p = (uchar*)(b+1);
	b->ep = b->p+ndata;
	if(data)
		memmove(b->p, data, ndata);
	return b;
}

Buf*
convR2M(Rpc *r)
{
	Buf s, *b;

	memset(&s, 0, sizeof s);
	if(_convR2M(r, &s) < 0)
		abort();
	b = mkbuf(nil, (intptr)s.p);
	if(_convR2M(r, b) < 0)
		abort();
	b->p = (uchar*)&b[1];
	return b;
}

void*
readbufbytes(Buf *b, long n)
{
	uchar *x;

	if(b->p+n > b->ep)
		longjmp(b->jmp, BufSpace);
	x = b->p;
	b->p += n;
	return x;
}

uchar
readbufc(Buf *b)
{
	if(b->p+1 > b->ep)
		longjmp(b->jmp, BufSpace);
	return *b->p++;
}

Datum
readbufdatum(Buf *b)
{
	Datum d;

	memset(&d, 0, sizeof d);
	d.n = readbufl(b);
	if(d.n){
		d.a = emalloc(d.n);
		memmove(d.a, readbufbytes(b, d.n), d.n);
	}
	return d;
}

ulong
readbufl(Buf *b)
{
	int i;
	ulong x;

	if(b->p+4 > b->ep)
		longjmp(b->jmp, BufSpace);
	x = 0;
	for(i=0; i<4; i++)
		x = (x<<8) | *b->p++;
	return x;
}

char*
readbufstring(Buf *b)
{
	int n;

	n = readbufl(b);
	if(n == -1)
		return nil;
	return readbufbytes(b, n);
}

uvlong
readbufvl(Buf *b)
{
	int i;
	uvlong x;

	if(b->p+8 > b->ep)
		longjmp(b->jmp, BufSpace);
	x = 0;
	for(i=0; i<8; i++)
		x = (x<<8) | *b->p++;
	return x;
}

void
writebufbytes(Buf *b, void *a, long n)
{
	if(b->ep == nil){
		b->p += n;
		return;
	}
	if(n==0)
		return;
	if(b->p+n > b->ep)
		longjmp(b->jmp, BufSpace);
	memmove(b->p, a, n);
	b->p += n;
}

void
writebufc(Buf *b, uchar c)
{
	if(b->ep == nil){
		b->p++;
		return;
	}
	if(b->p+1 > b->ep)
		longjmp(b->jmp, 1);
	*b->p++ = c;
}

void
writebufdatum(Buf *b, Datum d)
{
	writebufl(b, d.n);
	writebufbytes(b, d.a, d.n);
}

void
writebufl(Buf *b, ulong x)
{
	int i;

	if(b->ep == nil){
		b->p += 4;
		return;
	}
	if(b->p+4 > b->ep)
		longjmp(b->jmp, 1);
	for(i=0; i<4; i++){
		*b->p++ = (x>>(32-8));
		x <<= 8;
	}
}

void
writebufstring(Buf *b, char *s)
{
	if(s == nil){
		writebufl(b, -1);
		return;
	}
	writebufl(b, strlen(s)+1);
	writebufbytes(b, s, strlen(s)+1);
}

void
writebufvl(Buf *b, uvlong x)
{
	int i;

	if(b->ep == nil){
		b->p += 8;
		return;
	}
	if(b->p+8 > b->ep)
		longjmp(b->jmp, 1);
	for(i=0; i<8; i++){
		*b->p++ = (x>>(64-8));
		x <<= 8;
	}
}

int
rpcfmt(Fmt *fmt)
{
	int i;
	uchar *p;
	Rpc *r;

	r = va_arg(fmt->args, Rpc*);

	fmtprint(fmt, "%d ", r->tag);
	switch(r->type){
	default:
		return fmtprint(fmt, "<unknown type %d>", r->type);
	case Taddtime:
		return fmtprint(fmt, "Taddtime %P %V %V", r->p, r->st, r->mt);
	case Raddtime:
		return fmtprint(fmt, "Raddtime");
	case Tclose:
		return fmtprint(fmt, "Tclose %d", r->fd);
	case Rclose:
		return fmtprint(fmt, "Rclose");
	case Tcommit:
		return fmtprint(fmt, "Tcommit %d %$", r->fd, r->s);
	case Rcommit:
		return fmtprint(fmt, "Rcommit");
	case Thangup:
		return fmtprint(fmt, "Thangup");
	case Rhangup:
		return fmtprint(fmt, "Rhangup");
	case Tkids:
		return fmtprint(fmt, "Tkids %P", r->p);
	case Rkids:
		fmtprint(fmt, "Rkids %d", r->nk);
		for(i=0; i<r->nk; i++)
			fmtprint(fmt, " %s", r->k[i].name);
		return 0;
	case Tmkdir:
		return fmtprint(fmt, "Tmkdir %P %$", r->p, r->s);
	case Rmkdir:
		return fmtprint(fmt, "Rmkdir");
	case Topen:
		return fmtprint(fmt, "Topen %P %c", r->p, r->omode);
	case Ropen:
		return fmtprint(fmt, "Ropen %d", r->fd);
	case Tread:
		return fmtprint(fmt, "Tread %d %ld", r->fd, r->n);
	case Rread:
//		return fmtprint(fmt, "Rread '%.*s'", utfnlen(r->a, r->n), r->a);
		return fmtprint(fmt, "Rread %ld", r->n);
	case Treadonly:
		return fmtprint(fmt, "Treadonly%s", r->n ? " ignwr" : "");
	case Rreadonly:
		return fmtprint(fmt, "Rreadonly");
	case Tremove:
		return fmtprint(fmt, "Tremove %P %$", r->p, r->s);
	case Rremove:
		return fmtprint(fmt, "Rremove");
	case Tstat:
		return fmtprint(fmt, "Tstat %P", r->p);
	case Rstat:
		return fmtprint(fmt, "Rstat %$", r->s);
	case Twrite:
//		return fmtprint(fmt, "Twrite %d '%.*s'", r->fd, utfnlen(r->a, r->n), r->a);
		return fmtprint(fmt, "Twrite %d %ld", r->fd, r->n);
	case Rwrite:
		return fmtprint(fmt, "Rwrite %ld", r->n);
	case Twstat:
		return fmtprint(fmt, "Twstat %P %$", r->p, r->s);
	case Rwstat:
		return fmtprint(fmt, "Rwstat");
	case Rerror:
		return fmtprint(fmt, "Rerror %s", r->err);
	case Tdebug:
		return fmtprint(fmt, "Tdebug %lx", r->n);
	case Rdebug:
		return fmtprint(fmt, "Rdebug");
	case Tflate:
		return fmtprint(fmt, "Tflate %ld", r->n);
	case Rflate:
		return fmtprint(fmt, "Rflate");
	case Tmeta:
		return fmtprint(fmt, "Tmeta %s", r->str);
	case Rmeta:
		return fmtprint(fmt, "Rmeta %s", r->str);
	case Treadhash:
		return fmtprint(fmt, "Treadhash %d %ld", r->fd, r->n);
	case Rreadhash:
//		return fmtprint(fmt, "Rreadhash '%.*s'", utfnlen(r->a, r->n), r->a);
		fmtprint(fmt, "Rreadhash %ld", r->n/(2+SHA1dlen));
		p = r->a;
		for(i=0; i<r->n/(2+SHA1dlen); i++){
			fmtprint(fmt, " %d/%.*H", SHORT(p), SHA1dlen, p+2);
			p += 2+SHA1dlen;
		}
		return 0;
	case Twritehash:
//		return fmtprint(fmt, "Twritehash %d '%.*s'", r->fd, utfnlen(r->a, r->n), r->a);
		fmtprint(fmt, "Twritehash %d %ld", r->fd, r->n);
		p = r->a;
		for(i=0; i<r->n/SHA1dlen; i++){
			fmtprint(fmt, " %.*H", SHA1dlen, p);
			p += SHA1dlen;
		}
		return 0;
	case Rwritehash:
		return fmtprint(fmt, "Rwritehash %ld", r->n);
	case Tseek:
		return fmtprint(fmt, "Tseek %d %lld", r->fd, r->vn);
	case Rseek:
		return fmtprint(fmt, "Rseek");
	}
}
