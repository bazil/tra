#include "tra.h"

enum
{
	FdSize = 128*1024,
};

struct Fd
{
	int fd;
	int events;
	int roff, woff;
	int rmax, wmax;
	uchar rbuf[FdSize];
	uchar wbuf[FdSize];
};

Fd*
topen(int fd)
{
	Fd *f;

	f = emalloc(sizeof *f);
	f->fd = fd;
	f->roff = 0;
	f->rmax = 0;
	f->woff = 0;
	f->wmax = FdSize;
	return f;
}

int
treadn(Fd *f, void *av, int n)
{
	char *a;
	long m, t;

	a = av;
	t = 0;
	while(t < n){
		m = tread(f, a+t, n-t);
		if(m <= 0){
			if(t == 0)
				return m;
			break;
		}
		t += m;
	}
	return t;
}

int
twflush(Fd *f)
{
	int m;

	m = threadwrite(f->fd, f->wbuf, f->woff);
	if(m == -1)
		return -1;
	memmove(f->wbuf, f->wbuf+m, f->woff-m);
	f->woff -= m;
	return m;
}

int
_twrite(Fd *f, void *a, int n)
{
	int m;

	if(f->woff == f->wmax && twflush(f) == -1)
		return -1;
	m = n;
	if(f->wmax - f->woff < n)
		m = f->wmax - f->woff;
	memmove(f->wbuf+f->woff, a, m);
	f->woff += m;
	return m;
}

int
twrite(Fd *f, void *av, int n)
{
	char *a;
	long m, t;

	a = av;
	t = 0;
	while(t < n){
		m = _twrite(f, a+t, n-t);
		if(m <= 0){
			if(t == 0)
				return m;
			break;
		}
		t += m;
	}

	return t;
}

int
tcanread(Fd *f)
{
	return f->rmax - f->roff;
}

int
tread(Fd *f, void *a, int n)
{
	int m;

	if(f->roff == f->rmax){
		m = threadread(f->fd, f->rbuf, FdSize);
		if(m < 0)
			return -1;
		f->roff = 0;
		f->rmax = m;
	}
	m = f->rmax - f->roff;
	if(m > n)
		m = n;
	memmove(a, f->rbuf+f->roff, m);
	f->roff += m;
	return m;
}

void
tclose(Fd *f)
{
	close(f->fd);
	free(f);
}

