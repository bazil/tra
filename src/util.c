#include "tra.h"

int	debug;
char*	dbgname;

void
traversion(void)
{
	print("Tra Working Version SOSP03\n");
	exits(nil, 0);
}

void*
emalloc(ulong n)
{
	void *v;

	if(n > 1024*1024*1024)
		abort();
	v = mallocz(n, 1);
	if(v == nil)
		sysfatal("out of memory: emalloc(%ld), 0x%lx",
			 n, getcallerpc(&v));
	memset(v, 0, n);
	setmalloctag(v, getcallerpc(&n));
	return v;
}

void*
erealloc(void *v, ulong n)
{
	v = realloc(v, n);
	if(v == nil)
		sysfatal("out of memory: erealloc(0x%p, 0x%lud) from 0x%lux",
			 v, n, getcallerpc(&v));
	setrealloctag(v, getcallerpc(&v));
	return v;
}

char*
estrdup(char *s)
{
	if(s == nil)
		return nil;
	s = strdup(s);
	setmalloctag(s, getcallerpc(&s));
	if(s == nil)
		sysfatal("out of memory: estrdup(%s), %lux",
			 s, getcallerpc(&s));
	return s;
}

char*
esmprint(char *fmt, ...)
{
	va_list arg;
	char *s;

	va_start(arg, fmt);
	s = vsmprint(fmt, arg);
	va_end(arg);
	if(s == nil)
		sysfatal("out of memory: esmprint(%s, ...), %lux",
			 fmt, getcallerpc(&fmt));
	setmalloctag(s, getcallerpc(&fmt));
	return s;
}

void
_coverage(char *file, int line)
{
	fprint(2, "COVERAGE %s:%d\n", file, line);
}

void
dbg(int level, char *f, ...)
{
	va_list arg;
	Fmt fmt;
	char buf[128];

	if((debug&level) != level)
		return;

	fmtfdinit(&fmt, 2, buf, sizeof buf);
	va_start(arg, f);
	fmtprint(&fmt, "%s: ", dbgname ? dbgname : argv0);
	fmtvprint(&fmt, f, arg);
	va_end(arg);
	fmtfdflush(&fmt);
}

int
nilstrcmp(char *a, char *b)
{
	if(a==nil && b==nil)
		return 0;
	if(a==nil)
		return 1;
	if(b == nil)
		return -1;
	return strcmp(a, b);
}

/* keep in sync with tra.h:/Syncpath.state */
static char*
statetab[] =
{
	"start",
	"stat",
	"triage",
	"act",
	"done",
	"error",
	"kids",
};

/* keep in sync with tra.h:/Syncpath.triage */
static char*
dotab[] = {
	"nothing",
	"nothing1",
	"copyBtoA",
	"copyAtoB",
	"createA",
	"removeB",
	"removeA",
	"createB",
	"kids",
	"kids1",
};

char*
syncpathstate(int n)
{
	static char buf[40];

	if(n < nelem(statetab))
		return statetab[n];
	sprint(buf, "unknown state %d", n);
	return buf;
}

char*
syncpathaction(int n)
{
	static char buf[40];

	if(n < nelem(dotab))
		return dotab[n];
	sprint(buf, "unknown action %d", n);
	return buf;
}

static struct
{
	char *s;
	int n;
} tab[] =
{
	{ "coverage",	DbgCoverage, },
	{ "sync",	DbgSync, },
	{ "work",	DbgWork, },
	{ "rpc",	DbgRpc, },
	{ "ignore",	DbgIgnore, },
	{ "ghost",	DbgGhost, },
	{ "db",	DbgDb, },
	{ "cache",	DbgCache, },
	{ "all",	~0, }
};

int
dbglevel(char *s)
{
	int i;

	for(i=0; i<nelem(tab); i++)
		if(strcmp(tab[i].s, s) == 0)
			return tab[i].n;
	sysfatal("unknown debug level %s\n", s);
	return 0;
}

void
panic(char *f, ...)
{
	va_list arg;
	Fmt fmt;
	char buf[128];

	fmtfdinit(&fmt, 2, buf, sizeof buf);
	va_start(arg, f);
	fmtprint(&fmt, "%s: panic: ", dbgname ? dbgname : argv0);
	fmtvprint(&fmt, f, arg);
	fmtprint(&fmt, "\n");
	va_end(arg);
	fmtfdflush(&fmt);
	abort();
}

static int tlogfd = -2;
static ulong tlogid;

void
tlog(char *f, ...)
{
	char now[32];
	va_list arg;
	Fmt fmt;
	char buf[256];

	while(tlogid == 0)
		tlogid = fastrand();

	if(tlogfd == -2)
		tlogfd = open(trapath("minisync.log"), OWRITE);
	if(tlogfd == -1)
		return;

	seek(tlogfd, 0, 2);
	fmtfdinit(&fmt, tlogfd, buf, sizeof buf);
	va_start(arg, f);
	strcpy(now, sysctime(time(0)));
	now[strlen(now)-1] = '\0';
	fmtprint(&fmt, "%s %lux ", now, tlogid);
	fmtvprint(&fmt, f, arg);
	va_end(arg);
	fmtfdflush(&fmt);
}

vlong
VLONG(uchar *p)
{
	int i;
	vlong v;

	v = 0;
	for(i=0; i<8; i++){
		v <<= 8;
		v |= p[i];
	}
	return v;
}

void
PVLONG(uchar *p, vlong v)
{
	int i;

	for(i=7; i>=0; i--){
		p[i] = v;
		v >>= 8;
	}
}

void
spawn(void (*fn)(void*), void *arg)
{
	threadcreate(fn, arg, STACK);
}

Path*
strtopath(char *s)
{
	char *f[20];
	int i, nf;
	Path *p;

	nf = getfields(s, f, nelem(f), 1, "/");
	p = nil;
	for(i=0; i<nf; i++)
		p = mkpath(p, f[i]);
	return p;
}

#ifndef PLAN9
long
lrand(void)
{
	return rand()^(rand()<<16);
}

#endif

void
threadstate(char *fmt, ...)
{
}

void
initfmt(void)
{
	quotefmtinstall();
	fmtinstall('H', encodefmt);
	fmtinstall('P', pathfmt);
	fmtinstall('R', rpcfmt);
	fmtinstall('$', statfmt);
	fmtinstall('V', vtimefmt);
}
