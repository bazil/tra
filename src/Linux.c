#include <u.h>
#include <sys/param.h>
#include <time.h>
#include <sys/time.h>
#include <signal.h>
#include "tra.h"

char*
trapath(char *name)
{
	char *home;

	if(name[0] == '/'
	|| (name[0] == '.' && name[1] == '/')
	|| (name[0] == '.' && name[2] == '.' && name[3] == '/'))
		return estrdup(name);

	home = getenv("HOME");
	if(home == nil)
		sysfatal("cannot read $HOME");

	return esmprint("%s/.tra/%s", home, name);
}

#undef open

uint DMMASK =
	DMRWXBITS;

int
syscreateexcl(char *name)
{
	int fd;

	fd = open(name, O_RDWR|O_CREAT|O_EXCL/*|O_EXLOCK*/, 0666);
	if(fd < 0)
		return -1;
	return fd;
}

void*
mksig(struct stat *s, uint *np)
{
	uint n;
	uchar *p;
	void *a;

	n = sizeof(s->st_dev)
		+sizeof(s->st_ino)
		+sizeof(s->st_mtime);
	a = emalloc(n);
	*np = n;

	p = a;
	*(dev_t*)p = s->st_dev;
	p += sizeof(s->st_dev);
	*(ino_t*)p = s->st_ino;
	p += sizeof(s->st_ino);
	*(time_t*)p = s->st_mtime;
	p += sizeof(s->st_mtime);
	USED(p);

	return a;
}

ulong
modeflags2mode(ulong m, ulong f)
{
	ulong tra;

	tra = m&0777;
	if((m&S_IFMT) == S_IFDIR)
		tra |= DMDIR;

	/* BUG: should detect the chattr attributes */

	return tra;
}

void
mode2modeflags(ulong tra, ulong *m, ulong *f)
{
	*m = tra&0777;
	*f = 0;

	/* BUG: should detect the chattr attributes */
}


ulong
stat2mode(char *tpath, struct stat *st)
{
	USED(tpath);
	return modeflags2mode(st->st_mode, 0);  /* XXX: no flags */
}

ulong
trasetmode(char *tpath, ulong o, ulong n)
{
	ulong om, of, nm, nf;

	mode2modeflags(o, &om, &of);
	mode2modeflags(n, &nm, &nf);

	if(om != nm){
		if(chmod(tpath, nm) >= 0)
			om = nm;
		else
			fprint(2, "warning: chmod %s %o: %r\n",
				tpath, nm);
	}
	assert(of == nf);		/* XXX: no flags */
	return modeflags2mode(om, of);
}
