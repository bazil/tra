#include "tra.h"

//static Link *xfreestat;

Stat*
mkstat(void)
{
//	int i;
	Stat *s;

	s = emalloc(sizeof(Stat));
	return s;
/*
	if(xfreestat == nil){
		s = emalloc(64*sizeof(Stat));
		for(i=0; i<64; i++)
			freestat(s+i);
	}

	s = (Stat*)xfreestat;
	xfreestat = xfreestat->link;
	memset(s, 0, sizeof *s);
	return s;
*/
}

Stat*
mkghoststat(Vtime *t)
{
	Stat *s;

	s = mkstat();
	s->synctime = copyvtime(t);
	s->ctime = mkvtime();
	s->mtime = mkvtime();
	s->state = SNonexistent;
	s->mode = ~0;
	return s;
}

void
freestat(Stat *s)
{
	if(s == nil)
		return;

	if(s->uid)
		free(s->uid);
	if(s->gid)
		free(s->gid);
	if(s->muid)
		free(s->muid);
	if(s->localsig.a)
		free(s->localsig.a);
	if(s->localuid)
		free(s->localuid);
	if(s->localgid)
		free(s->localgid);
	if(s->localmuid)
		free(s->localmuid);

	freevtime(s->synctime);
	freevtime(s->mtime);
	freevtime(s->ctime);
	memset(s, 0xD5, sizeof(*s));
	free(s);
}

Stat*
copystat(Stat *s)
{
	Stat *ns;

	if(s == nil)
		return s;
	ns = mkstat();
	*ns = *s;
	if(ns->uid)
		ns->uid = estrdup(ns->uid);
	if(ns->gid)
		ns->gid = estrdup(ns->gid);
	if(ns->muid)
		ns->muid = estrdup(ns->muid);
	ns->localsig.a = emalloc(ns->localsig.n);
	memmove(ns->localsig.a, s->localsig.a, ns->localsig.n);
	if(ns->localuid)
		ns->localuid = estrdup(ns->localuid);
	if(ns->localgid)
		ns->localgid = estrdup(ns->localgid);
	if(ns->localmuid)
		ns->localmuid = estrdup(ns->localmuid);
	if(ns->synctime)
		ns->synctime = copyvtime(ns->synctime);
	if(ns->mtime)
		ns->mtime = copyvtime(ns->mtime);
	if(ns->ctime)
		ns->ctime = copyvtime(ns->ctime);
	return ns;
}

char*
readbufstringatom(Buf *b)
{
	return atom(readbufstring(b));
}

char*
readbufstringdup(Buf *b)
{
	char *s;

	s = readbufstring(b);
	if(s){
		s = estrdup(s);
		setmalloctag(s, getcallerpc(&b));
	}
	return s;
}

/*
 * If you edit this, also edit db.c's copy.
 */
Stat*
readbufstat(Buf *b)
{
	Stat *s;

	if(readbufc(b) != 0)
		longjmp(b->jmp, BufData);

	if(readbufc(b) == 0)
		return nil;

	s = mkstat();
	s->state = readbufl(b);
	s->synctime = readbufvtime(b);
	s->mtime = readbufvtime(b);
	s->ctime = readbufvtime(b);

	s->mode = readbufl(b);
	s->uid = readbufstringdup(b);
	s->gid = readbufstringdup(b);
	s->muid = readbufstringdup(b);
	s->sysmtime = readbufl(b);

	s->length = readbufvl(b);
	memmove(s->sha1, readbufbytes(b, sizeof s->sha1), sizeof s->sha1);

	s->localsig = readbufdatum(b);
	s->localmode = readbufl(b);
	s->localuid = readbufstringdup(b);
	s->localgid = readbufstringdup(b);
	s->localmuid = readbufstringdup(b);
	s->localsysmtime = readbufl(b);

	return s;
}

/*
 * If you edit this, also edit db.c's copy.
 */
void
writebufstat(Buf *b, Stat *s)
{
	writebufc(b, 0);

	if(s == nil){
		writebufc(b, 0);
		return;
	}

	writebufc(b, 1);
	writebufl(b, s->state);
	writebufvtime(b, s->synctime);
	writebufvtime(b, s->mtime);
	writebufvtime(b, s->ctime);

	writebufl(b, s->mode);
	writebufstring(b, s->uid);
	writebufstring(b, s->gid);
	writebufstring(b, s->muid);
	writebufl(b, s->sysmtime);

	writebufvl(b, s->length);
	writebufbytes(b, s->sha1, sizeof s->sha1);

	writebufdatum(b, s->localsig);
	writebufl(b, s->localmode);
	writebufstring(b, s->localuid);
	writebufstring(b, s->localgid);
	writebufstring(b, s->localmuid);
	writebufl(b, s->localsysmtime);
}

static char*
state2str(int s)
{
	static char buf[32];

	switch(s){
	case SDir:
		return "Dir";
	case SFile:
		return "File";
	case SWasNonreplicated:
		return "WasNonreplicated";
	case SNonreplicated|SDir:
		return "NonreplicatedDir";
	case SNonreplicated|SFile:
		return "NonreplicatedFile";
	case SNonexistent:
		return "Nonexistent";
	default:
		sprint(buf, "Unknown(0x%ux)", s);
		return buf;
	}
}

int
statfmt(Fmt *fmt)
{
	Stat *s;

	s = va_arg(fmt->args, Stat*);

	if(s == nil)
		return fmtstrcpy(fmt, "<null>");
	
	return fmtprint(fmt, "%s %V %V %V %luo/%luo %s/%s %s/%s %s/%s %lud/%lud %lld %.20H %.*H",
		state2str(s->state), s->synctime, s->mtime, s->ctime,
		s->mode, s->localmode, s->uid, s->localuid, s->gid, s->localgid,
		s->muid, s->localmuid, s->sysmtime, s->localsysmtime,
		s->length, s->sha1, s->localsig.n, s->localsig.a);
}
