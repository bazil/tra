#include <u.h>
#include <sys/stat.h>
#include <dirent.h>
#include <sys/time.h>
#include <errno.h>
#include <time.h>
#include <pwd.h>
#include <grp.h>
#include <stdio.h>	/* for _remove_, of course */
#include "tra.h"

Strcache uidcache;
Strcache gidcache;

char*
sysctime(long t)
{
	return ctime(t);
}

void
nonotes(void)
{
	setsid();
}

long
writen(int fd, void *p, long n)
{
	int m, k = 0;
	uchar *buf = p;

	while(n > 0){
		if((m = write(fd, buf, n)) < 0){
			if(errno == EINTR)
				continue;
			break;
		}
		buf += m, n -= m, k += m;
	}

	return k;
}

int
tramkwriteable(Fid *fid, char *tpath)
{
	struct stat sb;

	if(stat(tpath, &sb) < 0)
		return -1;
	if(chmod(tpath, sb.st_mode | S_IWUSR | S_IWGRP | S_IWOTH) < 0)
		return -1;
	fid->omode[0] = sb.st_mode;
	return 0;
}

int
sysopen(Fid *fid, char *file, int mode)
{
	int fd;
	char *tbuf;
	static char *tmp;
	struct stat st;

	if(lstat(file, &st) >= 0 && (st.st_mode&S_IFMT) == S_IFLNK){
		werrstr("will not touch symbolic links");
		return -1;
	}
	switch(mode){
	default:
		abort();
	case 'r':
		if((fd = open(file, OREAD)) < 0)
			return -1;
		fid->fd = fd;
		break;
	case 'w':
		fid->omode[0] = ~0;
		if(access(file, 0) >= 0 && access(file, 2) < 0)
			if(!config("mkwriteable") || tramkwriteable(fid, file) < 0)
				return -1;
		if(tmp == nil)
			tmp = getenv("TMP");
		if(tmp == nil || tmp[0]=='\0')
			tmp = "/var/tmp";
		tbuf = esmprint("%s/traXXXXXXXXX", tmp);
		if((fd = mkstemp(tbuf)) < 0){
			free(tbuf);
			return -1;
		}
		sysremove(tbuf);
		free(tbuf);
		fid->fd = fd;
		break;
	}
	return 0;
}


int
syswrite(Fid *fid, void *a, int n)
{
	return write(fid->fd, a, n);
}

int
sysread(Fid *fid, void *a, int n)
{
	return read(fid->fd, a, n);
}

int
sysseek(Fid *fid, vlong off)
{
	return lseek(fid->fd, off, 0) == off ? 0 : -1;
}

int
sysremove(char *tpath)
{
	struct stat st;

	if(lstat(tpath, &st) >= 0 && (st.st_mode&S_IFMT) == S_IFLNK){
		werrstr("will not touch symbolic links");
		return -1;
	}
	return remove(tpath);
}

int
sysclose(Fid *fid)
{
	close(fid->fd);
	fid->fd = -1;
	return 0;
}

int
syskids(char *tpath, Sysstat ***pk, Sysstat *ss)
{
	int n;
	Sysstat **k;
	struct stat st;
	struct dirent *de;
	char *s;
	int sl;
	DIR *d;

	USED(ss);

	*pk = nil;
	if((d = opendir(tpath)) == nil)
		return -1;

	n = 0;
	k = nil;
	sl = 0;
	s = nil;
	while((de = readdir(d)) != nil){
		/* skip . and .. */
		if(de->d_name[0]=='.' &&
		   (de->d_name[1]=='\0' ||
		    (de->d_name[1]=='.' && de->d_name[2]=='\0')))
			continue;
		if(strlen(de->d_name)+strlen(tpath) >= sl){
			s = erealloc(s, strlen(de->d_name)+strlen(tpath)+20);
			sl = strlen(de->d_name)+strlen(tpath)+10;
		}
		strcpy(s, tpath);
		strcat(s, "/");
		strcat(s, de->d_name);
		if(lstat(s, &st) >= 0 && (st.st_mode&S_IFMT) == S_IFLNK)
			continue;
		if(n%32==0)
			k = erealloc(k, (n+32)*sizeof(k[0]));
		k[n] = emalloc(sizeof(Sysstat));
#undef estrdup
		k[n]->name = estrdup(de->d_name);
#define estrdup atom
		n++;
	}
	free(s);
	closedir(d);
	*pk = k;
	return n;
}

void
freesysstatlist(Sysstat **k, int nk)
{
	int i;
	if(nk <= 0)
		return;
	for(i=0; i<nk; i++){
		free(k[i]->name);
		free(k[i]);
	}
	free(k);
}

int
syscommit(Fid *fid)
{
	int n, wfd;
	char *buf;

	if((wfd = creat(fid->tpath, 0666)) < 0)
		return -1;

	buf = emallocnz(IOCHUNK);
	if(seek(fid->fd, 0, 0) < 0)
		abort();
	while((n = read(fid->fd, buf, IOCHUNK)) > 0){
		if(writen(wfd, buf, n) != n)
			abort();	/* BUG: handle this better */
	}
	if(fid->omode[0] != ~0){
		if(chmod(fid->tpath, fid->omode[0]) < 0)
			fprint(2, "warning: cannot chmod %s back to %luo\n", fid->tpath, fid->omode[0]);
	}
	close(wfd);
	close(fid->fd);
	fid->fd = -1;
	free(buf);
	return 0;
}

int
sysmkdir(char *tpath, Stat *s)
{
	return mkdir(tpath, 0777);
}

int
shafile(uchar d[20], char *file)
{
	int fd, n;
	uchar *buf;
	DigestState *s;

	memset(d, 0, 20);

	if((fd = open(file, OREAD)) < 0)
		return -1;

	buf = emallocnz(IOCHUNK);
	s = nil;
	while((n = read(fd, buf, IOCHUNK)) > 0)
		s = sha1(buf, n, nil, s);
	close(fd);
	free(buf);
	if(s)
		sha1(nil, 0, d, s);
	return 0;
}

char*
gid2str(gid_t gid)
{
	char *s;
	struct group *g;

	if((s = strcachebyid(&gidcache, gid)) != nil)
		return s;

	g = getgrgid(gid);
	if(g == nil)
		s = "";
	else
		s = g->gr_name;
	s = atom(s);
	strcache(&gidcache, s, gid);
	return s;
}

char*
uid2str(uid_t uid)
{
	char *s;
	struct passwd *p;

	if((s = strcachebyid(&uidcache, uid)) != nil)
		return s;

	p = getpwuid(uid);
	if(p == nil)
		s = "";
	else
		s = p->pw_name;
	s = atom(s);
	strcache(&uidcache, s, uid);
	return s;
}

gid_t
str2gid(char *s)
{
	int id;
	struct group *g;

	if(strcachebystr(&gidcache, s, &id) >= 0)
		return id;
	
	g = getgrnam(s);
	if(g == nil)
		id = -1;
	else
		id = g->gr_gid;
	strcache(&gidcache, s, id);
	return id;
}

uid_t
str2uid(char *s)
{
	int id;
	struct passwd *p;

	if(strcachebystr(&uidcache, s, &id) >= 0)
		return id;
	
	p = getpwnam(s);
	if(p == nil)
		id = -1;
	else
		id = p->pw_uid;
	strcache(&uidcache, s, id);
	return id;
}

/*
 * Update stat structure s with information about path.
 * Return true if the exported info changes.
 * If recordchanges==0, don't touch the exported info;
 * just update the local stuff.  s->state always changes
 * for consistency.
 */
int
sysstat(char *tpath, Stat *s, int recordchanges, Sysstat *ss)
{
	char *duid, *dgid, *dmuid;
	int changed, contentschanged, nstate;
	ulong dmode, p;
	uchar sha[20];
	struct stat d;
	Datum dqid;

	USED(ss);
	if(stat(tpath, &d) < 0){
		if(s->state != SNonexistent){
			if(!recordchanges)
				abort();
			sysstatnotedelete(s);
			return 1;
		}
		return 0;
	}

	changed = 0;
	dmode = stat2mode(tpath, &d);
	duid = uid2str(d.st_uid);
	dgid = gid2str(d.st_gid);
	dmuid = atom("");	/* can be stat2muid(tpath, &d) if we need */

	if(dmode&DMDIR)
		nstate = SDir;
	else
		nstate = SFile;

	if(s->state != nstate){
		s->state = nstate;
		changed = 1;
	}

	/*
	 * metadata changed?
	 *
	 * the general form of these is:
	 *	if(s->localfoo != s->foo){
	 *		s->localfoo = s->foo;	
	 *		if(config("setfoo")){
	 *			s->foo = s->localfoo;
	 *			changed = 1;
	 *		}
	 *	}
	 *
	 * The localfoo variables aren't strictly necessary,
	 * but they're an attempt to deal gracefully with the
	 * case where the configuration changes between syncs.
	 * That is, if we weren't setting uids and then decide to
	 * start setting them, having the localuids keeps us from
	 * thinking that they've all changed all of a sudden.
	 */
	p = s->localmode;
	if(p == ~0)
		p = 0;
	p = (p&~DMMASK) | (dmode&DMMASK);
	if(s->localmode != p){
		s->localmode = p;
		if(s->mode == ~0)
			s->mode = p;
		if(recordchanges && config("setmode")){
			s->mode = s->localmode;
			changed = 1;
		}
	}

	if(s->localuid==nil || strcmp(s->localuid, duid) != 0){
		s->localuid = duid;
		if(recordchanges && config("setuid")){
			s->uid = s->localuid;
			changed = 1;
		}
	}

	if(s->localgid==nil || strcmp(s->localgid, dgid) != 0){
		s->localgid = dgid;
		if(recordchanges && config("setgid")){
			s->gid = s->localgid;
			changed = 1;
		}
	}

	contentschanged = 0;
	if(!(dmode & DMDIR)){
		if(s->localsysmtime != d.st_mtime){
			s->localsysmtime = d.st_mtime;
			if(recordchanges && config("setmtime")){
				s->sysmtime = s->localsysmtime;
				changed = 1;
			}
		}
	
		dqid.a = mksig(&d, &dqid.n);
		if(s->length != d.st_size /* || config("paranoid") */
		|| datumcmp(&dqid, &s->localsig) != 0){
if(0)			fprint(2, "shafile %s length %lud %lud datum %d/%.*H %d/%.*H\n",
				tpath, (ulong)s->length, (ulong)d.st_size,
				(int)s->localsig.n,
				(int)s->localsig.n, s->localsig.a, (int)dqid.n, (int)dqid.n, dqid.a);
			s->localsig = dqid;
			shafile(sha, tpath);
			if(s->length != d.st_size || memcmp(s->sha1, sha, 20) != 0){
				memmove(s->sha1, sha, 20);
				s->length = d.st_size;
				changed = 1;
				contentschanged = 1;
			}
		}
	}

	/*
	 * watch muid, but it doesn't cause a file change.
	 * (there should be an associated contents change.)
	 * we even watch the muid on systems without muids:
	 * if we don't know who made the change, we still need
	 * to record that fact.
	 */

	if(contentschanged){
		s->muid = dmuid;
	}

	return changed;
}

/*
 * Incorporate the stat info in t into s,
 * making changes to the underlying file system
 * metadata as appropriate.
 */
int
syswstat(char *tpath, Stat *s, Stat *t)
{
	int changed, contentschanged, err;
	ulong p;
	struct timeval tm[2];
	uid_t u;
	gid_t g;

	changed = 0;
	if(s->state != t->state){
		werrstr("cannot change from state %d to %d", s->state, t->state);
		return -1;
	}

	/* mode changed? */
	err = 0;
	p = t->mode;
	if(s->mode != p){
		if(config("setmode")){
			p = trasetmode(tpath, s->mode, p);
			s->localmode = (s->mode&~DMMASK) | (p&DMMASK);
		}
		s->mode = p;
		changed = 1;
	}

	/* uid changed? */
	if(nilstrcmp(s->uid, t->uid) != 0){
		if(config("setuid")){
			if((u=str2uid(t->uid))!=(uid_t)-1 
			&& chown(tpath, u, (gid_t)-1)>=0){
				s->localuid = atom(t->uid);
			}
		}
		s->uid = atom(t->uid);
		changed = 1;
	}

	/* gid changed? */
	if(nilstrcmp(s->gid, t->gid) != 0){
		if(config("setgid")){
			if((g=str2gid(t->gid))!=(gid_t)-1
			&& chown(tpath, (uid_t)-1, g)>=0){
				s->localgid = atom(t->gid);
			}
		}
		s->gid = atom(t->gid);
		changed = 1;
	}

	/* mtime changed? */
	if(s->sysmtime != t->sysmtime){
		if(config("setmtime")){
			tm[0].tv_sec = t->sysmtime;
			tm[0].tv_usec = 0;
			tm[1].tv_sec = t->sysmtime;
			tm[1].tv_usec = 0;
			if(utimes(tpath, tm) >= 0)
				s->localsysmtime = t->sysmtime;
		}
		s->sysmtime = t->sysmtime;
		changed = 1;
	}

	/* length changed? (could verify this) */
	contentschanged = 0;
	if(s->length != t->length){
		s->length = t->length;
		contentschanged = 1;
		changed = 1;
	}

	/* sha1 changed? (could verify this) */
	if(memcmp(s->sha1, t->sha1, 20) != 0){
		memmove(s->sha1, t->sha1, 20);
		contentschanged = 1;
		changed = 1;
	}

	if(contentschanged){
		s->muid = atom(t->muid);
	}

	return changed;
}

int
sysaccess(char *path)
{
	return access(path, 0) >= 0;
}

