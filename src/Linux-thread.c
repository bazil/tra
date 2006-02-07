#include <u.h>
#include <sys/param.h>
#include <time.h>
#include <sys/time.h>
#include <signal.h>
#include "tra.h"

char**
doauto(char *name)
{
	static char *argv[10], **argp;
	char *gnot;

	argp = argv;
	gnot = sysname();
	if(strcmp(name, "localhost") != 0
	&& strcmp(name, "local") != 0
	&& (gnot==nil || strcmp(name, gnot) != 0)){
		*argp++ = "ssh";
		*argp++ = "-x";
		*argp++ = "-C";
		*argp++ = name;
	}
	*argp++ = "trasrv";
	*argp++ = "-a";
	*argp++ = nil;
	return argv;
}

#undef pipe
Replica*
_dialreplica(char *name)
{
	char *bin, **argv, err[ERRMAX];
	int p[2], q[2], i;
	Replica *r;
	
	bin = trapath(name);
	argv = nil;
	if(access(bin, AEXEC) < 0){
		rerrstr(err, sizeof err);
		argv = doauto(name);
	}

	if(pipe(p) < 0 || pipe(q) < 0)
		sysfatal("pipe: %r");

	signal(SIGCHLD, SIG_IGN);
	switch(fork()){
	case -1:
		sysfatal("rfork dialreplica: %r");

	case 0:
		close(p[0]);
		close(q[1]);
		dup2(q[0], 0);
		dup2(p[1], 1);
		/*
		 * we're just sloppy; it's hard to fix.
		 * oh wait.  a hammer.
		 */
		for(i=3; i<20; i++)
			close(i);
		if(argv)
			execvp(argv[0], argv);
		else
			execl(bin, name, nil);
		sysfatal("exec %s: %r", argv ? argv[0] : bin);

	default:
		close(q[0]);
		close(p[1]);
		break;
	}
	dbg(DbgFdbuf, "replica %s: %d %d\n", name, p[0], q[1]);
	r = fd2replica(p[0], q[1]);
	if(r)
		replmuxinit(r);
	return r;
}

