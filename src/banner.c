#include "tra.h"

/* 
 * The "banner" code is a hack to ensure that a connection is 8-bit clean.
 * We make a particular effort to trip up SSH, since SSH hell is a common
 * source of problems for new users.   The "OK" at the end is a fixed-length
 * write to make sure we avoid buffering problems.
 * 
 * The protocol is:
 *
 *	A->B:  banner
 *	B->A:  \n~?\n~.\n
 *	A->B:  0x00, 0x01, ..., 0xFF
 *
 * We run the protocol in both directions just in case the channel 
 * is 8-bit safe in one direction but not the other.  The trasrv must
 * speak first -- it might be executed over an SSH connection that
 * can't stop printing things (like /etc/motd) during login.  We use
 * the initial banner to synchronize.
 */

static char tracmd[] = "TRACMD TRA XCHG\n";	/* CACM 11(6) 419-422 */
static char antissh[] = "\n~?\n~.\n";
enum
{
	BUFSZ = 2048
};

/*
 * The TTY might be turning \n into \r.  For now, just
 * pretend it's working.  The 8-bit test will fail with a 
 * good error message.
 */
static char*
rdln(Fd *fd)
{
	int i=0;
	static char buf[BUFSZ+1];

	do{
		if(tread(fd, &buf[i], 1) <= 0)
			break;
		if(buf[i] == '\r')
			buf[i] = '\n';
		if(buf[i++] == '\n')
			break;
	}while(i < BUFSZ);

	if(i == 0)
		return nil;

	buf[i] = '\0';
	return buf;
}

static int
readln(Fd *fd, char *expect)
{
	char *p;

	if((p = rdln(fd)) == nil)
		return -1;
	if(strcmp(p, expect) != 0)
		return -1;
	return 0;
}

/*
 * Assumes pipe is buffered at least 257 characters,
 * so both sides can send and then receive.
 */
int
banner(Replica *r, char *name)
{
	int i;
	char c, *p;
	char buf[257];

	dbg(DbgRpc, "sending tracmd\n");
	qlock(&r->rlock);
	qlock(&r->wlock);
	/* both sides send tracmd */
	if(twrite(r->wfd, tracmd, sizeof tracmd-1) < 0
	|| twflush(r->wfd) == -1){
		werrstr("writing banner: %r");
	err:
		qunlock(&r->wlock);
		qunlock(&r->rlock);
		return -1;
	}
	dbg(DbgRpc, "waiting for tracmd\n");
	while((p = rdln(r->rfd)) != nil){
		if(strcmp(tracmd, p) == 0)
			break;
		if(name)
			fprint(2, "%s# %s", name, p);
	}
	if(p == nil){
		werrstr("did not receive initial banner");
		goto err;
	}

	dbg(DbgRpc, "sending antissh\n");
	/* both sides send \n~?\n~.\n */
	if(twrite(r->wfd, antissh, sizeof antissh-1) < 0
	|| twflush(r->wfd) == -1){
		werrstr("writing antissh: %r");
		goto err;
	}
	dbg(DbgRpc, "waiting for antissh\n");
	if(readln(r->rfd, "\n")<0 || readln(r->rfd, "~?\n")<0 || readln(r->rfd, "~.\n") < 0){
		werrstr("corrupt anti-ssh banner");
		goto err;
	}

	dbg(DbgRpc, "sending byte map\n");
	/* both sides send 0x00, 0x01, ..., 0xFF, 0x00 */
	/* the second 0x00 gives us a way to notice 0xFF being dropped */
	for(i=0; i<257; i++)
		buf[i] = i;
	if(twrite(r->wfd, buf, sizeof buf) < 0 || twflush(r->wfd) == -1){
		werrstr("short channel test write: %r");
		goto err;
	}
	
	dbg(DbgRpc, "receiving byte map\n");
	/* could be more efficient but doesn't matter */
	for(i=0; i < sizeof buf; i++){
		if(tread(r->rfd, &c, 1) != 1){
			werrstr("8-bit test: expected 0x%x, got eof", buf[i]);
			goto err;
		}
		if(c != buf[i]){
			werrstr("8-bit test: expected 0x%x, got 0x%x", buf[i], c);
			goto err;
		}
	}
//fprint(2, "%s: pass\n", argv0);
	qunlock(&r->wlock);
	qunlock(&r->rlock);
	return 0;
}

