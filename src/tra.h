#include <u.h>
#include <sys/stat.h>
#include <libc.h>
#include <bio.h>
#include <libsec.h>
#include <mux.h>
#include <thread.h>
#include "storage.h"
#include "libzlib/trazlib.h"

/*
 * For compatibility, the Unix port of the Plan 9 libraries
 * save the signal mask, so that you can longjmp from
 * inside a note handler.  (Arguably this should be done
 * by having notejmp zero the signal mask rather than
 * requiring setjmp to save it.)  In any case, this makes
 * setjmp much cheaper.  We setjmp a lot. 
 */
#undef p9setjmp
#define p9setjmp(b)	sigsetjmp((void*)(b), 0)

ulong	stat2mode(char*, struct stat*);
ulong	trasetmode(char*, ulong, ulong);
void*	mksig(struct stat*, uint*);
long		writen(int, void*, long);
extern	uint	DMMASK;
typedef	intptr_t intptr;

enum
{
	STACK = 32768,
	SyncThreads = 100,
	WorkThreads = 100,
};


typedef struct Apath		Apath;
typedef struct Buf		Buf;
typedef struct Client		Client;
typedef struct Db		Db;
typedef struct Fid		Fid;
typedef struct Fd 	Fd;
typedef struct Hash		Hash;
typedef struct Hashlist	Hashlist;
typedef struct Kid		Kid;
typedef struct Link		Link;
typedef struct Ltime		Ltime;
typedef struct Path		Path;
typedef struct Replica	Replica;
typedef struct Rpc		Rpc;
typedef struct Stat		Stat;
typedef struct Str		Str;
typedef struct Strcache	Strcache;
typedef struct Sync		Sync;
typedef struct Syncpath	Syncpath;
typedef struct Sysstat	Sysstat;
typedef struct Queue	Queue;
typedef struct Vtime		Vtime;

enum
{
	/* must be < 64k */
	IOCHUNK = 48*1024,

	/*
	 * we want to avoid interpreting text as a size
	 * and then trying to allocate.  any text will have
	 * something in the top byte set.
	 * this is a gigabyte, which should be plenty.
	 */
	MAXPACKET = 0x00FFFFFF,
};

/*
 * Debugging levels.
 * keep in sync with util.c:/^dbglevel
 */
enum
{
	/* DON'T CHANGE -- USED ON WIRE */
	DbgCoverage = 1<<0,
	DbgSync = 1<<1,
	DbgWork = 1<<2,
	DbgRpc = 1<<3,
	DbgIgnore = 1<<4,
	DbgGhost = 1<<5,
	DbgDb = 1<<6,
	DbgCache = 1<<7,
	DbgError = 0
};

struct Apath
{
	char **e;
	int n;
};

/*
 * r/w buffers
 */
enum
{
	/* errors for longjmp to buf.jmp */
	BufSpace = 1,	/* out of space */
	BufData		/* bad data format */
};
struct Buf
{
	uchar *p;
	uchar *ep;
	jmp_buf jmp;
};

/*
 * string <-> id maps
 */
struct Str
{
	char *s;
	uint id;
};
struct Strcache
{
	int n;
	Str cache[4093];
};

struct Db
{
	u32int addr;
	uint pagesize;
	DStore *s;
	DMap *root;
	DMap *meta;
	Strcache strcache;
	DMap *strtoid;
	DMap *idtostr;
	Stat *rootstat;
	DBlock *super;
	DBlock *rootstatblock;
	int rootstatdirty;
	Buf *logbuf;
	uchar *logbase;
	int logfd;
	int breakwrite;
	int ignwr;
	int alwaysflush;
	Listcache *listcache;
	Vtime *now;
};

struct Fid 
{
	Apath *ap;
	Fid *next;
	char *tpath;
	int fid;
	int fd;
	ulong omode[2];
	int hoff;
	Hashlist *hashlist;
	int hsort;
	Fid *rfid;
};

struct Hash
{
	vlong off;
	int n;
	uchar sha1[SHA1dlen];
};

struct Hashlist
{
	Hash *h;
	int nh;
	vlong tot;
};

/*
 * children of a given path; returned by rpckids
 */
struct Kid
{
	char *name;
	Stat *stat;
	ulong addr;	/* not transmitted over wire; only used inside db.c */
};

/* for free lists */
struct Link
{
	Link *link;
};

/*
 * Local ``time'' on one system.
 */
struct Ltime
{
	ulong t;		/* local event counting clock */
	ulong wall;	/* local wall clock (debugging and error messages only) */
	char *m;		/* machine name */
};

/*
 * a file path, broken into components.
 * the root directory has up==nil.
 */
struct Path
{
	int ref;
	Path *up;
	char *s;
};

struct Replica
{
	char *name;
	char *sysname;
	Fd *rfd;
	Fd *wfd;
	Flate *inflate;
	Flate *deflate;
	char *err;
	Mux mux;
	QLock rlock;
	QLock wlock;
};

enum
{	/* Rpc.type */
	/* DON'T CHANGE -- USED ON WIRE */
	Taddtime,
	Raddtime,
	Tclose,
	Rclose,
	Tcommit,
	Rcommit,
	Thangup,
	Rhangup,
	Tkids,
	Rkids,
	Tmkdir,
	Rmkdir,
	Topen,
	Ropen,
	Tread,
	Rread,
	Treadonly,
	Rreadonly,
	Tremove,
	Rremove,
	Tstat,
	Rstat,
	Twrite,
	Rwrite,
	Twstat,
	Rwstat,
	Terror,
	Rerror,
	Tdebug,
	Rdebug,
	Tflate,
	Rflate,
	Tmeta,
	Rmeta,
	Treadhash,
	Rreadhash,
	Twritehash,
	Rwritehash,
	Tseek,
	Rseek,
	NRpc
};
struct Rpc
{
	int type;
	int tag;
	Path *p;
	Vtime *st;
	Vtime *mt;
	int fd;
	Kid *k;
	int nk;
	Stat *s;
	char omode;
	char *str;
	char *err;
	void *a;
	long n;
	vlong vn;
};

/*
 * channels with arbitrary buffering.
 * right now we only use them for Syncpath
 * structures, so we put a next pointer in
 * the Syncpath structure.
 */
struct Queue
{
	Syncpath *s;
	Syncpath **es;
	int isstack;
	QLock lk;
	Rendez send, recv;
	void (*printsend)(Syncpath*);
	void (*printrecv)(Syncpath*);
	int n;
	int m;
	int max;
	char *name;
};

/*
 * File metadata.
 */
enum	/* ON-DISK: DON'T CHANGE */
{	/* Stat.state */
	SUnknown,
	SWasNonreplicated,
	SNonexistent,			/* interested, but doesn't exist */
	SFile,				/* is a file */
	SDir,					/* is a directory */
	SNonreplicated = 1<<7		/* not interested in this file*/
};

#undef DMSETGID
#undef DMSETUID
/* Stat.mode */
#define DMRWXBITS	000000000777	/* read, write, execute: Plan 9, Unix*/
#define DMSTICKY	000000001000	/* Unix */
#define DMSETGID	000000002000	/* Unix */
#define DMSETUID	000000004000	/* Unix */
#define DMARCHIVE	000000010000	/* MS-DOS */
#define DMSYSTEM	000000020000	/* MS-DOS */
#define DMHIDDEN	000000040000	/* MS-DOS */
#define DMOPAQUE	000000100000	/* FreeBSD? */
#define DMNODUMP	000000200000	/* FreeBSD? */
#define DMSAPPEND	000000400000	/* system append-only: FreeBSD */
#define DMSIMMUTABLE	000001000000	/* system immutable: FreeBSD */
#define DMIMMUTABLE	000002000000	/* (user) immutable: FreeBSD */
#define DMNOREMOVE	000004000000	/* unremovable: FreeBSD */
	/* UNUSED	000010000000 */
	/* UNUSED	000020000000 */
	/* UNUSED	000040000000 */
	/* UNUSED	000100000000 */
	/* UNUSED	000200000000 */
	/* UNUSED	000400000000 */
	/* UNUSED	001000000000 */
	/* UNUSED	002000000000 */
/* #define DMEXCL		004000000000	*//* exclusive use: Plan 9 */
/* #define DMAPPEND	010000000000	*//* (user) append-only: Plan 9, FreeBSD */
/* #define DMDIR	020000000000		*//* is a directory */

struct Stat
{
	int state;			/* enum above */
	Vtime *synctime;		/* see paper */
	Vtime *mtime;
	Vtime *ctime;

	ulong mode;			/* enum above */
	char *uid;			/* owner */
	char *gid;			/* group */
	char *muid;			/* last writer of file */
	ulong sysmtime;			/* system modification time */

	vlong length;			/* length of file; 0 for directories */
	uchar sha1[20];			/* hash of file contents */

	/* local use only */
	Datum localsig;			/* some quick signature on contents */
	ulong localmode;
	char *localuid;
	char *localgid;
	char *localmuid;
	ulong localsysmtime;
};

struct Sysstat
{
	char *name;
	struct stat st;
};

struct Sync
{
	Replica *ra;
	Replica *rb;
	Queue *syncq;
	Queue *workq;
	Queue *finishq;
	Queue *triageq;
	int oneway;
};

enum
{	/* Syncpath.state */
	SyncStart,
	SyncStat,
	SyncTriage,
	SyncAct,
	SyncDone,
	SyncError,
	SyncKids,
};
enum
{	/* Syncpath triage, action */
	/* to find the opposite action, xor with 1 */
	/* backwards actions (B->A) are even, forward actions (A->B) are odd */
	DoNothing = 0,
	DoNothing1,
	DoCopyBtoA = 2,
	DoCopyAtoB,
	DoCreateA = 4,
	DoRemoveB,
	DoRemoveA = 6,
	DoCreateB,
	DoKids = 8,
	DoKids1,
};
struct Syncpath
{
	char *err;
	int tag;
	int conflict;
	int state;
	int triage;
	int action;
	int finishstate;
	struct {
		int complete;
		Stat *s;
	} a, b;
	Path *p;

/* not on wire */
	Sync *sync;
	Syncpath *parent;
	Syncpath *kid;
	Syncpath *nextq;
	int nkid;
	int npend;
};

/*
 * Vector time across a list of systems.
 */
struct Vtime
{
	Ltime *l;
	int nl;
};

extern	int	debug;
extern	int	mrpc;
extern	int	nrpc;
extern	ulong	start;
extern	char*	dbgname;
extern	int	oneway;

void		_coverage(char*, int);
Hashlist*	addhash(Hashlist*, uchar*, vlong, vlong);
char*	atom(char*);
int		banner(Replica*, char*);
int		clientrpc(Replica*, Rpc*);
int		closedb(Db*);
int		config(char*);
int		convM2R(Buf*, Rpc*);
Buf*		convR2M(Rpc*);
Stat*		copystat(Stat*);
Vtime*		copyvtime(Vtime*);
#define	coverage()	if((debug&DbgCoverage)==0){}else _coverage(__FILE__, __LINE__)
Db*		createdb(char*, int);
int		datumfmt(Fmt*);
int		dbdelmeta(Db*, char*);
int		dbdelstat(Db*, char**, int);
void		dbg(int, char*, ...);
#ifdef PLAN9
#pragma	varargck argpos dbg 2
#endif
int		dbgetkids(Db*, char**, int, Kid**);
char*		dbgetmeta(Db*, char*);
int		dbgetstat(Db*, char**, int, Stat**);
int		dbignorewrites(Db*);
int		dbglevel(char*);
int		dbputmeta(Db*, char*, char*);
int		dbputstat(Db*, char**, int, Stat*);
Replica*	dialreplica(char*);
Replica*	_dialreplica(char*);
void		dumpdb(Db*, int);
void*		emalloc(ulong);
void*		emallocnz(ulong);
void		endclient(void);
void*		erealloc(void*, ulong);
char*		esmprint(char*, ...);
char*		estrdup(char*);
Replica*	fd2replica(int, int);
Hash*		findhash(Hashlist*, uchar*);
Apath*		flattenpath(Path*);
int		flushdb(Db*);
void		flushstrcache(Strcache*);
void		freekids(Kid*, int);
void		freepath(Path*);
void		freestat(Stat*);
void		freesysstatlist(Sysstat**, int);
void		freevtime(Vtime*);
int		getstat(Db*, char**, int, Stat**);
int		hashcmp(const void*, const void*);
Vtime*		_infvtime(int);
int		ignorepath(Apath*);
#define		infvtime()	_infvtime(0)
void		threadstate(char*, ...);
void		initfmt(void);
int		intersectvtime(Vtime*, Vtime*);	/* does a intersect b? */
int		isinfvtime(Vtime*);
int		leqvtime(Vtime*, Vtime*);	/* is a <= b? */
void		loadignore(char*);
void		logflush(Db*);
Vtime*		maxvtime(Vtime*, Vtime*);	/* modifies and returns 1st arg */
char*		metadata(Db*, char*);
Vtime*		minvtime(Vtime*, Vtime*);	/* modifies and returns 1st arg */
Apath*		mkapath(char*);
Buf*		mkbuf(void*, int);
Stat*		mkghoststat(Vtime*);
Hashlist*	mkhashlist(void);
Path*		mkpath(Path*, char*);
Queue*		mkqueue(char*, int);
Path*		mkroot(void);
Queue*		mkstack(char*, int);
Stat*		mkstat(void);
Vtime*		_mkvtime(int);
#define		mkvtime()	_mkvtime(0)
Vtime*		mkvtime1(char*, ulong, ulong);
int		nilstrcmp(char*, char*);
void		nonotes(void);
Db*		opendb(char*);
Fd*		openconsole(void);
void		osinit(void);
void		panic(char*, ...);
int		pathcmp(const void*, const void*);
int		pathfmt(Fmt*);
int		pstringcmp(const void*, const void*);
void		queuesynckids(Syncpath*);
Syncpath*	qrecv(Queue*);
void		qsend(Queue*, Syncpath*);
void*		readbufbytes(Buf*, long);
uchar		readbufc(Buf*);
Datum		readbufdatum(Buf*);
ulong		readbufl(Buf*);
void		readbufltime(Buf*, Ltime*);
Path*		readbufpath(Buf*);
Stat*		readbufstat(Buf*);
char*		readbufstring(Buf*);
char*		readbufstringdup(Buf*);
uvlong		readbufvl(Buf*);
Vtime*		readbufvtime(Buf*);
void		replclose(Replica*);
Buf*		replread(Replica*);
int		replwrite(Replica*, Buf*);
int		replflush(Replica*);
void		resolve(Syncpath*, int);
int		rpcaddtime(Replica*, Path*, Vtime*, Vtime*);
int		rpcclose(Replica*, int);
int		rpccommit(Replica*, int, Stat*);
int		rpcdebug(Replica*, int);
char*		rpcerror(void);
int		rpcfmt(Fmt*);
int		rpcflate(Replica*, int);
int		rpchangup(Replica*);
Hashlist*	rpchashfile(Replica*, int);
int		rpckids(Replica*, Path*, Kid**);
char*		rpcmeta(Replica*, char*);
int		rpcmkdir(Replica*, Path*, Stat*);
int		rpcopen(Replica*, Path*, char);
long		rpcread(Replica*, int, void*, long);
long		rpcreadhash(Replica*, int, void*, long);
long		rpcreadn(Replica*, int, void*, long);
int		rpcreadonly(Replica*, int);
int		rpcremove(Replica*, Path*, Stat*);
int		rpcseek(Replica*, int, vlong);
Stat*		rpcstat(Replica*, Path*);
long		rpcwrite(Replica*, int, void*, long);
long		rpcwritehash(Replica*, int, void*, long);
int		rpcwstat(Replica*, Path*, Stat*);
char*	rsysname(Replica*);
void		run(char*[], int*, int*);
void		spawn(void (*fn)(void*), void *arg);
void		startclient(void);
int		statfmt(Fmt*);
void		strcache(Strcache*, char*, int);
char*	strcachebyid(Strcache*, int);
int		strcachebystr(Strcache*, char*, int*);
char*	stripdot(char*);
Path*	strtopath(char*);
void		synccleanup(Syncpath*);
void		syncfinish(Syncpath*);
int		synckids(Syncpath*);
void		syncstat(Syncpath*);
void		syncthread(void*);
void		synctriage(Syncpath*);
char*		syncpathstate(int);
char*		syncpathaction(int);
int		sysaccess(char*);
int		sysclose(Fid*);
int		syscommit(Fid*);
int		syscreateexcl(char*);
char*		sysctime(long);
void		sysinit(void);
int		syskids(char*, Sysstat***, Sysstat*);
int		sysmkdir(char*, Stat*);
int		sysopen(Fid*, char*, int);
int		sysread(Fid*, void*, int);
int		sysremove(char*);
int		sysseek(Fid*, vlong);
int		sysstat(char*, Stat*, int, Sysstat*);
void		sysstatnotedelete(Stat*);
int		syswrite(Fid*, void*, int);
int		syswstat(char*, Stat*, Stat*);
void		tclose(Fd*);
Fd*		topen(int, int);
void		tramkdb(char*, char*, int, int);
int		tramkwriteable(Fid*, char*);
char*		trapath(char*);
void			traversion(void);
int		tcanread(Fd*);
void		tralog(char*, ...);
int		tread(Fd*, void*, int);
int		treadn(Fd*, void*, int);
int		twrite(Fd*, void*, int);
int		twflush(Fd*);
Vtime*		unmaxvtime(Vtime*, Vtime*);
int		vtimefmt(Fmt*);
void		warn(const char*, ...);
void		workthread(void*);
char*	workstr(Syncpath*, int);
void		writebufbytes(Buf*, void*, long);
void		writebufc(Buf*, uchar);
void		writebufdatum(Buf*, Datum);
void		writebufl(Buf*, ulong);
void		writebufpath(Buf*, Path*);
void		writebufstat(Buf*, Stat*);
void		writebufstring(Buf*, char*);
void		writebufvl(Buf*, uvlong);
void		writebufvtime(Buf*, Vtime*);

vlong		VLONG(uchar*);
void		PVLONG(uchar*, vlong);

#ifdef PLAN9
//#pragma varargck type "D" Datum*
#pragma varargck type "P" Path*
#pragma varargck type "R" Rpc*
#pragma varargck type "V" Vtime*
#pragma varargck type "$" Stat*
#endif

extern int	inrpctot, outrpctot;
extern int	inzrpctot, outzrpctot;
extern int nop;
extern int superquiet;

#undef exits
#define exits(string, number) threadexitsall(string)
#define	chan(x)	chancreate(sizeof(*(x*)0), 0)
