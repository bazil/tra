#include "tra.h"

void
usage(void)
{
	fprint(2, "usage: tramkdb [-R] [-b blocksize] dbfile sysname\n");
	exits("usage", 1);
}

extern int __flagfmt(Fmt*);
void
threadmain(int argc, char **argv)
{
	int blocksize, norandom;
	char *name, *sysname;
	Db *db;

	initfmt();

	blocksize = 8192;
	norandom = 0;
	ARGBEGIN{
	case 'D':
		debug |= dbglevel(EARGF(usage()));
		break;
	case 'V':
		traversion();
	case 'b':
		blocksize = atoi(EARGF(usage()));
		break;
	case 'R':
		norandom = 1;
		break;
	}ARGEND

	if(argc != 2)
		usage();

	name = argv[0];
	sysname = argv[1];

	if(!norandom)
		sysname = esmprint("%s.%lux", sysname, lrand());

	db = createdb(name, blocksize);
	if(db == nil)
		sysfatal("create db: %r");

	db->rootstat = mkstat();
	db->rootstatdirty = 1;
	db->rootstat->state = SNonexistent;
	db->rootstat->synctime = mkvtime();
	db->rootstat->ctime = mkvtime();
	db->rootstat->mtime = mkvtime();
	dbputmeta(db, "sysname", sysname);
	dbputmeta(db, "now", "0");

	if(closedb(db) < 0)
		sysfatal("dbclose: %r");

	exits(nil, 0);
}

int
config(char *s)
{
	USED(s);
	return 0;
}
