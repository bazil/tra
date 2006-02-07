#include "tra.h"

void
usage(void)
{
	fprint(2, "usage: tradump dbfile\n");
	exits("usage");
}

void
main(int argc, char **argv)
{
	Db *db;

	fmtinstall('H', encodefmt);
	fmtinstall('P', pathfmt);
	fmtinstall('$', statfmt);
	fmtinstall('V', vtimefmt);

	ARGBEGIN{
	case 'V':
		traversion();
	default:
		usage();
	}ARGEND

	if(argc != 1)
		usage();

	db = opendb(argv[0]);
	if(db == nil)
		sysfatal("opendb '%s': %r", argv[0]);

	dumpdb(db, 1);
	exits(nil);
}

int
config(char *s)
{
	USED(s);
	return 0;
}
