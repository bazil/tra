#include "tra.h"

Replica *repl;

void
usage(void)
{
	fprint(2, "usage: trascan replica [path...]\n");
	exits("usage", 1);
}

void
threadmain(int argc, char **argv)
{
	int i;
	Path *p;

	fmtinstall('H', encodefmt);
	fmtinstall('P', pathfmt);
	fmtinstall('R', rpcfmt);
	fmtinstall('$', statfmt);
	fmtinstall('V', vtimefmt);

	ARGBEGIN{
	case 'D':
		debug |= dbglevel(EARGF(usage()));
		break;
	case 'V':
		traversion();
	default:
		usage();
	}ARGEND

	if(argc < 1)
		usage();

	startclient();
	repl = dialreplica(argv[0]);

	argc--;
	argv++;
	fprint(2, "scan...");
	if(argc == 0){
		fprint(2, "%P...", nil);
		if(rpcstat(repl, nil) == nil)
			sysfatal("rpcstat: %s", rpcerror());
	}else{
		for(i=0; i<argc; i++){
			p = strtopath(argv[i]);
			fprint(2, "%P...", p);
			if(rpcstat(repl, p) == nil)
				sysfatal("rpcstat: %s", rpcerror());
		}
	}
	fprint(2, "hangup...");
	rpchangup(repl);
	fprint(2, "close...");
	replclose(repl);
	fprint(2, "exit\n");
	exits(nil, 0);
}

