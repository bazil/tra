#include <u.h>
#include <time.h>
#include <sys/time.h>
#include <libc.h>

#undef ctime

char*
sysctime(long t)
{
	time_t tt;

	tt = t;
	return ctime(&tt);
}
