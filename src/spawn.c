#include "tra.h"
#include <thread.h>

void
spawn(void (*fn)(void*), void *arg)
{
	threadcreate(fn, arg, STACK);
}

