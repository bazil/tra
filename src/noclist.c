#include "tra.h"

void
closelistcache(Listcache *lc)
{
	USED(lc);
}

void
flushlistcache(Listcache *lc)
{
	USED(lc);
}

DMap*
dmapclist(Listcache *lc, DStore *s, u32int addr, uint size)
{
	USED(lc);
	return dmaplist(s, addr, size);
}

Listcache*
openlistcache(void)
{
	return (Listcache*)~0;
}
