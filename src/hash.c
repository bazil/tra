#include "tra.h"

Hashlist*
mkhashlist(void)
{
	Hashlist *hl;

	hl = emalloc(sizeof(Hashlist));
	hl->nh = 0;
	hl->h = (Hash*)&hl[1];
	return hl;
}

Hashlist*
addhash(Hashlist *hl, uchar *sha1, vlong off, vlong n)
{
	Hash *h;

	if(hl->nh%256 == 0){
		hl = erealloc(hl, sizeof(Hashlist)+(hl->nh+256)*sizeof(Hash));
		hl->h = (Hash*)&hl[1];
	}
	h = &hl->h[hl->nh++];
	memmove(h->sha1, sha1, SHA1dlen);
	h->off = off;
	h->n = n;
	return hl;
}

int
hashcmp(const void *va, const void *vb)
{
	Hash *a, *b;

	a = (Hash*)va;
	b = (Hash*)vb;
	return memcmp(a->sha1, b->sha1, sizeof a->sha1);
}

Hash*
findhash(Hashlist *hl, uchar *p)
{
	int i;
	Hash *h;
	int n;

	if(hl == nil)
		return nil;

	h = hl->h;
	n = hl->nh;

	while(n > 0){
		i = memcmp(p, h[n/2].sha1, SHA1dlen);
		if(i < 0)
			n = n/2;
		else if(i > 0){
			h += n/2+1;
			n -= n/2+1;
		}else
			return &h[n/2];
	}
	return nil;
}

