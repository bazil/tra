#include "tra.h"
#include "zlib.h"
#include "zutil.h"

struct Flate
{
	z_stream zs;
};

Flate*
deflateinit(int level)
{
	int err;
	Flate *f;

	f = emalloc(sizeof(Flate));
	if((err = deflateInit(&f->zs, level)) != Z_OK){
		werrstr("%s", z_errmsg[err]);
		free(f);
		return nil;
	}
	return f;
}

void
deflateclose(Flate *f)
{
	deflateEnd(&f->zs);
	free(f);
}

int
deflateblock(Flate *f, uchar *dst, int ndst, uchar *src, int nsrc)
{
	int err;

	f->zs.next_in = src;
	f->zs.avail_in = nsrc;
	f->zs.next_out = dst;
	f->zs.avail_out = ndst;

	err = deflate(&f->zs, Z_SYNC_FLUSH);
	if(err != Z_OK){
		werrstr("%s", z_errmsg[err]);
		return -1;
	}
	return ndst - f->zs.avail_out;
}

Flate*
inflateinit(void)
{
	int err;
	Flate *f;

	f = emalloc(sizeof(Flate));
	if((err = inflateInit(&f->zs)) != Z_OK){
		werrstr("%s", z_errmsg[err]);
		free(f);
		return nil;
	}
	return f;
}

void
inflateclose(Flate *f)
{
	inflateEnd(&f->zs);
	free(f);
}

int
inflateblock(Flate *f, uchar *dst, int ndst, uchar *src, int nsrc)
{
	int err;

	f->zs.next_in = src;
	f->zs.avail_in = nsrc;
	f->zs.next_out = dst;
	f->zs.avail_out = ndst;

	err = inflate(&f->zs, Z_SYNC_FLUSH);
	if(err != Z_OK){
		werrstr("%s", z_errmsg[err]);
		return -1;
	}
	return ndst - f->zs.avail_out;
}

