#include "tra.h"

static void encode(uchar*, u32int*, ulong);

void
_sha1block(uchar *p, ulong len, u32int *s)
{
	u32int a, b, c, d, e, x;
	uchar *end;
	u32int *wp, *wend;
	u32int w[80];

	/* at this point, we have a multiple of 64 bytes */
	for(end = p+len; p < end;){
		a = s[0];
		b = s[1];
		c = s[2];
		d = s[3];
		e = s[4];

		wend = w + 15;
		for(wp = w; wp < wend; wp += 5){
			wp[0] = (p[0]<<24) | (p[1]<<16) | (p[2]<<8) | p[3];
			e += ((a<<5) | (a>>27)) + wp[0];
			e += 0x5a827999 + (((c^d)&b)^d);
			b = (b<<30)|(b>>2);

			wp[1] = (p[4]<<24) | (p[5]<<16) | (p[6]<<8) | p[7];
			d += ((e<<5) | (e>>27)) + wp[1];
			d += 0x5a827999 + (((b^c)&a)^c);
			a = (a<<30)|(a>>2);

			wp[2] = (p[8]<<24) | (p[9]<<16) | (p[10]<<8) | p[11];
			c += ((d<<5) | (d>>27)) + wp[2];
			c += 0x5a827999 + (((a^b)&e)^b);
			e = (e<<30)|(e>>2);

			wp[3] = (p[12]<<24) | (p[13]<<16) | (p[14]<<8) | p[15];
			b += ((c<<5) | (c>>27)) + wp[3];
			b += 0x5a827999 + (((e^a)&d)^a);
			d = (d<<30)|(d>>2);

			wp[4] = (p[16]<<24) | (p[17]<<16) | (p[18]<<8) | p[19];
			a += ((b<<5) | (b>>27)) + wp[4];
			a += 0x5a827999 + (((d^e)&c)^e);
			c = (c<<30)|(c>>2);
			
			p += 20;
		}

		wp[0] = (p[0]<<24) | (p[1]<<16) | (p[2]<<8) | p[3];
		e += ((a<<5) | (a>>27)) + wp[0];
		e += 0x5a827999 + (((c^d)&b)^d);
		b = (b<<30)|(b>>2);

		x = wp[-2] ^ wp[-7] ^ wp[-13] ^ wp[-15];
		wp[1] = (x<<1) | (x>>31);
		d += ((e<<5) | (e>>27)) + wp[1];
		d += 0x5a827999 + (((b^c)&a)^c);
		a = (a<<30)|(a>>2);

		x = wp[-1] ^ wp[-6] ^ wp[-12] ^ wp[-14];
		wp[2] = (x<<1) | (x>>31);
		c += ((d<<5) | (d>>27)) + wp[2];
		c += 0x5a827999 + (((a^b)&e)^b);
		e = (e<<30)|(e>>2);

		x = wp[0] ^ wp[-5] ^ wp[-11] ^ wp[-13];
		wp[3] = (x<<1) | (x>>31);
		b += ((c<<5) | (c>>27)) + wp[3];
		b += 0x5a827999 + (((e^a)&d)^a);
		d = (d<<30)|(d>>2);

		x = wp[1] ^ wp[-4] ^ wp[-10] ^ wp[-12];
		wp[4] = (x<<1) | (x>>31);
		a += ((b<<5) | (b>>27)) + wp[4];
		a += 0x5a827999 + (((d^e)&c)^e);
		c = (c<<30)|(c>>2);

		wp += 5;
		p += 4;

		wend = w + 40;
		for(; wp < wend; wp += 5){
			x = wp[-3] ^ wp[-8] ^ wp[-14] ^ wp[-16];
			wp[0] = (x<<1) | (x>>31);
			e += ((a<<5) | (a>>27)) + wp[0];
			e += 0x6ed9eba1 + (b^c^d);
			b = (b<<30)|(b>>2);

			x = wp[-2] ^ wp[-7] ^ wp[-13] ^ wp[-15];
			wp[1] = (x<<1) | (x>>31);
			d += ((e<<5) | (e>>27)) + wp[1];
			d += 0x6ed9eba1 + (a^b^c);
			a = (a<<30)|(a>>2);

			x = wp[-1] ^ wp[-6] ^ wp[-12] ^ wp[-14];
			wp[2] = (x<<1) | (x>>31);
			c += ((d<<5) | (d>>27)) + wp[2];
			c += 0x6ed9eba1 + (e^a^b);
			e = (e<<30)|(e>>2);

			x = wp[0] ^ wp[-5] ^ wp[-11] ^ wp[-13];
			wp[3] = (x<<1) | (x>>31);
			b += ((c<<5) | (c>>27)) + wp[3];
			b += 0x6ed9eba1 + (d^e^a);
			d = (d<<30)|(d>>2);

			x = wp[1] ^ wp[-4] ^ wp[-10] ^ wp[-12];
			wp[4] = (x<<1) | (x>>31);
			a += ((b<<5) | (b>>27)) + wp[4];
			a += 0x6ed9eba1 + (c^d^e);
			c = (c<<30)|(c>>2);
		}

		wend = w + 60;
		for(; wp < wend; wp += 5){
			x = wp[-3] ^ wp[-8] ^ wp[-14] ^ wp[-16];
			wp[0] = (x<<1) | (x>>31);
			e += ((a<<5) | (a>>27)) + wp[0];
			e += 0x8f1bbcdc + ((b&c)|((b|c)&d));
			b = (b<<30)|(b>>2);

			x = wp[-2] ^ wp[-7] ^ wp[-13] ^ wp[-15];
			wp[1] = (x<<1) | (x>>31);
			d += ((e<<5) | (e>>27)) + wp[1];
			d += 0x8f1bbcdc + ((a&b)|((a|b)&c));
			a = (a<<30)|(a>>2);

			x = wp[-1] ^ wp[-6] ^ wp[-12] ^ wp[-14];
			wp[2] = (x<<1) | (x>>31);
			c += ((d<<5) | (d>>27)) + wp[2];
			c += 0x8f1bbcdc + ((e&a)|((e|a)&b));
			e = (e<<30)|(e>>2);

			x = wp[0] ^ wp[-5] ^ wp[-11] ^ wp[-13];
			wp[3] = (x<<1) | (x>>31);
			b += ((c<<5) | (c>>27)) + wp[3];
			b += 0x8f1bbcdc + ((d&e)|((d|e)&a));
			d = (d<<30)|(d>>2);

			x = wp[1] ^ wp[-4] ^ wp[-10] ^ wp[-12];
			wp[4] = (x<<1) | (x>>31);
			a += ((b<<5) | (b>>27)) + wp[4];
			a += 0x8f1bbcdc + ((c&d)|((c|d)&e));
			c = (c<<30)|(c>>2);
		}

		wend = w + 80;
		for(; wp < wend; wp += 5){
			x = wp[-3] ^ wp[-8] ^ wp[-14] ^ wp[-16];
			wp[0] = (x<<1) | (x>>31);
			e += ((a<<5) | (a>>27)) + wp[0];
			e += 0xca62c1d6 + (b^c^d);
			b = (b<<30)|(b>>2);

			x = wp[-2] ^ wp[-7] ^ wp[-13] ^ wp[-15];
			wp[1] = (x<<1) | (x>>31);
			d += ((e<<5) | (e>>27)) + wp[1];
			d += 0xca62c1d6 + (a^b^c);
			a = (a<<30)|(a>>2);

			x = wp[-1] ^ wp[-6] ^ wp[-12] ^ wp[-14];
			wp[2] = (x<<1) | (x>>31);
			c += ((d<<5) | (d>>27)) + wp[2];
			c += 0xca62c1d6 + (e^a^b);
			e = (e<<30)|(e>>2);

			x = wp[0] ^ wp[-5] ^ wp[-11] ^ wp[-13];
			wp[3] = (x<<1) | (x>>31);
			b += ((c<<5) | (c>>27)) + wp[3];
			b += 0xca62c1d6 + (d^e^a);
			d = (d<<30)|(d>>2);

			x = wp[1] ^ wp[-4] ^ wp[-10] ^ wp[-12];
			wp[4] = (x<<1) | (x>>31);
			a += ((b<<5) | (b>>27)) + wp[4];
			a += 0xca62c1d6 + (c^d^e);
			c = (c<<30)|(c>>2);
		}

		/* save state */
		s[0] += a;
		s[1] += b;
		s[2] += c;
		s[3] += d;
		s[4] += e;
	}
}

/*
 *  we require len to be a multiple of 64 for all but
 *  the last call.  There must be room in the input buffer
 *  to pad.
 */
SHA1state*
sha1(uchar *p, ulong len, uchar *digest, SHA1state *s)
{
	uchar buf[128];
	u32int x[16];
	int i;
	uchar *e;

	if(s == nil){
		s = malloc(sizeof(*s));
		if(s == nil)
			return nil;
		memset(s, 0, sizeof(*s));
		s->malloced = 1;
	}

	if(s->seeded == 0){
		/* seed the state, these constants would look nicer big-endian */
		s->state[0] = 0x67452301;
		s->state[1] = 0xefcdab89;
		s->state[2] = 0x98badcfe;
		s->state[3] = 0x10325476;
		s->state[4] = 0xc3d2e1f0;
		s->seeded = 1;
	}

	/* fill out the partial 64 byte block from previous calls */
	if(s->blen){
		i = 64 - s->blen;
		if(len < i)
			i = len;
		memmove(s->buf + s->blen, p, i);
		len -= i;
		s->blen += i;
		p += i;
		if(s->blen == 64){
			_sha1block(s->buf, s->blen, s->state);
			s->len += s->blen;
			s->blen = 0;
		}
	}

	/* do 64 byte blocks */
	i = len & ~0x3f;
	if(i){
		_sha1block(p, i, s->state);
		s->len += i;
		len -= i;
		p += i;
	}

	/* save the left overs if not last call */
	if(digest == 0){
		if(len){
			memmove(s->buf, p, len);
			s->blen += len;
		}
		return s;
	}

	/*
	 *  this is the last time through, pad what's left with 0x80,
	 *  0's, and the input count to create a multiple of 64 bytes
	 */
	if(s->blen){
		p = s->buf;
		len = s->blen;
	}else{
		memmove(buf, p, len);
		p = buf;
	}
	s->len += len;
	e = p + len;
	if(len < 56)
		i = 56 - len;
	else
		i = 120 - len;
	memset(e, 0, i);
	*e = 0x80;
	len += i;

	/* append the count */
	x[0] = s->len>>29;
	x[1] = s->len<<3;
	encode(p+len, x, 8);

	/* digest the last part */
	_sha1block(p, len+8, s->state);
	s->len += len+8;

	/* return result and free state */
	encode(digest, s->state, SHA1dlen);
	if(s->malloced == 1)
		free(s);
	return nil;
}

/*
 *	encodes input (ulong) into output (uchar). Assumes len is
 *	a multiple of 4.
 */
static void
encode(uchar *output, u32int *input, ulong len)
{
	u32int x;
	uchar *e;

	for(e = output + len; output < e;){
		x = *input++;
		*output++ = x >> 24;
		*output++ = x >> 16;
		*output++ = x >> 8;
		*output++ = x;
	}
}
