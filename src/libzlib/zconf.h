/* zconf.h -- configuration of the zlib compression library
 * Copyright (C) 1995-2002 Jean-loup Gailly.
 * For conditions of distribution and use, see copyright notice in zlib.h 
 */

/* @(#) $Id$ */

#ifndef _ZCONF_H
#define _ZCONF_H

/*
 * If you *really* need a unique prefix for all types and library functions,
 * compile with -DZ_PREFIX. The "standard" zlib should be compiled without it.
 */
#ifdef Z_PREFIX
#  define deflateInit_	z_deflateInit_
#  define deflate	z_deflate
#  define deflateEnd	z_deflateEnd
#  define inflateInit_ 	z_inflateInit_
#  define inflate	z_inflate
#  define inflateEnd	z_inflateEnd
#  define deflateInit2_	z_deflateInit2_
#  define deflateSetDictionary z_deflateSetDictionary
#  define deflateCopy	z_deflateCopy
#  define deflateReset	z_deflateReset
#  define deflateParams	z_deflateParams
#  define inflateInit2_	z_inflateInit2_
#  define inflateSetDictionary z_inflateSetDictionary
#  define inflateSync	z_inflateSync
#  define inflateSyncPoint z_inflateSyncPoint
#  define inflateReset	z_inflateReset
#  define compress	z_compress
#  define compress2	z_compress2
#  define uncompress	z_uncompress
#  define adler32	z_adler32
#  define crc32		z_crc32
#  define get_crc_table z_get_crc_table

#  define Byte		z_Byte
#  define uInt		z_uInt
#  define uLong		z_uLong
#  define Bytef	        z_Bytef
#  define charf		z_charf
#  define intf		z_intf
#  define uIntf		z_uIntf
#  define uLongf	z_uLongf
#  define voidpf	z_voidpf
#  define voidp		z_voidp
#endif

#define STDC
#define MAX_MEM_LEVEL	9
#define MAX_WBITS	15	/* 32K LZ77 window */
/* The memory requirements for deflate are (in bytes):
            (1 << (windowBits+2)) +  (1 << (memLevel+9))
 that is: 128K for windowBits=15  +  128K for memLevel = 8  (default values)
 plus a few kilobytes for small objects. For example, if you want to reduce
 the default memory requirements from 256K to 128K, compile with
     make CFLAGS="-O -DMAX_WBITS=14 -DMAX_MEM_LEVEL=7"
 Of course this will generally degrade compression (there's no free lunch).

   The memory requirements for inflate are (in bytes) 1 << windowBits
 that is, 32K for windowBits=15 (default value) plus a few kilobytes
 for small objects.
*/

#define OF(args)	args

#define ZEXPORT
#define ZEXPORTVA
#ifndef ZEXTERN
#  define ZEXTERN extern
#endif

#define FAR

typedef uchar Byte;
typedef uint uInt;
typedef ulong uLong;

typedef uchar FAR Bytef;
typedef char  FAR charf;
typedef int   FAR intf;
typedef uInt  FAR uIntf;
typedef uLong FAR uLongf;

typedef void FAR *voidpf;
typedef void	 *voidp;

#define z_off_t long

#endif /* _ZCONF_H */
