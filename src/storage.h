typedef struct Datum	Datum;
typedef struct DBlock	DBlock;
typedef struct DMap		DMap;
typedef struct DStore	DStore;
typedef struct Listcache	Listcache;

enum
{
	DDirty = 1<<0,

	DStoreHdrSize	= 8
};

struct Datum
{
	void*	a;
	uint		n;
};

struct DBlock
{
	u32int	addr;
	u32int	flags;
	void*	a;
	u32int	n;

	int		(*close)(DBlock*);
	int		(*free)(DBlock*);
	int		(*flush)(DBlock*);
};

enum
{
	DMapCreate = 1<<0,
	DMapReplace = 1<<1
};

struct DMap
{
	u32int	addr;
	int		(*insert)(DMap*, Datum*, Datum*, int);
	int		(*lookup)(DMap*, Datum*, Datum*);
	int		(*delete)(DMap*, Datum*);
	int		(*deleteall)(DMap*);
	int		(*walk)(DMap*, void (*)(void*, Datum*, Datum*), void*);
	int		(*close)(DMap*);
	int		(*free)(DMap*);
	int		(*flush)(DMap*);
	int		(*isempty)(DMap*);
	void		(*dump)(DMap*, int);	/* debugging */
};

struct DStore
{
	int		hdrsize;
	int		pagesize;
	int		(*flush)(DStore*);
	int		(*close)(DStore*);
	DBlock*	(*alloc)(DStore*, uint);
	DBlock*	(*read)(DStore*, u32int);
	int		(*free)(DStore*);
};

DStore*	createdstore(char*, uint);
DStore*	opendstore(char*);
int		dstoreignorewrites(DStore*);

DMap*	dmaplist(DStore*, u32int, uint);
DMap*	dmaptree(DStore*, u32int, uint);

int		datumcmp(Datum*, Datum*);

Listcache*	openlistcache(void);
void		flushlistcache(Listcache*);
void		closelistcache(Listcache*);
DMap*	dmapclist(Listcache*, DStore*, u32int, uint);

#define LONG(p)	(((p)[0]<<24)|((p)[1]<<16)|((p)[2]<<8)|((p)[3]))
#define PLONG(p, l) \
	(((p)[0]=(l)>>24),((p)[1]=(l)>>16),\
	 ((p)[2]=(l)>>8),((p)[3]=(l)))
#define SHORT(p) (((p)[0]<<8)|(p)[1])
#define PSHORT(p,l) \
	(((p)[0]=(l)>>8),((p)[1]=(l)))
