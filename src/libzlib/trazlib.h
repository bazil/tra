typedef struct Flate Flate;

int deflateblock(Flate*, uchar*, int, uchar*, int);
void deflateclose(Flate*);
Flate *deflateinit(int);
int inflateblock(Flate*, uchar*, int, uchar*, int);
void inflateclose(Flate*);
Flate *inflateinit(void);
