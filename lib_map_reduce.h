#ifndef lib_map_reduce_h
#define lib_map_reduce_h

#include "lib_map_reduce_types.h"
#include "lib_map_reduce.c"

#define BUF_SIZE 220
#define STORE_BUF_SIZE 110
#define P_SIZE 500

long store_buf[STORE_BUF_SIZE];

void sort(long *, int, int);

#endif
