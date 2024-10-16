#ifndef __WRITE_META_H__
#define __WRITE_META_H__
#include <stdlib.h>
#include <netcdf.h>

#ifdef __cplusplus
extern "C" {
#endif

int write_var_metadata(int varid, int ndims, size_t* dimlens, int* mask);

#ifdef __cplusplus
}
#endif

#endif