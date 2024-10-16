#ifndef __REGIONAL_READ_H__
#define __REGIONAL_READ_H__
#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>
#include <string.h>
#include <netcdf.h>
#include <stdint.h>
#include <assert.h>

int read_region_int(int ncid, int varid, int* data, size_t* dimlens, int mask_id, int relation_require);
int read_region_float(int ncid, int varid, float* data, size_t* dimlens, int mask_id, int relation_required);
int read_region_double(int ncid, int varid, double* data, size_t* dimlens, int mask_id, int relation_required);
int read_region_char(int ncid, int varid, char* data, size_t* dimlens, int mask_id, int relation_required);

#ifdef __cplusplus
}
#endif
#endif