#ifndef __READ_VAR_H__
#define __READ_VAR_H__

#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>
#include <string.h>
#include <netcdf.h>
#include <stdint.h>
#include <assert.h>

int read_var_int(int ncid, int varid, int* data, size_t* dimlens);
int read_var_float(int ncid, int varid, float* data, size_t* dimlens);
int read_var_double(int ncid, int varid, double* data, size_t* dimlens);
int read_var_char(int ncid, int varid, char* data, size_t* dimlens);

#ifdef __cplusplus
}
#endif

#endif // __READ_VAR_H__