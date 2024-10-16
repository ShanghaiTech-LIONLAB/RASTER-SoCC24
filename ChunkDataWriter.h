#ifndef __WRITE_VAR_H__
#define __WRITE_VAR_H__
#include <netcdf.h>
#include <assert.h>

#ifdef __cplusplus
extern "C" {
#endif

int write_var_int(int ncid, int var_grp_id, const int* data, size_t* data_shape);
int write_var_char(int ncid, int var_grp_id, const char* data, size_t* data_shape);
int write_var_double(int ncid, int var_grp_id, const double* data, size_t* data_shape);
int write_var_float(int ncid, int var_grp_id, const float* data, size_t* data_shape);

#ifdef __cplusplus
}
#endif

#endif