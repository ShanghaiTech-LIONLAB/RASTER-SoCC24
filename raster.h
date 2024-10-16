#ifndef __NC_REGION_H__
#define __NC_REGION_H__
#ifdef __cplusplus
extern "C" {
#endif

#include <netcdf.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "config.h"

int get_var_dimlens(int ncid, int varid, int* ndims, size_t* dimlens);
int raster_def_var_chunking(int ncid, int varid, int* mask);
int raster_def_var(int ncid, const char* name, nc_type xtype, int ndims, int* dimidp, int* varidp);

int raster_inq_varid(int ncid, const char* varname, int* varidp);
int raster_inq_varndims(int ncid, int varid, int* ndimsp);
int raster_inq_vardimid(int ncid, int varid, int* dimidsp);

int raster_put_var_int(int ncid, int varid, const int* data);
int raster_put_var_float(int ncid, int varid, const float* data);
int raster_put_var_double(int ncid, int varid, const double* data);
int raster_put_var_char(int ncid, int varid, const char* data);

int raster_get_region_int(int ncid, int varid, int maskid, int* data);
int raster_get_region_float(int ncid, int varid, int maskid, float* data);
int raster_get_region_double(int ncid, int varid, int maskid, double* data);
int raster_get_region_char(int ncid, int varid, int maskid, char* data);

int raster_get_var_int(int ncid, int varid, int* data);
int raster_get_var_float(int ncid, int varid, float* data);
int raster_get_var_double(int ncid, int varid, double* data);
int raster_get_var_char(int ncid, int varid, char* data);

int raster_get_vara_int(int ncid, int varid, size_t* startp, size_t* countp, int* data);
int raster_get_vara_float(int ncid, int varid, size_t* startp, size_t* countp, float* data);
int raster_get_vara_double(int ncid, int varid, size_t* startp, size_t* countp, double* data);
int raster_get_vara_char(int ncid, int varid, size_t* startp, size_t* countp, char* data);

#ifdef __cplusplus
}
#endif
#endif