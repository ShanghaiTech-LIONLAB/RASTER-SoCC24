#include "raster.h"
#include "MetadataHandler.h"
#include "ChunkDataWriter.h"
#include "RegionalRead.h"
#include "ChunkDataReader.h"

int get_var_dimlens(int ncid, int varid, int* ndims, size_t* dimlens)
{
    int status = NC_NOERR, dimids[32];
    status = nc_get_att_int(varid, NC_GLOBAL, "_ndims_", ndims);
    status = raster_inq_vardimid(ncid, varid, dimids);
    for (int i = 0; i < *ndims; i++)
        nc_inq_dimlen(ncid, dimids[i], &dimlens[i]);
    return status;
}

// This function creates a nc_group for this variable.
// (1) Create a group with nc_grp_id
// (2) Record number of dims of this var, as an attribute of the created group
// (3) Record related dimnames, also as an attribute of the group
// (4) If success, return nc_grp_id as new varid
int raster_def_var(int ncid, const char* name, nc_type xtype, int ndims, int* dimidp, int* varidp)
{
    int status = NC_NOERR, var_grp_id;
    status = nc_def_grp(ncid, name, &var_grp_id);
    status = nc_put_att_int(var_grp_id, NC_GLOBAL, "_ndims_", NC_INT, 1, &ndims);
    status = nc_put_att_int(var_grp_id, NC_GLOBAL, "_xtype_", NC_INT, 1, &xtype);
    char* dimnames[32];
    for (int i = 0; i < ndims; i++)
    {
        dimnames[i] = (char*)malloc(sizeof(char) * MAX_VARNAME_LEN);
        memset(dimnames[i], 0, MAX_VARNAME_LEN);
        status = nc_inq_dimname(ncid, dimidp[i], dimnames[i]);
    }
    status = nc_put_att_string(var_grp_id, NC_GLOBAL, "_dims_", ndims, (const char**)dimnames);
    *varidp = (status == NC_NOERR) ? var_grp_id : 0;
    for (int i = 0; i < ndims; i++)
        free(dimnames[i]);
    return status;
}

// This function defines variable chunking structure by given mask
// (1) Generate mask chunks by calling C++ interface in mask.cpp
// (2) Construct region structure (i.e. `class Region` objects)
// (3) Generate file chunks, record them in `_chunking_` as a part of metadata
// Assume that the last 2 dimensions represents spatial coodinarates,
//  - total data size = \Pi_{i}(dimlen[i])
//  - use pre-defined NX NY to merge 
int raster_def_var_chunking(int ncid, int varid, int* mask)
{
    int ndims, status = NC_NOERR;
    size_t dimlens[32];
    status = get_var_dimlens(ncid, varid, &ndims, dimlens);    
    status = write_var_metadata(varid, ndims, dimlens, mask);
    return status;
}

// This function inquires number of dimensions in `varid`
// This varid should actually be a GROUP ID
// In our organization, `_ndims_` is an attribute written in this group as metadata 
int raster_inq_varndims(int ncid, int varid, int* ndimsp)
{
    (void) ncid; // ncid is acutally unused
    return nc_get_att_int(varid, NC_GLOBAL, "_ndims_", ndimsp);
}

// This function inquires dimension ids of given variable, the process is:
// (1) Read attributes `_dims_` to get dimension names related to this variable
// (2) Inquire these dimensions from parent group (root group) which id is `ncid`
int raster_inq_vardimid(int ncid, int varid, int* dimidsp)
{
    int status = NC_NOERR, ndims;
    status = nc_get_att_int(varid, NC_GLOBAL, "_ndims_", &ndims);
    char* dimnames[32];
    status = nc_get_att_string(varid, NC_GLOBAL, "_dims_", (char**)dimnames);
    for (int i = 0; i < ndims; i++)
    {
        nc_inq_dimid(ncid, dimnames[i], &dimidsp[i]);
        free(dimnames[i]);
    }
    return 0;
}

// This function inquires varid of given variable
// Notice that this variable is a GROUP, so we use group id instead of varid
int raster_inq_varid(int ncid, const char* varname, int* varidp)
{
    return nc_inq_grp_ncid(ncid, varname, varidp);
}

// These functions writes data to the given variable `varid`
// It invokes `write_var_*` function in `write_var.h`, which 
// splits given data to chunks, auto detects region compression
// ratio, and finally writes data to chunks via `nc_put_var*`
int raster_put_var_int(int ncid, int varid, const int* data)
{
    int status, ndims; 
    size_t dimlens[32];
    status = get_var_dimlens(ncid, varid, &ndims, dimlens);
    status = write_var_int(ncid, varid, data, dimlens);
    return status;
}

int raster_put_var_float(int ncid, int varid, const float* data)
{
    int status, ndims; 
    size_t dimlens[32];
    status = get_var_dimlens(ncid, varid, &ndims, dimlens);
    status = write_var_float(ncid, varid, data, dimlens);
    return status;
}

int raster_put_var_double(int ncid, int varid, const double* data)
{
    int status, ndims; 
    size_t dimlens[32];
    status = get_var_dimlens(ncid, varid, &ndims, dimlens);
    status = write_var_double(ncid, varid, data, dimlens);
    return status;
}

int raster_put_var_char(int ncid, int varid, const char* data)
{
    int status, ndims; 
    size_t dimlens[32];
    status = get_var_dimlens(ncid, varid, &ndims, dimlens);
    status = write_var_char(ncid, varid, data, dimlens);
    return status;
}


// These functions reads data to the given variable `varid`, region = `maskid` 
// It invokes `read_region_*` function in `read_region.h`, which 
// reads data to `data` via `nc_get_var*`
int raster_get_region_int(int ncid, int varid, int maskid, int* data)
{
    int status, ndims; 
    size_t dimlens[32];
    status = get_var_dimlens(ncid, varid, &ndims, dimlens);
    status = read_region_int(ncid, varid, data, dimlens, maskid, 1);
    return status;
}

int raster_get_region_float(int ncid, int varid, int maskid, float* data)
{
    int status, ndims; 
    size_t dimlens[32];
    status = get_var_dimlens(ncid, varid, &ndims, dimlens);
    status = read_region_float(ncid, varid, data, dimlens, maskid, 1);
    return status;
}

int raster_get_region_double(int ncid, int varid, int maskid, double* data)
{
    int status, ndims; 
    size_t dimlens[32];
    status = get_var_dimlens(ncid, varid, &ndims, dimlens);
    status = read_region_double(ncid, varid, data, dimlens, maskid, 1);
    return status;
}

int raster_get_region_char(int ncid, int varid, int maskid, char* data)
{
    int status, ndims; 
    size_t dimlens[32];
    status = get_var_dimlens(ncid, varid, &ndims, dimlens);
    status = read_region_char(ncid, varid, data, dimlens, maskid, 1);
    return status;
}

int raster_get_var_int(int ncid, int varid, int* data)
{
    int status, ndims; 
    size_t dimlens[32];
    status = get_var_dimlens(ncid, varid, &ndims, dimlens);
    status = read_var_int(ncid, varid, data, dimlens);
    return status;
}

int raster_get_var_float(int ncid, int varid, float* data)
{
    int status, ndims; 
    size_t dimlens[32];
    status = get_var_dimlens(ncid, varid, &ndims, dimlens);
    status = read_var_float(ncid, varid, data, dimlens);
    return status;
}

int raster_get_var_double(int ncid, int varid, double* data)
{
    int status, ndims; 
    size_t dimlens[32];
    status = get_var_dimlens(ncid, varid, &ndims, dimlens);
    status = read_var_double(ncid, varid, data, dimlens);
    return status;
}

int raster_get_var_char(int ncid, int varid, char* data)
{
    int status, ndims; 
    size_t dimlens[32];
    status = get_var_dimlens(ncid, varid, &ndims, dimlens);
    status = read_var_char(ncid, varid, data, dimlens);
    return status;
}

int raster_get_vara_int(int ncid, int varid, size_t* startp, size_t* countp, int* data)
{
    int status = NC_NOERR;
    return status;
}

int raster_get_vara_float(int ncid, int varid, size_t* startp, size_t* countp, float* data)
{
    int status = NC_NOERR;
    return status;
}

int raster_get_vara_double(int ncid, int varid, size_t* startp, size_t* countp, double* data)
{
    int status = NC_NOERR;
    return status;
}

int raster_get_vara_char(int ncid, int varid, size_t* startp, size_t* countp, char* data)
{
    int status = NC_NOERR;
    return status;
}