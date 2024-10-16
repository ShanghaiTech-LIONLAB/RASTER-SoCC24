#include "ChunkDataReader.h"
#include "RegionalRead.h"
#include "IndexManager.h"

static int get_region_idx(int varid, int* & region_idx, size_t& num_region_idx)
{
    int region_idx_id, region_idx_dimid, status = NC_NOERR;
    status = nc_inq_varid(varid, (char*)"_meta_region_maskid_", &region_idx_id);
    status = nc_inq_dimid(varid, (char*)"_meta_region_maskid_", &region_idx_dimid);
    status = nc_inq_dimlen(varid, region_idx_dimid, &num_region_idx);
    region_idx = new int[num_region_idx];
    status = nc_get_var_int(varid, region_idx_id, region_idx);
    return status;
}

int read_var_int(int ncid, int varid, int* data, size_t* dimlens)
{
    int status, *region_idx;
    size_t num_region_idx;
    status = get_region_idx(varid, region_idx, num_region_idx);
    std::sort(&region_idx[0], &region_idx[num_region_idx], [](int& l, int& r){ return l > r; });
    for (int i = 0; i < num_region_idx; i++)
        status = read_region_int(ncid, varid, data, dimlens, region_idx[i], false);
    status = read_region_int(ncid, varid, data, dimlens, raster::REGION_MIXED_ID, false);
    return status;
}

int read_var_float(int ncid, int varid, float* data, size_t* dimlens)
{
    int status, *region_idx;
    size_t num_region_idx;
    status = get_region_idx(varid, region_idx, num_region_idx);
    std::sort(&region_idx[0], &region_idx[num_region_idx], [](int& l, int& r){ return l > r; });
    for (int i = 0; i < num_region_idx; i++)
        status = read_region_float(ncid, varid, data, dimlens, region_idx[i], false);
    status = read_region_float(ncid, varid, data, dimlens, raster::REGION_MIXED_ID, false);
    return status;
}

int read_var_double(int ncid, int varid, double* data, size_t* dimlens)
{
    int status, *region_idx;
    size_t num_region_idx;
    status = get_region_idx(varid, region_idx, num_region_idx);
    std::sort(&region_idx[0], &region_idx[num_region_idx], [](int& l, int& r){ return l > r; });
    for (int i = 0; i < num_region_idx; i++)
        status = read_region_double(ncid, varid, data, dimlens, region_idx[i], false);
    status = read_region_double(ncid, varid, data, dimlens, raster::REGION_MIXED_ID, false);
    return status;
}

int read_var_char(int ncid, int varid, char* data, size_t* dimlens)
{
    int status, *region_idx;
    size_t num_region_idx;
    status = get_region_idx(varid, region_idx, num_region_idx);
    std::sort(&region_idx[0], &region_idx[num_region_idx], [](int& l, int& r){ return l > r; });
    for (int i = 0; i < num_region_idx; i++)
        status = read_region_char(ncid, varid, data, dimlens, region_idx[i], false);
    status = read_region_char(ncid, varid, data, dimlens, raster::REGION_MIXED_ID, false);
    return status;
}