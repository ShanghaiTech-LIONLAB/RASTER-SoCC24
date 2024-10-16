#include <random>
#include <netcdf.h>
#include <zlib.h>
#include <chrono>

#include "ChunkDataWriter.h"
#include "IndexManager.h"
#include "MetaCache.h"
#include "VarCompress.h"
#include "config.h"

using namespace raster;
using namespace std::chrono;
static std::default_random_engine random_engine(time(0));

template <typename T>
static void memcpy_chk2d(T* dest, const T* src, size_t* start, size_t* varshape, size_t* chkshape)
{
    if (start[0] + chkshape[0] > varshape[0] || start[1] + chkshape[1] > varshape[1])
        throw std::runtime_error("Memcpy_chk2d - Error: index out of range");

    for (size_t i = 0; i < chkshape[0]; i++)
        memcpy(dest + i * chkshape[1], src + (i + start[0]) * varshape[1] + start[1], sizeof(T) * chkshape[1]);
}

template <typename T>
static void memcpy_chk3d(T* dest, const T* src, size_t* start, size_t* varshape, size_t* chkshape)
{
    if (start[0] + chkshape[0] > varshape[0])
        throw std::runtime_error("Memcpy_chk3d - Error: index out of range");

    for (size_t layer = 0; layer < chkshape[0]; layer++)
    {
        memcpy_chk2d<T>(dest + layer * chkshape[1] * chkshape[2], src + layer * varshape[1] * varshape[2], 
                        start + 1, varshape + 1, chkshape + 1);
    }
}

template <typename T>
static void memcpy_chk4d(T* dest, const T* src, size_t* start, size_t* varshape, size_t* chkshape)
{
    if (start[0] + chkshape[0] > varshape[0])
        throw std::runtime_error("Memcpy_chk4d - Error: index out of range");

    for (size_t layer = 0; layer < chkshape[0]; layer++)
    {
        memcpy_chk3d<T>(dest + layer * chkshape[1] * chkshape[2] * chkshape[3], src + layer * varshape[1] * varshape[2] * varshape[3], 
                        start + 1, varshape + 1, chkshape + 1);
    }
}

template <typename T>
static int do_write_region(int maskid, uint64_t* region_meta, int meta_rows, int meta_cols,
                               const T* data, size_t* data_shape, int var_grp_id, int* dimids, int var_type)
{
    int ndims = (meta_cols - 1) / 2, status = NC_NOERR, region_grp_id;
    int normal_height = data_shape[ndims - 2] / CHUNKSIZE_NX;
    std::string region_name = "region_" + std::to_string(maskid);
    assert(meta_rows >= 1);
    status = nc_def_grp(var_grp_id, region_name.c_str(), &region_grp_id);
    std::vector<T*> chunk_ptrs(meta_rows); // save pointers to each chunk
    std::vector<int> chunk_ids(meta_rows); // save chunk variable ids

    // Pass 1: memory copy + variable definition
    for (int i = 0; i < meta_rows; i++)
    {
        size_t* start = &region_meta[i * meta_cols + 1];
        size_t* count = &region_meta[i * meta_cols + 1 + ndims];
        size_t chunksize = std::accumulate(&count[0], &count[ndims], 1, [&](size_t a, size_t b){ return a * b; } );
        int varsize_dimid, old_dimid;
        char buffer[128];

        chunk_ptrs[i] = new T[chunksize];
        sprintf(buffer, "_chunk_%d_size_", (int)region_meta[i * meta_cols]);
        status = nc_def_dim(region_grp_id, buffer, chunksize * sizeof(T), &varsize_dimid);
        status = sprintf(buffer, "chunk_%d", (int)region_meta[i * meta_cols]);
        status = nc_def_var(region_grp_id, buffer, NC_UBYTE, 1, &varsize_dimid, &chunk_ids[i]);
    }

    // pass 2: do memory copy in parallel
    // #pragma omp parallel for
    for (int i = 0; i < meta_rows; i++)
    {
        switch (ndims)
        {
            case 2: memcpy_chk2d<T>(chunk_ptrs[i], data, &region_meta[i * meta_cols + 1], data_shape, 
                                    &region_meta[i * meta_cols + 1 + ndims]); break;
            case 3: memcpy_chk3d<T>(chunk_ptrs[i], data, &region_meta[i * meta_cols + 1], data_shape, 
                                    &region_meta[i * meta_cols + 1 + ndims]); break;
            case 4: memcpy_chk4d<T>(chunk_ptrs[i], data, &region_meta[i * meta_cols + 1], data_shape, 
                                    &region_meta[i * meta_cols + 1 + ndims]); break;
            default: throw std::runtime_error("Unsupported dimension: " + std::to_string(ndims));
        }
    }

    // Ziplevel detection, our algorithm will compress those regions with a high compress ratio
    // which indicates that region has a high probability to be a invalid region (at least it
    // has a low information entropy)
    int zlevel = 0;
    float avg_ratio = 1;
    std::uniform_int_distribution<unsigned> unif(0, meta_rows - 1);
    for (int i = 0; i < ZIP_DETECT_NSAMPLES; i++)
    {
        // select a sample chunk from chunks randomly
        T* sample_ptr = chunk_ptrs[unif(random_engine)];
        // TODO
    }
    if (avg_ratio < 0.05)       // [0, 0.05)
        zlevel = ZLEVEL_HIGH;
    else if (avg_ratio < 0.1)   // [0.05, 0.1)
        zlevel = ZLEVEL_MID;
    else if (avg_ratio < 0.2)   // [0.1, 0.2)
        zlevel = ZLEVEL_LOW;
    else                        // [0.2, 1]
        zlevel = ZLEVEL_NOZIP;

    // Pass 3: write data according to detected zip level
    for (int i = 0; i < meta_rows; i++)
    {
        size_t* count = &region_meta[i * meta_cols + 1 + ndims];
        if (zlevel != 0)
        {
            status = nc_def_var_chunking(region_grp_id, chunk_ids[i], NC_CHUNKED, count);
            status = nc_def_var_deflate(region_grp_id, chunk_ids[i], NC_NOSHUFFLE, 1, zlevel);            
        }
        
        status = nc_put_var_ubyte(region_grp_id, chunk_ids[i], reinterpret_cast<unsigned char*>(chunk_ptrs[i]));
        delete[] chunk_ptrs[i];
    }
    return status;
}

template<typename T>
int do_write_var(int ncid, int var_grp_id, const T* data, size_t* data_shape, int var_type)
{
    // (1) query region mask ids
    int mask_dimid, mask_varid, status, *mask_buffer = nullptr, *dimids = nullptr;
    size_t num_regions;
    status = nc_inq_dimid(var_grp_id, "_meta_region_maskid_", &mask_dimid);
    status = nc_inq_dimlen(var_grp_id, mask_dimid, &num_regions);
    status = nc_inq_varid(var_grp_id, "_meta_region_maskid_", &mask_varid);
    mask_buffer = new int[num_regions + 1];
    status = nc_get_var_int(var_grp_id, mask_varid, mask_buffer);
    mask_buffer[num_regions] = REGION_MIXED_ID; // mixed region chunks

    // sorting the masks in a desending order will significantly improve write performance
    // I don't know why it works, maybe the internal region chunks are organized in a desending order??
    std::sort(&mask_buffer[0], &mask_buffer[num_regions+1], [](int l, int r) { return l > r; } );

    // for each region, (a) query metadata; (b) copy data to chunk buffers; (c) write chunks to filesystem
    for (int i = 0; i < num_regions + 1; i++)
    {
        char name_buffer[128];
        int meta_id, meta_dimids[2];
        size_t nrows, ncols, *meta_buffer;
        auto blkptr = meta_cache->get_region(var_grp_id, mask_buffer[i]);
        int use_cache = false;

        if (blkptr != nullptr)
        {
            // cache hit, get data from cache
            use_cache = true;
            nrows = blkptr->m_nrows; 
            ncols = blkptr->m_ncols;
            meta_buffer = blkptr->m_data;
            if (nrows == 0) 
                continue; // skip empty regions
        }
        else
        {
            // cache miss, get data from file
            sprintf(name_buffer, "_meta_region_%d_chunks_", mask_buffer[i]);
            status = nc_inq_varid(var_grp_id, name_buffer, &meta_id);
            status = nc_inq_vardimid(var_grp_id, meta_id, meta_dimids);
            status = nc_inq_dimlen(var_grp_id, meta_dimids[0], &nrows);
            status = nc_inq_dimlen(var_grp_id, meta_dimids[1], &ncols);
            if (nrows == 0) 
                continue; // skip empty regions
            meta_buffer = new uint64_t[nrows * ncols + 1];
            nc_get_var_ulonglong(var_grp_id, meta_id, (unsigned long long*)meta_buffer);

            int* relation_chunks, relation_dimid;
            size_t num_chunks;
            sprintf(name_buffer, "_meta_region_%d_relations_", mask_buffer[i]);
            status = nc_inq_dimid(var_grp_id, name_buffer, &relation_dimid);
            status = nc_inq_dimlen(var_grp_id, relation_dimid, &num_chunks);
            relation_chunks = new int[num_chunks];
            status = nc_inq_varid(var_grp_id, name_buffer, &meta_id);
            status = nc_get_var_int(var_grp_id, meta_id, relation_chunks);
            // add data to cache
            meta_cache->add_region(var_grp_id, mask_buffer[i], nrows, ncols, num_chunks, relation_chunks, meta_buffer);
        }

        status = do_write_region<T>(mask_buffer[i], meta_buffer, nrows, ncols, data, data_shape, var_grp_id, dimids, var_type);

        if (!use_cache)
            delete[] meta_buffer;

        if (status != NC_NOERR)
            throw std::runtime_error("Error while writing region " + std::to_string(mask_buffer[i]) + ": " + nc_strerror(status));
    }
    if (dimids != NULL) delete[] dimids;
    if (mask_buffer != NULL) delete[] mask_buffer;
    return status;
}


int write_var_int(int ncid, int var_grp_id, const int* data, size_t* data_shape)
{
    return do_write_var<int>(ncid, var_grp_id, data, data_shape, NC_INT);
}

int write_var_char(int ncid, int var_grp_id, const char* data, size_t* data_shape)
{
    return do_write_var<char>(ncid, var_grp_id, data, data_shape, NC_CHAR);
}

int write_var_double(int ncid, int var_grp_id, const double* data, size_t* data_shape)
{
    return do_write_var<double>(ncid, var_grp_id, data, data_shape, NC_DOUBLE);
}

int write_var_float(int ncid, int var_grp_id, const float* data, size_t* data_shape)
{
    return do_write_var<float>(ncid, var_grp_id, data, data_shape, NC_FLOAT);
}

