#include <iostream>
#include <algorithm>
#include <numeric>
#include <vector>

#include "RegionalRead.h"
#include "IndexManager.h"
#include "MetaCache.h"

// dest: user buffer(var); src: chunk buffer
template <typename T>
static void memread_chk2d(T* dest, const T* src, size_t* start, size_t* varshape, size_t* chkshape)
{
    if (start[0] + chkshape[0] > varshape[0] || start[1] + chkshape[1] > varshape[1])
        throw std::runtime_error("Memread_chk2d - Error: index out of range");

    for (size_t i = 0; i < chkshape[0]; i++)
        memcpy(dest + (i + start[0]) * varshape[1] + start[1], src + i * chkshape[1], sizeof(T) * chkshape[1]);
}

template <typename T>
static void memread_chk3d(T* dest, const T* src, size_t* start, size_t* varshape, size_t* chkshape)
{
    if (start[0] + chkshape[0] > varshape[0])
        throw std::runtime_error("Memread_chk3d - Error: index out of range");
    for (size_t layer = 0; layer < chkshape[0]; layer++)
    {
        memread_chk2d<T>(dest + layer * varshape[1] * varshape[2], src + layer * chkshape[1] * chkshape[2],
                        start + 1, varshape + 1, chkshape + 1);
    }
}

template <typename T>
static void memread_chk4d(T* dest, const T* src, size_t* start, size_t* varshape, size_t* chkshape)
{
    if (start[0] + chkshape[0] > varshape[0])
        throw std::runtime_error("Memread_chk4d - Error: index out of range");

    for (size_t layer = 0; layer < chkshape[0]; layer++)
    {
        memread_chk3d<T>(dest + layer * varshape[1] * varshape[2] * varshape[3], src + layer * chkshape[1] * chkshape[2] * chkshape[3], 
                        start + 1, varshape + 1, chkshape + 1);
    }
}

template <typename T>
int do_read_region(int maskid, uint64_t* region_meta, int meta_rows, int meta_cols, T* data, 
                   size_t* data_shape, int var_grp_id, int var_type, std::vector<int>&& indices)
{
    int ndims = (meta_cols - 1) / 2, status = NC_NOERR, region_grp_id;
    std::string region_name = "region_" + std::to_string(maskid);
    size_t indices_size = 0;
    assert(meta_rows >= 0);

    // corner case: this region is empty
    if (meta_rows == 0)
        return status;

    status = nc_inq_grp_ncid(var_grp_id, region_name.c_str(), &region_grp_id);
    if (indices.size() == 0)
    {
        indices.resize(meta_rows);
        for (int i = 0; i < meta_rows; i++) indices[i] = i;
    }
    indices_size = indices.size();
    std::vector<unsigned char*> chkdatas(indices_size);
    size_t poolsize = std::accumulate(&region_meta[ndims + 1], &region_meta[2 * ndims], 1, [&](size_t a, size_t b){ return a * b; } );
    unsigned char* pool = (unsigned char*)malloc(poolsize * data_shape[ndims - 1] * sizeof(T));
    for (int id = 0; id < indices.size(); id++)
    {
        int i = indices[id];
        size_t* start = &region_meta[i * meta_cols + 1];
        size_t* count = &region_meta[i * meta_cols + 1 + ndims];
        
        int chunk_id;
        char buffer[128];
        status = sprintf(buffer, "chunk_%d", (int)region_meta[i * meta_cols]);
        status = nc_inq_varid(region_grp_id, buffer, &chunk_id);
        status = nc_get_var_ubyte(region_grp_id, chunk_id, pool);
    // }
    
    // // copy memory from chunks to user data buffer
    // #pragma omp parallel for
    // for (int id = 0; id < indices_size; id++)
    // {
    //     int i = indices[id];
    //     size_t* start = &region_meta[i * meta_cols + 1];
    //     size_t* count = &region_meta[i * meta_cols + 1 + ndims];
        switch (ndims)
        {
            case 2: memread_chk2d<T>(data, reinterpret_cast<T*>(pool), start, data_shape, count); break;
            case 3: memread_chk3d<T>(data, reinterpret_cast<T*>(pool), start, data_shape, count); break;
            case 4: memread_chk4d<T>(data, reinterpret_cast<T*>(pool), start, data_shape, count); break;
            default: throw std::runtime_error("Unsupported dimension: " + std::to_string(ndims));
        }
        // delete[] pool;
    }
    delete[] pool;
    return status;
}

template <typename T>
int read_region(int var_grp_id, int mask_id, T* data, size_t* data_shape, int var_type, bool relation_required=true)
{
    int status = NC_NOERR;
    int* relation_chunks = nullptr; 
    uint64_t* region_meta = nullptr;
    size_t meta_rows, meta_cols, nrelations = 0;
    auto region_chunks = raster::meta_cache->get_region(var_grp_id, mask_id);

    // 1st pass: read region chunks
    if (region_chunks != nullptr) // cache hit
    {
        region_meta = region_chunks->m_data;
        meta_rows = region_chunks->m_nrows;
        meta_cols = region_chunks->m_ncols;
        nrelations = region_chunks->m_nrelations;
        relation_chunks = region_chunks->m_relation;
    }
    else
    {
        int meta_id, relation_dimid, meta_dimids[2];
        char buffer[128];
        sprintf(buffer, "_meta_region_%d_rows_", mask_id);
        status = nc_inq_dimid(var_grp_id, buffer, &meta_dimids[0]);
        sprintf(buffer, "_meta_region_%d_cols_", mask_id);
        status = nc_inq_dimid(var_grp_id, buffer, &meta_dimids[1]);
        if (status != NC_NOERR) // we need to handle invalid maskid here
            return status;

        status = nc_inq_dimlen(var_grp_id, meta_dimids[0], &meta_rows);
        status = nc_inq_dimlen(var_grp_id, meta_dimids[1], &meta_cols);
        region_meta = new uint64_t[meta_rows * meta_cols];
        sprintf(buffer, "_meta_region_%d_chunks_", mask_id);
        status = nc_inq_varid(var_grp_id, buffer, &meta_id);
        status = nc_get_var_ulonglong(var_grp_id, meta_id, (unsigned long long*)region_meta);

        if (mask_id != raster::REGION_MIXED_ID)
        {
            sprintf(buffer, "_meta_region_%d_relations_", mask_id);
            status = nc_inq_dimid(var_grp_id, buffer, &relation_dimid);
            status = nc_inq_dimlen(var_grp_id, relation_dimid, &nrelations);
            relation_chunks = new int[nrelations];
            status = nc_inq_varid(var_grp_id, buffer, &meta_id);
            status = nc_get_var_int(var_grp_id, meta_id, relation_chunks);            
        } 

        raster::meta_cache->add_region(var_grp_id, mask_id, meta_rows, meta_cols, nrelations, relation_chunks, region_meta);  
    }
    status = do_read_region<T>(mask_id, region_meta, meta_rows, meta_cols, data, data_shape, var_grp_id, var_type, std::vector<int>(0));

    // 2nd pass: read related chunks from relation table
    if (relation_required)
    {
        auto mixed_table = raster::meta_cache->get_mixed_table(var_grp_id);
        size_t mixed_rows, mixed_cols;
        uint64_t* mixed_data;
        if (mixed_table != nullptr) // cache hit
        {
            mixed_rows = mixed_table->m_nrows;
            mixed_cols = mixed_table->m_ncols;
            mixed_data = mixed_table->m_data;
        }
        else
        {
            char buffer[128];
            int mixed_varid, mixed_dimids[2];
            sprintf(buffer, "_meta_region_%d_rows_", raster::REGION_MIXED_ID);
            status = nc_inq_dimid(var_grp_id, buffer, &mixed_dimids[0]);
            sprintf(buffer, "_meta_region_%d_cols_", raster::REGION_MIXED_ID);
            status = nc_inq_dimid(var_grp_id, buffer, &mixed_dimids[1]);
            if (status != NC_NOERR) // we need to handle invalid maskid here
                return status;

            status = nc_inq_dimlen(var_grp_id, mixed_dimids[0], &mixed_rows);
            status = nc_inq_dimlen(var_grp_id, mixed_dimids[1], &mixed_cols);
            mixed_data = new uint64_t[mixed_rows * mixed_cols];
            sprintf(buffer, "_meta_region_%d_chunks_", raster::REGION_MIXED_ID);
            status = nc_inq_varid(var_grp_id, buffer, &mixed_varid);
            status = nc_get_var_ulonglong(var_grp_id, mixed_varid, (unsigned long long*)mixed_data);

            raster::meta_cache->add_mixed_table(var_grp_id, mixed_rows, mixed_cols, mixed_data);
        }
        std::vector<int> relation_indices(nrelations);
        std::vector<int> all_mixed_chunks(mixed_rows);
        for (int i = 0; i < mixed_rows; i++)
            all_mixed_chunks[i] = mixed_data[i * mixed_cols];
        
        for (int i = 0; i < nrelations; i++)
            relation_indices[i] = std::find(all_mixed_chunks.begin(), all_mixed_chunks.end(), relation_chunks[i])\
                                  - all_mixed_chunks.begin();
        
        status = do_read_region<T>(raster::REGION_MIXED_ID, mixed_data, mixed_rows, mixed_cols, data, data_shape, 
                                   var_grp_id, var_type, std::move(relation_indices));
    }
    return status;
}

int read_region_int(int ncid, int varid, int* data, size_t* dimlens, int mask_id, int relation_required)
{
    return read_region<int>(varid, mask_id, data, dimlens, NC_INT, relation_required == 1 ? true : false);
}

int read_region_float(int ncid, int varid, float* data, size_t* dimlens, int mask_id, int relation_required)
{
    return read_region<float>(varid, mask_id, data, dimlens, NC_FLOAT, relation_required == 1 ? true : false);
}

int read_region_double(int ncid, int varid, double* data, size_t* dimlens, int mask_id, int relation_required)
{
    return read_region<double>(varid, mask_id, data, dimlens, NC_DOUBLE, relation_required == 1 ? true : false);
}

int read_region_char(int ncid, int varid, char* data, size_t* dimlens, int mask_id, int relation_required)
{
    return read_region<char>(varid, mask_id, data, dimlens, NC_CHAR, relation_required == 1 ? true : false);
}
