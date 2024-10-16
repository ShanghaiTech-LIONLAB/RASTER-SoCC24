#include "MetadataHandler.h"
#include "IndexManager.h"
#include "MeshBuilder.h"
#include "MetaCache.h"
#include "config.h"

using namespace raster;

int write_var_metadata(int varid, int ndims, size_t* dimlens, int* mask)
{
    using namespace raster;
    int rows = dimlens[ndims - 2], cols = dimlens[ndims - 1];
    int status, index_id, index_dimid;
    Mesh mesh(mask, rows, cols, CHUNKSIZE_NX, CHUNKSIZE_NY);

    chunk_info_list blist = mesh.partition();
    std::vector<int> mask_ids = mesh.get_all_mask_id();
    std::vector<size_t> chunkshape(ndims);

    for (int i = 0; i < ndims; i++) 
        chunkshape[i] = dimlens[i];
    chunkshape[ndims - 2] /= CHUNKSIZE_NX;
    chunkshape[ndims - 1] /= CHUNKSIZE_NY;
    std::vector<Region> regions = construct_region_chunks(blist, mask_ids, ndims, chunkshape);
    std::vector<int> indices = mesh.get_all_mask_id();
    status = nc_def_dim(varid, "_meta_region_maskid_", indices.size(), &index_dimid);
    status = nc_def_var(varid, "_meta_region_maskid_", NC_INT, 1, &index_dimid, &index_id);
    status = nc_put_var_int(varid, index_id, &indices[0]);

    for (auto& region : regions)
    {
        int meta_id, meta_dimid[2], nrows, ncols, rblks;
        int dims[2], metaid, relation_dimid;
        char region_metaname[64];
        size_t* meta = nullptr;
        int* relation = nullptr;
        
        construct_region_meta(region, &nrows, &ncols, meta);
        construct_region_relation(region, &rblks, relation);

        sprintf(region_metaname, "_meta_region_%d_rows_", region.get_maskid());
        status = nc_def_dim(varid, region_metaname, nrows, &dims[0]);
        sprintf(region_metaname, "_meta_region_%d_cols_", region.get_maskid());
        status = nc_def_dim(varid, region_metaname, ncols, &dims[1]);
        sprintf(region_metaname, "_meta_region_%d_chunks_", region.get_maskid());
        status = nc_def_var(varid, region_metaname, NC_UINT64, 2, dims, &metaid);
        nc_put_var_ulonglong(varid, metaid, (unsigned long long*)meta);
        if (region.get_maskid() != REGION_MIXED_ID)
        {
            sprintf(region_metaname, "_meta_region_%d_relations_", region.get_maskid());
            nc_def_dim(varid, region_metaname, rblks, &relation_dimid);
            nc_def_var(varid, region_metaname, NC_INT, 1, &relation_dimid, &metaid);
            nc_put_var_int(varid, metaid, relation);
        }
        meta_cache->add_region(varid, region.get_maskid(), nrows, ncols, rblks, relation, meta);
    }
    return status;
}
