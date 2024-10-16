#ifndef __CONSTRUCT_REGION_H__
#define __CONSTRUCT_REGION_H__

#include <iostream>
#include <vector>
#include <netcdf.h>
#include "MeshBuilder.h"

namespace raster
{

constexpr int REGION_MIXED_ID = 65535;

// for a Nd array Arr[d1, d2, ..., dn-1, dn], assume last 2 dims are spatial coordinates
// In current design, only the fastest varying dimension chunksize is unfixed, hence we can
// make start = [rest_startp]; chunksize = [rest_chunksize]
struct file_chunk_t
{
    file_chunk_t() = default;
    file_chunk_t(int ndims, std::vector<size_t> startp, std::vector<size_t> chunksizep)
                : m_ndims(ndims), m_start(startp), m_chunksize(chunksizep) {};
    std::vector<size_t>     m_chunksize;
    std::vector<size_t>     m_start;
    int                     m_ndims;
};

class Region
{
public:
    Region();
    Region(int mask_id, int ndims);

    void    add_region(file_chunk_t&& blk, int id, bool is_related);
    void    set_deflate(int level);
    int     get_deflate_level() const;
    int     get_maskid() const;
    void    set_maskid(int id);
    int     get_ndims() const;
    void    set_ndims(int ndims);
    int     get_nchunks() const;
    int     get_id(int id) const;

    const std::vector<file_chunk_t>& get_regions() const;
    const file_chunk_t& get_region(int id) const;
    const std::vector<int>& get_related_ids() const;

private:
    std::vector<file_chunk_t>   m_chunks;
    std::vector<int>            m_chunk_ids;
    std::vector<int>            m_related_chunk_ids;
    int                         m_deflate_level;
    int                         m_mask_id;
    int                         m_ndims;
}; 


std::vector<Region> construct_region_chunks(const chunk_info_list& blklist, std::vector<int>& mask_ids, int varndims, 
                                            std::vector<size_t> chunkshape);

void construct_region_meta(Region& region, int* nrows, int* ncols, size_t* &metadata);

void construct_region_relation(Region& region, int* nrelated_blks, int* &related_ids);

} // end of namespace raster
#endif