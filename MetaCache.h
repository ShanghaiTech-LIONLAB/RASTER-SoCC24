#ifndef __META_CACHE_H__
#define __META_CACHE_H__

#include <iostream>
#include <memory>
#include <map>
#include <netcdf.h>

namespace raster
{

#define CACHE_DEFAULT_CAPACITY 128

struct CacheBlock
{
    CacheBlock() : m_nrows(0), m_ncols(0), m_relation(nullptr), m_data(nullptr) {};
    CacheBlock(int nrows, int ncols, int nrelations, int* relation, size_t* data) : m_nrows(nrows), m_ncols(ncols), 
                                                    m_nrelations(nrelations), m_relation(relation), m_data(data) {};
    ~CacheBlock() 
    {
        if (m_data) delete[] m_data; 
        if (m_relation) delete[] m_relation;
    };

    int m_nrows, m_ncols, m_nrelations;
    int* m_relation;
    size_t* m_data;
};

struct MixedBlockTable
{
    MixedBlockTable(int nrows, int ncols, size_t* data) : m_nrows(nrows), m_ncols(ncols), m_data(data) {};
    ~MixedBlockTable() { if (m_data) delete[] m_data; }
    size_t* operator[] (int row) { return &m_data[row * m_ncols]; }

    int m_nrows, m_ncols;
    size_t* m_data;
};

class MetaCache
{
public:
    MetaCache(const int cache_size) : m_size(cache_size) {};
    ~MetaCache() {};
    void add_region(int varid, int mask_id, int nrows, int ncols, int nrelations, int* relation, size_t* data);
    const std::shared_ptr<CacheBlock> get_region(int varid, int mask_id) const;

    void add_mixed_table(int varid, int nrows, int ncols, size_t* data);
    const std::shared_ptr<MixedBlockTable> get_mixed_table(int varid) const;

private:
    void evict();

private:
    int m_size;
    std::map<uint64_t, std::shared_ptr<CacheBlock> > m_region_cache; // key: region maskid, value: region metadata
    std::map<int, std::shared_ptr<MixedBlockTable> > m_vartable_cache;
};

extern std::unique_ptr<MetaCache> meta_cache;

} // namespace raster

#endif