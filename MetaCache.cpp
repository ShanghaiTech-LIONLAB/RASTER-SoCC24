#include "MetaCache.h"

namespace raster
{

std::unique_ptr<MetaCache> meta_cache(new MetaCache(CACHE_DEFAULT_CAPACITY));

void MetaCache::add_region(int varid, int mask_id, int nrows, int ncols, int nrelations, int* relation, size_t* data)
{
    // use (varid ^ maskid) to be cache key
    int key = mask_id ^ varid;
    // already in cache
    if (m_region_cache.find(key) != m_region_cache.end())
        return; 
    // need to evict
    if (m_region_cache.size() >= m_size)
        this->evict();
    m_region_cache.insert({key, std::shared_ptr<CacheBlock>(new CacheBlock(nrows, ncols, nrelations, relation, data))}); 
}

void MetaCache::evict()
{
    // always remove the chunk with smallest maskid, it may be similar to random policy in practice
    m_region_cache.erase(m_region_cache.begin()->first);
}

const std::shared_ptr<CacheBlock> MetaCache::get_region(int varid, int mask_id) const
{
    auto res = m_region_cache.find(mask_id ^ varid);
    // return nullptr if not found
    if (res == m_region_cache.end())
        return nullptr;
    // return the region if found
    return res->second;
}

void MetaCache::add_mixed_table(int varid, int nrows, int ncols, size_t* data)
{
    if (m_vartable_cache.find(varid) != m_vartable_cache.end())
        return; 
    m_vartable_cache.insert({varid, std::shared_ptr<MixedBlockTable>(new MixedBlockTable(nrows, ncols, data))}); 
}

const std::shared_ptr<MixedBlockTable> MetaCache::get_mixed_table(int varid) const
{
    auto res = m_vartable_cache.find(varid);
    // return nullptr if not found
    if (res == m_vartable_cache.end())
        return nullptr;
    // return the table if found
    return res->second;
}


} // namespace raster