#ifndef __MESH_H__
#define __MESH_H__

#include <iostream>
#include <memory>
#include <assert.h>
#include <string.h>
#include <vector>
#include <list>
#include <numeric>
#include <algorithm>
#include <set>
#include <map>

#include <netcdf.h>

namespace raster
{
// RASTER_chunk_t types
enum class BLOCK_TYPE : char {PURE = 'P', MAJOR = 'M', MIXED = 'X'};
enum class EXPORT_TYPE : int {PYTHON = 0, HUMAN = 1};

// A single RASTER chunk
struct RASTER_chunk_t
{
    RASTER_chunk_t();
    RASTER_chunk_t(int r, int c, int sizeR, int sizeC);
    RASTER_chunk_t(RASTER_chunk_t& other);
    RASTER_chunk_t(RASTER_chunk_t&& other);

    int& operator[](int maskId);
    std::ostream& operator<<(std::ostream& os);
    RASTER_chunk_t& operator=(const RASTER_chunk_t&& other);
    RASTER_chunk_t& operator=(const RASTER_chunk_t& other);

    void print_info(std::ostream& os=std::cout) const;
    int  get_major_index() const;
    void set_type();
    
    std::vector<int> keys() const;
    std::vector<int> values() const;

    int m_start_row, m_start_col;
    int m_size_row, m_size_col;
    std::map<int, int> m_maskid2nelems;
    BLOCK_TYPE m_type;

    static constexpr int    REGION_NO_MAJOR = -10000;
    static constexpr double REGION_MAJOR_THRESHOLD = 0.5;
};

// mesh for data partitioning
using chunk_info_list = std::vector<std::list<std::shared_ptr<RASTER_chunk_t> > >;

// mesh for data partitioning
class Mesh
{
public:
    Mesh() = default;
    Mesh(int* mask, int mask_rows, int mask_cols, size_t rows_in_grid, size_t cols_in_grid, double merge_threshold=1);     
    ~Mesh();

    void export_to(EXPORT_TYPE type=EXPORT_TYPE::HUMAN, std::ostream& os=std::cout) const;
    std::vector<int> get_all_mask_id();
    const chunk_info_list& partition();
    size_t rows() const;
    size_t cols() const;
    
private:
    void merge_chunks(std::list<std::shared_ptr<RASTER_chunk_t> >& chunks_in_row);
    
private:
    size_t m_cols_in_grid;
    size_t m_rows_in_grid;
    size_t m_nrows;
    size_t m_ncols;
    int m_ncid;
    int* m_mask;
    double m_merge_threshold;
    chunk_info_list m_partitions;
    bool m_done;
};

}; 

#endif