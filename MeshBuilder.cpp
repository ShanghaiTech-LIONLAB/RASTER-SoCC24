#include "MeshBuilder.h"

namespace raster
{

// implementation of RASTER chunk
RASTER_chunk_t::RASTER_chunk_t() : m_start_row(0), m_start_col(0), m_size_row(0), m_size_col(0), 
                   m_type(BLOCK_TYPE::PURE)
{}

RASTER_chunk_t::RASTER_chunk_t(int r, int c, int size_r, int size_c)
            : m_start_row(r), m_start_col(c), m_size_row(size_r), m_size_col(size_c),
              m_type(BLOCK_TYPE::PURE)
{}

RASTER_chunk_t& RASTER_chunk_t::operator=(const RASTER_chunk_t&& other)
{
    this->m_size_col = other.m_size_col;
    this->m_size_row = other.m_size_row;
    this->m_start_col = other.m_start_col;
    this->m_start_row = other.m_size_row;
    this->m_type = other.m_type;
    this->m_maskid2nelems = other.m_maskid2nelems;
    return *this;
}

RASTER_chunk_t& RASTER_chunk_t::operator=(const RASTER_chunk_t& other)
{
    *this = std::move(other);
    return *this;
}

RASTER_chunk_t::RASTER_chunk_t(RASTER_chunk_t&& other)
{
    *this = other;
}

RASTER_chunk_t::RASTER_chunk_t(RASTER_chunk_t& other)
{
    *this = other;
}

int& RASTER_chunk_t::operator[](int maskId)
{
    return m_maskid2nelems[maskId];
}

std::ostream& operator<<(std::ostream& os, RASTER_chunk_t& b)
{
    os << "<RASTER_chunk_t start: (" << b.m_start_row << "," << b.m_start_col << "), ";
    os << "size: (" << b.m_size_row << "," << b.m_size_col << "), content = ";
    b.print_info(os);
    os << ">";
    return os;
}

void RASTER_chunk_t::print_info(std::ostream& os) const
{
    os << "{";
    os << std::accumulate(m_maskid2nelems.begin(), m_maskid2nelems.end(), std::string(""), 
                    [&](auto& item, auto& other) -> std::string {
                        return item + (item.size() == 0 ? "" : ", ")
                             + std::to_string(other.first) + ": " + std::to_string(other.second); 
                    } );
    os << "}, major = " << get_major_index();
}

int RASTER_chunk_t::get_major_index() const
{
    auto maxPair = *std::max_element(m_maskid2nelems.begin(), m_maskid2nelems.end(), 
                                        [&](auto& l, auto& r) {return l.second < r.second; }
                                    );
    if ((double(maxPair.second) / double(m_size_row * m_size_col)) < RASTER_chunk_t::REGION_MAJOR_THRESHOLD)
        return RASTER_chunk_t::REGION_NO_MAJOR;
    return maxPair.first;
}

void RASTER_chunk_t::set_type()
{
    auto maxPair = *std::max_element(m_maskid2nelems.begin(), m_maskid2nelems.end(), 
                                        [&](auto& l, auto& r) {return l.second < r.second; }
                                    );
    double percentage = (double(maxPair.second) / double(m_size_row * m_size_col));
    if (percentage == 1.0)
        m_type = BLOCK_TYPE::PURE;
    else if (percentage > RASTER_chunk_t::REGION_MAJOR_THRESHOLD)
        m_type = BLOCK_TYPE::MAJOR;
    else
        m_type = BLOCK_TYPE::MIXED;
}

std::vector<int> RASTER_chunk_t::keys() const
{
    std::vector<int> ret;
    for (auto& kv : m_maskid2nelems)
        ret.push_back(kv.first);
    return ret;
}

std::vector<int> RASTER_chunk_t::values() const
{
    std::vector<int> ret;
    for (auto& kv : m_maskid2nelems)
        ret.push_back(kv.second);
    return ret;
}


// implementation of Mesh

Mesh::Mesh(int* mask, int mask_rows, int mask_cols, size_t rows_in_grid, size_t cols_in_grid, double merge_threshold)
           : m_cols_in_grid(cols_in_grid), m_rows_in_grid(rows_in_grid), m_nrows(mask_rows), m_ncols(mask_cols), 
             m_mask(nullptr), m_merge_threshold(merge_threshold), m_done(false)
{
    // copy the given mask
    m_mask = new int[m_nrows * m_ncols];
    memcpy(m_mask, mask, m_nrows * m_ncols * sizeof(int));
}

Mesh::~Mesh()
{
    if (m_mask)
        delete[] m_mask;
}

const chunk_info_list& Mesh::partition()
{
    int row_length = m_nrows / m_rows_in_grid;
    int col_length = m_ncols / m_cols_in_grid;
    if (m_done)
        return m_partitions;

    for (size_t i = 0; i*row_length < m_nrows; i++)
    {
        int sr = ((i + 1) * row_length > m_nrows) ? m_nrows - i * row_length : row_length;
        std::list<std::shared_ptr<RASTER_chunk_t> > chunks_in_row;
        for (size_t j = 0; j < m_cols_in_grid; j++)
        {
            int sc = (j == m_cols_in_grid - 1) ? m_ncols - j * col_length : col_length;
            chunks_in_row.emplace_back(std::shared_ptr<RASTER_chunk_t>(new RASTER_chunk_t(i * row_length, j * col_length, sr, sc)));
        }
        merge_chunks(chunks_in_row);
        m_partitions.push_back(chunks_in_row);
    }
    m_done = true;
    return m_partitions;
}

void Mesh::merge_chunks(std::list<std::shared_ptr<RASTER_chunk_t> >& chunks_in_row)
{
    // Pre-processing, calculate the distribution of each chunk
    for (auto& r : chunks_in_row)
    {
        for (int i = 0; i < r->m_size_row; i++)
        {
            for (int j = 0; j < r->m_size_col; j++)
            {
                int offset = (i + r->m_start_row) * m_ncols + j + r->m_start_col;
                (*r)[m_mask[offset]] ++;
            }
        }
        r->set_type();
    }

    // Do merge
    auto curr_chunk = chunks_in_row.begin();
    auto last_chunk = --chunks_in_row.end();

    #define NEXT_ITERATION {  /* std::cout << "Skip:\n    " << (**curr_chunk) << std::endl; */  curr_chunk++; continue;}
    while (curr_chunk != last_chunk && curr_chunk != chunks_in_row.end())
    {
        if ((*curr_chunk)->m_size_col > m_ncols * m_merge_threshold)
            NEXT_ITERATION; // chunks with more than `threshold * total` columns will be rejected
                            // This is to improve performance of out of order read
        auto next_chunk = std::next(curr_chunk);
        if ((*curr_chunk)->m_type == BLOCK_TYPE::MIXED && (*curr_chunk)->keys() != (*next_chunk)->keys())
            NEXT_ITERATION; // mixed chunks with different components can not be merged together
        if ((*curr_chunk)->get_major_index() != (*next_chunk)->get_major_index())
            NEXT_ITERATION; // chunks with different majors cannot be merged
        
        // merge keys
        auto curr_key = (*curr_chunk)->keys();
        auto next_key = (*next_chunk)->keys();
        int original_size = curr_key.size();
        for (int& k : next_key)
        {
            bool exists = false;
            for (int i = 0; i < original_size; i++)
                if (curr_key[i] == k) exists = true;
            if (!exists)
                curr_key.push_back(k);
        }
        if (curr_key.size() >= 3)
            NEXT_ITERATION; // reject chunks with >= 3 regions

        // std::cout << "Merge: \n    " << (**curr_chunk) << "\n    " << (**next_chunk) << std::endl;

        // start merge !
        (*curr_chunk)->m_size_col += (*next_chunk)->m_size_col; // reset size
        for (auto& nextKV : (*next_chunk)->m_maskid2nelems)
            (**curr_chunk)[nextKV.first] += nextKV.second; // merge keys and values
        (*curr_chunk)->set_type(); // reset type
        chunks_in_row.erase(next_chunk); // remove next chunk
        last_chunk = --chunks_in_row.end(); // update list tail
    }
#undef NEXT_ITERATION
}

void Mesh::export_to(EXPORT_TYPE type, std::ostream& os) const
{
    if (type == EXPORT_TYPE::HUMAN)
    {
        int i = 0;
        for (auto& row : m_partitions)
        {
            os << "ROW " << i++ << std::endl;
            std::for_each(row.begin(), row.end(), [&](auto& item){ os << "  " << (*item) << "\n"; });
            os << std::endl;
        }
    }
    else
    {
        for (auto& row : m_partitions)
            for (auto& chunk : row)
                os << chunk->m_start_col << " " << chunk->m_start_row << " "
                << chunk->m_size_col << " " << chunk->m_size_row
                << " " << chunk->get_major_index() << std::endl;         
    }
}

size_t Mesh::rows() const { return m_nrows; }

size_t Mesh::cols() const { return m_ncols; }

std::vector<int> Mesh::get_all_mask_id()
{
    std::set<int> indexs;
    if (!m_done)
        this->partition();
    for (auto& row : m_partitions)
        for (auto& chunk : row)
            for (int& index : chunk->keys())
                indexs.insert(index);
    return std::vector<int>(indexs.begin(), indexs.end());
}

} // namspace raster
