#include <iostream>
#include <string>
#include <numeric>
#include <tuple>
#include <chrono>
#include <adios2.h>
// #include <netcdf.h>
#include <assert.h>
#include <cstring>
#include <mpi.h>
// #include "../raster.h"
#define ERR do{ if (status != NC_NOERR){ printf("Error at line %d: %s\n", __LINE__, nc_strerror(status)); exit(status);} } while(0);
using namespace std::chrono;

static bool verbose = true;

// scan the mask to find the upper-left corber, height and width
auto get_bounding_box(int* mask, int h, int w, int region_id)
{
    int rs = h + 1, cs = w + 1, rt = -1, ct = -1;
    for (int i = 0; i < h; i++)
    {
        for (int j = 0; j < w; j++)
        {
            if (mask[i*w + j] == region_id)
            {
                if (i < rs) rs = i;
                if (j < cs) cs = j;
                if (i > rt) rt = i;
                if (j > ct) ct = j;
            }
        }
    }
    return std::make_tuple(rs, cs, rt, ct);
}

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    if(argc <= 5)
    {
        std::cerr << "Usage: ./masked_multi_read <INPUT_NETCDF> <INPUT_RASTER> <VARNAME> <MASKNAME> [<REGION_ID>]+\n";
        std::cerr << " It reads VARNAME in INPUT_FILE and REGION_FILE, compares read peformance of accessing [REGION_ID]\n";
        return 1;
    };
    int status, ncid, varid, ndims, vartype, rank, dimids[5], region_id;
    size_t dimlens[5];
    float* buffer;
    std::string infile = argv[1], regionfile = argv[2], var = argv[3], mask = argv[4];
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // adios2::ADIOS adios(MPI_COMM_WORLD);
    // adios2::ADIOS adios();
    // adios
    adios2::ADIOS adios;
    adios2::IO h5IO = adios.DeclareIO("ReadHDF5");
    h5IO.SetEngine("HDF5");
    std::string if1 = infile.substr(0, infile.length() - 3) + "_" + std::to_string(rank) + ".nc";
    adios2::Engine h5Reader = h5IO.Open(if1.c_str(), adios2::Mode::Read);
    adios2::VariableNT mask_var = h5IO.InquireVariable(mask.c_str());
    ndims = mask_var.Shape().size();
    for (size_t i = 0; i < ndims; ++i) {
        dimlens[i] = mask_var.Shape()[i];
    }
    size_t masksize = std::accumulate(&dimlens[0], &dimlens[ndims], 1, [](size_t a, size_t b){ return a * b; });
    int* maskbuffer = new int[masksize];
    h5Reader.Get(mask_var, maskbuffer, adios2::Mode::Sync);
    auto type = mask_var.Type();
    if (type == "float") {
        for (int i = 0; i < masksize; i++)
            maskbuffer[i] = int(*((float*)(maskbuffer + i)));
    }
    if (rank == 0 && verbose)
        printf("Mask dim: %d, mask shape = (%ld %ld), total = %ld\n", ndims, dimlens[ndims-2], dimlens[ndims-1], masksize);

    adios2::VariableNT data_var = h5IO.InquireVariable(var.c_str());
    ndims = data_var.Shape().size();
    for (int i = 0; i < ndims; i++)
        dimlens[i] = data_var.Shape()[i];
    size_t bufsize = std::accumulate(&dimlens[0], &dimlens[ndims], 1, [](size_t a, size_t b){ return a * b; });
    buffer = new float[bufsize];
    std::fill(&buffer[0], &buffer[bufsize], 0.0);
    if (rank == 0 && verbose)
    {
        printf("Data dim: %d, data shape = ( ", ndims);
        for (int i = 0; i < ndims; i++) printf("%d ", dimlens[i]);
            printf("), total = %ld\n", bufsize);        
    }

    // final bounding box
    int rf = 10000, cf = 10000, rtf = -1, ctf = -1;
    for (int i = 5; i < argc; i++)
    {
        region_id = std::atoi(argv[i]);
        auto [r, c, rt, ct] = get_bounding_box(maskbuffer, dimlens[ndims-2], dimlens[ndims-1], region_id);
        if (verbose && rank == 0)
            printf(" box starts at: (%d %d %d %d)\n", r,c,rt,ct);
        if (r < rf) rf = r;
        if (c < cf) cf = c;
        if (rt > rtf) rtf = rt;
        if (ct > ctf) ctf = ct;
    }
    size_t* start = new size_t[ndims], *count = new size_t[ndims];
    for (int i = 0; i < ndims-2; i++) { start[i] = 0; count[i] = dimlens[i]; }
    start[ndims - 2] = rf, start[ndims - 1] = cf;
    count[ndims - 2] = rtf-rf+1, count[ndims - 1] = ctf-cf+1;
    if (rank == 0 && verbose)
    {
        printf("bounding box starts at: (%d %d %d %d)\n", start[0], start[1], start[2], start[3]);
        printf("bounding box size: (%d %d %d %d)\n", count[0], count[1], count[2], count[3]);        
    }
    printf("%ld\n", count[0]*count[1]*count[2]*count[3]);
    adios2::Dims start_vec(start, start+4);
    adios2::Dims count_vec(count, count+4);
    data_var.SetSelection({start_vec, count_vec});
    // data_var.SetSelection({{0,0,0,0}, {1,1,1,1}});
    auto t1 = high_resolution_clock::now();
    h5Reader.Get(data_var, buffer, adios2::Mode::Sync);
    auto t2 = high_resolution_clock::now();
    h5Reader.Close();
    printf("Time_adios2_Read=%fs\n", duration_cast<microseconds>(t2 - t1).count() / 1000000.0);
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();

    delete[] start; delete[] count;
    delete[] buffer;
    delete[] maskbuffer;
    return 0;
}