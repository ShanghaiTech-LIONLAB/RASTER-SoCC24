#include <iostream>
#include <string>
#include <numeric>
#include <tuple>
#include <chrono>
#include <netcdf.h>
#include <assert.h>
#include <mpi.h>
#include <adios2.h>
#include "../raster.h"
#define ERR do{ if (status != NC_NOERR){ printf("Error at line %d: %s\n", __LINE__, nc_strerror(status)); exit(status);} } while(0);
using namespace std::chrono;

static bool verbose = true;

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    if(argc < 2)
    {
        std::cerr << "Usage: ./benchmark <INPUT_ADIOS> <VARNAME>\n";
        return 1;
    };
    int status, ncid, varid, ndims, vartype, rank, dimids[5], region_id;
    size_t dimlens[5];
    float* buffer;
    std::string infile = argv[1], var = argv[2];
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // if (rank == 0)
        // system("echo 3 > /proc/sys/vm/drop_caches");
    MPI_Barrier(MPI_COMM_WORLD);
    // get some properties of dataset
    std::string if1 = infile.substr(0, infile.length() - 3) + "_" + std::to_string(rank) + ".nc";
    
    size_t bufsize;

    // adios2
    adios2::ADIOS adios;
    adios2::IO h5IO = adios.DeclareIO("ReadHDF5");
    h5IO.SetEngine("HDF5");
    h5IO.SetParameter("H5CollectiveMPIO", "yes"); // bad
    // h5IO.SetParameter("H5ChunkVar", "/CaCO3_FLUX_IN");
    // h5IO.SetParameter("H5ChunkDim", "1 1 20 20");
    //adios2::Engine h5Reader = h5IO.Open(if1.c_str(), adios2::Mode::Read, MPI_COMM_WORLD);
    adios2::Engine h5Reader = h5IO.Open(if1.c_str(), adios2::Mode::Read);
    std::string root_prefix = "";

    // auto mask_var = h5IO.InquireVariable((root_prefix + "REGION_MASK").c_str());
    // auto shape = mask_var.Shape();
    // std::cout << shape.size() << std::endl;
    // std::cout << shape[0] << " " << shape[1] << std::endl;
    var = root_prefix + var;
    adios2::VariableNT data_var = h5IO.InquireVariable(var.c_str());
    ndims = data_var.Shape().size();
    if (rank == 0)
        std::cout << "Data dim: " << ndims << ", data shape: ( ";
    for (int i = 0; i < ndims; i++) {
        dimlens[i] = data_var.Shape()[i];
        if (rank == 0)
            std::cout << dimlens[i] << " ";
    }
    bufsize = std::accumulate(&dimlens[0], &dimlens[ndims], 1, [](size_t a, size_t b){ return a * b; });
    if (rank == 0)
        std::cout << "), total = " << bufsize << std::endl;
    buffer = new float[bufsize];
    std::fill(&buffer[0], &buffer[bufsize], 0.0);

    auto t5 = high_resolution_clock::now();
    // status = nc_get_vara_float(ncid, varid, start, count, buffer); ERR;
    h5Reader.Get(data_var, buffer, adios2::Mode::Sync);
    h5Reader.PerformGets();
    auto t6 = high_resolution_clock::now();
    h5Reader.Close();
    printf("Time_adios2_Read=%fs\n", duration_cast<microseconds>(t6 - t5).count() / 1000000.0);
    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0)
        system("echo 3 > /proc/sys/vm/drop_caches");

    MPI_Finalize();

    delete[] buffer;
    return 0;
}
