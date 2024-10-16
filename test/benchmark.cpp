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
    if(argc < 3)
    {
        std::cerr << "Usage: ./benchmark <INPUT_NETCDF> <INPUT_RASTER> <VARNAME>\n";
        return 1;
    };
    int status, ncid, varid, ndims, vartype, rank, dimids[5], region_id;
    size_t dimlens[5];
    float* buffer;
    std::string infile = argv[1], regionfile = argv[2], var = argv[3];
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // get some properties of dataset
    std::string if1 = infile.substr(0, infile.length() - 3) + "_" + std::to_string(rank) + ".nc";
    status = nc_open(if1.c_str(), NC_NETCDF4 | NC_NOWRITE, &ncid);

    status = nc_inq_varid(ncid, var.c_str(), &varid); ERR;
    status = nc_inq_varndims(ncid, varid, &ndims); ERR;
    status = nc_inq_vardimid(ncid, varid, dimids); ERR;
    for (int i = 0; i < ndims; i++)
        status = nc_inq_dimlen(ncid, dimids[i], &dimlens[i]);
    size_t bufsize = std::accumulate(&dimlens[0], &dimlens[ndims], 1, [](size_t a, size_t b){ return a * b; });
    buffer = new float[bufsize];
    assert(buffer != NULL);
    std::fill(&buffer[0], &buffer[bufsize], 0.0);
    if (rank == 0 && verbose)
    {
        printf("Data dim: %d, data shape = ( ", ndims);
        for (int i = 0; i < ndims; i++) printf("%d ", dimlens[i]);
            printf("), total = %ld\n", bufsize);        
    }

    // original process: (1) open; (2) read mask; (3) get BB; (4) read data using hyperslab reading
    auto t1 = high_resolution_clock::now();
    status = nc_get_var(ncid, varid, buffer); ERR;
    auto t2 = high_resolution_clock::now();
    status = nc_close(ncid); ERR;
    printf("Time_netCDF_Read=%fs\n", duration_cast<microseconds>(t2 - t1).count() / 1000000.0);
    MPI_Barrier(MPI_COMM_WORLD);

    // raster read process
    std::string if2 = regionfile.substr(0, regionfile.length() - 3) + "_" + std::to_string(rank) + ".nc"; 
    status = nc_open(if2.c_str(), NC_NETCDF4 | NC_NOWRITE, &ncid); ERR;
    status = raster_inq_varid(ncid, var.c_str(), &varid); ERR;
    status = get_var_dimlens(ncid, varid, &ndims, dimlens); ERR;
    delete[] buffer;
    bufsize = std::accumulate(&dimlens[0], &dimlens[ndims], 1, [](size_t a, size_t b){ return a * b; });
    buffer = new float[bufsize];
    auto t3 = high_resolution_clock::now();
    status = raster_get_var_float(ncid, varid, buffer); ERR;
    auto t4 = high_resolution_clock::now();
    status = nc_close(ncid); ERR;
    printf("Time_RASTER_Read=%fs\n", duration_cast<microseconds>(t4 - t3).count() / 1000000.0);
    MPI_Barrier(MPI_COMM_WORLD);

    MPI_Finalize();

    delete[] buffer;
    return status;
}
