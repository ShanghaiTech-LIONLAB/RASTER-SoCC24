#include <iostream>
#include <string>
#include <numeric>
#include <chrono>
#include <netcdf.h>
#include <assert.h>
#include <mpi.h>
#include <fcntl.h>
#include <unistd.h>
#include <adios2.h>
#include "../raster.h"
#define ERR do{if (status != NC_NOERR){ fprintf(stderr, "Error: at line %d, %s\n", __LINE__, nc_strerror(status)); exit(status);} } while(0);
using namespace std::chrono;

#define DIM1_NCHUNKS 20
#define DIM2_NCHUNKS 20

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    int rank, size;
    if(argc <= 4)
    {
        std::cerr << "Usage: ./convert <INPUT_FILENAME> <OUTPUT_FILENAME> <MASKNAME> <VARNAME> <SCLAING_FACTOR>\n";
        std::cerr << " It writes VARNAME in INPUT_FILE to OUTPUT_FILE, according to MASK\n";
        return 1;
    };
    std::string infile = argv[1], outfile = argv[2], mask = argv[3], varname = argv[4];
    int status, ncid, varid, ndims, vartype, dimids[5], scaling_factor = std::atoi(argv[5]);
    size_t dimlens[5], chunksize[5];
    char* dimnames[5] = {(char*)"x", (char*)"y", (char*)"z", (char*)"w", (char*)"h"};
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // read mask
    status = nc_open(infile.c_str(), NC_NETCDF4 | NC_NOWRITE, &ncid);
    status = nc_inq_varid(ncid, mask.c_str(), &varid); ERR;
    status = nc_inq_varndims(ncid, varid, &ndims); ERR;
    status = nc_inq_vardimid(ncid, varid, dimids); ERR;
    status = nc_inq_vartype(ncid, varid, &vartype); ERR;
    for (int i = 0; i < ndims; i++)
        status = nc_inq_dimlen(ncid, dimids[i], &dimlens[i]);
    size_t masksize = std::accumulate(&dimlens[0], &dimlens[ndims], 1, [](size_t a, size_t b){ return a * b; });
    int* maskbuffer = new int[masksize];
    status = nc_get_var(ncid, varid, maskbuffer); ERR;
    if (vartype == NC_FLOAT)
    {
        for (int i = 0; i < masksize; i++)
            maskbuffer[i] = int(*((float*)(maskbuffer + i)));
    }
    if (rank == 0)
        printf("Mask dim: %d, mask shape = ( %ld %ld ), total = %ld\n", ndims, dimlens[ndims-2], dimlens[ndims-1], masksize);

    // read data
    status = nc_inq_varid(ncid, varname.c_str(), &varid); ERR;
    status = nc_inq_varndims(ncid, varid, &ndims); ERR;
    status = nc_inq_vardimid(ncid, varid, dimids); ERR;
    for (int i = 0; i < ndims; i++)
        status = nc_inq_dimlen(ncid, dimids[i], &dimlens[i]);
    size_t bufsize = std::accumulate(&dimlens[0], &dimlens[ndims], 1, [](size_t a, size_t b){ return a * b; });
    // scale the data
    bufsize *= scaling_factor;
    dimlens[0] *= scaling_factor;
    float* buffer = new float[bufsize];
    // read data
    status = nc_get_var_float(ncid, varid, buffer); ERR;
    status = nc_close(ncid); ERR;
    if (rank == 0)
    {
        printf("Data dim: %d, data shape = ( ", ndims);
        for (int i = 0; i < ndims; i++) printf("%ld ", dimlens[i]);
            printf("), total = %ld\n", bufsize);        
    }
    MPI_Barrier(MPI_COMM_WORLD);

    adios2::ADIOS adios;
    adios2::IO hdf5IO = adios.DeclareIO("WriteHDF5");
    hdf5IO.SetEngine("HDF5");
    hdf5IO.SetParameter("IdleH5Writer",
                            "true"); // set this if not all ranks are writting

    adios2::Variable<int> mask_var = hdf5IO.DefineVariable<int>((mask).c_str(), std::vector<size_t>(dimlens+ndims-2, dimlens+ndims), {0, 0}, std::vector<size_t>(dimlens+ndims-2, dimlens+ndims), adios2::ConstantDims);

    std::string of_adios = outfile.substr(0, outfile.length() - 3) + "_adios_" + std::to_string(rank) + ".nc"; 
    // std::string cmd = "cp " + ofn2 + " " + of_adios;
    // system(cmd.c_str());
    adios2::Engine h5Writer = hdf5IO.Open(of_adios, adios2::Mode::Write);
    h5Writer.Put<int>(mask_var, maskbuffer);

    adios2::Variable<float> var_var = hdf5IO.DefineVariable<float>((varname).c_str(), std::vector<size_t>(dimlens, dimlens+ndims), {0, 0, 0, 0}, std::vector<size_t>(dimlens, dimlens+ndims), adios2::ConstantDims);

    MPI_Barrier(MPI_COMM_WORLD);
    auto t5 = high_resolution_clock::now();
    h5Writer.Put<float>(var_var, buffer);
    h5Writer.Close();
    auto fd2 = open(of_adios.c_str(), O_RDONLY);
    fsync(fd2);
    close(fd2);
    auto t6 = high_resolution_clock::now();
    printf("Time_adios2=%fs\n", duration_cast<microseconds>(t6 - t5).count() / 1000000.0);

    MPI_Barrier(MPI_COMM_WORLD);

    delete[] buffer;
    delete[] maskbuffer;
    MPI_Finalize();
    return status;
}