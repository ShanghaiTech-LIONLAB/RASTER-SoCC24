#include <iostream>
#include <string>
#include <numeric>
#include <chrono>
#include <netcdf.h>
#include <assert.h>
#include <mpi.h>
#include <fcntl.h>
#include <unistd.h>
#include "../raster.h"
#define ERR do{if (status != NC_NOERR){ fprintf(stderr, "Error: at line %d, %s\n", __LINE__, nc_strerror(status)); exit(status);} } while(0);
using namespace std::chrono;

#define DIM1_NCHUNKS 20
#define DIM2_NCHUNKS 20

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    if(argc <= 4)
    {
        std::cerr << "Usage: ./convert <INPUT_FILENAME> <OUTPUT_FILENAME> <MASKNAME> <VARNAME> <SCLAING_FACTOR>\n";
        std::cerr << " It writes VARNAME in INPUT_FILE to OUTPUT_FILE, according to MASK\n";
        return 1;
    };
    std::string infile = argv[1], outfile = argv[2], mask = argv[3], varname = argv[4];
    int status, ncid, varid, ndims, vartype, rank, dimids[5], scaling_factor = std::atoi(argv[5]);
    size_t dimlens[5], chunksize[5];
    char* dimnames[5] = {(char*)"x", (char*)"y", (char*)"z", (char*)"w", (char*)"h"};
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

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

    // convert
    printf("rank=%d: writing netCDF data...\n", rank);
    std::string ofn2 = outfile.substr(0, outfile.length() - 3) + "_plain_" + std::to_string(rank) + ".nc";
    status = nc_create(ofn2.c_str(), NC_NETCDF4 | NC_CLOBBER, &ncid); ERR;
    for (int i = 0; i < ndims; i++)
        status = nc_def_dim(ncid, dimnames[i], dimlens[i], &dimids[i]);
    status = nc_def_var(ncid, mask.c_str(), NC_FLOAT, 2, dimids+ndims-2, &varid); ERR;
    status = nc_put_var_int(ncid, varid, maskbuffer); ERR;
    // define chunksize
    memcpy(chunksize, dimlens, 5*sizeof(size_t));
    chunksize[ndims - 1] /= DIM1_NCHUNKS;
    chunksize[ndims - 2] /= DIM2_NCHUNKS;
    status = nc_def_var(ncid, varname.c_str(), NC_FLOAT, ndims, dimids, &varid); ERR;
    // for (int i = 0; i < ndims; i++)
    status = nc_def_var_chunking(ncid, varid, NC_CHUNKED, chunksize); ERR;
    auto t3 = high_resolution_clock::now();
    status = nc_put_var_float(ncid, varid, buffer); ERR;
    nc_close(ncid); ERR;
    int fd2 = open(ofn2.c_str(), O_RDONLY);
    fsync(fd2);
    close(fd2);
    auto t4 = high_resolution_clock::now();
    printf("Time_ordinary_netCDF=%fs\n", duration_cast<microseconds>(t4 - t3).count() / 1000000.0); 
    printf("netCDF: done\n");
    MPI_Barrier(MPI_COMM_WORLD);

    // std::string of_adios = outfile.substr(0, outfile.length() - 3) + "_adios_" + std::to_string(rank) + ".nc"; 
    // std::string cmd = "cp " + ofn2 + " " + of_adios;
    // system(cmd.c_str());
    // fd2 = open(of_adios.c_str(), O_RDONLY);
    // fsync(fd2);
    // close(fd2);
    // MPI_Barrier(MPI_COMM_WORLD);

    printf("rank=%d: writing the same data using RASTER...\n", rank);
    std::string ofn1 = outfile.substr(0, outfile.length() - 3) + "_region_" + std::to_string(rank) + ".nc";
    status = nc_create(ofn1.c_str(), NC_NETCDF4 | NC_CLOBBER, &ncid); ERR;
    for (int i = 0; i < ndims; i++)
        status = nc_def_dim(ncid, dimnames[i], dimlens[i], &dimids[i]);
    status = raster_def_var(ncid, varname.c_str(), NC_FLOAT, ndims, dimids, &varid); ERR;
    status = raster_def_var_chunking(ncid, varid, maskbuffer); ERR;
    auto t1 = high_resolution_clock::now();
    status = raster_put_var_float(ncid, varid, buffer); ERR;
    nc_close(ncid); ERR;
    int fd1 = open(ofn1.c_str(), O_RDONLY);
    fsync(fd1);
    close(fd1);
    auto t2 = high_resolution_clock::now();
    printf("Time_RASTER=%fs\n", duration_cast<microseconds>(t2 - t1).count() / 1000000.0);
    printf("RASTER: done\n");
    MPI_Barrier(MPI_COMM_WORLD);

    delete[] buffer;
    delete[] maskbuffer;
    MPI_Finalize();
    return status;
}
