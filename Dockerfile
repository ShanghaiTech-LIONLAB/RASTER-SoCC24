FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update \
    && apt install -y \
    wget \
    unzip \
    build-essential \
    cmake \
    git \
    libnetcdf-dev \
    python3-pip \
    openmpi-bin \
    openmpi-common \
    libopenmpi-dev \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install -i https://pypi.tuna.tsinghua.edu.cn/simple numpy

WORKDIR /build

RUN apt install -y wget \
    && wget https://github.com/HDFGroup/hdf5/archive/refs/tags/hdf5-1_14_1-2.zip \
    && unzip hdf5-1_14_1-2.zip \
    && cd hdf5-hdf5-1_14_1-2 \
    && CC=$(which mpicc) ./configure --enable-parallel --prefix=/phdf5 \
    && make -j64 \
    && make install

WORKDIR /build

RUN git clone --depth=1 https://github.com/ornladios/ADIOS2.git ADIOS2 \
    && mkdir adios2-build \
    && cd adios2-build \
    && cmake -DHDF5_ROOT=/phdf5 -DADIOS2_USE_HDF5=ON -DADIOS2_USE_MPI=ON ../ADIOS2 \
    && make -j64 \
    && make install
 
WORKDIR /RASTER

COPY . .

RUN mkdir build \
    && cd build \
    && cmake .. \
    && make -j64 \
    && cp ../script/* ./