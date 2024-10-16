#!/usr/bin/python3
import os, sys, json
import subprocess
import re
import numpy as np
import shutil

def benchmark(conf, times):
    fpath   = conf['filepath']
    nprocs  = conf['nprocs']
    host    = conf['hostfile']
    mask    = conf['mask']
    varname = conf['varname']
    scale   = conf['scale']
    outfn   = conf['outfn']
    prefix  = outfn[:-3]

    outdir = os.path.dirname(outfn)
    shutil.rmtree(outdir)
    os.makedirs(outdir, exist_ok=True)

    os.system("echo 3 > /proc/sys/vm/drop_caches")

    convertargs  = ['mpirun', '--allow-run-as-root', '-np', str(nprocs), './test_convert', fpath, outfn, mask, varname, str(scale)]
    print(' '.join(convertargs))

    proc = subprocess.Popen(convertargs, stdout=subprocess.PIPE)
    out  = proc.stdout.read().decode('ASCII')

    os.system("echo 3 > /proc/sys/vm/drop_caches")

    result = list()

    for _ in range(times):

        os.system("echo 3 > /proc/sys/vm/drop_caches")

        readargs = ['mpirun', '--allow-run-as-root', '-np', str(nprocs), './test_benchmark', 
                    prefix+"_plain.nc", prefix+"_region.nc", varname]    
        
        proc = subprocess.Popen(readargs, stdout=subprocess.PIPE)
        out  = proc.stdout.read().decode('ASCII')

        print(out)

        r1 = np.average([float(i) for i in re.compile('Time_netCDF_Read=([.\d]+)s').findall(out)])
        r2 = np.average([float(i) for i in re.compile('Time_RASTER_Read=([.\d]+)s').findall(out)])

        os.system("echo 3 > /proc/sys/vm/drop_caches")

        adiosargs = ['mpirun', '--allow-run-as-root', '-np', str(nprocs), './test_benchmark_adios', 
                        prefix+"_adios.nc", varname]

        proc = subprocess.Popen(adiosargs, stdout=subprocess.PIPE)
        out  = proc.stdout.read().decode('ASCII') 
        print(out)

        r3 = np.average([float(i) for i in re.compile('Time_adios2_Read=([.\d]+)s').findall(out)])


        # proc = subprocess.Popen(convertargs, stdout=subprocess.PIPE)
        # out  = proc.stdout.read().decode('ASCII')

        # os.system("echo 3 > /proc/sys/vm/drop_caches")

        print(r1, r2, r3)
        result.append((r1, r2, r3))

    print(result)
    print(np.average(result, axis=0))
    return (r1, r2, r3)


if __name__ == '__main__':
    config, times = sys.argv[1], sys.argv[2]
    if not os.path.exists(config):
        print("Config file not found\n")
        exit(1)
    with open(config) as f:
        conf = json.load(f)

    benchmark(conf, int(times))
    # l = []
    # for _ in range(int(times)):
    #     l.append(benchmark(conf))
    
    # print(l)
    # print(np.average(l, axis=0))
