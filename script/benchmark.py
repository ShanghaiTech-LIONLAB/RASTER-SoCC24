#!/usr/bin/python3
import os, sys, json
import subprocess
import re
import numpy as np
import shutil

def drop_cache():
    try:
        os.system("echo 3 > /proc/sys/vm/drop_caches")
    except:
        pass


def benchmark(conf, times):
    fpath   = conf['filepath']
    nprocs  = conf['nprocs']
    host    = conf['hostfile']
    if len(host) > 0:
        hostfile = ['--hostfile', host]
    else:
        hostfile = []
    mask    = conf['mask']
    varname = conf['varname']
    scale   = conf['scale']
    outfn   = conf['outfn']
    prefix  = outfn[:-3]
    outdir = os.path.dirname(outfn)
    os.makedirs(outdir, exist_ok=True)
    shutil.rmtree(outdir)
    os.makedirs(outdir, exist_ok=True)

    drop_cache()
    
    mpirun = ['mpirun'] + hostfile

    convertargs  = mpirun + ['--allow-run-as-root', '-np', str(nprocs), './test_convert', fpath, outfn, mask, varname, str(scale)]
    print(' '.join(convertargs))

    proc = subprocess.Popen(convertargs, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    out  = proc.stdout.read().decode('ASCII')
    # print(out)

    c1 = np.average([float(i) for i in re.compile('Time_ordinary_netCDF=([.\d]+)s').findall(out)])
    c2 = np.average([float(i) for i in re.compile('Time_RASTER=([.\d]+)s').findall(out)])

    print("Convert time:", c1, c2)

    os.system("echo 3 > /proc/sys/vm/drop_caches")

    result = list()

    for time in range(times):

        drop_cache()

        readargs = mpirun + ['--allow-run-as-root', '-np', str(nprocs), './test_benchmark', 
                    prefix+"_plain.nc", prefix+"_region.nc", varname]    
        print(' '.join(readargs))
        
        proc = subprocess.Popen(readargs, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        out  = proc.stdout.read().decode('ASCII')

        #print(out)
        
        assert len(re.compile('Time_netCDF_Read=([.\d]+)s').findall(out)) == nprocs
        assert len(re.compile('Time_RASTER_Read=([.\d]+)s').findall(out)) == nprocs

        r1 = np.average([float(i) for i in re.compile('Time_netCDF_Read=([.\d]+)s').findall(out)])
        r2 = np.average([float(i) for i in re.compile('Time_RASTER_Read=([.\d]+)s').findall(out)])

        drop_cache()

        print("Time: ", time, r1, r2)

        result.append((r1, r2))

    print(result)
    print('Read time:', np.average(result, axis=0))
    return (r1, r2)


if __name__ == '__main__':
    config, times = sys.argv[1], sys.argv[2]
    if not os.path.exists(config):
        print("Config file not found\n")
        exit(1)
    with open(config) as f:
        conf = json.load(f)

    benchmark(conf, int(times))
