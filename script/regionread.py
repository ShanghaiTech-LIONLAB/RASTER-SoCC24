#!/usr/bin/python3
import os, sys, json
import subprocess
import re
import numpy as np
import shutil
import logging

regions = [['6'], ['1']]
nps = [1,2,4,8]

logging.basicConfig(encoding='utf-8', level=logging.WARNING)

def drop_cache():
    try:
        os.system("echo 3 > /proc/sys/vm/drop_caches")
    except:
        pass

def benchmark(conf, times, nump=-1):
    fpath   = conf['filepath']
    nprocs  = conf['nprocs']
    if nump!=-1:
        nprocs = nump
    host    = conf['hostfile']
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

    convertargs  = ['mpirun', '--allow-run-as-root', '-np', str(nprocs), './test_convert', fpath, outfn, mask, varname, str(scale)]
    logging.info(' '.join(convertargs))

    proc = subprocess.Popen(convertargs, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    out  = proc.stdout.read().decode('ASCII')
    logging.debug(out)
    logging.info([float(i) for i in re.compile('Time_ordinary_netCDF=([.\d]+)s').findall(out)])

    c1 = np.average([float(i) for i in re.compile('Time_ordinary_netCDF=([.\d]+)s').findall(out)])
    c2 = np.average([float(i) for i in re.compile('Time_RASTER=([.\d]+)s').findall(out)])

    adiosconvertargs = ['mpirun', '--allow-run-as-root', '-np', str(nprocs), './test_convert_adios', fpath, outfn, mask, varname, str(scale)]
    adiosconvertproc = subprocess.Popen(adiosconvertargs, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    out  = adiosconvertproc.stdout.read().decode('ASCII')
    logging.debug(out)

    c3 = np.average([float(i) for i in re.compile('Time_adios2=([.\d]+)s').findall(out)])

    print("Convert time:", c1, c2, c3)

    drop_cache()

    result = list()

    for region in regions:
        for _ in range(times):

            drop_cache()

            readargs = ['mpirun', '--allow-run-as-root', '-np', str(nprocs), './test_masked_multi_read', 
                        prefix+"_plain.nc", prefix+"_region.nc", varname, mask]    
            
            logging.info(' '.join(readargs + region))
            proc = subprocess.Popen(readargs + region, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            out  = proc.stdout.read().decode('ASCII')

            logging.debug(out)
            
            assert len(re.compile('Time_netCDF_Read=([.\d]+)s').findall(out)) == nprocs
            assert len(re.compile('Time_RASTER_Read=([.\d]+)s').findall(out)) == nprocs

            r1 = np.average([float(i) for i in re.compile('Time_netCDF_Read=([.\d]+)s').findall(out)])
            r2 = np.average([float(i) for i in re.compile('Time_RASTER_Read=([.\d]+)s').findall(out)])

            drop_cache()

            adiosargs = ['mpirun', '--allow-run-as-root', '-np', str(nprocs), './test_masked_multi_read_adios2', 
                            prefix+"_plain.nc", prefix+"_region.nc", '/' + varname, '/' + mask]

            logging.info(' '.join(adiosargs + region))
            proc = subprocess.Popen(adiosargs + region, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            out  = proc.stdout.read().decode('ASCII') 

            logging.debug(out)

            assert len(re.compile('Time_adios2_Read=([.\d]+)s').findall(out)) == nprocs
            r3 = np.average([float(i) for i in re.compile('Time_adios2_Read=([.\d]+)s').findall(out)])

            drop_cache()

            # print(r1, r2, r3)
            result.append((r1, r2, r3))

        logging.info(result)
        print(f'{region} Read time:', np.average(result, axis=0))
    return (r1, r2, r3)


if __name__ == '__main__':
    config, times = sys.argv[1], sys.argv[2]
    if not os.path.exists(config):
        print("Config file not found\n")
        exit(1)
    with open(config) as f:
        conf = json.load(f)

    for nump in nps:
        print(f"Process {nump}")
        benchmark(conf, int(times), nump)
