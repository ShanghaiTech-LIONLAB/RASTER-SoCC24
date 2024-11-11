#!/usr/bin/python3
import os, sys, json
import subprocess
import re
import numpy as np
import shutil
import logging

regions = [['1'], ['2'], ['3'], ['6'], ['10'], ['10', '2', '3'], ['1', '3', '6'], ['2', '3', '6'], ['2', '6', '1'], ['1', '2', '10']] # CESM Region
#regions = [['12'], ['0'], ['5'], ['25'], ['22']] # WRF3 dataset, hubei, anhui, guangdong, sichuan, shandong
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
    logging.info(' '.join(convertargs))

    proc = subprocess.Popen(convertargs, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    out  = proc.stdout.read().decode('ASCII')
    logging.debug(out)
    logging.info([float(i) for i in re.compile('Time_ordinary_netCDF=([.\d]+)s').findall(out)])

    c1 = np.average([float(i) for i in re.compile('Time_ordinary_netCDF=([.\d]+)s').findall(out)])
    c2 = np.average([float(i) for i in re.compile('Time_RASTER=([.\d]+)s').findall(out)])

    print("Convert time:", c1, c2)

    drop_cache()

    result = list()

    for region in regions:
        print('Region:', region)
        for time in range(times):

            drop_cache()

            readargs = mpirun +  ['--allow-run-as-root', '-np', str(nprocs), './test_masked_multi_read', 
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

            print("Time:", time, r1, r2)
            result.append((r1, r2))

        logging.info(result)
        print(f'{region} Read time:', np.average(result, axis=0))
    return (r1, r2)


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
