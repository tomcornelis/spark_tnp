import os
import glob
import getpass
from pyspark.sql import SparkSession

from muon_definitions import (get_allowed_sub_eras)


def run_convert(spark, particle, probe, resonance, era, subEra):
    '''
    Converts a directory of root files into parquet
    '''

    baseDir = os.path.join(
        '/hdfs/analytix.cern.ch/user',
        getpass.getuser(), 'root',
        particle, resonance, era, subEra
        )

    fnames = glob.glob(os.path.join(baseDir, f'{baseDir}/*.root'))
    fnames = [f.replace('/hdfs/analytix.cern.ch',
                        'hdfs://analytix') for f in fnames]

    outDir = os.path.join('parquet', particle, resonance, era, subEra)
    outname = os.path.join(outDir, 'tnp.parquet')

    treename = 'tpTree/fitter_tree'

    # process batchsize files at a time
    batchsize = 500
    new = True
    while fnames:
        current = fnames[:batchsize]
        fnames = fnames[batchsize:]

        rootfiles = spark.read.format("root")\
                         .option('tree', treename)\
                         .load(current)
        # merge rootfiles. chosen to make files of 8-32 MB (input)
        # become at most 1 GB (parquet recommendation)
        # https://parquet.apache.org/documentation/latest/
        # .coalesce(int(len(current)/32)) \
        # but it is too slow for now, maybe try again later
        if new:
            rootfiles.write.parquet(outname)
            new = False
        else:
            rootfiles.write.mode('append')\
                     .parquet(outname)


def run_all(particle, probe, resonance, era):
    subEras = get_allowed_sub_eras(resonance, era)

    local_jars = ','.join([
        './laurelin-1.0.0.jar',
        './log4j-api-2.13.0.jar',
        './log4j-core-2.13.0.jar',
    ])

    spark = SparkSession\
        .builder\
        .appName("TnP")\
        .config("spark.jars", local_jars)\
        .config("spark.driver.extraClassPath", local_jars)\
        .config("spark.executor.extraClassPath", local_jars)\
        .getOrCreate()

    sc = spark.sparkContext
    print(sc.getConf().toDebugString())

    for subEra in subEras:
        print('Converting', particle, probe, resonance, era, subEra)
        run_convert(spark, particle, probe, resonance, era, subEra)

    spark.stop()
