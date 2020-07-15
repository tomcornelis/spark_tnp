#!/usr/bin/env python3
import os
import glob
import getpass
from pyspark.sql import SparkSession

from muon_definitions import (get_allowed_sub_eras)


def run_convert(spark, particle, probe, resonance, era, subEra):
    '''
    Converts a directory of root files into parquet
    '''

  #  fnames = glob.glob(os.path.join('/*.root'))
    fnames = glob.glob('/hdfs/analytix.cern.ch/user/tomc/tnpTuples_muons/MC_Moriond17_DY_tranch4Premix_part*.root')
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

        rootfiles = spark.read.format("root").option('tree', treename).load(current)
        # merge rootfiles. chosen to make files of 8-32 MB (input)
        # become at most 1 GB (parquet recommendation)
        # https://parquet.apache.org/documentation/latest/
        # .coalesce(int(len(current)/32)) \
        # but it is too slow for now, maybe try again later
        if new:
            rootfiles.write.parquet(outname)
            new = False
        else:
            rootfiles.write.mode('append').parquet(outname)


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
        if not 'madgraph' in subEra: continue
        print('Converting', particle, probe, resonance, era, subEra)
        run_convert(spark, particle, probe, resonance, era, subEra)

    spark.stop()




baseDir = '/eos/user/t/tomc/tnpTuples_muons/updated'

fnamesMap = {
    'Z': {
        'Run2016': {
            'Run2016B': os.path.join(baseDir, 'TnPTreeZ_LegacyRereco07Aug17_SingleMuon_Run2016B_GoldenJSON.root'),
            'Run2016C': os.path.join(baseDir, 'TnPTreeZ_LegacyRereco07Aug17_SingleMuon_Run2016C_GoldenJSON.root'),
            'Run2016D': os.path.join(baseDir, 'TnPTreeZ_LegacyRereco07Aug17_SingleMuon_Run2016D_GoldenJSON.root'),
            'Run2016E': os.path.join(baseDir, 'TnPTreeZ_LegacyRereco07Aug17_SingleMuon_Run2016E_GoldenJSON.root'),
            'Run2016F': os.path.join(baseDir, 'TnPTreeZ_LegacyRereco07Aug17_SingleMuon_Run2016F_GoldenJSON.root'),
            'Run2016G': os.path.join(baseDir, 'TnPTreeZ_LegacyRereco07Aug17_SingleMuon_Run2016G2_GoldenJSON.root'),
            'Run2016H': os.path.join(baseDir, 'TnPTreeZ_LegacyRereco07Aug17_SingleMuon_Run2016H_GoldenJSON.root'),
            'DY_madgraph': [f for f in glob.glob(os.path.join(baseDir, 'MC_Moriond17_DY_tranch4Premix_part*.root')) if 'hadd' not in f],
        },
    },
}

run_all('muon', 'generalTracks', 'Z', 'Run2016')
