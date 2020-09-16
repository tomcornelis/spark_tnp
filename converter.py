#!/usr/bin/env python3
import os, shutil
import glob
import getpass
from pyspark.sql import SparkSession

from muon_definitions import (get_allowed_sub_eras)

# Using wildcards for the filenames, the glob will catch the right files
user      = getpass.getuser() 
sourceDir = '/eos/user/%s/%s/tnpTuples_muons/updated7' % (user[0], user)
fnamesMap = {
    'Z': {
        'Run2016': {
            'Run2016B': '*Run2016B*.root',
            'Run2016C': '*Run2016C*.root',
            'Run2016D': '*Run2016D*.root',
            'Run2016E': '*Run2016E*.root',
            'Run2016F': '*Run2016F*.root',
            'Run2016G': '*Run2016G*.root',
            'Run2016H': '*Run2016H*.root',
            'DY_madgraph': 'MC_Moriond17_DY*.root',
        },
        'Run2017': {
            'Run2017B': '*Run2017B*.root',
            'Run2017C': '*Run2017C*.root',
            'Run2017D': '*Run2017D*.root',
            'Run2017E': '*Run2017E*.root',
            'Run2017F': '*Run2017F*.root',
            'DY_madgraph': 'TnPTree_94X_DYJetsToLL_M50_Madgraph*.root',
        },
        'Run2018': {
            'Run2018A': '*Run2018A*.root',
            'Run2018B': '*Run2018B*.root',
            'Run2018C': '*Run2018C*.root',
            'Run2018D': '*Run2018D*.root',
            'DY_madgraph': 'TnPTreeZ_102XAutumn18_DYJetsToLL_M50_MadgraphMLM*.root'
        },
    },
    'JPsi': {
        'Run2016': {
            'Run2016B': 'jpsi/*Run2016B*.root',
            'Run2016C': 'jpsi/*Run2016C*.root',
            'Run2016D': 'jpsi/*Run2016D*.root',
            'Run2016E': 'jpsi/*Run2016E*.root',
            'Run2016F': 'jpsi/*Run2016F*.root',
            'Run2016G': 'jpsi/*Run2016G*.root',
            'Run2016H': 'jpsi/*Run2016H*.root',
            'DY_madgraph': 'jpsi/tnpJPsi_MC_JpsiPt8_TuneCUEP8M1*.root',
        },
        'Run2017': {
            'Run2017B': 'jpsi/*Run2017B*.root',
            'Run2017C': 'jpsi/*Run2017C*.root',
            'Run2017D': 'jpsi/*Run2017D*.root',
            'Run2017E': 'jpsi/*Run2017E*.root',
            'Run2017F': 'jpsi/*Run2017F*.root',
            'DY_madgraph': 'jpsi/TnPTreeJPsi_94X_JpsiToMuMu_Pythia8*.root',
        },
        'Run2018': {
            'Run2018A': 'jpsi/*Run2018A*.root',
            'Run2018B': 'jpsi/*Run2018B*.root',
            'Run2018C': 'jpsi/*Run2018C*.root',
            'Run2018D': 'jpsi/*Run2018D*.root',
            'DY_madgraph': 'jpsi/TnPTreeJPsi_102XAutumn18_JpsiToMuMu_JpsiPt8_Pythia8*.root'
        },
    },
}



def run_convert(spark, particle, probe, resonance, era, subEra):
    '''
    Converts a directory of root files into parquet
    '''



    fnames = glob.glob(os.path.join(sourceDir, fnamesMap[resonance][era][subEra]))
 
    outDir = os.path.join('parquet', particle, resonance, era, subEra)
    outname = os.path.join(outDir, 'tnp.parquet')
    try:    shutil.rmtree('/hdfs/analytix.cern.ch/user/%s/%s' % (user, outname)) # remove previous tests or ROOT to parquet conversions
    except: pass

    treename = 'tpTree/fitter_tree'

    # process batchsize files at a time
    batchsize = 1
    new = True
    while fnames:
        current = fnames[:batchsize]
        fnames = fnames[batchsize:]

        current_analytix = [i.replace(sourceDir, 'hdfs://analytix/user/%s/' % user).replace('jpsi/','') for i in current]
        for x, y in zip(current, current_analytix):
          shutil.copy(x, y.replace('hdfs://analytix', '/hdfs/analytix.cern.ch').replace('jpsi/',''))

        print('%s %s' % (treename, current))
        rootfiles = spark.read.format("root").option('tree', treename).load(current_analytix)
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

        for i in current_analytix: # immediately delete the files from analytix because of ridicously low disk quota
          os.remove(i.replace('hdfs://analytix', '/hdfs/analytix.cern.ch').replace('jpsi/', ''))
        shutil.rmtree('/hdfs/analytix.cern.ch/user/%s/.Trash' % user) # because otherwise you reach the disk quota because of the trash bin


def run_all(particle, probe, resonance, era):
    subEras = get_allowed_sub_eras(resonance, era)

    local_jars = ','.join([
        './laurelin-1.1.1.jar',
        './log4j-api-2.13.0.jar',
        './log4j-core-2.13.0.jar',
    ])

    spark = SparkSession\
        .builder\
        .appName("TnP")\
        .config("spark.jars", local_jars)\
        .config("spark.driver.extraClassPath", local_jars)\
        .config("spark.executor.extraClassPath", local_jars)\
        .config("spark.debug.maxToStringFields", 500)\
        .getOrCreate()

    sc = spark.sparkContext
    print(sc.getConf().toDebugString())

    for subEra in fnamesMap[resonance][era]:
        print('Converting', particle, probe, resonance, era, subEra)
        run_convert(spark, particle, probe, resonance, era, subEra)

    spark.stop()


#run_all('muon', 'generalTracks', 'Z', 'Run2016')
#run_all('muon', 'generalTracks', 'Z', 'Run2017')
#run_all('muon', 'generalTracks', 'Z', 'Run2018')
run_all('muon', 'generalTracks', 'JPsi', 'Run2016')
#run_all('muon', 'generalTracks', 'JPsi', 'Run2017')
#run_all('muon', 'generalTracks', 'JPsi', 'Run2018')
