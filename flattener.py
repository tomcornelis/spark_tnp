from __future__ import print_function

import os
import itertools

import numpy as np
import pandas as pd
import uproot
from uproot_methods.classes import TH1

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from muon_definitions import (get_files,
                              get_weighted_dataframe,
                              get_binned_dataframe,
                              get_extended_eff_name,
                              get_full_name)

useParquet = True


def run_conversion(spark, particle, resonance, era, subEra,
                   config, shift='Nominal', **kwargs):
    _numerator = kwargs.pop('numerator', [])
    _denominator = kwargs.pop('denominator', [])
    _baseDir = kwargs.pop('baseDir', '')

    testing = False
    print('Running conversion for', resonance, era, subEra, shift)

    fnames = get_files(resonance, era, subEra, useParquet)

    # for when we use root files instead of parquet
    treename = 'tpTree/fitter_tree'

    jobPath = os.path.join(particle, resonance, era, subEra)
    if shift:
        jobPath = os.path.join(jobPath, shift)
    if testing:
        jobPath = os.path.join('testing', jobPath)
    else:
        jobPath = os.path.join('flat', jobPath)
    if _baseDir:
        jobPath = os.path.join(_baseDir, jobPath)
    os.makedirs(jobPath, exist_ok=True)

    doGen = subEra in ['DY_madgraph', 'DY_powheg']

    # default numerator/denominator defintions
    efficiencies = config.efficiencies()

    # get the dataframe
    if useParquet:
        print('Loading parquet files:', fnames)
        if isinstance(fnames, list):
            baseDF = spark.read.parquet(*fnames)
        else:
            baseDF = spark.read.parquet(fnames)
    else:
        baseDF = spark.read.format("root")\
                      .option('tree', treename)\
                      .load(fnames)

    # create the definitions columns
    definitions = config.definitions()
    defDF = baseDF
    for d in definitions:
        defDF = defDF.withColumn(d, F.expr(definitions[d]))

    # select tags
    tagsDF = defDF.filter(config.selection())

    # build the weights (pileup for MC)
    weightedDF = get_weighted_dataframe(
        tagsDF, doGen, resonance, era, subEra, shift=shift)

    # create the binning structure
    fitVariable = config.fitVariable()
    binningSet = set([fitVariable])
    if doGen:
        fitVariableGen = config.fitVariableGen()
        binningSet = binningSet.union(set([fitVariableGen]))
    binVariables = config.binVariables()
    for bvs in binVariables:
        binningSet = binningSet.union(set(bvs))

    binning = config.binning()
    variables = config.variables()
    binnedDF = weightedDF
    for bName in binningSet:
        binnedDF = get_binned_dataframe(
            binnedDF, bName+"Bin",
            variables[bName]['variable'],
            binning[bName])

    # build the unrealized yield dataframes
    # they are binned in the ID, bin variables, and fit variable
    yields = {}
    yields_gen = {}

    for numLabel, denLabel in efficiencies:
        den = binnedDF.filter(denLabel)
        for binVars in binVariables:
            key = (numLabel, denLabel, tuple(binVars))
            yields[key] = den.groupBy(
                numLabel, *[b+'Bin' for b in
                            binVars+[fitVariable]])\
                .agg({'weight2': 'sum', 'weight': 'sum'})
            if doGen:
                yields_gen[key] = den.groupBy(
                    numLabel, *[b+'Bin' for b in
                                binVars+[fitVariableGen]])\
                    .agg({'weight2': 'sum', 'weight': 'sum'})

    def get_values(df, mLabel, **binValues):
        for k, v in binValues.items():
            df = df[df[k] == v]
        df = df.set_index(mLabel)
        # fill empty bins with 0
        # includes underflow and overflow in the ROOT numbering scheme
        # (0 is underflow, len(binning)+1 is overflow)
        values = pd.Series(np.zeros(len(binning['mass'])+1))
        values[df.index] = df['sum(weight)']
        values = values.to_numpy()
        sumw2 = pd.Series(np.zeros(len(binning['mass'])+1))
        if 'sum(weight2)' in df.columns:
            sumw2[df.index] = df['sum(weight2)']
        else:
            sumw2[df.index] = df['sum(weight)']  # no weights provided
        sumw2 = sumw2.to_numpy()
        return values, sumw2

    def get_hist(values, sumw2, edges, overflow=True):
        if overflow:
            hist = TH1.from_numpy((values[1:-1], edges))
            hist[0] = values[0]
            hist[-1] = values[-1]
            hist._fSumw2 = sumw2
        else:
            hist = TH1.from_numpy((values, edges))
            hist._fSumw2[1:-1] = sumw2
        return hist

    # realize each of the yield tables
    # then produce the histograms and saves them
    # this is the first time things are put into memory
    for num_den_binVars in yields:
        num, den, binVars = num_den_binVars
        if _numerator and num not in _numerator:
            continue
        if _denominator and den not in _denominator:
            continue
        extended_eff_name = get_extended_eff_name(num, den, binVars)

        eff_outname = f'{jobPath}/{extended_eff_name}.root'
        hists = {}

        print('Processing', eff_outname)
        realized = yields[num_den_binVars].toPandas()

        for bins in itertools.product(
                *[range(1, len(binning[b])) for b in binVars]):
            binname = get_full_name(num, den, binVars, bins)
            binargs = {b+'Bin': v for b, v in zip(binVars, bins)}
            mLabel = fitVariable + 'Bin'

            passargs = {num: True}
            passargs.update(binargs)
            values, sumw2 = get_values(realized, mLabel, **passargs)
            edges = binning[fitVariable]
            hists[binname+'_Pass'] = get_hist(values, sumw2, edges)

            failargs = {num: False}
            failargs.update(binargs)
            values, sumw2 = get_values(realized, mLabel, **failargs)
            edges = binning[fitVariable]
            hists[binname+'_Fail'] = get_hist(values, sumw2, edges)

        if doGen:
            realized = yields_gen[num_den_binVars].toPandas()
            for bins in itertools.product(
                    *[range(1, len(binning[b])) for b in binVars]):
                binname = get_full_name(num, den, binVars, bins)
                binargs = {b+'Bin': v for b, v in zip(binVars, bins)}
                mLabel = fitVariableGen + 'Bin'

                passargs = {num: True}
                passargs.update(binargs)
                values, sumw2 = get_values(realized, mLabel, **passargs)
                edges = binning[fitVariableGen]
                hists[binname+'_Pass_Gen'] = get_hist(values, sumw2, edges)

                failargs = {num: False}
                failargs.update(binargs)
                values, sumw2 = get_values(realized, mLabel, **failargs)
                edges = binning[fitVariableGen]
                hists[binname+'_Fail_Gen'] = get_hist(values, sumw2, edges)

        with uproot.recreate(eff_outname) as f:
            for h, hist in sorted(hists.items()):
                f[h] = hist


subEras = {
    'Z': {
        # ultra legacy
        'Run2017_UL': ['Run2017', 'DY_madgraph'],
        'Run2018_UL': ['Run2018', 'DY_madgraph', 'DY_powheg'],
        # alternatively split by data taking era
        # 'Run2017_UL': [f'Run2017{b}' for b in 'BCDEF']+['DY_madgraph'],
    },
    'JPsi': {
    },
}


def run_all(spark, particle, resonance, era,
            config, shift='Nominal', **kwargs):
    for subEra in subEras.get(resonance, {}).get(era, []):
        run_conversion(spark, particle, resonance, era, subEra,
                       config, shift, **kwargs)


def run_spark(particle, resonance, era, config, **kwargs):
    _shiftType = kwargs.pop('shiftType', [])

    spark = SparkSession\
        .builder\
        .appName("TnP")\
        .getOrCreate()

    sc = spark.sparkContext
    print(sc.getConf().toDebugString())

    shiftTypes = config.shifts()
    for shiftType in shiftTypes:
        if _shiftType and shiftType not in _shiftType:
            continue
        run_all(spark, particle, resonance, era,
                config.shift(shiftType), shift=shiftType, **kwargs)

    spark.stop()


if __name__ == "__main__":
    particle = 'muon'
    resonance = 'Z'
    era = 'Run2017_UL'
    run_spark(particle, resonance, era)
