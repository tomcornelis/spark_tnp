from __future__ import print_function

import os
import itertools

import numpy as np
import pandas as pd
import uproot
from uproot_methods.classes import TH1

from pyspark.sql import SparkSession

from muon_definitions import (get_files, get_default_num_denom,
                              get_tag_dataframe, get_weighted_dataframe,
                              get_binned_dataframe,
                              get_default_selections_dataframe)

useParquet = True

# bin definitions
binning = {
    'pt': np.array([15, 20, 25, 30, 40, 50, 60, 120]),
    'abseta': np.array([0, 0.9, 1.2, 2.1, 2.4]),
    'eta': np.array([-2.4, -2.1, -1.6, -1.2, -0.9, -0.3, -0.2,
                     0.2, 0.3, 0.9, 1.2, 1.6, 2.1, 2.4]),
    'nvtx': np.array(range(10, 85, 5)),
    'njets': np.array([-0.5, 0.5, 1.5, 2.5, 3.5, 4.5]),
    'mass': np.array(range(60, 131, 1)),
}
binning['mcMass'] = binning['mass']

binning['mcMass'] = binning['mass']

# efficiency definitions
idLabels = ['LooseID', 'MediumID', 'MediumPromptID', 'TightID', 'SoftID']
isoLabels = ['LooseRelIso', 'TightRelIso']
idLabelsTuneP = ['HighPtID', 'TrkHighPtID']
isoLabelsTuneP = ['LooseRelTkIso', 'TightRelTkIso']
denomLabels = ['genTracks', 'TrackerMuons']

# maps between custom variable names and names in tree
variableMap = {
    'nvtx': 'tag_nVertices',
    'njets': 'pair_nJets30',
}

variableMapTuneP = {
    'pt': 'pair_newTuneP_probe_pt',
    'nvtx': 'tag_nVertices',
    'njets': 'pair_nJets30',
    'mass': 'pair_newTuneP_mass',
}

# the binnings to produce efficiencies in
binVariables = [
    ('abseta', 'pt', ),
    # ('pt', ),
    # ('eta', ),
    # ('nvtx', ),
    # ('njets', ),
]

# the variable to fit
fitVariable = 'mass'
fitVariableGen = 'mcMass'


def get_eff_name(num, denom):
    return 'NUM_{num}_DEN_{den}'.format(num=num, den=denom)


def get_bin_name(variableNames, index):
    return '_'.join(['{}_{}'.format(variableName, ind)
                     for variableName, ind in zip(variableNames, index)])


def get_variables_name(variableNames):
    return '_'.join(variableNames)


def get_full_name(num, denom, variableNames, index):
    eff_name = get_eff_name(num, denom)
    bin_name = get_bin_name(variableNames, index)
    return '{}_{}'.format(eff_name, bin_name)


def get_full_pass_name(num, denom, variableNames, index):
    full_name = get_full_name(num, denom, variableNames, index)
    return '{}_Pass'.format(full_name)


def get_full_fail_name(num, denom, variableNames, index):
    full_name = get_full_name(num, denom, variableNames, index)
    return '{}_Fail'.format(full_name)


def get_extended_eff_name(num, denom, variableNames):
    eff_name = get_eff_name(num, denom)
    variables_name = get_variables_name(variableNames)
    return '{}_{}'.format(eff_name, variables_name)


def run_conversion(spark, particle, resonance, era, subEra,
                   shift='Nominal', **kwargs):
    _numerator = kwargs.pop('numerator', [])
    _denominator = kwargs.pop('denominator', [])
    _baseDir = kwargs.pop('baseDir', '')

    testing = False

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
        os.path.join(_baseDir, jobPath)
    os.makedirs(jobPath, exist_ok=True)

    doGen = subEra in ['DY_madgraph']

    # default numerator/denominator defintions
    definitions = get_default_num_denom()

    # get the dataframe
    if useParquet:
        if isinstance(fnames, list):
            baseDF = spark.read.parquet(*fnames)
        else:
            baseDF = spark.read.parquet(fnames)
    else:
        baseDF = spark.read.format("root")\
                      .option('tree', treename)\
                      .load(fnames)

    # select tags
    tagsDF = get_tag_dataframe(baseDF, resonance, era, subEra, shift=shift)

    # build the weights (pileup for MC)
    weightedDF = get_weighted_dataframe(
        tagsDF, doGen, resonance, era, subEra, shift=shift)

    # create the binning structure
    binningSet = set([fitVariable])
    if doGen:
        binningSet = binningSet.union(set([fitVariableGen]))
    for bvs in binVariables:
        binningSet = binningSet.union(set(bvs))

    binnedDF = weightedDF
    for bName in binningSet:
        binnedDF = get_binned_dataframe(
            binnedDF, bName+"Bin", variableMap.get(bName, bName),
            binning[bName])
        binnedDF = get_binned_dataframe(
            binnedDF, bName+"BinTuneP", variableMapTuneP.get(bName, bName),
            binning[bName])

    # create the id columns
    binnedDF = get_default_selections_dataframe(binnedDF)

    # build the unrealized yield dataframes
    # they are binned in the ID, bin variables, and fit variable
    yields = {}
    yields_gen = {}

    for numLabel, denLabel in definitions:
        den = binnedDF.filter(denLabel)
        den_pt20 = den.filter('pt>20')
        den_pt20TuneP = den.filter('pair_newTuneP_probe_pt>20')
        for binVars in binVariables:
            thisden = den if 'pt' in binVars else den_pt20
            thisdenTuneP = den if 'pt' in binVars else den_pt20TuneP
            key = (numLabel, denLabel, binVars)
            if numLabel in idLabelsTuneP+isoLabelsTuneP:
                yields[key] = thisdenTuneP.groupBy(
                    numLabel, *[b+'BinTuneP' for b in
                                list(binVars)+['mass']])\
                    .agg({'weight2': 'sum', 'weight': 'sum'})
                if doGen:
                    yields_gen[key] = thisdenTuneP.groupBy(
                        numLabel, *[b+'BinTuneP' for b in
                                    list(binVars)+['mcMass']])\
                        .agg({'weight2': 'sum', 'weight': 'sum'})
            else:
                yields[key] = thisden.groupBy(
                    numLabel, *[b+'Bin' for b in
                                list(binVars)+['mass']])\
                    .agg({'weight2': 'sum', 'weight': 'sum'})
                if doGen:
                    yields_gen[key] = thisden.groupBy(
                        numLabel, *[b+'Bin' for b in
                                    list(binVars)+['mcMass']])\
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

        print('processing', eff_outname)
        realized = yields[num_den_binVars].toPandas()

        for bins in itertools.product(
                *[range(1, len(binning[b])) for b in binVars]):
            binname = get_full_name(num, den, binVars, bins)
            if num in idLabelsTuneP+isoLabelsTuneP:
                binargs = {b+'BinTuneP': v for b, v in zip(binVars, bins)}
                mLabel = 'massBinTuneP'
            else:
                binargs = {b+'Bin': v for b, v in zip(binVars, bins)}
                mLabel = 'massBin'

            passargs = {num: True}
            passargs.update(binargs)
            values, sumw2 = get_values(realized, mLabel, **passargs)
            edges = binning['mass']
            hists[binname+'_Pass'] = get_hist(values, sumw2, edges)

            failargs = {num: False}
            failargs.update(binargs)
            values, sumw2 = get_values(realized, mLabel, **failargs)
            edges = binning['mass']
            hists[binname+'_Fail'] = get_hist(values, sumw2, edges)

        if doGen:
            realized = yields_gen[num_den_binVars].toPandas()
            for bins in itertools.product(
                    *[range(1, len(binning[b])) for b in binVars]):
                binname = get_full_name(num, den, binVars, bins)
                if num in idLabelsTuneP+isoLabelsTuneP:
                    binargs = {b+'BinTuneP': v for b, v in zip(binVars, bins)}
                    mLabel = 'mcMassBinTuneP'
                else:
                    binargs = {b+'Bin': v for b, v in zip(binVars, bins)}
                    mLabel = 'mcMassBin'

                passargs = {num: True}
                passargs.update(binargs)
                values, sumw2 = get_values(realized, mLabel, **passargs)
                edges = binning['mass']
                hists[binname+'_Pass_Gen'] = get_hist(values, sumw2, edges)

                failargs = {num: False}
                failargs.update(binargs)
                values, sumw2 = get_values(realized, mLabel, **failargs)
                edges = binning['mass']
                hists[binname+'_Fail_Gen'] = get_hist(values, sumw2, edges)

        with uproot.recreate(eff_outname) as f:
            for h, hist in sorted(hists.items()):
                f[h] = hist


subEras = {
    'Z': {
        # ultra legacy
        'Run2017_UL': ['Run2017', 'DY_madgraph'],
        # alternatively split by data taking era
        # 'Run2017_UL': [f'Run2017{b}' for b in 'BCDEF']+['DY_madgraph'],
    },
    'JPsi': {
    },
}


def run_all(spark, particle, resonance, era, shift='Nominal', **kwargs):
    for subEra in subEras.get(resonance, {}).get(era, []):
        run_conversion(spark, particle, resonance, era, subEra,
                       shift, **kwargs)


def run_spark(particle, resonance, era, **kwargs):
    _shiftType = kwargs.pop('shiftType', [])

    spark = SparkSession\
        .builder\
        .appName("TnP")\
        .getOrCreate()

    sc = spark.sparkContext
    print(sc.getConf().toDebugString())

    shiftTypes = ['Nominal', 'tagIsoUp', 'tagIsoDown']
    for shiftType in shiftTypes:
        if _shiftType and shiftType not in _shiftType:
            continue
        run_all(spark, particle, resonance, era, shift=shiftType, **kwargs)

    spark.stop()


if __name__ == "__main__":
    particle = 'muon'
    resonance = 'Z'
    era = 'Run2017_UL'
    run_spark(particle, resonance, era)
