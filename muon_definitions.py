import os
import uproot
import itertools
from pyspark.sql import functions as F
from pyspark.ml.feature import Bucketizer


# allowed choices
def get_allowed_resonances():
    resonances = [
        'Z',
        'JPsi'
    ]
    return resonances


def get_allowed_eras(resonance):

    eras = {
        'Z': [
            # ultra legacy
            'Run2017_UL',
            'Run2018_UL',
            # rereco
            'Run2016',
            'Run2017',
            'Run2018',
        ],
        'JPsi': [
            # heavy ion
            'Run2016_HI_pPb_8TeV',
        ],
    }
    return eras.get(resonance, [])


def get_allowed_sub_eras(resonance, era):
    subEras = {
        'Z': {
            # ultra legacy
            'Run2017_UL': ['Run2017'] + [
                f'Run2017{b}' for b in 'BCDEF']+['DY_madgraph'],
            'Run2018_UL': ['Run2018'] + [
                f'Run2018{b}' for b in 'ABCD']+['DY_madgraph', 'DY_powheg'],
            # rereco
            'Run2016': ['Run2016'] + [
               f'Run2016{b}' for b in 'BCDEFGH']+['DY_madgraph'],
            'Run2017': ['Run2017'] + [
               f'Run2017{b}' for b in 'BCDEF']+['DY_madgraph'],
            'Run2018': ['Run2018'] + [
               f'Run2018{b}' for b in 'ABCD']+['DY_madgraph'],
        },
        'JPsi': {
            # ultra legacy
            'Run2017_UL': ['Run2017'] + [
                f'Run2017{b}' for b in 'BCDEF']+['Jpsi'],
            # heavy ion
            'Run2016_HI_pPb_8TeV': ['Run2016'],
        },
    }
    return subEras.get(resonance, {}).get(era, [])


def get_data_mc_sub_eras(resonance, era):
    eraMap = {
        'Z': {
            # ultra legacy
            'Run2017_UL': ['Run2017', 'DY_madgraph'],
            # TODO: decide how to handle alternate generators
            'Run2018_UL': ['Run2018', 'DY_madgraph'],
            # rereco
            'Run2016': ['Run2016', 'DY_madgraph'],
            'Run2017': ['Run2017', 'DY_madgraph'],
            'Run2018': ['Run2018', 'DY_madgraph'],
        },
        'JPsi': {
            # ultra legacy
            'Run2017_UL': ['Run2017', 'Jpsi'],
            # heavy ion
            'Run2016_HI_pPb_8TeV': ['Run2016', None],
        },
    }
    return eraMap.get(resonance, {}).get(era, [None, None])


def get_pileup(resonance, era, subEra):
    '''
    Get the pileup distribution scalefactors to apply to simulation
    for a given era.
    '''
    # get the pileup
    dataPileup = {
        # Note: for now use ReReco version of pileup
        'Run2016_UL': 'pileup/data/Run2016.root',
        'Run2017_UL': 'pileup/data/Run2017.root',
        'Run2018_UL': 'pileup/data/Run2018.root',
        'Run2016': 'pileup/data/Run2016.root',
        'Run2017': 'pileup/data/Run2017.root',
        'Run2018': 'pileup/data/Run2018.root',
    }
    mcPileup = {
        'Run2017_UL': 'pileup/mc/Run2017_UL.root',
        'Run2018_UL': 'pileup/mc/Run2018_UL.root',
        'Run2016': 'pileup/mc/Run2016.root',
        'Run2017': 'pileup/mc/Run2017.root',
        'Run2018': 'pileup/mc/Run2018.root',
    }
    # get absolute path
    baseDir = os.path.dirname(__file__)
    dataPileup = {k: os.path.join(baseDir, dataPileup[k]) for k in dataPileup}
    mcPileup = {k: os.path.join(baseDir, mcPileup[k]) for k in mcPileup}
    with uproot.open(dataPileup[era]) as f:
        data_edges = f['pileup'].edges
        data_pileup = f['pileup'].values
        data_pileup /= sum(data_pileup)
    with uproot.open(mcPileup[era]) as f:
        mc_edges = f['pileup'].edges
        mc_pileup = f['pileup'].values
        mc_pileup /= sum(mc_pileup)
    pileup_edges = data_edges if len(data_edges) < len(mc_edges) else mc_edges
    pileup_ratio = [d/m if m else 1.0 for d, m in zip(
        data_pileup[:len(pileup_edges)-1], mc_pileup[:len(pileup_edges)-1])]

    return pileup_ratio, pileup_edges


def get_tag_dataframe(df, resonance, era, subEra, shift=None):
    '''
    Produces a dataframe reduced by the default tag selection
    used by the Muon POG.
    The optional shift parameter will change the tag
    for systematic uncertainties.
    '''
    if resonance == 'Z':
        if '2017' in era:
            tag_sel = 'tag_pt>29 and tag_abseta<2.4 and tag_IsoMu27==1'\
                      + ' and pair_probeMultiplicity==1'
        else:
            tag_sel = 'tag_pt>26 and tag_abseta<2.4 and tag_IsoMu24==1'\
                      + ' and pair_probeMultiplicity==1'
        if shift == 'TagIsoUp':
            tag_sel = tag_sel + ' and tag_combRelIsoPF04dBeta<0.3'
        elif shift == 'TagIsoDown':
            tag_sel = tag_sel + ' and tag_combRelIsoPF04dBeta<0.1'
        else:
            tag_sel = tag_sel + ' and tag_combRelIsoPF04dBeta<0.2'

    return df.filter(tag_sel)


def get_weighted_dataframe(df, doGen, resonance, era, subEra, shift=None):
    '''
    Produces a dataframe with a weight and weight2 column
    with weight corresponding to:
        1 for data
    or
        pileup for mc
    The optional shift parameter allows for a different
    systematic shift to the weights
    '''
    # TODO: implement systematic shifts in the weight such as PDF, pileup, etc.
    # get the pileup
    pileup_ratio, pileup_edges = get_pileup(resonance, era, subEra)

    # build the weights (pileup for MC)
    # TODO: if there is a weight column (ie, gen weight) get that first
    if doGen:
        pileupMap = {e: r for e, r in zip(pileup_edges[:-1], pileup_ratio)}
        mapping_expr = F.create_map(
            [F.lit(x) for x in itertools.chain(*pileupMap.items())])
        weightedDF = df.withColumn(
            'weight', mapping_expr.getItem(F.col('tag_nVertices')))
    else:
        weightedDF = df.withColumn('weight', F.lit(1.0))
    weightedDF = weightedDF.withColumn(
        'weight2', F.col('weight') * F.col('weight'))

    return weightedDF


def get_binned_dataframe(df, bin_name, variable_name, edges):
    '''
    Produces a dataframe with a new column `bin_name` corresponding
    to the variable `variable_name` binned with the given `edges`.
    '''
    splits = [-float('inf')]+list(edges)+[float('inf')]
    bucketizer = Bucketizer(
        splits=splits, inputCol=variable_name, outputCol=bin_name)
    binnedDF = bucketizer.transform(df)
    return binnedDF


def get_selection_dataframe(df, selection_name, selection_func):
    '''
    Produces a new dataframe with a new column `selection_name`
    from the function `selection_func`.
    '''
    return df.withColumn(selection_name, selection_func(df))


# common names for the fit bins
# used to read the appropriate histogram for fitting
# and get the correct labels for saving things
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
