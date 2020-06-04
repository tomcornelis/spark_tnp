import os
import glob
import uproot
import itertools
import numpy as np
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
            # heavy ion
            'Run2016_HI_pPb_8TeV': ['Run2016', None],
        },
    }
    return eraMap.get(resonance, {}).get(era, [None, None])


# build the file lists once
def _build_parquet_file_lists(standalone=False):
    # hdfs analytix
    def _hdfs_path(resonance, era, subEra):
        baseDir = '/cms/muon_pog/parquet'
        if standalone:
            return os.path.join(
                baseDir, resonance, era, subEra, 'tnpSta.parquet')
        else:
            return os.path.join(
                baseDir, resonance, era, subEra, 'tnp.parquet')
    # this is always the same, so automate it
    fnamesMap = {
        _r: {
            _e: {
                _s: _hdfs_path(_r, _e, _s)
                for _s in get_allowed_sub_eras(_r, _e)
            } for _e in get_allowed_eras(_r)
        } for _r in get_allowed_resonances()
    }
    # then we need to replace the data RunXXXX with a list
    # of all the data sub eras
    for _r in fnamesMap:
        for _e in fnamesMap[_r]:
            combined = _e.split('_')[0]
            subEras = [k for k in fnamesMap[_r][_e].keys()
                       if k != combined and k.startswith(combined)]
            # only do the replacement if there are suberas
            # e.g. heavy ion does not
            if subEras:
                fnamesMap[_r][_e][combined] = [
                    fnamesMap[_r][_e][_s]
                    for _s in subEras]
    return fnamesMap


def _build_root_file_lists():
    # there is no good pattern for these, manually set them all
    def _UL17path(era):
        baseDir = os.path.join('/eos/cms/store/group/phys_muon',
                               'TagAndProbe/ULRereco/2017/102X')
        return [f for f in glob.glob(
            os.path.join(baseDir, era, 'tnpZ*.root'))
            if 'hadd' not in f]

    def _UL18path(era):
        baseDir = os.path.join('/eos/cms/store/group/phys_muon',
                               'TagAndProbe/ULRereco/2018/102X')
        return [f for f in glob.glob(
            os.path.join(baseDir, era, 'tnpZ*.root'))
            if 'hadd' not in f]

    fnamesMap = {
        'Z': {
            'Run2017_UL': {
                'Run2017B': _UL17path('Run2017B'),
                'Run2017C': _UL17path('Run2017C'),
                'Run2017D': _UL17path('Run2017D'),
                'Run2017E': _UL17path('Run2017E_99Percent'),
                'Run2017F': _UL17path('Run2017F_99Percent'),
                'DY_madgraph': _UL17path('DY_M50_pdfwgt'),
            },
            'Run2018_UL': {
                'Run2018A': _UL18path('SingleMuon_Run2018A'),
                'Run2018B': _UL18path('SingleMuon_Run2018B'),
                'Run2018C': _UL18path('SingleMuon_Run2018C'),
                'Run2018D': _UL18path('SingleMuon_Run2018D'),
                'DY_madgraph': _UL18path('DY_M50_Madgraph'),
                'DY_powheg': _UL18path('DY_M50to120_Powheg'),
            },
        },
        'JPsi': {
        },
    }

    return fnamesMap


_parquet_fnamesMap = _build_parquet_file_lists()
_parquet_sta_fnamesMap = _build_parquet_file_lists(standalone=True)
_root_fnamesMap = _build_root_file_lists()


def get_files(resonance, era, subEra, useParquet=False, standalone=False):
    '''
    Get the list of centrally produced tag and probe trees.
    Some datasets have been converted to the parquet format
    (which is much more efficient).
    '''
    if useParquet:
        if standalone:
            fnames = _parquet_sta_fnamesMap.get(
                resonance, {}).get(era, {}).get(subEra, '')
        else:
            fnames = _parquet_fnamesMap.get(
                resonance, {}).get(era, {}).get(subEra, '')
    else:

        fnames = [
            'root://eoscms.cern.ch/'+f for f in
            _root_fnamesMap.get(resonance, {}).get(era, {}).get(subEra, [])
        ]

    return fnames


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


# Customization for Muon POG's officially supported IDs
_idLabels = ['LooseID', 'MediumID', 'MediumPromptID', 'TightID', 'SoftID']
_isoLabels = ['LooseRelIso', 'TightRelIso']
_idLabelsTuneP = ['HighPtID', 'TrkHighPtID']
_isoLabelsTuneP = ['LooseRelTkIso', 'TightRelTkIso']
_denomLabels = ['genTracks', 'TrackerMuons']

_definitionMap = {
    'genTracks': lambda df: F.lit(True),
    'TrackerMuons': lambda df: df.TM == 1,
    'GlobalMuons': lambda df: df.Glb == 1,
    'LooseID': lambda df: df.CutBasedIdLoose == 1,
    'MediumID': lambda df: df.CutBasedIdMedium == 1,
    'MediumPromptID': lambda df: df.CutBasedIdMediumPrompt == 1,
    'TightID': lambda df: df.CutBasedIdTight == 1,
    'SoftID': lambda df: df.SoftCutBasedId == 1,
    'HighPtID': lambda df: df.CutBasedIdGlobalHighPt_new == 1,
    'TrkHighPtID': lambda df: df.CutBasedIdTrkHighPt == 1,
    'LooseRelIso': lambda df: df.combRelIsoPF04dBeta < 0.25,
    'TightRelIso': lambda df: df.combRelIsoPF04dBeta < 0.15,
    'LooseRelTkIso': lambda df: df.relTkIso < 0.10,
    'TightRelTkIso': lambda df: df.relTkIso < 0.05,
}


# map of alternative namings used for selections (for isolation)
_selectionMap = {
    'TightIDandIPCut': 'TightID',
    'HighPtIDandIPCut': 'HighPtID',
    'TrkHighPtIDandIPCut': 'TrkHighPtID',
}

# this is a helper to build the num/denom pairs
# we don't want to do all possible combinatorics
# a product between the first entry (nums)
# and the second entry (denoms) will be performed
_defs = [
    [
        ['TrackerMuons', 'GlobalMuons'],
        ['genTracks'],
    ],
    [
        ['LooseID', 'MediumID', 'MediumPromptID', 'TightID',
         'SoftID', 'HighPtID', 'TrkHighPtID'],
        ['genTracks', 'TrackerMuons'],
    ],
    [
        ['LooseRelIso'],
        ['LooseID', 'MediumID', 'MediumPromptID', 'TightIDandIPCut'],
    ],
    [
        ['TightRelIso'],
        ['MediumID', 'MediumPromptID', 'TightIDandIPCut'],
    ],
    [
        ['LooseRelTkIso', 'TightRelTkIso'],
        ['HighPtIDandIPCut', 'TrkHighPtIDandIPCut'],
    ],
]

# define the numerator/denominator definitions
# a fit will be done for each num/denom pair
_definitions = []

for num_denoms in _defs:
    for num, denom in itertools.product(*num_denoms):
        _definitions += [(num, denom)]

# bin definitions
_binning = {
    'pt': np.array([15, 20, 25, 30, 40, 50, 60, 120]),
    'abseta': np.array([0, 0.9, 1.2, 2.1, 2.4]),
    'eta': np.array([-2.4, -2.1, -1.6, -1.2, -0.9, -0.3, -0.2,
                     0.2, 0.3, 0.9, 1.2, 1.6, 2.1, 2.4]),
    'nvtx': np.array(range(10, 85, 5)),
    'njets': np.array([-0.5, 0.5, 1.5, 2.5, 3.5, 4.5]),
    'mass': np.array(range(60*4, 140*4+1, 1)) * 0.25,
}
_binning['mcMass'] = _binning['mass']

# friendly label for plots
_variableLabelMap = {
    'pt': 'p_{T} (GeV)',
    'abseta': '|#eta|',
    'eta': '#eta',
    'nvtx': 'Number of primary vertices',
    'njets': 'Number of jets (p_{T} > 30 GeV)',
    'mass': 'm(#mu#mu) (GeV)',
}

# maps between custom variable names and names in tree
_variableMap = {
    'nvtx': 'tag_nVertices',
    'njets': 'pair_nJets30',
}

_variableMapTuneP = {
    'pt': 'pair_newTuneP_probe_pt',
    'nvtx': 'tag_nVertices',
    'njets': 'pair_nJets30',
    'mass': 'pair_newTuneP_mass',
}

# the binnings to produce efficiencies in
_binVariables = [
    ('abseta', 'pt', ),
    # ('pt', ),
    # ('eta', ),
    # ('nvtx', ),
    # ('njets', ),
]

# the variable to fit
_fitVariable = 'mass'
_fitVariableGen = 'mcMass'


def get_default_fit_variable(gen=False):
    '''
    Return the fit variable
    '''
    return _fitVariableGen if gen else _fitVariable


def get_default_binning():
    '''
    Return the default binning map
    '''
    return _binning


def get_default_binning_variables():
    '''
    Return the binning variables used by the Muon POG
    '''
    return _binVariables


def get_default_variable_name(variable, tuneP=False):
    '''
    Return the map between variable names and
    name in the tree.
    '''
    _map = _variableMapTuneP if tuneP else _variableMap
    return _map.get(variable, variable)


def get_default_ids(tuneP=False):
    '''
    Returns the list of the Muon POG's
    officially supported IDs
    '''
    return _idLabelsTuneP if tuneP else _idLabels


def get_default_isos(tuneP=False):
    '''
    Returns the list of the Muon POG's
    officially supported isolations
    '''
    return _isoLabelsTuneP if tuneP else _isoLabels


def get_default_denoms():
    '''
    Returns the list of the Muon POG's
    officially supported denominators
    '''
    return _denomLabels


def get_default_num_denom():
    '''
    This function returns the Muon POG's
    officially supported id/iso efficiencies.
    '''
    return _definitions


def get_default_selection(selection_name):
    '''
    The function returns a function to produce
    a new column with the Muon POG's officially supported
    ids/isos given the standard name.
    '''

    return _definitionMap.get(
        _selectionMap.get(selection_name, selection_name), None)


def get_default_selections_dataframe(df):
    '''
    Produces a dataframe with all of the Muon POG's
    officially supported id/isos.
    '''
    for sel_name, sel_func in _definitionMap.items():
        df = get_selection_dataframe(df, sel_name, sel_func)

    # and renamed for convenience
    for alt_sel_name, sel_name in _selectionMap.items():
        df = df.withColumn(alt_sel_name, df[sel_name])

    return df


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


def get_variable_name_pretty(v):
    '''
    Return the pretty version of a variable name
    '''
    return _variableLabelMap.get(v, v)
