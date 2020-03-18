import os
import glob
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
            # rereco
            #'Run2016',
            #'Run2017',
            #'Run2018',
        ],
        'JPsi': [
        ],
    }
    return eras.get(resonance,[])

def get_allowed_sub_eras(resonance,era):
    subEras = {
        'Z': {
            # ultra legacy
            'Run2017_UL': ['Run2017'] + [f'Run2017{b}' for b in 'BCDEF']+['DY_madgraph'],
            # rereco
            #'Run2016': ['Run2016'] + [f'Run2016{b}' for b in 'BCDEFGH']+['DY_madgraph'],
            #'Run2017': ['Run2017'] + [f'Run2017{b}' for b in 'BCDEF']+['DY_madgraph'],
            #'Run2018': ['Run2018'] + [f'Run2018{b}' for b in 'ABCD']+['DY_madgraph'],
        },
        'JPsi': {
        },
    }
    return subEras.get(resonance,{}).get(era,[])

def get_files(resonance,era,subEra,useParquet=False):
    '''
    Get the list of centrally produced tag and probe trees.
    Some datasets have been converted to the parquet format (which is much more efficient).
    '''
    # Configure the efficiencies
    if useParquet:
        # eos
        #baseDir_Run2017_UL = 'root://eosuser/eos/cms/store/group/phys_muon/dntaylor/parquet/Z/Run2017_UL'
        # hdfs analytix
        baseDir_Run2017_UL = 'parquet/Z/Run2017_UL'
        fnamesMap = {
            'Z': {
                'Run2017_UL': {
                    'Run2017B': os.path.join(baseDir_Run2017_UL,'Run2017B','tnp.parquet'),
                    'Run2017C': os.path.join(baseDir_Run2017_UL,'Run2017C','tnp.parquet'),
                    'Run2017D': os.path.join(baseDir_Run2017_UL,'Run2017D','tnp.parquet'),
                    'Run2017E': os.path.join(baseDir_Run2017_UL,'Run2017E','tnp.parquet'),
                    'Run2017F': os.path.join(baseDir_Run2017_UL,'Run2017F','tnp.parquet'),
                    'DY_madgraph': os.path.join(baseDir_Run2017_UL,'DY_madgraph','tnp.parquet'),
                },
            },
            'JPsi': {
            },
        }
        fnamesMap['Z']['Run2017_UL']['Run2017'] = [fnamesMap['Z']['Run2017_UL']['Run2017{}'.format(x)] for x in 'BCDEF']
        fnames = fnamesMap.get(resonance,{}).get(era,{}).get(subEra,'')
    else:
        baseDir_Run2017_UL = '/eos/cms/store/group/phys_muon/TagAndProbe/ULRereco/2017/102X'
        fnamesMap = {
            'Z': {
                'Run2017_UL': {
                    'Run2017B': [f for f in glob.glob(os.path.join(baseDir_Run2017_UL, 'Run2017B/tnpZ*.root')) if 'hadd' not in f],
                    'Run2017C': [f for f in glob.glob(os.path.join(baseDir_Run2017_UL, 'Run2017C/tnpZ*.root')) if 'hadd' not in f],
                    'Run2017D': [f for f in glob.glob(os.path.join(baseDir_Run2017_UL, 'Run2017D/tnpZ*.root')) if 'hadd' not in f],
                    'Run2017E': [f for f in glob.glob(os.path.join(baseDir_Run2017_UL, 'Run2017E_99Percent/tnpZ*.root')) if 'hadd' not in f],
                    'Run2017F': [f for f in glob.glob(os.path.join(baseDir_Run2017_UL, 'Run2017F_99Percent/tnpZ*.root')) if 'hadd' not in f],
                    'DY_madgraph': [f for f in glob.glob(os.path.join(baseDir_Run2017_UL, 'DY_M50_pdfwgt/tnpZ*.root')) if 'hadd' not in f],
                },
            },
            'JPsi': {
            },
        }


        fnames = ['root://eoscms.cern.ch/'+f for f in fnamesMap.get(resonance,{}).get(era,{}).get(subEra,[])]
        
    return fnames


def get_pileup(resonance,era,subEra):
    '''
    Get the pileup distribution scalefactors to apply to simulation for a given era.
    '''
    # get the pileup
    dataPileup = {
        # Use ReReco golden json:
        # /afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions17/13TeV/ReReco/Cert_294927-306462_13TeV_EOY2017ReReco_Collisions17_JSON.txt
        # /afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions17/13TeV/PileUp/pileup_latest.txt
        # Centrally produced:
        # /afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions17/13TeV/PileUp/PileupHistogram-goldenJSON-13tev-2017-69200ub.root
        'Run2017_UL': 'pileup/data/Run2017.root',
    }
    mcPileup = {
        # SimGeneral/MixingModule/python/mix_2017_25ns_UltraLegacy_PoissonOOTPU_cfi.py
        'Run2017_UL': 'pileup/mc/Run2017_UL.root', 
    }
    with uproot.open(dataPileup[era]) as f:
        data_edges = f['pileup'].edges
        data_pileup = f['pileup'].values
        data_pileup /= sum(data_pileup)
    with uproot.open(mcPileup[era]) as f:
        mc_edges = f['pileup'].edges
        mc_pileup = f['pileup'].values
        mc_pileup /= sum(mc_pileup)
    pileup_edges = data_edges if len(data_edges)<len(mc_edges) else mc_edges
    pileup_ratio = [d/m if m else 1.0 for d,m in zip(data_pileup[:len(pileup_edges)-1],mc_pileup[:len(pileup_edges)-1])]
    
    return pileup_ratio, pileup_edges

def get_tag_dataframe(df,resonance,era,subEra,shift=None):
    '''
    Produces a dataframe reduced by the default tag selection used by the Muon POG
    The optional shift parameter will change the tag for systematic uncertainties.
    '''
    if resonance=='Z':
        if '2017' in era:
            tag_sel = 'tag_pt>29 and tag_abseta<2.4 and tag_IsoMu27==1 and pair_probeMultiplicity==1'
        else:
            tag_sel = 'tag_pt>26 and tag_abseta<2.4 and tag_IsoMu24==1 and pair_probeMultiplicity==1'
        if shift == 'TagIsoUp':
            tag_sel = tag_sel + ' and tag_combRelIsoPF04dBeta<0.3'
        elif shift == 'TagIsoDown':
            tag_sel = tag_sel + ' and tag_combRelIsoPF04dBeta<0.1'
        else:
            tag_sel = tag_sel + ' and tag_combRelIsoPF04dBeta<0.2'

    return df.filter(tag_sel)

def get_weighted_dataframe(df,doGen,resonance,era,subEra,shift=None):
    '''
    Produces a dataframe with a weight and weight2 column with weight corresponding to:
        1 for data
    or
        pileup for mc
    The optional shift parameter allows for a different systematic shift to the weights
    '''
    # TODO: implement any systematic shifts in the weight such as PDF, pileup, etc.
    # get the pileup
    pileup_ratio, pileup_edges = get_pileup(resonance,era,subEra)
    
    # build the weights (pileup for MC)
    # TODO: if there is already a weight column (ie, gen weight) get that first
    if doGen:
        pileupMap = {e:r for e,r in zip(pileup_edges[:-1], pileup_ratio)}
        mapping_expr = F.create_map([F.lit(x) for x in itertools.chain(*pileupMap.items())])
        weightedDF = df.withColumn('weight', mapping_expr.getItem(F.col('tag_nVertices')))
    else:
        weightedDF = df.withColumn('weight', F.lit(1.0))
    weightedDF = weightedDF.withColumn('weight2', F.col('weight') * F.col('weight'))
    
    return weightedDF

def get_binned_dataframe(df,bin_name,variable_name,edges):
    '''
    Produces a dataframe with a new column `bin_name` corresponding
    to the variable `variable_name` binned with the given `edges`.
    '''
    splits = [-float('inf')]+list(edges)+[float('inf')]
    bucketizer = Bucketizer(splits=splits, inputCol=variable_name, outputCol=bin_name)
    binnedDF = bucketizer.transform(df)
    return binnedDF

def get_selection_dataframe(df,selection_name,selection_func):
    '''
    Produces a new dataframe with a new column `selection_name` and from the function `selection_func`.
    '''
    return df.withColumn(selection_name,selection_func(df))


# Customization for Muon POG's officially supported IDs
_idLabels = ['LooseID', 'MediumID', 'MediumPromptID', 'TightID', 'SoftID']
_isoLabels = ['LooseRelIso','TightRelIso']
_idLabelsTuneP = ['HighPtID', 'TrkHighPtID']
_isoLabelsTuneP = ['LooseRelTkIso','TightRelTkIso']
_denomLabels = ['genTracks', 'TrackerMuons']

_definitionMap = {
    'genTracks'     : lambda df: F.lit(True),
    'TrackerMuons'  : lambda df: df.TM==1,
    'LooseID'       : lambda df: df.CutBasedIdLoose==1,
    'MediumID'      : lambda df: df.CutBasedIdMedium==1,
    'MediumPromptID': lambda df: df.CutBasedIdMediumPrompt==1,
    'TightID'       : lambda df: df.CutBasedIdTight==1,
    'SoftID'        : lambda df: df.SoftCutBasedId==1,
    'HighPtID'      : lambda df: df.CutBasedIdGlobalHighPt_new==1,
    'TrkHighPtID'   : lambda df: df.CutBasedIdTrkHighPt==1,
    'LooseRelIso'   : lambda df: df.combRelIsoPF04dBeta<0.25,
    'TightRelIso'   : lambda df: df.combRelIsoPF04dBeta<0.15,
    'LooseRelTkIso' : lambda df: df.relTkIso<0.10,
    'TightRelTkIso' : lambda df: df.relTkIso<0.05,
}


# map of alternative namings used for selections (for isolation)
_selectionMap = {
    'TightIDandIPCut'       : 'TightID',
    'HighPtIDandIPCut'      : 'HighPtID',
    'TrkHighPtIDandIPCut'   : 'TrkHighPtID',
}
    
# this is a helper to build the num/denom pairs
# we don't want to do all possible combinatorics since many would not be used
# a product between the first entry (nums) and the second entry (denoms) will be performed
_defs = [
    [
        ['TrackerMuons'],
        ['genTracks'],
    ],
    [
        ['LooseID', 'MediumID', 'MediumPromptID', 'TightID', 'SoftID', 'HighPtID', 'TrkHighPtID'],
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
        ['LooseRelTkIso','TightRelTkIso'],
        ['HighPtIDandIPCut', 'TrkHighPtIDandIPCut'],
    ],
]

# define the numerator/denominator definitions
# a fit will be done for each num/denom pair
_definitions = []

for num_denoms in _defs:
    for num, denom in itertools.product(*num_denoms):
        _definitions += [(num, denom)]
    
def get_default_ids():
    '''
    Returns the list of the Muon POG's officially supported IDs
    '''
    return _idLabels + _idLabelsTuneP

def get_default_isos():
    '''
    Returns the list of the Muon POG's officially supported isolations
    '''
    return _isoLabels + _isoLabelsTuneP

def get_default_denoms():
    '''
    Returns the list of the Muon POG's officially supported denominators
    '''
    return _denomLabels

def get_default_num_denom():
    '''
    This function returns the Muon POG's officially supported id/iso efficiencies.
    '''
    return _definitions

def get_default_selection(selection_name):
    '''
    The function returns a function to produce a new column with the Muon POG's officially supported
    ids/isos given the standard name.
    '''

    return _definitionMap.get(_selectionMap.get(selection,selection), None)

def get_default_selections_dataframe(df):
    '''
    Produces a dataframe with all of the Muon POG's officially supported id/isos.
    '''
    for sel_name, sel_func in _definitionMap.items():
        df = get_selection_dataframe(df, sel_name, sel_func)
        
    # and renamed for convenience
    for alt_sel_name, sel_name in _selectionMap.items():
        df = df.withColumn(alt_sel_name, df[sel_name])
        
    return df


# common names for the fit bins
# used to read the appropriate histogram for fitting and get the correct labels for saving things
def get_eff_name(num,denom):
    return 'NUM_{num}_DEN_{den}'.format(num=num, den=denom)

def get_bin_name(variableNames,index):
    return '_'.join(['{}_{}'.format(variableName,ind) for variableName, ind in zip(variableNames,index)])

def get_variables_name(variableNames):
    return '_'.join(variableNames)

def get_full_name(num,denom,variableNames,index):
    eff_name = get_eff_name(num,denom)
    bin_name = get_bin_name(variableNames,index)
    return '{}_{}'.format(eff_name,bin_name)

def get_full_pass_name(num,denom,variableNames,index):
    full_name = get_full_name(num,denom,variableNames,index)
    return '{}_Pass'.format(full_name)

def get_full_fail_name(num,denom,variableNames,index):
    full_name = get_full_name(num,denom,variableNames,index)
    return '{}_Fail'.format(full_name)

def get_extended_eff_name(num,denom,variableNames):
    eff_name = get_eff_name(num,denom)
    variables_name = get_variables_name(variableNames)
    return '{}_{}'.format(eff_name,variables_name)
