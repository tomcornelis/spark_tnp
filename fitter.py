from __future__ import print_function
import os
import subprocess
import itertools

from muon_definitions import (definitions, binVariables, binning,
                              get_full_name, get_eff_name)


# This is (a lot) slower since every subprocess has to load ROOT.
# However, it has the benefit of allowing us to redirect the output to a file
# on a per fit basis.
def run_single_fit(outFName, inFName, binName, templateFName, plotDir,
                   fitType, histType, shiftType='Nominal'):

    try:
        os.makedirs(os.path.dirname(outFName))
    except OSError:
        pass

    try:
        # this allows us to save the output to a txt file
        # but is slower since each job loads ROOT
        # txtFName = outFName.replace('.root', '.log')
        txtFName = '/dev/null'
        with open(txtFName, 'w') as f:
            subprocess.check_call([
                './run_single_fit.py', outFName, inFName, binName,
                templateFName, plotDir, fitType, histType, shiftType
                ], stdout=f)
    except BaseException:
        print('Error processing', binName, fitType, histType)


def build_fit_jobs(baseDir, **kwargs):
    _numerator = kwargs.pop('numerator', [])
    _denominator = kwargs.pop('denominator', [])
    _fitType = kwargs.pop('fitType', [])
    _shiftType = kwargs.pop('shiftType', [])
    _sampleType = kwargs.pop('sampleType', [])
    _efficiencyBin = kwargs.pop('efficiencyBin', [])
    doData = (not _sampleType) or ('data' in _sampleType)
    doMC = (not _sampleType) or ('mc' in _sampleType)

    jobs = []
    # iterate through the efficiencies
    for num, denom in definitions:
        if _numerator and num not in _numerator:
            continue
        if _denominator and denom not in _denominator:
            continue

        # iterate through the output binning structure
        for variableLabels in binVariables:
            # iterate through the bin indices
            # this does nested for loops of the N-D binning (e.g. pt, eta)
            indices = [list(range(len(binning[variableLabel])-1))
                       for variableLabel in variableLabels]
            for index in itertools.product(*indices):
                binName = get_full_name(num, denom, variableLabels, index)
                effName = get_eff_name(num, denom)
                if _efficiencyBin and binName not in _efficiencyBin:
                    continue

                for fitType in ['Nominal', 'AltSig', 'AltBkg',
                                'NominalOld', 'AltSigOld']:
                    if (_fitType or _shiftType):
                        if not (_fitType and fitType in _fitType):
                            continue
                    shiftType = 'Nominal'
                    templateFName = os.path.join(baseDir, 'converted',
                                                 'Nominal', 'dy.root')
                    outFName = os.path.join(baseDir, 'fits_data', fitType,
                                            effName, binName+'.root')
                    inFName = os.path.join(baseDir, 'converted',
                                           'Nominal', 'data.root')
                    plotDir = os.path.join(baseDir, 'plots', 'fits_data',
                                           fitType, effName)
                    if doData:
                        jobs += [(outFName, inFName, binName, templateFName,
                                  plotDir, fitType, 'data', shiftType)]
                    outFName = os.path.join(baseDir, 'fits_dy', fitType,
                                            effName, binName+'.root')
                    inFName = os.path.join(baseDir, 'converted',
                                           'Nominal', 'dy.root')
                    plotDir = os.path.join(baseDir, 'plots', 'fits_dy',
                                           fitType, effName)
                    if doMC:
                        jobs += [(outFName, inFName, binName, templateFName,
                                  plotDir, fitType, 'mc', shiftType)]

                for shiftType in ['tagIsoUp', 'tagIsoDown',
                                  'massBinUp', 'massBinDown',
                                  'massRangeUp', 'massRangeDown']:
                    if (_fitType or _shiftType):
                        if not (_shiftType and shiftType in _shiftType):
                            continue
                    inType = 'Nominal'
                    if 'tagIso' in shiftType:
                        inType = shiftType
                    fitType = 'Nominal'
                    templateFName = os.path.join(baseDir, 'converted',
                                                 inType, 'dy.root')
                    outFName = os.path.join(baseDir, 'fits_data', shiftType,
                                            effName, binName+'.root')
                    inFName = os.path.join(baseDir, 'converted',
                                           inType, 'data.root')
                    plotDir = os.path.join(baseDir, 'plots', 'fits_data',
                                           shiftType, effName)
                    if doData:
                        jobs += [(outFName, inFName, binName, templateFName,
                                  plotDir, fitType, 'data', shiftType)]
                    outFName = os.path.join(baseDir, 'fits_dy', shiftType,
                                            effName, binName+'.root')
                    inFName = os.path.join(baseDir, 'converted',
                                           inType, 'dy.root')
                    plotDir = os.path.join(baseDir, 'plots', 'fits_dy',
                                           shiftType, effName)
                    if doMC:
                        jobs += [(outFName, inFName, binName, templateFName,
                                  plotDir, fitType, 'mc', shiftType)]

    return jobs
