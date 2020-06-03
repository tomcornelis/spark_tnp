from __future__ import print_function
import os
import subprocess
import itertools
import math

from muon_definitions import (get_default_num_denom,
                              get_data_mc_sub_eras,
                              get_default_binning,
                              get_default_binning_variables,
                              get_full_name, get_eff_name,
                              get_extended_eff_name)


# This is (a lot) slower since every subprocess has to load ROOT.
# However, it has the benefit of allowing us to redirect the output to a file
# on a per fit basis.
def run_single_fit(outFName, inFName, binName, templateFName, plotDir,
                   fitType, histType, shiftType='Nominal'):

    os.makedirs(os.path.dirname(outFName), exist_ok=True)

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


def build_condor_submit(joblist, test=False, jobsPerSubmit=1, njobs=1):

    # for now, hard coded for lxplus
    args = ['outFName', 'inFName', 'binName', 'templateFName',
            'plotDir', 'version', 'histType', 'shiftType']
    files = ['env.sh', 'TagAndProbeFitter.py',
             'run_single_fit.py',
             'RooCMSShape.cc', 'RooCMSShape.h',
             'tdrstyle.py', 'CMS_lumi.py']

    if jobsPerSubmit > 1:
        arguments = './run_multiple_fits.sh {} {} {}'.format(
            joblist,
            '$(ProcId)',
            jobsPerSubmit,
        )
        queue = 'queue {}'.format(math.ceil(njobs/jobsPerSubmit))
        files += [joblist, 'run_multiple_fits.sh']
        flavour = 'longlunch'
    else:
        arguments = '/run_single_fit.py {}'.format(
            ' '.join([f'$({a})' for a in args]),
        )
        queue = 'queue {} from {}'.format(
            ','.join(args),
            joblist,
        )
        flavour = 'espresso'

    output = 'condor/job.$(ClusterId).$(ProcId).out' if test else '/dev/null'
    error = 'condor/job.$(ClusterId).$(ProcId).err' if test else '/dev/null'
    log = 'condor/job.$(ClusterId).$(ProcId).log' if test else '/dev/null'

    config = '''universe    = vanilla
executable  = condor_wrapper.sh
arguments   = {arguments}
transfer_input_files = {files}
output      = {output}
error       = {error}
log         = {log}
+JobFlavour = "{flavour}"
{queue}'''.format(
        arguments=arguments,
        files=','.join(files),
        output=output,
        error=error,
        log=log,
        flavour=flavour,
        queue=queue,
    )

    return config


def build_fit_jobs(particle, resonance, era, **kwargs):
    _baseDir = kwargs.pop('baseDir', '')
    _numerator = kwargs.pop('numerator', [])
    _denominator = kwargs.pop('denominator', [])
    _fitType = kwargs.pop('fitType', [])
    _shiftType = kwargs.pop('shiftType', [])
    _sampleType = kwargs.pop('sampleType', [])
    _efficiencyBin = kwargs.pop('efficiencyBin', [])
    doData = (not _sampleType) or ('data' in _sampleType)
    doMC = (not _sampleType) or ('mc' in _sampleType)

    dataSubEra, mcSubEra = get_data_mc_sub_eras(resonance, era)

    jobs = []
    # iterate through the efficiencies
    definitions = get_default_num_denom()
    binning = get_default_binning()
    for num, denom in definitions:
        if _numerator and num not in _numerator:
            continue
        if _denominator and denom not in _denominator:
            continue

        # iterate through the output binning structure
        for variableLabels in get_default_binning_variables():
            # iterate through the bin indices
            # this does nested for loops of the N-D binning (e.g. pt, eta)
            indices = [list(range(len(binning[variableLabel])-1))
                       for variableLabel in variableLabels]
            for index in itertools.product(*indices):
                # binning goes from 1 to N
                index = [i+1 for i in index]
                binName = get_full_name(num, denom, variableLabels, index)
                extEffName = get_extended_eff_name(num, denom, variableLabels)
                effName = get_eff_name(num, denom)
                if _efficiencyBin and binName not in _efficiencyBin:
                    continue

                def get_jobs(fitType, shiftType, inType, outType):
                    _jobs = []
                    templateFName = os.path.join(_baseDir, 'flat',
                                                 particle, resonance, era,
                                                 mcSubEra, inType,
                                                 extEffName+'.root')
                    outFName = os.path.join(_baseDir, 'fits_data',
                                            particle, resonance, era,
                                            outType, effName,
                                            binName+'.root')
                    inFName = os.path.join(_baseDir, 'flat',
                                           particle, resonance, era,
                                           dataSubEra, inType,
                                           extEffName+'.root')
                    plotDir = os.path.join(_baseDir, 'plots',
                                           particle, resonance, era,
                                           'fits_data',
                                           outType, effName)
                    if doData:
                        _jobs += [(outFName, inFName, binName, templateFName,
                                   plotDir, fitType, 'data', shiftType)]
                    outFName = os.path.join(_baseDir, 'fits_mc',
                                            particle, resonance, era,
                                            outType, effName,
                                            binName+'.root')
                    inFName = os.path.join(_baseDir, 'flat',
                                           particle, resonance, era,
                                           mcSubEra, inType,
                                           extEffName+'.root')
                    plotDir = os.path.join(_baseDir, 'plots',
                                           particle, resonance, era,
                                           'fits_mc',
                                           outType, effName)
                    # there is no need to fit MC for templates
                    # PDF based fits are:
                    #   NominalOld, AltSigOld
                    if doMC and fitType in ['NominalOld', 'AltSigOld']:
                        _jobs += [(outFName, inFName, binName, templateFName,
                                   plotDir, fitType, 'mc', shiftType)]
                    return _jobs

                for fitType in ['Nominal', 'AltSig', 'AltBkg',
                                'NominalOld', 'AltSigOld']:
                    if (_fitType or _shiftType):
                        if not (_fitType and fitType in _fitType):
                            continue
                    shiftType = 'Nominal'
                    inType = 'Nominal'
                    outType = fitType
                    jobs += get_jobs(fitType, shiftType, inType, outType)

                for shiftType in ['tagIsoUp', 'tagIsoDown',
                                  'massBinUp', 'massBinDown',
                                  'massRangeUp', 'massRangeDown']:
                    if (_fitType or _shiftType):
                        if not (_shiftType and shiftType in _shiftType):
                            continue
                    fitType = 'Nominal'
                    inType = 'Nominal'
                    if 'tagIso' in shiftType:
                        inType = shiftType
                    outType = shiftType
                    jobs += get_jobs(fitType, shiftType, inType, outType)

    return jobs
