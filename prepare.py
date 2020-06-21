from __future__ import print_function
import os
import math
import itertools
import json
from array import array
import ctypes
import ROOT
import tdrstyle
import CMS_lumi

from muon_definitions import (get_data_mc_sub_eras,
                              get_full_name, get_eff_name,
                              get_bin_name,
                              get_extended_eff_name,
                              get_variables_name)

ROOT.gROOT.SetBatch()
ROOT.gROOT.ProcessLine("gErrorIgnoreLevel = 1001;")
tdrstyle.setTDRStyle()


def computeEff(n1, n2, e1, e2):
    eff = n1 / (n1 + n2)
    err = 1 / (n1 + n2) * math.sqrt(
        e1 * e1 * n2 * n2 + e2 * e2 * n1 * n1) / (n1 + n2)
    return eff, err


def getEff(binName, fname):
    try:
        tfile = ROOT.TFile(fname, 'read')
        hP = tfile.Get('{}_GenPass'.format(binName))
        hF = tfile.Get('{}_GenFail'.format(binName))
        bin1 = 1
        bin2 = hP.GetXaxis().GetNbins()
        eP = ctypes.c_double(-1.0)
        eF = ctypes.c_double(-1.0)
        nP = hP.IntegralAndError(bin1, bin2, eP)
        nF = hF.IntegralAndError(bin1, bin2, eF)
        eff, err = computeEff(nP, nF, eP.value, eF.value)
        tfile.Close()
        return eff, err
    except Exception as e:
        print('Exception for', binName)
        raise e
        return 1., 0.


def getDataEff(binName, fname):
    tfile = ROOT.TFile(fname, 'read')
    fitresP = tfile.Get('{}_resP'.format(binName))
    fitresF = tfile.Get('{}_resF'.format(binName))

    fitP = fitresP.floatParsFinal().find('nSigP')
    fitF = fitresF.floatParsFinal().find('nSigF')

    try:
        nP = fitP.getVal()
        nF = fitF.getVal()
        eP = fitP.getError()
        eF = fitF.getError()

        hP = tfile.Get('{}_Pass'.format(binName))
        hF = tfile.Get('{}_Fail'.format(binName))

        if eP > math.sqrt(hP.Integral()):
            eP = math.sqrt(hP.Integral())
        if eF > math.sqrt(hF.Integral()):
            eF = math.sqrt(hF.Integral())
        tfile.Close()

        return computeEff(nP, nF, eP, eF)
    except Exception as e:
        print('Exception for', binName)
        raise e
        return 1., 0.


def getSF(binName, fname):
    mcEff, mcErr = getEff(binName, fname)
    dataEff, dataErr = getDataEff(binName, fname)
    sf = dataEff / mcEff if mcEff else 0.0
    sf_err = 0.0
    if dataEff and mcEff:
        sf_err = sf * ((dataErr / dataEff)**2 + (mcErr / mcEff)**2)**0.5
    return sf, sf_err, dataEff, dataErr, mcEff, mcErr


def getSyst(binName, datafname, dataEff, mcEff, fitTypes, shiftTypes):
    syst = {}
    syst_sq = 0
    for isyst in fitTypes:
        systfname = datafname.replace('Nominal', isyst)
        tmpEff, tmpErr = getDataEff(binName, systfname)
        syst.update({isyst: abs(tmpEff - dataEff)})
        syst_sq += (tmpEff - dataEff)**2

    for isyst in shiftTypes:
        systUpfname = datafname.replace('Nominal', isyst+'Up')
        systDnfname = datafname.replace('Nominal', isyst+'Down')
        tmpEffUp, tmpErr = getEff(binName, systUpfname)
        tmpEffDn, tmpErr = getEff(binName, systDnfname)
        upDiff = abs(tmpEffUp - dataEff)
        dnDiff = abs(tmpEffDn - dataEff)
        syst.update({isyst: (upDiff + dnDiff)/2})
        syst_sq += ((upDiff + dnDiff)/2)**2

    syst.update({'combined': (syst_sq)**0.5})
    return syst


def prepare(baseDir, particle, probe, resonance, era,
            config, num, denom, variableLabels):
    hists = {}

    effName = get_eff_name(num, denom)
    extEffName = get_extended_eff_name(num, denom, variableLabels)
    binning = config.binning()
    dataSubEra, mcSubEra = get_data_mc_sub_eras(resonance, era)

    systList = config.get('systematics',
                          {x: {'fitTypes': [],
                               'shittTypes': []}
                           for x in ['SF', 'dataEff', 'mcEff']})

    def get_variable_name_pretty(variableLabel):
        variables = config.variables()
        return variables.get(variableLabel, {}).get('pretty', variableLabel)

    # create output histograms
    nVars = len(variableLabels)
    if nVars == 1:
        THX = ROOT.TH1F
    elif nVars == 2:
        THX = ROOT.TH2F
    elif nVars == 3:
        THX = ROOT.TH3F
    else:
        raise NotImplementedError(
            'More than 3 dimensions are not supported for scale factors'
        )

    hargs = [extEffName, extEffName]
    for variableLabel in variableLabels:
        hargs += [len(binning[variableLabel]) - 1,
                  array('d', binning[variableLabel])]
    hist = THX(*hargs)
    axes = [hist.GetXaxis(), hist.GetYaxis(), hist.GetZaxis()]
    for vi, variableLabel in enumerate(variableLabels):
        axes[vi].SetTitle(get_variable_name_pretty(variableLabel))
    if nVars == 1:
        hist.GetYaxis().SetTitle('Scalefactor')
    if nVars == 2:
        hist.SetOption('colz')
        hist.GetZaxis().SetTitle('Scalefactor')
    hist_stat = hist.Clone(extEffName+'_stat')
    hist_syst = hist.Clone(extEffName+'_syst')
    histList_syst = {'combined': hist.Clone(effName+'_combinedSyst')}

    hist_dataEff = hist.Clone(extEffName+'_efficiencyData')
    if nVars == 1:
        hist_dataEff.GetYaxis().SetTitle('Efficiency')
    if nVars == 2:
        hist_dataEff.GetZaxis().SetTitle('Efficiency')
    hist_dataEff_stat = hist_dataEff.Clone(extEffName+'_efficiencyData_stat')
    hist_dataEff_syst = hist_dataEff.Clone(extEffName+'_efficiencyData_syst')
    histList_dataEff_syst = {
        'combined': hist_dataEff.Clone(effName+'_efficiencyData_combinedSyst')
    }
    hist_mcEff = hist_dataEff.Clone(extEffName+'_efficiencyMC')
    hist_mcEff_stat = hist_dataEff.Clone(extEffName+'_efficiencyMC_stat')
    hist_mcEff_syst = hist_dataEff.Clone(extEffName+'_efficiencyMC_syst')
    histList_mcEff_syst = {
        'combined': hist_dataEff.Clone(effName+'_efficiencyMC_combinedSyst')
    }
    for iSyst in itertools.chain(systList['SF']['fitTypes'],
                                 systList['SF']['shiftTypes']):
        histList_syst.update({iSyst: hist.Clone(effName+'_'+iSyst)})
    for iSyst in itertools.chain(systList['dataEff']['fitTypes'],
                                 systList['dataEff']['shiftTypes']):
        histList_dataEff_syst.update({iSyst: hist.Clone(effName+'_'+iSyst)})
    for iSyst in itertools.chain(systList['mcEff']['fitTypes'],
                                 systList['mcEff']['shiftTypes']):
        histList_mcEff_syst.update({iSyst: hist.Clone(effName+'_'+iSyst)})

    varName = get_variables_name(variableLabels)

    # iterate through the bin indices
    # this does nested for loops of the N-D binning (e.g. pt, eta)
    # binning starts at 1 (0 is underflow), same as ROOT
    indices = [list(range(1, len(binning[variableLabel])))
               for variableLabel in variableLabels]
    output = {effName: {varName: {}}}
    for index in itertools.product(*indices):
        binName = get_full_name(num, denom, variableLabels, index)
        subVarKeys = [
            '{}:[{},{}]'.format(
                variableLabels[i],
                binning[variableLabels[i]][ind-1],
                binning[variableLabels[i]][ind]
            ) for i, ind in enumerate(index)
        ]
        _out = output[effName][varName]

        # add binning definitions
        _out['binning'] = [
            {
                'variable': vl,
                'binning': binning[vl].tolist(),
            }
            for vl in variableLabels
        ]

        for subVarKey in subVarKeys:
            if subVarKey not in _out:
                _out[subVarKey] = {}
            _out = _out[subVarKey]

        # the fitted distributions
        fitType = 'Nominal'
        dataFNameFit = os.path.join(baseDir, 'fits_data',
                                    particle, probe,
                                    resonance, era,
                                    fitType, effName,
                                    binName + '.root')
        sf, sf_stat, dataEff, dataStat, mcEff, mcStat = getSF(
            binName, dataFNameFit)
        sf_syst = getSyst(binName, dataFNameFit,
                          dataEff, mcEff,
                          systList['SF']['fitTypes'],
                          systList['SF']['shiftTypes'])
        dataSyst = getSyst(binName, dataFNameFit,
                           dataEff, mcEff,
                           systList['dataEff']['fitTypes'],
                           systList['dataEff']['shiftTypes'])
        mcSyst = getSyst(binName, dataFNameFit,
                         dataEff, mcEff,
                         systList['mcEff']['fitTypes'],
                         systList['mcEff']['shiftTypes'])
        sf_err = (sf_stat**2 + sf_syst['combined']**2)**0.5
        dataErr = (dataStat**2 + dataSyst['combined']**2)**0.5
        mcErr = (mcStat**2 + mcSyst['combined']**2)**0.5
        _out['value'] = sf
        _out['stat'] = sf_stat
        _out['syst'] = sf_syst['combined']
        for s in itertools.chain(systList['SF']['fitTypes'],
                                 systList['SF']['shiftTypes']):
            _out[s] = sf_syst[s]

        def set_bin(hist, index, val, err):
            index = list(index)
            val_args = index + [val]
            err_args = index + [err]
            hist.SetBinContent(*val_args)
            if err >= 0:
                hist.SetBinError(*err_args)

        set_bin(hist, index, sf, sf_err)
        set_bin(hist_stat, index, sf, sf_stat)
        set_bin(hist_syst, index, sf, sf_syst['combined'])
        for iKey in sf_syst.keys():
            set_bin(histList_syst[iKey], index, sf_syst[iKey], -1)

        set_bin(hist_dataEff, index, dataEff, dataErr)
        set_bin(hist_dataEff_stat, index, dataEff, dataStat)
        set_bin(hist_dataEff_syst, index, dataEff, dataSyst['combined'])
        for iKey in dataSyst.keys():
            set_bin(histList_dataEff_syst[iKey], index, dataSyst[iKey], -1)

        set_bin(hist_mcEff, index, mcEff, mcErr)
        set_bin(hist_mcEff_stat, index, mcEff, mcStat)
        set_bin(hist_mcEff_syst, index, mcEff, mcSyst['combined'])
        for iKey in mcSyst.keys():
            set_bin(histList_mcEff_syst[iKey], index, mcSyst[iKey], -1)

    hists[extEffName] = hist
    hists[extEffName+'_stat'] = hist_stat
    hists[extEffName+'_syst'] = hist_syst
    hists[extEffName+'_efficiencyData'] = hist_dataEff
    hists[extEffName+'_efficiencyData_stat'] = hist_dataEff_stat
    hists[extEffName+'_efficiencyData_syst'] = hist_dataEff_syst
    hists[extEffName+'_efficiencyMC'] = hist_mcEff
    hists[extEffName+'_efficiencyMC_stat'] = hist_mcEff_stat
    hists[extEffName+'_efficiencyMC_syst'] = hist_mcEff_syst
    for iKey in histList_syst.keys():
        hists[effName+'_'+iKey] = histList_syst[iKey]
    for iKey in histList_dataEff_syst.keys():
        hists[effName+'_efficiencyData_'+iKey] = histList_dataEff_syst[iKey]
    for iKey in histList_mcEff_syst.keys():
        hists[effName+'_efficiencyMC_'+iKey] = histList_mcEff_syst[iKey]

    # save the efficiency
    plotDir = os.path.join(baseDir, 'plots',
                           particle, probe,
                           resonance, era,
                           effName, 'efficiency')
    os.makedirs(plotDir, exist_ok=True)

    effDir = os.path.join(baseDir, 'efficiencies',
                          particle, probe,
                          resonance, era,
                          effName)
    os.makedirs(effDir, exist_ok=True)
    effPath = os.path.join(effDir, extEffName)

    # JSON format
    with open('{}.json'.format(effPath), 'w') as f:
        f.write(json.dumps(output, indent=4, sort_keys=True))

    # ROOT histogram format
    tfile = ROOT.TFile.Open('{}.root'.format(effPath), 'recreate')
    for h in sorted(hists):
        hists[h].Write(h)

        if nVars == 2:
            cName = 'c' + h
            canvas = ROOT.TCanvas(cName, cName, 1000, 800)
            ROOT.gStyle.SetPaintTextFormat("5.3f")
            canvas.SetRightMargin(0.24)
            hists[h].Draw('colz text')
            plotPath = os.path.join(plotDir, h)
            canvas.Modified()
            canvas.Update()
            canvas.Print('{}.png'.format(plotPath))
            canvas.Print('{}.pdf'.format(plotPath))

    tfile.Close()

    # gets a graph projection of an ND histogram for a given axis
    # with axis index (ie x,y,z = 0,1,2) and other dimensions ind
    def get_graph(hist, axis, axis_ind, *ind):
        ind = list(ind)
        ni = axis.GetNbins()
        xvals = [axis.GetBinCenter(i+1) for i in range(ni)]
        xvals_errLow = [xvals[i]-axis.GetBinLowEdge(i+1) for i in range(ni)]
        xvals_errHigh = [axis.GetBinUpEdge(i+1)-xvals[i] for i in range(ni)]
        yvals = [
            hist.GetBinContent(
                *ind[:axis_ind]
                + [i+1]
                + ind[axis_ind:]
            ) for i in range(ni)]
        yvals_err = [
            hist.GetBinError(
                *ind[:axis_ind]
                + [i+1]
                + ind[axis_ind:]
            ) for i in range(ni)]
        graph = ROOT.TGraphAsymmErrors(
            ni,
            array('d', xvals),
            array('d', yvals),
            array('d', xvals_errLow),
            array('d', xvals_errHigh),
            array('d', yvals_err),
            array('d', yvals_err),
        )
        return graph

    # plot the efficiencies
    # enumerate over the axis/variable to plot
    axes = [hists[extEffName].GetXaxis(),
            hists[extEffName].GetYaxis(),
            hists[extEffName].GetZaxis()]
    for vi, variableLabel in enumerate(variableLabels):

        # iterate over the other axis indices
        otherVariableLabels = [ovl for ovl in variableLabels
                               if ovl != variableLabel]
        otherVariableIndices = [ovi for ovi, ovl in enumerate(variableLabels)
                                if ovl != variableLabel]
        indices = [list(range(1, len(binning[vl])))
                   for vl in otherVariableLabels]
        if indices:
            for index in itertools.product(*indices):
                graph_data = get_graph(hists[extEffName+'_efficiencyData'],
                                       axes[vi], vi, *index)
                graph_data.SetLineColor(ROOT.kBlack)
                graph_data.SetMarkerColor(ROOT.kBlack)
                graph_mc = get_graph(hists[extEffName+'_efficiencyMC'],
                                     axes[vi], vi, *index)
                graph_mc.SetLineColor(ROOT.kBlue)
                graph_mc.SetMarkerColor(ROOT.kBlue)
                mg = ROOT.TMultiGraph()
                mg.Add(graph_data)
                mg.Add(graph_mc)

                cName = 'c' + extEffName + '_'.join([str(i) for i in index])\
                    + variableLabel
                canvas = ROOT.TCanvas(cName, cName, 800, 800)
                mg.Draw('AP0')
                mg.GetXaxis().SetTitle(get_variable_name_pretty(variableLabel))
                xRange = [axes[vi].GetBinLowEdge(1),
                          axes[vi].GetBinUpEdge(axes[vi].GetNbins())]
                mg.GetXaxis().SetRangeUser(*xRange)
                mg.GetYaxis().SetTitle('Efficiency')
                mg.GetYaxis().SetRangeUser(0.8, 1.10)
                legend = ROOT.TLegend(0.5, 0.70, 0.92, 0.92)
                legend.SetTextFont(42)
                legend.SetBorderSize(0)
                legend.SetFillColor(0)
                legend.AddEntry(graph_data, 'Data', 'l')
                legend.AddEntry(graph_mc, 'Simulation', 'l')
                legend.SetHeader('{} / {}'.format(num, denom))
                legend.Draw()

                nother = len(indices)
                dims = [0.18, 0.84-nother*0.04-0.02, 0.35, 0.84]
                text = ROOT.TPaveText(*dims+['NB NDC'])
                text.SetTextFont(42)
                text.SetBorderSize(0)
                text.SetFillColor(0)
                text.SetTextAlign(11)
                text.SetTextSize(0.03)
                for novi, (ovi, ovl) in enumerate(zip(otherVariableIndices,
                                                      otherVariableLabels)):
                    xlow = axes[ovi].GetBinLowEdge(index[novi])
                    xhigh = axes[ovi].GetBinUpEdge(index[novi])
                    rtext = '{} < {} < {}'.format(
                        xlow, get_variable_name_pretty(ovl), xhigh)
                    text.AddText(rtext)
                text.Draw()
                CMS_lumi.cmsText = 'CMS'
                CMS_lumi.writeExtraText = True
                CMS_lumi.extraText = 'Preliminary'
                CMS_lumi.lumi_13TeV = "%0.1f fb^{-1}" % (41.5)
                CMS_lumi.CMS_lumi(canvas, 4, 11)
                plotDir = os.path.join(baseDir, 'plots',
                                       particle, probe,
                                       resonance, era,
                                       effName, 'efficiency')
                os.makedirs(plotDir, exist_ok=True)
                otherVariableLabel = get_bin_name(otherVariableLabels, index)
                plotName = '{}_{}_vs_{}'.format(effName,
                                                otherVariableLabel,
                                                variableLabel)
                plotPath = os.path.join(plotDir, plotName)
                canvas.Print('{}.png'.format(plotPath))
                canvas.Print('{}.pdf'.format(plotPath))

        # if no indices, easier, just itself
        else:
            graph_data = get_graph(hists[extEffName+'_efficiencyData'],
                                   axes[vi], vi)
            graph_data.SetLineColor(ROOT.kBlack)
            graph_data.SetMarkerColor(ROOT.kBlack)
            graph_mc = get_graph(hists[extEffName+'_efficiencyMC'],
                                 axes[vi], vi)
            graph_mc.SetLineColor(ROOT.kBlue)
            graph_mc.SetMarkerColor(ROOT.kBlue)
            mg = ROOT.TMultiGraph()
            mg.Add(graph_data)
            mg.Add(graph_mc)

            canvas = ROOT.TCanvas('c'+extEffName, 'c', 800, 800)
            mg.Draw('AP0')
            mg.GetXaxis().SetTitle(get_variable_name_pretty(variableLabel))
            mg.GetYaxis().SetTitle('Efficiency')
            mg.GetYaxis().SetRangeUser(0.8, 1.10)
            legend = ROOT.TLegend(0.5, 0.70, 0.92, 0.92)
            legend.SetTextFont(42)
            legend.SetBorderSize(0)
            legend.SetFillColor(0)
            legend.AddEntry(graph_data, 'Data', 'l')
            legend.AddEntry(graph_mc, 'Simulation', 'l')
            legend.SetHeader('{} / {}'.format(num, denom))
            legend.Draw()

            CMS_lumi.cmsText = 'CMS'
            CMS_lumi.writeExtraText = True
            CMS_lumi.extraText = 'Preliminary'
            CMS_lumi.lumi_13TeV = "%0.1f fb^{-1}" % (41.5)
            CMS_lumi.CMS_lumi(canvas, 4, 11)
            plotDir = os.path.join(baseDir, 'plots',
                                   particle, probe,
                                   resonance, era,
                                   effName, 'efficiency')
            os.makedirs(plotDir, exist_ok=True)
            plotName = '{}_vs_{}'.format(effName, variableLabel)
            plotPath = os.path.join(plotDir, plotName)
            canvas.Print('{}.png'.format(plotPath))
            canvas.Print('{}.pdf'.format(plotPath))


def build_prepare_jobs(particle, probe, resonance, era,
                       config, **kwargs):
    _baseDir = kwargs.pop('baseDir', '')
    _numerator = kwargs.pop('numerator', [])
    _denominator = kwargs.pop('denominator', [])

    jobs = []
    # iterate through the efficiencies
    efficiencies = config.efficiencies()
    for num, denom in efficiencies:
        if _numerator and num not in _numerator:
            continue
        if _denominator and denom not in _denominator:
            continue

        # iterate through the output binning structure
        for variableLabels in config.binVariables():

            jobs += [[_baseDir, particle, probe, resonance, era,
                     config, num, denom, tuple(variableLabels)]]

    return jobs
