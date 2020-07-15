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
from registry import registry

ROOT.gROOT.SetBatch()
ROOT.gROOT.ProcessLine("gErrorIgnoreLevel = 1001;")
tdrstyle.setTDRStyle()


def computeEff(n1, n2, e1, e2):
    eff = n1 / (n1 + n2)
    err = 1 / (n1 + n2) * math.sqrt(
        e1 * e1 * n2 * n2 + e2 * e2 * n1 * n1) / (n1 + n2)
    return eff, err


def getEff(binName, fname, shift=None):
    try:
        tfile = ROOT.TFile(fname, 'read')
        hP = tfile.Get('{}_GenPass'.format(binName))
        hF = tfile.Get('{}_GenFail'.format(binName))
        # hard code Z for now (same as in run_single_fit.py)
        if shift == 'massRangUp':
            blow, bhigh = 75, 135
        elif shift == 'massRangeDown':
            blow, bhigh = 65, 125
        else:
            blow, bhigh = 70, 130
        bin1 = hP.GetXaxis().FindBin(blow)
        bin2 = hP.GetXaxis().FindBin(bhigh)
        eP = ctypes.c_double(-1.0)
        eF = ctypes.c_double(-1.0)
        nP = hP.IntegralAndError(bin1, bin2, eP)
        nF = hF.IntegralAndError(bin1, bin2, eF)
        eff, err = computeEff(nP, nF, eP.value, eF.value)
        tfile.Close()
        return eff, err
    except Exception as e:
        print('Exception for', binName)
        print(e)
        # raise e
        return 1., 0.


def getDataEff(binName, fname, shift=None):
    try:
        tfile = ROOT.TFile(fname, 'read')
        fitresP = tfile.Get('{}_resP'.format(binName))
        fitresF = tfile.Get('{}_resF'.format(binName))

        fitP = fitresP.floatParsFinal().find('nSigP')
        fitF = fitresF.floatParsFinal().find('nSigF')

        nP = fitP.getVal()
        nF = fitF.getVal()
        eP = fitP.getError()
        eF = fitF.getError()

        hP = tfile.Get('{}_Pass'.format(binName))
        hF = tfile.Get('{}_Fail'.format(binName))
        # hard code Z for now (same as in run_single_fit.py)
        if shift == 'massRangUp':
            blow, bhigh = 75, 135
        elif shift == 'massRangeDown':
            blow, bhigh = 65, 125
        else:
            blow, bhigh = 70, 130
        bin1 = hP.GetXaxis().FindBin(blow)
        bin2 = hP.GetXaxis().FindBin(bhigh)
        ePalt = ctypes.c_double(-1.0)
        eFalt = ctypes.c_double(-1.0)
        hP.IntegralAndError(bin1, bin2, ePalt)
        hF.IntegralAndError(bin1, bin2, eFalt)

        eP = min(eP, ePalt.value)
        eF = min(eF, eFalt.value)

        tfile.Close()

        return computeEff(nP, nF, eP, eF)
    except Exception as e:
        print('Exception for', binName)
        print(e)
        # raise e
        return 1., 0.


def getSF(binName, fname, shift=None):
    mcEff, mcErr = getEff(binName, fname, shift)
    dataEff, dataErr = getDataEff(binName, fname, shift)
    sf = dataEff / mcEff if mcEff else 0.0
    sf_err = 0.0
    if dataEff and mcEff:
        sf_err = sf * ((dataErr / dataEff)**2 + (mcErr / mcEff)**2)**0.5
    return sf, sf_err, dataEff, dataErr, mcEff, mcErr


def getSyst(binName, fname, fitTypes, shiftTypes):
    sf, sf_err, dataEff, dataErr, mcEff, mcErr = getSF(binName, fname)

    syst = {}
    for isyst in fitTypes:
        systfname = fname.replace('Nominal', isyst)
        # sf, sf_err, dataEff, dataErr, mcEff, mcErr
        tmp = getSF(binName, systfname, isyst)
        syst[isyst] = {
            'sf': tmp[0],
            'err': abs(tmp[0]-sf),
            'dataEff': tmp[2],
            'dataErr': abs(tmp[2]-dataEff),
            'mcEff': tmp[4],
            'mcErr': abs(tmp[4]-mcEff),
        }

    for isyst in shiftTypes:
        systUpfname = fname.replace('Nominal', isyst+'Up')
        systDnfname = fname.replace('Nominal', isyst+'Down')
        # sf, sf_err, dataEff, dataErr, mcEff, mcErr
        tmpUp = getSF(binName, systUpfname, isyst+'Up')
        tmpDn = getSF(binName, systDnfname, isyst+'Down')
        tmp = [
            (tmpUp[0]+tmpDn[0])/2,
            (abs(tmpUp[0]-sf)+abs(tmpDn[0]-sf))/2,
            (tmpUp[2]+tmpDn[2])/2,
            (abs(tmpUp[2]-dataEff)+abs(tmpDn[2]-dataEff))/2,
            (tmpUp[4]+tmpDn[4])/2,
            (abs(tmpUp[4]-mcEff)+abs(tmpDn[4]-mcEff))/2,
        ]
        syst[isyst] = {
            'sf': tmp[0],
            'err': tmp[1],
            'dataEff': tmp[2],
            'dataErr': tmp[3],
            'mcEff': tmp[4],
            'mcErr': tmp[5],
        }
        syst[isyst+'Up'] = {
            'sf': tmpUp[0],
            'err': abs(tmpUp[0]-sf),
            'dataEff': tmpUp[2],
            'dataErr': abs(tmpUp[2]-dataEff),
            'mcEff': tmpUp[4],
            'mcErr': abs(tmpUp[4]-mcEff),
        }
        syst[isyst+'Down'] = {
            'sf': tmpDn[0],
            'err': abs(tmpDn[0]-sf),
            'dataEff': tmpDn[2],
            'dataErr': abs(tmpDn[2]-dataEff),
            'mcEff': tmpDn[4],
            'mcErr': abs(tmpDn[4]-mcEff),
        }

    return syst


def prepare(baseDir, particle, probe, resonance, era,
            config, num, denom, variableLabels):

    subEra = era.split('_')[0]  # data subera is beginning of era
    lumi = registry.luminosity(particle, probe, resonance, era, subEra)
    hists = {}

    effName = get_eff_name(num, denom)
    extEffName = get_extended_eff_name(num, denom, variableLabels)
    binning = config.binning()
    dataSubEra, mcSubEra = get_data_mc_sub_eras(resonance, era)

    systList = config.get('systematics',
                          {x: {'fitTypes': [],
                               'shiftTypes': []}
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
    histList_syst = {
        'combined_syst': hist.Clone(extEffName+'_combined_syst'),
    }
    histList_syst['combined_syst'].GetZaxis().SetTitle('Uncertainty')

    hist_dataEff = hist.Clone(extEffName+'_efficiencyData')
    if nVars == 1:
        hist_dataEff.GetYaxis().SetTitle('Efficiency')
    if nVars == 2:
        hist_dataEff.GetZaxis().SetTitle('Efficiency')
    hist_dataEff_stat = hist_dataEff.Clone(extEffName+'_efficiencyData_stat')
    hist_dataEff_syst = hist_dataEff.Clone(extEffName+'_efficiencyData_syst')
    histList_dataEff_syst = {
        'combined_syst': hist_dataEff.Clone(
            extEffName+'_efficiencyData_combined_syst'),
    }
    histList_dataEff_syst['combined_syst'].GetZaxis().SetTitle('Uncertainty')
    hist_mcEff = hist_dataEff.Clone(extEffName+'_efficiencyMC')
    hist_mcEff_stat = hist_dataEff.Clone(extEffName+'_efficiencyMC_stat')
    hist_mcEff_syst = hist_dataEff.Clone(extEffName+'_efficiencyMC_syst')
    histList_mcEff_syst = {
        'combined_syst': hist_dataEff.Clone(
            extEffName+'_efficiencyMC_combined_syst'),
    }
    histList_mcEff_syst['combined_syst'].GetZaxis().SetTitle('Uncertainty')

    # the individual systematics
    for iSyst in itertools.chain(systList['SF']['fitTypes'],
                                 systList['SF']['shiftTypes']):
        histList_syst[iSyst] = hist.Clone(extEffName+'_'+iSyst)
        histList_syst[iSyst+'_syst'] = hist.Clone(extEffName+'_'+iSyst+'_syst')
        histList_syst[iSyst+'_syst'].GetZaxis().SetTitle('Uncertainty')
    for iSyst in itertools.chain(systList['dataEff']['fitTypes'],
                                 systList['dataEff']['shiftTypes']):
        histList_dataEff_syst[iSyst] = hist_dataEff.Clone(extEffName+'_'+iSyst)
        histList_dataEff_syst[iSyst+'_syst'] = hist_dataEff.Clone(
            extEffName+'_'+iSyst+'_syst')
        histList_dataEff_syst[iSyst+'_syst'].GetZaxis().SetTitle('Uncertainty')
    for iSyst in itertools.chain(systList['mcEff']['fitTypes'],
                                 systList['mcEff']['shiftTypes']):
        histList_mcEff_syst[iSyst] = hist_mcEff.Clone(extEffName+'_'+iSyst)
        histList_mcEff_syst[iSyst+'_syst'] = hist_mcEff.Clone(
            extEffName+'_'+iSyst+'_syst')
        histList_mcEff_syst[iSyst+'_syst'].GetZaxis().SetTitle('Uncertainty')

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
        fitTypes = set(systList['SF']['fitTypes']
                       + systList['dataEff']['fitTypes']
                       + systList['mcEff']['fitTypes'])
        shiftTypes = set(systList['SF']['shiftTypes']
                         + systList['dataEff']['shiftTypes']
                         + systList['mcEff']['shiftTypes'])
        sf_syst = getSyst(binName, dataFNameFit,
                          fitTypes, shiftTypes)

        combined_syst = {}
        for kind in ['SF', 'dataEff', 'mcEff']:
            combined_syst[kind] = 0
            errKey = 'err'
            if kind == 'dataEff':
                errKey = 'dataErr'
            if kind == 'mcEff':
                errKey = 'mcErr'
            for t in itertools.chain(systList[kind]['fitTypes'],
                                     systList[kind]['shiftTypes']):
                combined_syst[kind] += sf_syst[t][errKey]**2
            combined_syst[kind] = combined_syst[kind]**0.5

        sf_err = (sf_stat**2 + combined_syst['SF']**2)**0.5
        dataErr = (dataStat**2 + combined_syst['dataEff']**2)**0.5
        mcErr = (mcStat**2 + combined_syst['mcEff']**2)**0.5
        _out['value'] = sf
        _out['stat'] = sf_stat
        _out['syst'] = combined_syst['SF']
        for s in itertools.chain(systList['SF']['fitTypes'],
                                 systList['SF']['shiftTypes']):
            _out[s] = sf_syst[s]['err']

        def set_bin(hist, index, val, err):
            index = list(index)
            val_args = index + [val]
            err_args = index + [err]
            hist.SetBinContent(*val_args)
            if err >= 0:
                hist.SetBinError(*err_args)

        set_bin(hist, index, sf, sf_err)
        set_bin(hist_stat, index, sf, sf_stat)
        set_bin(hist_syst, index, sf, combined_syst['SF'])
        set_bin(histList_syst['combined_syst'], index,
                combined_syst['SF'], -1)
        set_bin(hist_dataEff, index, dataEff, dataErr)
        set_bin(hist_dataEff_stat, index, dataEff, dataStat)
        set_bin(hist_dataEff_syst, index, dataEff, combined_syst['dataEff'])
        set_bin(histList_dataEff_syst['combined_syst'], index,
                combined_syst['dataEff'], -1)
        set_bin(hist_mcEff, index, mcEff, mcErr)
        set_bin(hist_mcEff_stat, index, mcEff, mcStat)
        set_bin(hist_mcEff_syst, index, mcEff, combined_syst['mcEff'])
        set_bin(histList_mcEff_syst['combined_syst'], index,
                combined_syst['mcEff'], -1)
        for iKey in sf_syst.keys():
            if iKey in histList_syst:
                set_bin(histList_syst[iKey], index,
                        sf_syst[iKey]['sf'], sf_syst[iKey]['err'])
                set_bin(histList_syst[iKey+'_syst'], index,
                        sf_syst[iKey]['err'], -1)

            if iKey in histList_dataEff_syst:
                set_bin(histList_dataEff_syst[iKey], index,
                        sf_syst[iKey]['dataEff'], sf_syst[iKey]['dataErr'])
                set_bin(histList_dataEff_syst[iKey+'_syst'], index,
                        sf_syst[iKey]['dataErr'], -1)

            if iKey in histList_mcEff_syst:
                set_bin(histList_mcEff_syst[iKey], index,
                        sf_syst[iKey]['mcEff'], sf_syst[iKey]['mcErr'])
                set_bin(histList_mcEff_syst[iKey+'_syst'], index,
                        sf_syst[iKey]['mcErr'], -1)

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
        hname = extEffName+'_'+iKey
        hists[hname] = histList_syst[iKey]
    for iKey in histList_dataEff_syst.keys():
        hname = extEffName+'_efficiencyData_'+iKey
        hists[hname] = histList_dataEff_syst[iKey]
    for iKey in histList_mcEff_syst.keys():
        hname = extEffName+'_efficiencyMC_'+iKey
        hists[hname] = histList_mcEff_syst[iKey]

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

            CMS_lumi.cmsText = 'CMS'
            CMS_lumi.writeExtraText = True
            CMS_lumi.extraText = 'Preliminary'
            CMS_lumi.lumi_13TeV = "%0.1f fb^{-1}" % (lumi)
            CMS_lumi.CMS_lumi(canvas, 4, 0)

            canvas.Print('{}.png'.format(plotPath))
            canvas.Print('{}.pdf'.format(plotPath))
            canvas.Print('{}.root'.format(plotPath))

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
    # some default colors for plots
    colors = [ROOT.kBlack, ROOT.kBlue, ROOT.kRed, ROOT.kGreen+2,
              ROOT.kMagenta+1, ROOT.kOrange+1, ROOT.kTeal-1,
              ROOT.kRed-3, ROOT.kCyan+2]

    def plot_1d_eff(savename, graphs,
                    labels=['Data', 'Simulation'],
                    colors=colors,
                    xlabel='', ylabel='Efficiency',
                    xRange=[], additional_text=[]):
        ng = len(graphs)
        mg = ROOT.TMultiGraph()
        for gi in range(ng):
            graphs[gi].SetLineColor(colors[gi])
            graphs[gi].SetMarkerColor(colors[gi])
            mg.Add(graphs[gi])

        canvas = ROOT.TCanvas(savename, savename, 800, 800)
        mg.Draw('AP0')
        mg.GetXaxis().SetTitle(xlabel)
        if xRange:
            mg.GetXaxis().SetRangeUser(*xRange)
        mg.GetYaxis().SetTitle(ylabel)
        mg.GetYaxis().SetRangeUser(0.8, 1.10)
        legend = ROOT.TLegend(0.5, 0.70, 0.92, 0.92)
        legend.SetTextFont(42)
        legend.SetBorderSize(0)
        legend.SetFillColor(0)
        for gi in range(ng):
            legend.AddEntry(graphs[gi], labels[gi], 'l')
        legend.SetHeader('{} / {}'.format(num, denom))
        legend.Draw()

        if additional_text:
            nother = len(additional_text)
            dims = [0.18, 0.84-nother*0.04-0.02, 0.35, 0.84]
            text = ROOT.TPaveText(*dims+['NB NDC'])
            text.SetTextFont(42)
            text.SetBorderSize(0)
            text.SetFillColor(0)
            text.SetTextAlign(11)
            text.SetTextSize(0.03)
            for rtext in additional_text:
                text.AddText(rtext)
            text.Draw()

        CMS_lumi.cmsText = 'CMS'
        CMS_lumi.writeExtraText = True
        CMS_lumi.extraText = 'Preliminary'
        CMS_lumi.lumi_13TeV = "%0.1f fb^{-1}" % (lumi)
        CMS_lumi.CMS_lumi(canvas, 4, 11)

        canvas.Print('{}.png'.format(savename))
        canvas.Print('{}.pdf'.format(savename))
        canvas.Print('{}.root'.format(savename))

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
                graph_mc = get_graph(hists[extEffName+'_efficiencyMC'],
                                     axes[vi], vi, *index)
                xlabel = get_variable_name_pretty(variableLabel)
                ylabel = 'Efficiency'
                xRange = [axes[vi].GetBinLowEdge(1),
                          axes[vi].GetBinUpEdge(axes[vi].GetNbins())]
                additional_text = []
                for novi, (ovi, ovl) in enumerate(zip(otherVariableIndices,
                                                      otherVariableLabels)):
                    xlow = axes[ovi].GetBinLowEdge(index[novi])
                    xhigh = axes[ovi].GetBinUpEdge(index[novi])
                    rtext = '{} < {} < {}'.format(
                        xlow, get_variable_name_pretty(ovl), xhigh)
                    additional_text += [rtext]
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
                plot_1d_eff(plotPath, [graph_data, graph_mc],
                            xlabel=xlabel, ylabel=ylabel,
                            xRange=xRange, additional_text=additional_text)

                # dataEfficiency systs
                graphs = [get_graph(hists[extEffName+'_efficiencyData'],
                                    axes[vi], vi, *index)]
                labels = ['Nominal']
                for iSyst in itertools.chain(
                        systList['dataEff']['fitTypes'],
                        systList['dataEff']['shiftTypes']):
                    graphs += [get_graph(
                        hists[extEffName+'_efficiencyData_'+iSyst],
                        axes[vi], vi, *index)]
                    labels += [iSyst]
                plotName = '{}_{}_vs_{}_efficiencyData_syst'.format(
                    effName,
                    otherVariableLabel,
                    variableLabel,
                )
                plotPath = os.path.join(plotDir, plotName)
                plot_1d_eff(plotPath, graphs,
                            labels=labels,
                            xlabel=xlabel, ylabel=ylabel,
                            xRange=xRange, additional_text=additional_text)

                # mcEfficiency systs
                graphs = [get_graph(hists[extEffName+'_efficiencyMC'],
                                    axes[vi], vi, *index)]
                labels = ['Nominal']
                for iSyst in itertools.chain(
                        systList['mcEff']['fitTypes'],
                        systList['mcEff']['shiftTypes']):
                    graphs += [get_graph(
                        hists[extEffName+'_efficiencyMC_'+iSyst],
                        axes[vi], vi, *index)]
                    labels += [iSyst]
                plotName = '{}_{}_vs_{}_efficiencyMC_syst'.format(
                    effName,
                    otherVariableLabel,
                    variableLabel,
                )
                plotPath = os.path.join(plotDir, plotName)
                plot_1d_eff(plotPath, graphs,
                            labels=labels,
                            xlabel=xlabel, ylabel=ylabel,
                            xRange=xRange, additional_text=additional_text)

        # if no indices, easier, just itself
        else:
            graph_data = get_graph(hists[extEffName+'_efficiencyData'],
                                   axes[vi], vi)
            graph_mc = get_graph(hists[extEffName+'_efficiencyMC'],
                                 axes[vi], vi)

            xlabel = get_variable_name_pretty(variableLabel)
            ylabel = 'Efficiency'
            xRange = [axes[0].GetBinLowEdge(1),
                      axes[0].GetBinUpEdge(axes[0].GetNbins())]
            plotDir = os.path.join(baseDir, 'plots',
                                   particle, probe,
                                   resonance, era,
                                   effName, 'efficiency')
            os.makedirs(plotDir, exist_ok=True)
            plotName = '{}_vs_{}'.format(effName, variableLabel)
            plotPath = os.path.join(plotDir, plotName)
            plot_1d_eff(plotPath, [graph_data, graph_mc],
                        xlabel=xlabel, ylabel=ylabel,
                        xRange=xRange)


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
