#!/usr/bin/env python
from __future__ import print_function
import os
import sys
import argparse
import json
import math
from array import array
import shutil
import numpy as np
import ROOT
ROOT.gROOT.SetBatch()

import tdrstyle
InitStyle=ROOT.gStyle.Clone()
InitStyle.SetName("InitStyle")
tdrstyle.setTDRStyle()
import CMS_lumi

ROOT.gROOT.ProcessLine("gErrorIgnoreLevel = 1001;") # kInfo = 1001, kWarning = 2001, ...
ROOT.gROOT.SetBatch(True)

#from settings_muon import *
#from utils_muon import *
from muon_definitions import * 

fitBase = '/eos/user/w/wiwong/phys_muon/wiwong/TagAndProbe/fits_mc/muon/Z/Run2017_UL/'
#'/eos/cms/store/group/phys_muon/dntaylor/TagAndProbe/UL17'
logy = True

def computeEff( n1,n2,e1,e2):
    effout = []
    eff   = n1/(n1+n2)
    err = 1/(n1+n2)*math.sqrt(e1*e1*n2*n2+e2*e2*n1*n1)/(n1+n2)
    #if err < 0.001 : err = 0.001
    return eff, err

def getHistEff(binName, fname):
    try:
        tfile = ROOT.TFile(fname, 'read')
        hP = tfile.Get('{}_Pass'.format(binName))
        hF = tfile.Get('{}_Fail'.format(binName))
        bin1 = 1
        bin2 = hP.GetXaxis().GetNbins()
        eP = ROOT.Double(-1.0)
        eF = ROOT.Double(-1.0)
        nP = hP.IntegralAndError(bin1,bin2,eP)
        nF = hF.IntegralAndError(bin1,bin2,eF)
        eff, err = computeEff(nP,nF,eP,eF)
        tfile.Close()
        return eff, err
    except:
        print('exception for', binName)
        return 1., 0.

def getFitEff(binName, fname, fnameFit):
    #print (fnameFit)
    tfile = ROOT.TFile(fnameFit, 'read')
    fitresP = tfile.Get( '%s_resP' % binName)
    fitresF = tfile.Get( '%s_resF' % binName)

    fitP = fitresP.floatParsFinal().find('nSigP')
    fitF = fitresF.floatParsFinal().find('nSigF')

    try:
        nP = fitP.getVal()
        nF = fitF.getVal()
        eP = fitP.getError()
        eF = fitF.getError()
        tfile.Close()

        #tfile = ROOT.TFile(fname, 'read' )
        #hP = tfile.Get('%s_Pass'%binName)
        #hF = tfile.Get('%s_Fail'%binName)

        #if eP > math.sqrt(hP.Integral()) : eP = math.sqrt(hP.Integral())
        #if eF > math.sqrt(hF.Integral()) : eF = math.sqrt(hF.Integral())
        #tfile.Close()

        return computeEff(nP,nF,eP,eF)
    except:
        print('exception for', binName)
        return 1., 0.

def getSF(binName, dataFName, mcFName, dataFNameFit, mcFNameFit):
    print('Loading',binName)
    mcEff, mcErr = getFitEff(binName, mcFName, mcFNameFit)
    dataEff, dataErr = getFitEff(binName, dataFName, dataFNameFit)
    sf = dataEff/mcEff if mcEff else 0.0
    sf_err = sf * ((dataErr/dataEff)**2 + (mcErr/mcEff)**2)**0.5 if dataEff and mcEff else 0.0
    return sf, sf_err, dataEff, dataErr, mcEff, mcErr

def getSyst(binName, dataFName, mcFName, dataFNameFit, mcFNameFit, dataEff, mcEff, systFitTypes, systShiftTypes):
    #print('Loading',binName)
    #systFitTypes = ['AltBkg','AltSig']
    #systShiftTypes = ['tagIso','massBin']
    syst = {}
    syst_sq = 0
    for isyst in systFitTypes:
        systFName = dataFName.replace('Nominal', isyst)
        systFNameFit = dataFNameFit.replace('Nominal', isyst)
        tmpEff, tmpErr = getFitEff(binName,systFName,systFNameFit)
        syst.update({isyst : math.fabs(tmpEff - dataEff)})
        syst_sq += (tmpEff - dataEff)**2

    for isyst in systShiftTypes:
        systUpFName = mcFName.replace('Nominal', isyst+'Up')
        systDnFName = mcFName.replace('Nominal', isyst+'Down')
        systUpFNameFit = mcFNameFit.replace('Nominal', isyst+'Up')
        systDnFNameFit = mcFNameFit.replace('Nominal', isyst+'Down')
        tmpEffUp, tmpErr = getFitEff(binName,systUpFName,systUpFNameFit)
        tmpEffDn, tmpErr = getFitEff(binName,systDnFName,systDnFNameFit)
        syst.update({isyst : (math.fabs(tmpEffUp-tmpEffDn)/2)})
        syst_sq += ((tmpEffUp-tmpEffDn)/2)**2
    #mcEff, mcErr = getFitEff(binName, mcFName, mcFNameFit)
    #dataEff, dataErr = getFitEff(binName, dataFName, dataFNameFit)
    #sf = dataEff/mcEff if mcEff else 0.0
    #sf_err = sf * ((dataErr/dataEff)**2 + (mcErr/mcEff)**2)**0.5 if dataEff and mcEff else 0.0
    syst.update({'combined' : (syst_sq)**0.5})
    return syst

def main():
    output = {}
    hists = {}
    definitions = get_default_num_denom()
    definitions = [('TightIDandMiniIso','TightID')] 
    binVariables = get_default_binning_variables()
    labelVariableMap = get_default_ids(False) 
    binning = get_default_binning()
    systList = {
        'SF': {'fitTypes': ['AltBkg','AltSig'], 'shiftTypes': ['tagIso','massBin']},
        'dataEff' : {'fitTypes': ['AltBkg','AltSig'], 'shiftTypes': []},
        'mcEff' : {'fitTypes': [], 'shiftTypes': ['tagIso','massBin']}
    }
    # iterate through the efficiencies
    for num, denom in definitions:

        # iterate through the output binning structure
        for variableLabels in binVariables:
            # old style
            #variableNames = [labelVariableMap[num].get(variableLabel,variableLabel) for variableLabel in variableLabels]

            effName = get_eff_name(num,denom)
            nVars = len(variableLabels)

            if nVars==1:
                THX = ROOT.TH1F
            elif nVars==2:
                THX = ROOT.TH2F
            elif nVars==3:
                THX = ROOT.TH3F
            else:
                raise NotImplementedError('More than 3 dimensions are not supported for scale factors')

            print(effName)
            hargs = [effName,effName]
            for variableLabel in variableLabels:
                hargs += [len(binning[variableLabel]) - 1 , array('d', binning[variableLabel])]
                print(binning[variableLabel])
            hist = THX(*hargs)
            axes = [hist.GetXaxis(), hist.GetYaxis(), hist.GetZaxis()]
            for vi, variableLabel in enumerate(variableLabels):
                axes[vi].SetTitle(variableLabel)
            if nVars==1:
                hist.GetYaxis().SetTitle('Scalefactor')
            if nVars==2:
                hist.SetOption('colz')
                hist.GetZaxis().SetTitle('Scalefactor')
            hist_stat = hist.Clone(effName+'_stat')
            hist_syst = hist.Clone(effName+'_syst')
            hists_syst = {'combined' : hist.Clone(effName+'_combinedSyst')}
            hist_dataEff = hist.Clone(effName+'_efficiencyData')
            if nVars==1:
                hist_dataEff.GetYaxis().SetTitle('Efficiency')
            if nVars==2:
                hist_dataEff.GetZaxis().SetTitle('Efficiency')
            hist_dataEff_stat = hist_dataEff.Clone(effName+'_efficiencyData_stat')
            hist_dataEff_syst = hist_dataEff.Clone(effName+'_efficiencyData_syst')
            hists_dataEff_syst = {'combined': hist_dataEff.Clone(effName+'_efficiencyData_combinedSyst')}
            hist_mcEff = hist_dataEff.Clone(effName+'_efficiencyMC')
            hist_mcEff_stat = hist_dataEff.Clone(effName+'_efficiencyMC_stat')
            hist_mcEff_syst = hist_dataEff.Clone(effName+'_efficiencyMC_syst')
            hists_mcEff_syst = {'combined': hist_dataEff.Clone(effName+'_efficiencyMC_combinedSyst')}
            for iSyst in itertools.chain(systList['SF']['fitTypes'],systList['SF']['shiftTypes']):
                hists_syst.update({iSyst: hist.Clone(effName+'_'+iSyst)})
            for iSyst in itertools.chain(systList['dataEff']['fitTypes'],systList['dataEff']['shiftTypes']):
                hists_dataEff_syst.update({iSyst: hist.Clone(effName+'_'+iSyst)})
            for iSyst in itertools.chain(systList['mcEff']['fitTypes'],systList['mcEff']['shiftTypes']):
                hists_mcEff_syst.update({iSyst: hist.Clone(effName+'_'+iSyst)})

            #varName = get_variables_name(variableLabels)
            varName = variableLabels[0]
            for i in range(1,nVars):
                varName += '_'+variableLabels[i]
            output[effName] = {varName: {}}

            # iterate through the bin indices
            # this does nested for loops of the N-D binning (e.g. pt, eta)
            indices = [list([i+1 for i in range(len(binning[variableLabel])-1)]) for variableLabel in variableLabels]
            print(indices)
 
            for index in itertools.product(*indices):
                binName = get_full_name(num,denom,variableLabels,index)
                subVarKeys = ['{}:[{},{}]'.format(variableLabels[i], binning[variableLabels[i]][ind-1], binning[variableLabels[i]][ind]) for i,ind in enumerate(index)]
                _out = output[effName][varName]
                for subVarKey in subVarKeys:
                    if subVarKey not in _out:
                        _out[subVarKey] = {}
                    _out = _out[subVarKey]

                fitType = 'Nominal'
                #fitType = 'NominalOld'
                dataFNameFit = '{}/{}/{}/{}.root'.format(fitBase,fitType,effName,binName).replace('mc','data')
                dyFNameFit = '{}/{}/{}/{}.root'.format(fitBase,fitType,effName,binName)
                dataFName = '{}/converted/data.root'.format(fitBase)
                dyFName = '{}/converted/dy.root'.format(fitBase)
                sf, sf_stat, dataEff, dataStat, mcEff, mcStat = getSF(binName,dataFName,dyFName,dataFNameFit,dyFNameFit)
                sf_syst = getSyst(binName,dataFName,dyFName,dataFNameFit,dyFNameFit,dataEff,mcEff,systList['SF']['fitTypes'],systList['SF']['shiftTypes'])
                dataSyst = getSyst(binName,dataFName,dyFName,dataFNameFit,dyFNameFit,dataEff,mcEff,systList['dataEff']['fitTypes'],systList['dataEff']['shiftTypes'])
                mcSyst = getSyst(binName,dataFName,dyFName,dataFNameFit,dyFNameFit,dataEff,mcEff,systList['mcEff']['fitTypes'],systList['mcEff']['shiftTypes'])
                sf_err = (sf_stat**2 + sf_syst['combined']**2)**0.5
                dataErr = (dataStat**2 + dataSyst['combined']**2)**0.5
                mcErr = (mcStat**2 + mcSyst['combined']**2)**0.5
                #print(sf_err,dataErr,mcErr)
                print('sf       :', sf_stat, sf_syst['combined'])
                print('data eff :', dataStat, dataSyst['combined'])
                print('mc eff   :', mcStat, mcSyst['combined'])
                _out['value'] = sf
                _out['stat'] = sf_stat
                _out['syst'] = sf_syst

                # TODO: generalize
                ei, pi = index
                hist.SetBinContent(ei,pi,sf)
                hist.SetBinError(ei,pi,sf_err)
                hist_stat.SetBinContent(ei,pi,sf)
                hist_stat.SetBinError(ei,pi,sf_stat)
                hist_syst.SetBinContent(ei,pi,sf)
                hist_syst.SetBinError(ei,pi,sf_syst['combined'])
                for iKey in sf_syst.keys():
                    hists_syst[iKey].SetBinContent(ei,pi,sf_syst[iKey])

                hist_dataEff.SetBinContent(ei,pi,dataEff)
                hist_dataEff.SetBinError(ei,pi,dataErr)
                hist_dataEff_stat.SetBinContent(ei,pi,dataEff)
                hist_dataEff_stat.SetBinError(ei,pi,dataStat)
                hist_dataEff_syst.SetBinContent(ei,pi,dataEff)
                hist_dataEff_syst.SetBinError(ei,pi,dataSyst['combined'])
                for iKey in dataSyst.keys():
                    hists_dataEff_syst[iKey].SetBinContent(ei,pi,dataSyst[iKey])

                hist_mcEff.SetBinContent(ei,pi,mcEff)
                hist_mcEff.SetBinError(ei,pi,mcErr)
                hist_mcEff_stat.SetBinContent(ei,pi,mcEff)
                hist_mcEff_stat.SetBinError(ei,pi,mcStat)
                hist_mcEff_syst.SetBinContent(ei,pi,mcEff)
                hist_mcEff_syst.SetBinError(ei,pi,mcSyst['combined'])
                for iKey in mcSyst.keys():
                    hists_mcEff_syst[iKey].SetBinContent(ei,pi,mcSyst[iKey])

            hists[effName] = hist
            hists[effName+'_stat'] = hist_stat
            hists[effName+'_syst'] = hist_syst
            hists[effName+'_efficiencyData'] = hist_dataEff
            hists[effName+'_efficiencyData_stat'] = hist_dataEff_stat
            hists[effName+'_efficiencyData_syst'] = hist_dataEff_syst
            hists[effName+'_efficiencyMC'] = hist_mcEff
            hists[effName+'_efficiencyMC_stat'] = hist_mcEff_stat
            hists[effName+'_efficiencyMC_syst'] = hist_mcEff_syst
            for iSyst in itertools.chain(systList['SF']['fitTypes'],systList['SF']['shiftTypes'],['combined']):
                hists[effName+'_'+iSyst] = hists_syst[iSyst]
            for iSyst in itertools.chain(systList['dataEff']['fitTypes'],systList['dataEff']['shiftTypes'],['combined']):
                hists[effName+'_efficiencyData_'+iSyst] = hists_dataEff_syst[iSyst]
            for iSyst in itertools.chain(systList['mcEff']['fitTypes'],systList['mcEff']['shiftTypes'],['combined']):
                hists[effName+'_efficiencyMC_'+iSyst] = hists_mcEff_syst[iSyst]


            # gets a graph projection of an ND histogram for a given axis with axis index (ie x,y,z = 0,1,2) and other dimensions ind
            def get_graph(hist, axis, axis_ind, *ind):
                ni = axis.GetNbins()
                xvals = [axis.GetBinCenter(i+1) for i in range(ni)]
                xvals_errLow = [xvals[i]-axis.GetBinLowEdge(i+1) for i in range(ni)]
                xvals_errHigh = [axis.GetBinUpEdge(i+1)-xvals[i] for i in range(ni)]
                yvals = [hist.GetBinContent(*[ii+1 for ii in ind[:axis_ind]]+[i+1]+[ii+1 for ii in ind[axis_ind:]]) for i in range(ni)]
                yvals_err = [hist.GetBinError(*[ii+1 for ii in ind[:axis_ind]]+[i+1]+[ii+1 for ii in ind[axis_ind:]]) for i in range(ni)]
                graph = ROOT.TGraphAsymmErrors(ni,array('d',xvals),array('d',yvals),array('d',xvals_errLow),array('d',xvals_errHigh),array('d',yvals_err),array('d',yvals_err))
                return graph

            # plot the efficiencies
            # enumerate over the axis/variable to plot
            axes = [hists[effName].GetXaxis(), hists[effName].GetYaxis(), hists[effName].GetZaxis()]
            for vi, variableLabel in enumerate(variableLabels):

                # iterate over the other axis indices
                otherVariableLabels = [ovl for ovl in variableLabels if ovl!=variableLabel]
                #otherVariableNames = [labelVariableMap[num].get(ovl,ovl) for ovl in otherVariableLabels]
                otherVariableIndices = [ovi for ovi, ovl in enumerate(variableLabels) if ovl!=variableLabel]
                indices = [list(range(len(binning[vl])-1)) for vl in otherVariableLabels]
                if indices:
                    for index in itertools.product(*indices):
                        graph_data = get_graph(hists[effName+'_efficiencyData'], axes[vi], vi, *index)
                        graph_data.SetLineColor(ROOT.kBlack)
                        graph_data.SetMarkerColor(ROOT.kBlack)
                        graph_mc = get_graph(hists[effName+'_efficiencyMC'], axes[vi], vi, *index)
                        graph_mc.SetLineColor(ROOT.kBlue)
                        graph_mc.SetMarkerColor(ROOT.kBlue)
                        mg = ROOT.TMultiGraph()
                        mg.Add(graph_data)
                        mg.Add(graph_mc)

                        canvas = ROOT.TCanvas('c'+effName+'_'.join([str(i) for i in index]),'c',800,800)
                        mg.Draw('AP0')
                        mg.GetXaxis().SetTitle(variableLabel)#variableLabelMap[variableLabel])
                        mg.GetXaxis().SetRangeUser(axes[vi].GetBinLowEdge(1), axes[vi].GetBinUpEdge(axes[vi].GetNbins()))
                        mg.GetYaxis().SetTitle('Efficiency')
                        mg.GetYaxis().SetRangeUser(0.8,1.10)
                        legend = ROOT.TLegend(0.5,0.70,0.92,0.92)
                        legend.SetTextFont(42)
                        legend.SetBorderSize(0)
                        legend.SetFillColor(0)
                        legend.AddEntry(graph_data,'Data','l')
                        legend.AddEntry(graph_mc,'Simulation','l')
                        legend.SetHeader('{} / {}'.format(num,denom))
                        legend.Draw()

                        nother = len(indices)
                        text = ROOT.TPaveText(0.18,0.84-nother*0.04-0.02,0.35,0.84,'NB NDC')
                        text.SetTextFont(42)
                        text.SetBorderSize(0)
                        text.SetFillColor(0)
                        text.SetTextAlign(11)
                        text.SetTextSize(0.03)
                        for novi, (ovi, ovl) in enumerate(zip(otherVariableIndices,otherVariableLabels)):
                            xlow = axes[ovi].GetBinLowEdge(index[novi]+1)
                            xhigh = axes[ovi].GetBinUpEdge(index[novi]+1)
                            rtext = '{} < {} < {}'.format(xlow,variableLabel,xhigh)#variableLabelMap[ovl],xhigh)
                            text.AddText(rtext)
                        text.Draw()
                        CMS_lumi.cmsText = 'CMS'
                        CMS_lumi.writeExtraText = True
                        CMS_lumi.extraText = 'Preliminary'
                        CMS_lumi.lumi_13TeV = "%0.1f fb^{-1}" % (41.5)
                        CMS_lumi.CMS_lumi(canvas,4,11)
                        plotDir = os.path.join(fitBase,'plots','efficiency',effName)
                        try:
                            os.makedirs(plotDir)
                            shutil.copy('/eos/user/c/cmsmupog/www/Validation/index.php','{}/index.php'.format(plotDir))
                        except OSError:
                            pass
                        #otherVariableName = get_bin_name(otherVariableNames,index)
                        otherVariableLabel = get_bin_name(otherVariableLabels,index)
                        canvas.Print('{}/{}_{}_vs_{}.png'.format(plotDir,effName,otherVariableLabel,variableLabel))
                        canvas.Print('{}/{}_{}_vs_{}.pdf'.format(plotDir,effName,otherVariableLabel,variableLabel))

                # if no indices, easier, just itself
                else:
                    graph_data = get_graph(hists[effName+'_efficiencyData'], axes[vi], vi)
                    graph_data.SetLineColor(ROOT.kBlack)
                    graph_data.SetMarkerColor(ROOT.kBlack)
                    graph_mc = get_graph(hists[effName+'_efficiencyMC'], axes[vi], vi)
                    graph_mc.SetLineColor(ROOT.kBlue)
                    graph_mc.SetMarkerColor(ROOT.kBlue)
                    mg = ROOT.TMultiGraph()
                    mg.Add(graph_data)
                    mg.Add(graph_mc)

                    canvas = ROOT.TCanvas('c'+effName,'c',800,800)
                    mg.Draw('AP0')
                    mg.GetXaxis().SetTitle(variableLabel)#variableLabelMap[variableLabel])
                    mg.GetYaxis().SetTitle('Efficiency')
                    mg.GetYaxis().SetRangeUser(0.8,1.10)
                    legend = ROOT.TLegend(0.5,0.70,0.92,0.92)
                    legend.SetTextFont(42)
                    legend.SetBorderSize(0)
                    legend.SetFillColor(0)
                    legend.AddEntry(graph_data,'Data','l')
                    legend.AddEntry(graph_mc,'Simulation','l')
                    legend.SetHeader('{} / {}'.format(num,denom))
                    legend.Draw()

                    CMS_lumi.cmsText = 'CMS'
                    CMS_lumi.writeExtraText = True
                    CMS_lumi.extraText = 'Preliminary'
                    CMS_lumi.lumi_13TeV = "%0.1f fb^{-1}" % (41.5)
                    CMS_lumi.CMS_lumi(canvas,4,11)
                    plotDir = os.path.join(fitBase,'plots','efficiency',effName)
                    try:
                        os.makedirs(plotDir)
                        shutil.copy('/eos/user/c/cmsmupog/www/Validation/index.php','{}/index.php'.format(plotDir))
                    except OSError:
                        pass
                    canvas.Print('{}/{}_vs_{}.png'.format(plotDir,effName,variableLabel))
                    canvas.Print('{}/{}_vs_{}.pdf'.format(plotDir,effName,variableLabel))



    outname = 'scalefactors'
    with open('{}.json'.format(outname),'w') as f:
        f.write(json.dumps(output, indent=4, sort_keys=True))
    tfile = ROOT.TFile.Open('{}.root'.format(outname),'recreate')
    #ROOT.gROOT.SetStyle("InitStyle")
    #ROOT.gStyle.Reset()
    InitStyle.cd()
    ROOT.gStyle.SetOptStat(0)
    c = ROOT.TCanvas('c','c',800,600)
    for h in sorted(hists):
        hists[h].Write(h)
        hists[h].SetTitle(h)
        hists[h].GetXaxis().SetLabelSize(0.03)
        hists[h].GetYaxis().SetLabelSize(0.03)
        hists[h].GetZaxis().SetLabelSize(0.03)
        hists[h].GetXaxis().SetTitleSize(0.04)
        hists[h].GetYaxis().SetTitleSize(0.04)
        hists[h].GetYaxis().SetTitleOffset(1.0)
        #print(hists[h].GetTitle())
        hists[h].Draw("colz,e,text")
        c.SetLogy()
        if h == effName:
            hists[h].SetMaximum(1.08)
            hists[h].SetMinimum(0.98)
        c.Modified()
        c.Update()
        c.Print(plotDir+'/'+h+'.pdf')
        #c.Clear()
    tfile.Close()

if __name__ == "__main__":
    status = main()
    sys.exit(status)
