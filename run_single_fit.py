#!/usr/bin/env python
from __future__ import print_function
import os
import sys
from TagAndProbeFitter import TagAndProbeFitter
import ROOT
ROOT.gROOT.SetBatch()
# kInfo = 1001, kWarning = 2001, ...
ROOT.gROOT.ProcessLine("gErrorIgnoreLevel = 2001;")
ROOT.gROOT.LoadMacro('RooCMSShape.cc+')


def hist_fitter(outFName, inFName, binName, templateFName, plotDir,
                version='Nominal', histType='data', shiftType='Nominal'):

    tnpNomFit = [
        "meanP[-0.0, -5.0, 5.0]", "sigmaP[0.9, 0.05, 5.0]",
        "meanF[-0.0, -5.0, 5.0]", "sigmaF[0.9, 0.05, 5.0]",
        "acmsP[60., 50., 190.]", "betaP[0.05, 0.01, 0.08]",
        "gammaP[0.1, -2, 2]", "peakP[91.0]",
        "acmsF[60., 50., 190.]", "betaF[0.05, 0.01, 0.08]",
        "gammaF[0.1, -2, 2]", "peakF[91.0]",
        "Gaussian::sigResPass(x, meanP, sigmaP)",
        "Gaussian::sigResFail(x, meanF, sigmaF)",
        "RooCMSShape::bkgPass(x, acmsP, betaP, gammaP, peakP)",
        "RooCMSShape::bkgFail(x, acmsF, betaF, gammaF, peakF)",
        ]

    tnpNomFitOld = [
        "meanP1[90.0, 80.0, 100.0]", "sigmaP1[0.9, 0.5, 3.0]",
        "widthP1[2.495]",
        "meanF1[90.0, 80.0, 100.0]", "sigmaF1[0.9, 0.5, 3.0]",
        "widthF1[2.495]",
        "meanP2[90.0.80.0.100.0]", "sigmaP2[4.0, 3.0, 10.0]",
        "widthP2[2.495]",
        "meanF2[90.0.80.0.100.0]", "sigmaF2[4.0, 3.0, 10.0]",
        "widthF2[2.495]",
        "acmsP[70., 50., 90.]", "betaP[0.05, 0.01, 0.08]",
        "gammaP[0.1, -2, 2]", "peakP[90.0]",
        "acmsF[70., 50., 90.]", "betaF[0.05, 0.01, 0.08]",
        "gammaF[0.1, -2, 2]", "peakF[90.0]",
        "Voigtian::sigPass1(x, meanP1, widthP1, sigmaP1)",
        "Voigtian::sigFail1(x, meanF1, widthF1, sigmaF1)",
        "Voigtian::sigPass2(x, meanP2, widthP2, sigmaP2)",
        "Voigtian::sigFail2(x, meanF2, widthF2, sigmaF2)",
        "SUM::sigPass(fP[0.7, 0.5, 1]*sigPass1, sigPass2)",
        "SUM::sigFail(fF[0.7, 0.5, 1]*sigFail1, sigFail2)",
        "RooCMSShape::bkgPass(x, acmsP, betaP, gammaP, peakP)",
        "RooCMSShape::bkgFail(x, acmsF, betaF, gammaF, peakF)",
        ]

    tnpAltSigFit = [
        "meanP[-0.0, -5.0, 5.0]", "sigmaP[1, 0.7, 6.0]",
        "alphaP[2.0, 1.2, 3.5]",
        'nP[3, -5, 5]', "sigmaP_2[1.5, 0.5, 6.0]", "sosP[1, 0.5, 5.0]",
        "meanF[-0.0, -5.0, 5.0]", "sigmaF[2, 0.7, 15.0]",
        "alphaF[2.0, 1.2, 3.5]",
        'nF[3, -5, 5]', "sigmaF_2[2.0, 0.5, 6.0]", "sosF[1, 0.5, 5.0]",
        "acmsP[60., 50., 75.]", "betaP[0.04, 0.01, 0.06]",
        "gammaP[0.1, 0.005, 1]", "peakP[90.0]",
        "acmsF[60., 50., 75.]", "betaF[0.04, 0.01, 0.06]",
        "gammaF[0.1, 0.005, 1]", "peakF[90.0]",
        # alternative for faster fitting, just use gen truth for template
        "Gaussian::sigResPass(x, meanP, sigmaP)",
        "Gaussian::sigResFail(x, meanF, sigmaF)",
        "RooCMSShape::bkgPass(x, acmsP, betaP, gammaP, peakP)",
        "RooCMSShape::bkgFail(x, acmsF, betaF, gammaF, peakF)",
        ]

    tnpAltSigFitOld = [
        "meanP[90.0, 80.0, 100.0]", "sigmaP[0.9, 0.5, 5.0]", "widthP[2.495]",
        "meanF[90.0, 80.0, 100.0]", "sigmaF[0.9, 0.5, 5.0]", "widthF[2.495]",
        "acmsP[60., 50., 80.]", "betaP[0.05, 0.01, 0.08]",
        "gammaP[0.1, -2, 2]", "peakP[90.0]",
        "acmsF[60., 50., 80.]", "betaF[0.05, 0.01, 0.08]",
        "gammaF[0.1, -2, 2]", "peakF[90.0]",
        "Voigtian::sigPass(x, meanP, widthP, sigmaP)",
        "Voigtian::sigFail(x, meanF, widthF, sigmaF)",
        "RooCMSShape::bkgPass(x, acmsP, betaP, gammaP, peakP)",
        "RooCMSShape::bkgFail(x, acmsF, betaF, gammaF, peakF)",
        ]

    tnpAltBkgFit = [
        "meanP[-0.0, -5.0, 5.0]", "sigmaP[0.9, 0.5, 5.0]",
        "meanF[-0.0, -5.0, 5.0]", "sigmaF[0.9, 0.5, 5.0]",
        "alphaP[0., -5., 5.]",
        "alphaF[0., -5., 5.]",
        "Gaussian::sigResPass(x, meanP, sigmaP)",
        "Gaussian::sigResFail(x, meanF, sigmaF)",
        "Exponential::bkgPass(x, alphaP)",
        "Exponential::bkgFail(x, alphaF)",
        ]

    tnpWorkspace = []
    doTemplate = True
    if version == 'Nominal':
        tnpWorkspace.extend(tnpNomFit)
    if version == 'NominalOld':
        tnpWorkspace.extend(tnpNomFitOld)
        doTemplate = False
    if version == 'AltSigOld':
        tnpWorkspace.extend(tnpAltSigFitOld)
        doTemplate = False
    elif version == 'AltSig':
        tnpWorkspace.extend(tnpAltSigFit)
    elif version == 'AltBkg':
        tnpWorkspace.extend(tnpAltBkgFit)

    def rebin(hP, hF):
        if shiftType == 'massBinUp':
            pass  # no rebin, bin widths are 0.25 GeV
        elif shiftType == 'massBinDown':
            hP = hP.Rebin(4)  # 1.0 GeV bins
            hF = hF.Rebin(4)  # 1.0 GeV bins
        else:
            hP = hP.Rebin(2)  # 0.5 GeV bins
            hF = hF.Rebin(2)  # 0.5 GeV bins
        return hP, hF

    # init fitter
    infile = ROOT.TFile(inFName, "read")
    hP = infile.Get(f'{binName}_Pass')
    hF = infile.Get(f'{binName}_Fail')
    hP, hF = rebin(hP, hF)
    fitter = TagAndProbeFitter(binName)
    fitter.set_histograms(hP, hF)
    infile.Close()

    # mass range systematic
    if shiftType == 'massRangeUp':
        fitter.set_fit_range(75, 135)
    elif shiftType == 'massRangeDown':
        fitter.set_fit_range(65, 125)
    else:
        fitter.set_fit_range(70, 130)

    # setup
    os.makedirs(os.path.dirname(outFName), exist_ok=True)

    # generated Z LineShape
    # for high pT change the failing spectra to any probe to get statistics
    fileTruth = ROOT.TFile(templateFName, 'read')
    if version == 'AltSig':
        # TODO: truth file for ZmmGenLevel instead of reco in case of AltSig
        histZLineShapeP = fileTruth.Get(f'{binName}_Pass_Gen')
        histZLineShapeF = fileTruth.Get(f'{binName}_Fail_Gen')
    else:
        histZLineShapeP = fileTruth.Get(f'{binName}_Pass')
        histZLineShapeF = fileTruth.Get(f'{binName}_Fail')
    histZLineShapeP, histZLineShapeF = rebin(histZLineShapeP, histZLineShapeF)
    fitter.set_gen_shapes(histZLineShapeP, histZLineShapeF)

    fileTruth.Close()

    # set workspace
    fitter.set_workspace(tnpWorkspace, doTemplate)
    fitter.fit(outFName, histType == 'mc', doTemplate)


# TODO other fits and argparse
if __name__ == "__main__":
    argv = sys.argv[1:]
    hist_fitter(*argv)
    sys.exit(0)
