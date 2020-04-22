#!/usr/bin/env python
from __future__ import print_function
import sys
import uproot
from uproot_methods.classes import TH1
import numpy as np
import argparse

argv = sys.argv[1:]
parser = argparse.ArgumentParser(description='Make pileup')
parser.add_argument('era', help='Era to produce')
args = parser.parse_args(argv)

if args.era == 'Run2016_UL':
    from SimGeneral\
        .MixingModule\
        .mix_2016_25ns_UltraLegacy_PoissonOOTPU_cfi import mix
elif args.era == 'Run2017_UL':
    from SimGeneral\
        .MixingModule\
        .mix_2017_25ns_UltraLegacy_PoissonOOTPU_cfi import mix
elif args.era == 'Run2018_UL':
    from SimGeneral\
        .MixingModule\
        .mix_2018_25ns_UltraLegacy_PoissonOOTPU_cfi import mix
elif args.era == 'Run2016':
    from SimGeneral\
        .MixingModule\
        .mix_2016_25ns_Moriond17MC_PoissonOOTPU_cfi import mix
elif args.era == 'Run2017':
    from SimGeneral\
        .MixingModule\
        .mix_2017_25ns_WinterMC_PUScenarioV1_PoissonOOTPU_cfi import mix
elif args.era == 'Run2018':
    from SimGeneral\
        .MixingModule\
        .mix_2018_25ns_JuneProjectionFull18_PoissonOOTPU_cfi import mix
else:
    print('Unrecognized era', args.era)
    sys.exit(0)

values = np.array([float(x) for x in mix.input.nbPileupEvents.probValue])
edges = np.arange(len(values)+1)
hist = TH1.from_numpy((values, edges))

with uproot.recreate('pileup/mc/{era}.root'.format(era=args.era)) as f:
    f['pileup'] = hist
