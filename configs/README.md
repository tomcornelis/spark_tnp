# Configuration
Common configuration files, and examples, are stored here.
Configuration files have the structure (note: comments are not valid JSON):

```javascript
{
    // The efficiencies to be measured
    // represented as [numerator, denominator] pairs.
    // Each name must have a corresponding entry in the "definitions" object
    "efficiencies": [
        ["MyNum", "MyDen"]
    ],
    // Selection to apply on the input dataset.
    // For example, in a cut-and-count, you may want a narrow mass range
    // whereas for a fit, no cut on mass is necessary (it will be binned)
    "selection": "tag_pt>26 and tag_abseta<2.4",
    // Definitions of selections used.
    // They will produce a new column with the given name with value
    // given by the expression.
    // These will be loaded first (and in order), so they can be used in other
    // parts of the definitions or selection.
    // NOTE: numerator/denominator keys must evaluate to bools.
    "definitions": {
        "qOverP": "charge/p",
        "MyNum": "CutBasedIdTight == 1",
        "MyDen": "TM == 1"
    },
    // Binning of the efficiencies and fit variables.
    // Every variable used must be defined.
    // Strings will be evaluated using python's "eval()" method.
    // Numpy is loaded.
    "binning": {
        "pt": [15, 20, 25, 30, 40, 50, 60, 120],
        "abseta": [0, 0.9, 1.2, 2.1, 2.4],
        "eta": [-2.4, -2.1, -1.6, -1.2, -0.9, -0.3, -0.2, 0.2, 0.3, 0.9, 1.2, 1.6, 2.1, 2.4],
        "mass": "np.array(range(60*4, 140*4+1)) * 0.25",
        "mcMass": "np.array(range(60*4, 140*4+1)) * 0.25"
    },
    // The variableName-column map, with optional "pretty" representation
    // in the form of ROOT TText syntax
    "variables": {
        "pt": {"variable": "pt", "pretty": "p_{T} (GeV)"},
        "abseta": {"variable": "abseta", "pretty": "|#eta|"},
        "eta": {"variable": "eta", "pretty": "#eta"},
        "mass": {"variable": "mass", "pretty": "m(#mu#mu) (GeV)"},
        "mcMass": {"variable": "mcMass", "pretty": "m(#mu#mu) (GeV)"}
    },
    // the variable to fit, used when fitting, not necessary when
    // performing a cut-and-count efficiency
    "fitVariable": "mass",
    // similarly, the generator-level variable used in alternative background
    // model fits
    "fitVariableGen": "mcMass",
    // The bins to calculate efficiency in.
    // One set of efficiencies will be produced for each entry.
    // Up to 3 dimensions supported.
    // The N+1 dimension of the binning will be the fitVariable
    // (and should not be included)
    "binVariables": [
        ["abseta", "pt"],
        ["eta"]
    ],
    // an optional set of shifts
    // The above arguments refer to the "Nominal" shift
    // From the Nominal arguments above, each shift
    // will update the configuration with the given parameters
    "shifts": {
        // e.g. changing the tag definition
        "TagPtUp": {
            "selection": "tag_pt>30 and tag_abseta<2.4"
        }
    },
    // An optional set of alternative fits to perform
    // The nominal fit function is a template fit using the Nominal configuration
    // This can be overridden by redefining "Nominal"
    // Alternatively, new fits are implemented as:
    "fitShifts": {
        // Changing the fit type (PDF used)
        "AltSig": {"fitType": "AltSig"},
        // Changing the shift type (alternative fit inputs/configurations)
        "massBinUp": {"shiftType": "massBinUp"},
        "massBinDown": {"shiftType": "massBinDown"},
        // Changing the input flat histograms
        "tagIsoUp": {"inType": "TagIsoUp"}
        "tagIsoDown": {"inType": "TagIsoDown"}
        // or combinations of the above
    },
    // An optional set of systematics to include in the uncertainies
    // "Up" and "Down" (from above) should not be included
    "systematics" : {
        "SF": {
            "fitTypes": ["AltSig"],
            "shiftTypes": ["tagIso", "massBin"]
        },
        "dataEff": {
            "fitTypes": ["AltSig"],
            "shiftTypes": ["tagIso", "massBin"]
        },
        "mcEff": {
            "fitTypes": [],
            "shiftTypes": ["tagIso"]
        }
    }
}
```

Some examples can be found in:
* Simple example: [muon_example.json](muon_example.json)
* Muon POG Z to mumu ID/Iso for Run-2: [muon_pog_official_run2_Z.json](muon_pog_official_run2_Z.json)
* Muon POG Z to mumu ID/Iso for 2017 (with higher trigger threshold): [muon_pog_official_run2_Z_2017.json](muon_pog_official_run2_Z_2017.json)
