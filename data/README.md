# Data registry
All tag-and-probe data needs to be registered here.
If you add a new json file, don't forget to add it to [registry.py](../registry.py).
The structure is a list of objects with the following required entries:

```javascript
{
    // The first 4 arguments will be used at the command line
    // to run a given set of efficiencies
    // particle type
    "particle": "muon",
    // probe type
    "probe": "generalTracks",
    // resonance to fit
    "resonance": "Z",
    // global era name, unique for a given data reprocessing-mc production pair
    "era": "Run2016",
    // subera, e.g. data taking era or MC dataset
    "subEra": "Run2016B",
    // a list of parquet datasets
    "parquet": ["/path/to/tnp.parquet"],
    // a list of root files (unix glob syntax supported)
    "root": ["/path/to/tnp*.root"],
    // the tree inside the rootfile
    "treename": "tpTree/fitter_tree",
    // version number, in case a dataset is remade
    "version": 1
}
```
