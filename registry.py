import os
import glob
import pandas as pd
import itertools


class Registry:

    def __init__(self):
        self._data = pd.DataFrame()

    def load_json(self, fname):
        df = pd.read_json(fname)
        self._data = self._data.append(df)

    def _reduce(self, particle=None, probe=None,
                resonance=None, era=None, subEra=None):
        df = self._data
        if particle is not None:
            df = df[df.particle == particle]
        if probe is not None:
            df = df[df.probe == probe]
        if resonance is not None:
            df = df[df.resonance == resonance]
        if era is not None:
            df = df[df.era == era]
        # special handling for data eras of the form
        # Run20XY selecting all Run20XYZ subEras
        if subEra is not None:
            df = df[df.subEra.str.startswith(subEra)]
        return df

    def parquet(self, particle, probe, resonance, era, subEra):
        df = self._reduce(particle, probe, resonance, era, subEra)
        return itertools.chain.from_iterable(df.parquet.values)

    def root(self, particle, probe, resonance, era, subEra):
        df = self._reduce(particle, probe, resonance, era, subEra)
        globs = itertools.chain.from_iterable(df.parquet.values)
        return itertools.chain.from_iterable((glob.glob(g) for g in globs))

    def treename(self, particle, probe, resonance, era, subEra):
        df = self._reduce(particle, probe, resonance, era, subEra)
        treename = df.treename.iloc[0]
        if not (df.treename.values == treename).all():
            raise ValueError('Multiple treenames for query')
        return treename

    def luminosity(self, particle, probe, resonance, era, subEra):
        df = self._reduce(particle, probe, resonance, era, subEra)
        return df.luminosity.sum()


registry = Registry()

_rpath = os.path.abspath(os.path.dirname(__file__))
_jsons = [
    'data/registry_leptonMva.json',
]

for fname in _jsons:
    registry.load_json(os.path.join(_rpath, fname))
