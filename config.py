import os
import json
import numpy as np


class Configuration(dict):

    def __init__(self, data):
        if isinstance(data, dict):
            super().__init__(data)
        elif isinstance(data, str) and os.path.exists(data):
            with open(data) as f:
                super().__init__(json.load(f))
        else:
            raise TypeError('Wrong configuration data type', type(data), data)

    def __getattr__(self, attr):
        return lambda: self[attr]

    def shifts(self):
        return ['Nominal'] + list(self.get('shifts', {}).keys())

    def fitShifts(self):
        return ['Nominal'] + list(self.get('fitShifts', {}).keys())

    def shift(self, shift):
        '''
        Return a configuration modified by
        the requested shift
        '''
        if shift == 'Nominal':
            return self
        _self = self.copy()
        _self.update(self.get('shifts', {}).get(shift, {}))
        return Configuration(_self)

    def fitShift(self, fitShift):
        '''
        Return the parameters of the given fit.
        '''
        result = {
            'fitType': 'Nominal',
            'shiftType': 'Nominal',
            'inType': 'Nominal',
        }
        result.update(self.get('fitShifts', {}).get(fitShift, {}))
        return result

    def selection(self):
        return self.get('selection', 'True')

    def binning(self):
        binning = self.get('binning', {})
        for b in binning:
            if isinstance(binning[b], str):
                binning[b] = eval(binning[b])
            if isinstance(binning[b], list):
                binning[b] = np.array(binning[b])
        return binning
