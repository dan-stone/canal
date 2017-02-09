import unittest

import numpy as np


class NumpyTestCase(unittest.TestCase):
    def assertndArrayEqual(self, array1, array2):
        assert isinstance(array1, np.ndarray), "Not an ndarray: {}".format(array1)
        assert isinstance(array2, np.ndarray), "Not an ndarray: {}".format(array2)
        self.assertTrue(
            (array1==array2).all(),
            "Numpy arrays are not equal:\n{}\n{}".format(array1, array2)
        )