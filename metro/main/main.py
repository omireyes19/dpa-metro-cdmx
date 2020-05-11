import unittest
from ParametrizedTestCase import ParametrizedTestCase
from TestOne import TestOne

suite = unittest.TestSuite()
suite.addTest(ParametrizedTestCase.parametrize(TestOne, year=2020, month=1, station="Chabacano"))
suite.addTest(ParametrizedTestCase.parametrize(TestOne, year=2020, month=2, station="Chabacano"))
unittest.TextTestRunner(verbosity=2).run(suite)