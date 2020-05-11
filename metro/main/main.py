import unittest
from ParametrizedTestCase import ParametrizedTestCase
from TestOne import TestOne

suite = unittest.TestSuite()
suite.addTest(ParametrizedTestCase.parametrize(TestOne, param=42))
suite.addTest(ParametrizedTestCase.parametrize(TestOne, param=13))
unittest.TextTestRunner(verbosity=2).run(suite)