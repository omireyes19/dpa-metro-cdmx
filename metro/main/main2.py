import marbles.core
from ParametrizedTestCase import ParametrizedTestCase
from TestOne import TestOne

suite = marbles.TestSuite()
suite.addTest(ParametrizedTestCase.parametrize(TestOne, param=42))
suite.addTest(ParametrizedTestCase.parametrize(TestOne, param=13))
marbles.TextTestRunner(verbosity=2).run(suite)