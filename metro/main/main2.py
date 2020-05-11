import marbles.core
from ParametrizedTestCase import ParametrizedTestCase
from TestOne import TestOne

suite = marbles.core.TestSuite()
suite.addTest(ParametrizedTestCase.parametrize(TestOne, param=42))
suite.addTest(ParametrizedTestCase.parametrize(TestOne, param=13))
marbles.core.TextTestRunner(verbosity=2).run(suite)