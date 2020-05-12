import unittest

class ParametrizedCallToAPITest(unittest.TestCase):
    """ TestCase classes that want to be parametrized should
        inherit from this class.
    """
    def __init__(self, methodName='runTest', year=None, month=None, station=None):
        super(ParametrizedTestCase, self).__init__(methodName)
        self.year = year
        self.month = month
        self.station = station

    @staticmethod
    def parametrize(testcase_klass, year=None, month=None, station=None):
        """ Create a suite containing all tests taken from the given
            subclass, passing them the parameter 'param'.
        """
        testloader = unittest.TestLoader()
        testnames = testloader.getTestCaseNames(testcase_klass)
        suite = unittest.TestSuite()
        for name in testnames:
            suite.addTest(testcase_klass(name, year = year, month = month, station = station)
        return suite
