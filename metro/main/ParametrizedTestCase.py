import unittest

class ParametrizedTestCase(unittest.TestCase):
    """ TestCase classes that want to be parametrized should
        inherit from this class.
    """
    def __init__(self, methodName='runTest', year=None, month=None, station=None, raw_json=None, csv_data=None,
    cleaned_data=None, model_data=None):
        super(ParametrizedTestCase, self).__init__(methodName)
        self.year = year
        self.month = month
        self.station = station
        self.raw_json = raw_json
        self.csv_data = csv_data
        self.cleaned_data = cleaned_data
        self.model_data = model_data

    @staticmethod
    def parametrize(testcase_klass, year=None, month=None, station=None, raw_json=None, csv_data=None,
    cleaned_data=None, model_data=None):
        """ Create a suite containing all tests taken from the given
            subclass, passing them the parameter 'param'.
        """
        testloader = unittest.TestLoader()
        testnames = testloader.getTestCaseNames(testcase_klass)
        suite = unittest.TestSuite()
        for name in testnames:
            suite.addTest(testcase_klass(name, year = year, month = month, station = station, raw_json = raw_json,
             csv_data = csv_data, cleaned_data = cleaned_data, model_data = model_data))
        return suite