import unittest

class ParametrizedPredictionsTest(unittest.TestCase):
    """ TestCase classes that want to be parametrized should
        inherit from this class.
    """
    def __init__(self, methodName='runTest', X_train=None, y_train=None):
        super(ParametrizedPredictionsTest, self).__init__(methodName)
        self.X_train = X_train
        self.y_train = y_train

    @staticmethod
    def parametrize(testcase_klass,  X_train=None, y_train=None):
        """ Create a suite containing all tests taken from the given
            subclass, passing them the parameter 'param'.
        """
        testloader = unittest.TestLoader()
        testnames = testloader.getTestCaseNames(testcase_klass)
        suite = unittest.TestSuite()
        for name in testnames:
            suite.addTest(testcase_klass(name,  X_train=X_train, y_train=y_train))
        return suite
