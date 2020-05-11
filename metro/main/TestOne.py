class TestOne(ParametrizedTestCase):
    def test_something(self):
        print 'param =', self.param
        self.assertEqual(1, 1)

    def test_something_else(self):
        self.assertEqual(2, 2)