import marbles.core

class TestMarbles(marbles.core.TestCase):

    def test_upper_w_marbles(self):
        self.assertEqual('foo'.upper(), 'FOO', note=" the names should be uppercase because bla bla bla")

    def test_isupper_w_marbles(self):
        self.assertTrue('FOO'.isupper())
        self.assertFalse('Foo'.isupper())

    def test_split_w_marbles(self):
        s = 'hello world'
        self.assertEqual(s.split(), ['hello', 'world'])
        # check that s.split fails when the separator is not a string
        with self.assertRaises(TypeError):
            s.split(2)