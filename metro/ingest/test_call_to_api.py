import marbles.core
from call_to_api import call_to_api
import argparse
import sys

class TestMarbles(marbles.core.TestCase):

    def test_records_not_empty(self):
        print(self.args)
        records = call_to_api.get_information(self,2020,1,"Chabacano")
        self.assertNotEqual(len(records), 0, note=" the names should be uppercase because bla bla bla")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--year')
    parser.add_argument('--month')
    parser.add_argument('--station')

    args = parser.parse_args()
    # TODO: Go do something with args.input and args.filename

    # Now set the sys.argv to the unittest_args (leaving sys.argv[0] alone)
    sys.argv[1:] = args.unittest_args
    marbles.main()