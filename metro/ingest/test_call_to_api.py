import marbles.core
from call_to_api import call_to_api
import sys

class TestMarbles(marbles.core.TestCase):

    def test_records_not_empty(self):

        records = call_to_api.get_information(year,month,station)
        self.assertNotEqual(len(records), 0, note=" the names should be uppercase because bla bla bla")

if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.exit("ERROR command-line parameter must be supplied for these tests")
    year = sys.argv[1]
    month = sys.argv[2]
    station = sys.argv[33]
    del sys.argv[1:]
    marbles.core.main()