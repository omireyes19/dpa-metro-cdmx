import marbles.core
from call_to_api import call_to_api
import sys

class TestMarbles(marbles.core.TestCase):

    def test_records_not_empty(self):
        print(self.year)
        records = call_to_api.get_information(self.year,self.month,self.station)
        self.assertNotEqual(len(records), 0, note=" the names should be uppercase because bla bla bla")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        TestMarbles.year = sys.argv.pop()
        TestMarbles.month = sys.argv.pop()
        TestMarbles.station = sys.argv.pop()
    marbles.core.main()