from ingest.translation import translation
from ingest.ParametrizedTranslationTest import ParametrizedTranslationTest


class TranslationTest(ParametrizedTranslationTest):
    def test_full_layout(self):
        df = translation.get_dataframe(self, self.raw_json)
        number_of_columns = len(df.columns)
        self.assertEqual(number_of_columns, 3)
