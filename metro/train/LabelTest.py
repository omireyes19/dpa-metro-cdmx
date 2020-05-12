from train.interquartile_range import interquartile_range
from train.ParametrizedLabelTest import ParametrizedLabelTest


class LabelTest(ParametrizedLabelTest):
    def test_label_creation(self):
        label_df = interquartile_range.create_label(self, interquartile_range.join_range(self, self.cleaned_data, interquartile_range.calculate_range(self, self.cleaned_data)))
        distinct_labels = label_df['label'].distinct()
        self.assertIn(distinct_labels, range(1,4))
