import numpy as np

class interquartile_range:
    line = "line"
    station = "station"
    year = "year"
    month = "month"
    influx = "influx"
    q3 = "percentile_0.75"
    q1 = "percentile_0.25"
    iqr = "iqr"
    min_range = "min_range"
    max_range = "max_range"
    prom = "mean"
    date = "date"

    def percentile(n):
        def percentile_(x):
            return np.percentile(x, n)
        percentile_.__name__ = 'percentile_%s' % n
        return percentile_

    def get_statistics(self, df):
        return df.groupby([self.line, self.station]).agg([self.percentile(.25), self.percentile(.75), 'mean'])[self.influx].reset_index()

    def calculate_range(self, df):
        stats = self.get_statistics(df)
        stats[self.iqr] = stats[self.q3] - stats[self.q1]
        stats[self.min_range] = stats[self.prom] - 1.5 * stats[self.iqr]
        stats[self.max_range] = stats[self.prom] + 1.5 * stats[self.iqr]
        stats = stats[[self.line, self.station, self.min_range, self.max_range]]
        return stats

    def join_range(self, df, stats):
        return df.merge(stats, on = [self.line, self.station], how = 'left')

    def create_label(self, df):
        df["label"] = np.where(df[self.influx] <= df[self.min_range], 1, np.where(df[self.influx] <= df[self.max_range], 2, 3))
        return df
