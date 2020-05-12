import pandas as pd
from workalendar.america import Mexico

class date_variables:
    def add_date_variables(self,df):
        cal = Mexico()

        df["date"] = pd.to_datetime(df['date'])

        df["day_of_week"] = df["date"].dt.dayofweek
        df['holiday'] = df.date.apply(lambda x: cal.is_working_day(x))
        df['line_crossing']= df.date.map(df.date.value_counts())
        return df