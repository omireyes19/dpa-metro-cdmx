import pandas as pd

class to_pandas:
    def get_dataframe(self,json_content):
        df = pd.DataFrame(json_content)[["fecha","linea","afluencia"]]
        df.columns = ["date","line","influx"]
