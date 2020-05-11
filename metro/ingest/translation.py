import pandas as pd

class translation:
    def get_dataframe(self,json_content):
        df = pd.DataFrame(json_content)[["fecha","linea","afluencia"]]
        df.columns = ["date","line","influx"]
