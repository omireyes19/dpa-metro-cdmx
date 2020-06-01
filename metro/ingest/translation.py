import pandas as pd

class translation:
    def get_dataframe(self,json_content):
        df = pd.DataFrame(json_content)[["fecha", "ano", "dia", "mes", "afluencia", "estacion", "linea"]]
        df.columns = ["date", "year", "day", "month", "influx", "station", "line"]
        return df
