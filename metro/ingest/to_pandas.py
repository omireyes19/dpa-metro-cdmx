import pandas as pd

class call_to_api:
    def __init__(self, year, month, station):
        self.year = year
        self.month = month
        self.station = station
    
    def get_information(self):
        df = pd.DataFrame(json_content)[["fecha","linea","afluencia"]]
        df.columns = ["date","line","influx"]
