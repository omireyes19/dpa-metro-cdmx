from calendar import monthrange
import requests

class call_to_api:
    def __init__(self, year, month, station):
        self.year = year
        self.month = month
        self.station = station
    
    def get_information(self):
        days_in_month = monthrange(self.year, self.month)[1]

        records = []
        for day in range(days_in_month):
            fecha = str(self.year)+"-"+str(self.month).zfill(2)+"-"+str(day+1).zfill(2)
            api_url = "https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=afluencia-diaria-del-metro-cdmx&" \
                      "sort=-fecha&facet=fecha&facet=linea&facet=estacion&refine.fecha="+fecha+"&refine.estacion="+self.station

            r = requests.get(url = api_url)
            data = r.json()

            for obs in data["records"]:
                records.append(obs["fields"])
