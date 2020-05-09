from calendar import monthrange
import requests
import json

class call_to_api:
    def get_information(self, year, month, station):
        days_in_month = monthrange(year, month)[1]

        records = []
        for day in range(days_in_month):
            fecha = str(year)+"-"+str(month).zfill(2)+"-"+str(day+1).zfill(2)
            api_url = "https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=afluencia-diaria-del-metro-cdmx&" \
                      "sort=-fecha&facet=fecha&facet=linea&facet=estacion&refine.fecha="+fecha+"&refine.estacion="+station

            r = requests.get(url = api_url)
            data = r.json()

            for obs in data["records"]:
                records.append(obs["fields"])
