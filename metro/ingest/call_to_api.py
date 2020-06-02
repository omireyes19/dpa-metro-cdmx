from calendar import monthrange
import requests

class call_to_api:
    def get_information(self, year, month):
        days_in_month = monthrange(year, month)[1]

        records = []
        for day in range(days_in_month):
            fecha = str(year)+"-"+str(month).zfill(2)+"-"+str(day+1).zfill(2)
            api_url = "https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=afluencia-diaria-del-metro-cdmx" \
                      "&q=&rows=-1&sort=-fecha&facet=fecha&refine.fecha="+fecha

            r = requests.get(url = api_url)
            data = r.json()
            for obs in data["records"]:
                records.append(obs["fields"])

        return records
