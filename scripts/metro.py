import luigi
import pandas as pd
import os
import requests
import json
import glob

class Extract(luigi.Task):
  year = luigi.Parameter()
  station = luigi.Parameter()

  def requires(self):
        return None

  def output(self):
    return luigi.LocalTarget("~/data/metro.json")

  def run(self):
    api_url = "https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=afluencia-diaria-del-metro-cdmx&rows=10000&sort=-fecha&facet=ano&facet=linea&facet=estacion&refine.ano=" + self.year + "&refine.estacion=" + self.station
    r = requests.get(url = api_url)
    data = r.json()
    try:
      os.remove("~/data/metro.json")
    except:
      None
    with open(self.station+'_'+self.year+ '.json', 'w') as outfile:
       json.dump(data, outfile)

if __name__ == '__main__':
    luigi.run()
