import requests
import luigi
import boto3
import s3fs
import json
import glob
import os

class data_acq_task(luigi.Task):

  def run(self):
    years = ['2010', '2011']
    stations = ["Chabacano"]

    for req_year in years:
        for station in stations:

            ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
            s3_resource = ses.resource('s3')

            obj = s3_resource.Bucket(self.bucket)
            print(ses)

            api_url = "https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=afluencia-diaria-del-metro-cdmx&rows=10000&sort=-fecha&facet=ano&facet=linea&facet=estacion&refine.ano=" + req_year + "&refine.estacion=" + station
            r = requests.get(url = api_url)
            data = r.json()
            with s3.open('metro-dpa-dacq/dpa-test'+station+'_'+req_year+'.json', 'w') as outfile:
            #with s3.open(f"{'metro-dpa-dacq'}/'dpa-test' + station+'_'+req_year.json", 'w') as outfile:
                json.dump(data, outfile)

if __name__ == '__main__':
    luigi.run()
