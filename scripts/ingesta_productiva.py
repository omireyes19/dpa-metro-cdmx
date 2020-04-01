import requests
import luigi
import luigi.contrib.s3
import boto3
import s3fs
import json
import glob
import os
from datetime import date

class data_acq_task(luigi.Task):
    bucket = 'dpa-metro'
    bucket_metadata = 'dpa-metro-metadata'
    today = date.today().strftime("%d%m%Y")
    year = luigi.Parameter()
    station = luigi.Parameter()

    def run(self):
        ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
        s3_resource = ses.resource('s3')

        obj = s3_resource.Bucket(self.bucket)
        print(ses)

        api_url = "https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=afluencia-diaria-del-metro-cdmx&rows=10000&sort=-fecha&facet=ano&facet=linea&facet=estacion&refine.ano=" + self.year + "&refine.estacion=" + self.station
        r = requests.get(url = api_url)
        data = r.json()

    #    with self.output().open('w') as output_file:
    #        json.dump(data, output_file)

        with self.output_metadata().open('w') as output_file:
            output_file.write("test,luigi,s3")

    #def output(self):
    #    output_path = "s3://{}/YEAR={}/STATION={}/{}.json".\
    #    format(self.bucket,self.year,self.station,self.year+self.station)
    #    return luigi.contrib.s3.S3Target(path=output_path)

    def output_metadata(self):
        output_path = "s3://{}/DATE={}/{}.csv".\
        format(self.bucket_metadata,str(self.today),str(self.today))
        return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == '__main__':
    luigi.run()
