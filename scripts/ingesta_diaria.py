import requests
import luigi
import luigi.contrib.s3
import boto3
import s3fs
import json
import glob
import os
from datetime import date
from calendar import monthrange

class data_acq_task(luigi.Task):
    bucket = 'dpa-metro'
    year = luigi.Parameter()
    month = luigi.Parameter()
    station = luigi.Parameter()

    def run(self):
        ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
        s3_resource = ses.resource('s3')

        obj = s3_resource.Bucket(self.bucket)
        print(ses)

        days_in_month = monthrange(self.year, self.month)[1]

        for day in range(days_in_month):
        	fecha = str(self.year)+"-"+str(self.month).zfill(2)+"-"+str(day+1).zfill(2)

	        api_url = "https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=afluencia-diaria-del-metro-cdmx&sort=-fecha&facet=fecha&facet=linea&facet=estacion&refine.fecha="+fecha+"&refine.estacion="+self.station

	        r = requests.get(url = api_url)
	        data = r.json()

	        with self.output(self,fecha).open('w') as output_file:
	            json.dump(data, output_file)

    def output(self,fecha):
        output_path = "s3://{}/information_date={}/station={}/{}.json".format(self.bucket,fecha,self.station,fecha+self.station)
        return luigi.contrib.s3.S3Target(path=output_path)

class data_acq_metadata(luigi.Task):
    bucket_metadata = 'dpa-metro-metadata'
    today = date.today().strftime("%d%m%Y")
    year = luigi.Parameter()
    station = luigi.Parameter()

    def run(self):
        ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
        s3_resource = ses.resource('s3')

        obj = s3_resource.Bucket(self.bucket_metadata)
        print(ses)

        with self.output_metadata().open('w') as output_file:
            output_file.write(str(self.today)+","+self.year+","+self.station)

    def output_metadata(self):
        output_path = "s3://{}/DATE={}/{}.csv".\
        format(self.bucket_metadata,str(self.today),str(self.today))
        return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == '__main__':
    luigi.run()
