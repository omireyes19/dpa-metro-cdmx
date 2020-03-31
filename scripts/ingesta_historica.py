import requests
import luigi
import luigi.contrib.s3
import boto3
import s3fs
import json
import glob
import os

class data_acq_task(luigi.Task):
    bucket = 'dpa-metro'
    years = ['2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019']
	stations = ['Pantitlán']	
	for year in years:
  		for station in stations:
		    def run(self):
		        ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
		        s3_resource = ses.resource('s3')

		        obj = s3_resource.Bucket(self.bucket)
		        print(ses)

		        api_url = "https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=afluencia-diaria-del-metro-cdmx&rows=10000&sort=-fecha&facet=ano&facet=linea&facet=estacion&refine.ano=" + year + "&refine.estacion=" + station
		    	r = requests.get(url = api_url)
		        data = r.json()
		        with self.output().open('w') as output_file:
		        #with s3.open(f"{'metro-dpa-dacq'}/'dpa-test' + station+'_'+req_year.json", 'w') as outfile:
		            json.dump(data, output_file)

		    def output(self):
		        output_path = "s3://{}/YEAR={}/STATION={}/{}.json".\
		        format(self.bucket,year,station,year+station)
		        return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == '__main__':
    luigi.run()
