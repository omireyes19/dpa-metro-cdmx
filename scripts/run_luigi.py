# -*- coding: utf-8 -*-
"""
Created on Fri Apr 17 19:55:37 2020

@author: card1
"""
import requests
import luigi
#import luigi.contrib.s3
#import boto3
#import s3fs
import json
import glob
import os
import pandas as pd
from datetime import date
import datetime


import luigi
 
class PrintNumbers(luigi.Task):
    #n=luigi.IntParameter() 
    station=luigi.Parameter()
    Tabla=pd.DataFrame(columns=['anio','mes','dia','linea','estacion','afluencia'] )
    f=open("D://python//output//Archivo.csv","w+")
    
    def requires(self):
        return []
 
    def output(self):
        #return luigi.LocalTarget()
        return luigi.LocalTarget("D://python//output//numbers_up_to_10.txt")
 
    def run(self):
        self.station=self.station.split(' ')
        print(self.station)
        
        for estacion in self.station:
            api_url ="https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=afluencia-diaria-del-metro-cdmx&rows=100&sort=-fecha&refine.estacion="+ estacion
            #api_url ="https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=afluencia-diaria-del-metro-cdmx&rows=10000&sort=-fecha&refine.estacion="+ estacion
            print(api_url)
            print(estacion)
            r = requests.get(url = api_url)
            data = r.json()
            for item in data['records']:
                estacion=item['fields']['estacion']
                fecha=item['fields']['fecha']
                linea=item['fields']['linea']
                afluencia=item['fields']['afluencia']
                fecha2=fecha.split('-')    
                self.Tabla = self.Tabla.append({'anio':fecha2[0],'mes':fecha2[1],'dia':fecha2[2],'linea':linea,'estacion':estacion,'afluencia':afluencia} , ignore_index=True)
        
        print(self.Tabla)
        print(fecha)
        #with self.f.open('w') as f:
        self.Tabla.to_csv(self.f)
        
        with self.output().open('w') as f:
        #    for i in range(1, self.n+1):
               f.write(fecha)
        #self.output=fecha
        self.station=''.join(self.station)
class SquaredNumbers(luigi.Task):
    #n=luigi.IntParameter() 
    station=luigi.Parameter()
    #date = luigi.DateParameter(default=date.today())
    date = luigi.Parameter()
    TablaA=pd.DataFrame(columns=['anio','mes','dia','linea','estacion','afluencia'] )
    def requires(self):
        return [PrintNumbers(station=self.station)]
 
    def output(self):
        return luigi.LocalTarget("D://python//output//squares.txt")
 
    def run(self):
        #print("Me pasaron este parámetro")
        #print(self.date)
        with self.input()[0].open() as fin, self.output().open('w') as fout:
        #with self.output().open('w') as fout:
        #    fout.write(self.input)
            for line in fin:
                n = line.strip()
                #out = n * n
                fout.write("{}\n".format(n)) 
         
        api_url ="https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=afluencia-diaria-del-metro-cdmx&sort=-fecha&facet=fecha&facet=linea&facet=estacion&refine.fecha="+ self.date +"&refine.estacion=Balbuena"
        print(api_url)
        r = requests.get(url = api_url)
        data = r.json()
        print("Coincidencias: ")
        nhits=data['nhits']
        print(nhits)
                
        #data_aux=data['records']
        #data_aux=data_aux[0]
        #data_aux=data_aux['fields']['fecha']
        #print(data_aux)
        if nhits==0:
            print("No hay nueva información para cargar")
        else:
            fA=open("D://python//output//ArchivoA.csv","w+")
            print("cargar nueva información")
            print(n)
            print(self.date)
        
        date1 = n
        date2 = self.date
        start = datetime.datetime.strptime(date1, '%Y-%m-%d')
        end = datetime.datetime.strptime(date2, '%Y-%m-%d')
        step = datetime.timedelta(days=1)
        start += step
        while start <= end:
            fecha=start.date()
            api_url ="https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=afluencia-diaria-del-metro-cdmx&rows=10000&sort=-fecha&facet=anio&facet=linea&facet=estacion&refine.fecha="+fecha.strftime('%Y-%m-%d')
            #api_url ="https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=afluencia-diaria-del-metro-cdmx&sort=-fecha&facet=fecha&facet=linea&facet=estacion&refine.fecha="+ fecha.strftime('%Y-%m-%d') +"&refine.estacion=Balbuena"
            r = requests.get(url = api_url)
            data = r.json()    
            for item in data['records']:
                estacion=item['fields']['estacion']
                fecha=item['fields']['fecha']
                linea=item['fields']['linea']
                afluencia=item['fields']['afluencia']
                fecha2=fecha.split('-')
                self.TablaA = self.TablaA.append({'anio':fecha2[0],'mes':fecha2[1],'dia':fecha2[2],'linea':linea,'estacion':estacion,'afluencia':afluencia} , ignore_index=True)
            start += step
        print(self.TablaA)
        self.TablaA.to_csv(fA)
        #print(data['records'])
if __name__ == '__main__':
    luigi.run()

