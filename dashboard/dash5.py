# -*- coding: utf-8 -*-
"""
Created on Wed May 20 12:49:03 2020

@author: card1

"""
import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import numpy
import os
import requests

#os.environ['NO_PROXY'] = '127.0.0.1'
r = requests.get('http://localhost:5000/date/202002')
r.encoding  = 'utf8'
print(r.encoding)
data = r.json()

df = pd.read_json(data, orient='table')
dfA = pd.read_json(data, orient='table')

#df = df.astype(str)
#dfA = dfA.astype(str)
df['date'] = df['date'].astype(str)
dfA['date'] = dfA['date'].astype(str)

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


available_D = df['date'].unique()
available_D =numpy.append(available_D,['Todos los dias'])

available_L = df['line'].unique()
available_L =numpy.append(available_L,['Todas las lineas'])

available_E = df['station'].unique()
available_E =numpy.append(available_E,['Todas las estaciones'])

colors = {
    'background': '#528969',
    'text': '#111111'
}

app.layout = html.Div([ 
    html.Div([html.H1("Afluencia del Metro de la ciudad de Mexico"),
                       html.H4('''
        Monitoreo de valores observados contra valores predichos
    '''),

        html.Div([ html.H6('Selecciona un dia'),
            dcc.Dropdown(
                id='FDia',
                options=[{'label': i, 'value': i} for i in available_D],
                value='Todos los dias'
            )
        ],style={'width': '25%', 'display': 'inline-block'}),
      
        html.Div([html.H6('Selecciona una linea'),
            dcc.Dropdown(
                id='FLinea',
                options=[{'label': i, 'value': i} for i in available_L],
                value='Todas las lineas'
            )
        ],style={'width': '25%', 'float': 'center', 'display': 'inline-block'}),

html.Div([ html.H6('Selecciona una estacion'),
            dcc.Dropdown(
                id='FEstacion',
                options=[{'label': i, 'value': i} for i in available_E],
                value='Todas las estaciones'
            )
        ],style={'width': '25%', 'display': 'inline-block'}), 
dcc.Graph(id='indicator-graphic')]),
                               html.Table([
        html.Tr([html.Td(['Verdaderos Positivos']), html.Td(id='TP')]),
        html.Tr([html.Td(['Falsos positivos']), html.Td(id='FP')]),
        html.Tr([html.Td(['Verdaderos negativos']), html.Td(id='TN')]),
        html.Tr([html.Td(['Falsos negativos']), html.Td(id='FN')]),
        html.Tr([html.Td(['Tasa de falsos positivos']), html.Td(id='FPR')])
        
    ])
                               ])


@app.callback(
    Output('indicator-graphic', 'figure'),
   # Output('TP', 'children'),
   # Output('FP', 'children'),
   # Output('TN', 'children'),
    #Output('FN', 'children')],  
    [Input('FDia', 'value'),
     Input('FLinea', 'value'),
     Input('FEstacion', 'value')])
def update_graph(Dia_value, Linea_value,Estacion_value):
    
    TablaL=pd.DataFrame(columns=['Clase','label'] )
    TablaC=pd.DataFrame(columns=['Clase','label_predicted'] )
    
    if(Dia_value=='Todos los dias' and Linea_value=='Todas las lineas' and Estacion_value=='Todas las estaciones'):
        dff=df
    elif (Dia_value != 'Todos los dias' and Linea_value=='Todas las lineas' and Estacion_value=='Todas las estaciones'):
        dff=df[df['date'] == Dia_value]
    elif (Dia_value == 'Todos los dias' and Linea_value!='Todas las lineas' and Estacion_value=='Todas las estaciones'):
        dff=df[df['line'] == Linea_value]
    elif (Dia_value == 'Todos los dias' and Linea_value=='Todas las lineas' and Estacion_value!='Todas las estaciones'):
        dff=df[df['station'] == Estacion_value]
    elif (Dia_value != 'Todos los dias' and Linea_value=='Todas las lineas' and Estacion_value!='Todas las estaciones'):
        dff=df[df['date'] == Dia_value]
        dff=dff[dff['station'] == Estacion_value]
    elif (Dia_value != 'Todos los dias' and Linea_value!='Todas las lineas' and Estacion_value=='Todas las estaciones'):
        dff=df[df['date'] == Dia_value]
        dff=dff[dff['line'] == Linea_value]
    elif (Dia_value == 'Todos los dias' and Linea_value !='Todas las lineas' and Estacion_value !='Todas las estaciones'):
        dff=df[df['line'] == Linea_value]
        dff=dff[dff['station'] == Estacion_value]
    else:
        dff=df[df['station'] == Estacion_value]
        dff=dff[dff['line'] == Linea_value]
        dff=dff[dff['date'] == Dia_value]
 
    n=dff['label'].count()    
    for i in dff.label_c.unique():
        TablaL = TablaL.append({'Clase':i,'label':dff[dff['label_c'] == i]['label_c'].count()/n} , ignore_index=True)

    for i in dff.label_predicted_c.unique():
        TablaC = TablaC.append({'Clase':i,'label_predicted':dff[dff['label_predicted_c'] == i]['label_predicted_c'].count()/n} , ignore_index=True)
    

    #df2=dff
    #for i in df2.index:
    #    if (df2.at[i, 'label_c'] != 'Alto'):
    #        df2.at[i, 'label_c'] = 'AltoCom'
       
    #    if (df2.at[i, 'label_predicted_c'] != 'Alto'):
    #        df2.at[i, 'label_predicted_c'] = 'AltoCom'    
  
    #MC=pd.crosstab(df2.label_c,df2.label_predicted_c)


    #try:
    #    FPR=MC.at['AltoCom','Alto']/(MC.at['AltoCom','Alto']+MC.at['AltoCom','AltoCom'])

    #except KeyError as error:
    #    FPR='No esta definida la FPR para solo una observacion'
    
    return {
        'data': [
                {'x': TablaL.Clase, 'y': TablaL.label, 'type': 'bar', 'name': 'label'},
                {'x': TablaC.Clase, 'y': TablaC.label_predicted, 'type': 'bar', 'name': 'label_predicted'},
            ],
        'layout':{
                'plot_bgcolor': colors['background'],
                'paper_bgcolor': colors['background'],
                'font': {
                    'color': colors['text']
                }
            } 
    }
#,{MC.at['Alto','Alto']},{MC.at['Alto','AltoCom']},
#    {MC.at['AltoCom','Alto']},{MC.at['AltoCom','AltoCom']}



@app.callback(
    [Output('TP', 'children'),
     Output('FP', 'children'),
     Output('TN', 'children'),
     Output('FN', 'children'),
     Output('FPR', 'children')],
    [Input('FDia', 'value'),
     Input('FLinea', 'value'),
     Input('FEstacion', 'value')])
def update_graph(Dia_value, Linea_value,Estacion_value):
    #index=['Alto','AltoCom']
    #columns=['Alto','AltoCom']
    #MC = pd.DataFrame(index=index, columns=columns)
    #MC = MC.fillna(0)    
       
    if(Dia_value=='Todos los dias' and Linea_value=='Todas las lineas' and Estacion_value=='Todas las estaciones'):
        df2=dfA
    elif (Dia_value != 'Todos los dias' and Linea_value=='Todas las lineas' and Estacion_value=='Todas las estaciones'):
        df2=dfA[dfA['date'] == Dia_value]
    elif (Dia_value == 'Todos los dias' and Linea_value!='Todas las lineas' and Estacion_value=='Todas las estaciones'):
        df2=dfA[dfA['line'] == Linea_value]
    elif (Dia_value == 'Todos los dias' and Linea_value=='Todas las lineas' and Estacion_value!='Todas las estaciones'):
        df2=dfA[dfA['station'] == Estacion_value]
    elif (Dia_value != 'Todos los dias' and Linea_value=='Todas las lineas' and Estacion_value!='Todas las estaciones'):
        df2=dfA[dfA['date'] == Dia_value]
        df2=df2[df2['station'] == Estacion_value]
    elif (Dia_value != 'Todos los dias' and Linea_value!='Todas las lineas' and Estacion_value=='Todas las estaciones'):
        df2=dfA[dfA['date'] == Dia_value]
        df2=df2[df2['line'] == Linea_value]
    elif (Dia_value == 'Todos los dias' and Linea_value !='Todas las lineas' and Estacion_value !='Todas las estaciones'):
        df2=dfA[dfA['line'] == Linea_value]
        df2=df2[df2['station'] == Estacion_value]
    else:
        df2=dfA[dfA['station'] == Estacion_value]
        df2=df2[df2['line'] == Linea_value]
        df2=df2[df2['date'] == Dia_value]
 
    for i in df2.index:
        if (df2.at[i, 'label_c'] != 'Alto'):
           df2.at[i, 'label_c'] = 'AltoCom'
       
        if (df2.at[i, 'label_predicted_c'] != 'Alto'):
            df2.at[i, 'label_predicted_c'] = 'AltoCom'    
  
    MC=pd.crosstab(df2.label_c,df2.label_predicted_c)


    try:
        MC.at['Alto','Alto']
    except KeyError:
        TP="Indefinido"
    else:
        TP=MC.at['Alto','Alto'] 
 
    try:
        MC.at['Alto','AltoCom']
    except KeyError:
        FN="Indefinido"
    else:
        FN=MC.at['Alto','AltoCom']
       
    try:
        MC.at['AltoCom','Alto']
    except KeyError:
        FP="Indefinido"
    else:
        FP=MC.at['AltoCom','Alto']   

    try:
        MC.at['AltoCom','AltoCom']
    except KeyError:
        TN="Indefinido"
    else:
        TN=MC.at['AltoCom','AltoCom']
        
    try:
        FPR=round(MC.at['AltoCom','Alto']/(MC.at['AltoCom','Alto']+MC.at['Alto','Alto']),2)
    except KeyError:
        FPR='No esta definida la FPR para solo una observacion'
     
       
    return TP,FN,FP,TN,FPR

if __name__ == '__main__':
    app.run_server(debug=True)
