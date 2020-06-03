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



external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
df = pd.read_csv('D://python//preds_ejem.csv')
dfA = pd.read_csv('D://python//preds_ejem.csv')


app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

available_D = df['dia'].unique()
available_D =numpy.append(available_D,['Todos los días'])

available_L = df['Linea'].unique()
available_L =numpy.append(available_L,['Todas las lineas'])

available_E = df['Estacion'].unique()
available_E =numpy.append(available_E,['Todas las estaciones'])

colors = {
    'background': '#528969',
    'text': '#111111'
}

app.layout = html.Div([ 
    html.Div([html.H1("Afluencia del Metro de la ciudad de México"),
                       html.H4('''
        Monitoreo de valores observados contra valores predichos
    '''),

        html.Div([ html.H6('Selecciona un día'),
            dcc.Dropdown(
                id='FDia',
                options=[{'label': i, 'value': i} for i in available_D],
                value='Todos los días'
            )
        ],style={'width': '25%', 'display': 'inline-block'}),
      
        html.Div([html.H6('Selecciona una linea'),
            dcc.Dropdown(
                id='FLinea',
                options=[{'label': i, 'value': i} for i in available_L],
                value='Todas las lineas'
            )
        ],style={'width': '25%', 'float': 'center', 'display': 'inline-block'}),

html.Div([ html.H6('Selecciona una estación'),
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
    
    TablaL=pd.DataFrame(columns=['Clase','Label'] )
    TablaC=pd.DataFrame(columns=['Clase','Predicted'] )
    
    if(Dia_value=='Todos los días' and Linea_value=='Todas las lineas' and Estacion_value=='Todas las estaciones'):
        dff=df
    elif (Dia_value != 'Todos los días' and Linea_value=='Todas las lineas' and Estacion_value=='Todas las estaciones'):
        dff=df[df['dia'] == Dia_value]
    elif (Dia_value == 'Todos los días' and Linea_value!='Todas las lineas' and Estacion_value=='Todas las estaciones'):
        dff=df[df['Linea'] == Linea_value]
    elif (Dia_value == 'Todos los días' and Linea_value=='Todas las lineas' and Estacion_value!='Todas las estaciones'):
        dff=df[df['Estacion'] == Estacion_value]
    elif (Dia_value != 'Todos los días' and Linea_value=='Todas las lineas' and Estacion_value!='Todas las estaciones'):
        dff=df[df['dia'] == Dia_value]
        dff=dff[dff['Estacion'] == Estacion_value]
    elif (Dia_value != 'Todos los días' and Linea_value!='Todas las lineas' and Estacion_value=='Todas las estaciones'):
        dff=df[df['dia'] == Dia_value]
        dff=dff[dff['Linea'] == Linea_value]
    elif (Dia_value == 'Todos los días' and Linea_value !='Todas las lineas' and Estacion_value !='Todas las estaciones'):
        dff=df[df['Linea'] == Linea_value]
        dff=dff[dff['Estacion'] == Estacion_value]
    else:
        dff=df[df['Estacion'] == Estacion_value]
        dff=dff[dff['Linea'] == Linea_value]
        dff=dff[dff['dia'] == Dia_value]
 
    n=dff['Label'].count()    
    for i in dff.Label_c.unique():
        TablaL = TablaL.append({'Clase':i,'Label':dff[dff['Label_c'] == i]['Label_c'].count()/n} , ignore_index=True)

    for i in dff.Predicted_c.unique():
        TablaC = TablaC.append({'Clase':i,'Predicted':dff[dff['Predicted_c'] == i]['Predicted_c'].count()/n} , ignore_index=True)
    

    #df2=dff
    #for i in df2.index:
    #    if (df2.at[i, 'Label_c'] != 'Alto'):
    #        df2.at[i, 'Label_c'] = 'AltoCom'
       
    #    if (df2.at[i, 'Predicted_c'] != 'Alto'):
    #        df2.at[i, 'Predicted_c'] = 'AltoCom'    
  
    #MC=pd.crosstab(df2.Label_c,df2.Predicted_c)


    #try:
    #    FPR=MC.at['AltoCom','Alto']/(MC.at['AltoCom','Alto']+MC.at['AltoCom','AltoCom'])

    #except KeyError as error:
    #    FPR='No está definida la FPR para sólo una observación'
    
    return {
        'data': [
                {'x': TablaL.Clase, 'y': TablaL.Label, 'type': 'bar', 'name': 'Label'},
                {'x': TablaC.Clase, 'y': TablaC.Predicted, 'type': 'bar', 'name': 'Predicted'},
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
       
    if(Dia_value=='Todos los días' and Linea_value=='Todas las lineas' and Estacion_value=='Todas las estaciones'):
        df2=dfA
    elif (Dia_value != 'Todos los días' and Linea_value=='Todas las lineas' and Estacion_value=='Todas las estaciones'):
        df2=dfA[dfA['dia'] == Dia_value]
    elif (Dia_value == 'Todos los días' and Linea_value!='Todas las lineas' and Estacion_value=='Todas las estaciones'):
        df2=dfA[dfA['Linea'] == Linea_value]
    elif (Dia_value == 'Todos los días' and Linea_value=='Todas las lineas' and Estacion_value!='Todas las estaciones'):
        df2=dfA[dfA['Estacion'] == Estacion_value]
    elif (Dia_value != 'Todos los días' and Linea_value=='Todas las lineas' and Estacion_value!='Todas las estaciones'):
        df2=dfA[dfA['dia'] == Dia_value]
        df2=df2[df2['Estacion'] == Estacion_value]
    elif (Dia_value != 'Todos los días' and Linea_value!='Todas las lineas' and Estacion_value=='Todas las estaciones'):
        df2=dfA[dfA['dia'] == Dia_value]
        df2=df2[df2['Linea'] == Linea_value]
    elif (Dia_value == 'Todos los días' and Linea_value !='Todas las lineas' and Estacion_value !='Todas las estaciones'):
        df2=dfA[dfA['Linea'] == Linea_value]
        df2=df2[df2['Estacion'] == Estacion_value]
    else:
        df2=dfA[dfA['Estacion'] == Estacion_value]
        df2=df2[df2['Linea'] == Linea_value]
        df2=df2[df2['dia'] == Dia_value]
 
    for i in df2.index:
        if (df2.at[i, 'Label_c'] != 'Alto'):
           df2.at[i, 'Label_c'] = 'AltoCom'
       
        if (df2.at[i, 'Predicted_c'] != 'Alto'):
            df2.at[i, 'Predicted_c'] = 'AltoCom'    
  
    MC=pd.crosstab(df2.Label_c,df2.Predicted_c)


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
        FPR='No está definida la FPR para sólo una observación'
     
       
    return TP,FN,FP,TN,FPR

if __name__ == '__main__':
    app.run_server(debug=True)
