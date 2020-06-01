from flask import Flask, request
from flask_restplus import API, Resource
app = Flask(__name__)
api = API(app)

predictions = {{'date': '2018-01-06',
                'line': 'Linea 8',
                'station': 'Chabacano',
                'prediction': 2.0},
               {'date': '2018-01-06',
                'line': 'Linea 9',
                'station': 'Chabacano',
                'prediction': 1.0}
               {'date': '2018-01-06',
                'line': 'Linea 2',
                'station': 'Chabacano',
                'prediction': 0.0}}

@api.route("/date/<string:date>/line/<string:line>/")
class GetPredictions(Resource):
    def get(self, date, line):
        return predictions[date,line]





