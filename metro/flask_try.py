from flask import Flask, request
from flask_restplus import API, Resource
import boto3
app = Flask(__name__)
api = API(app)

@api.route("/date/<string:date>")
class GetPredictions(Resource):
    def get(self, year, month):
        ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
        s3_resource = ses.resource('s3')

        obj = s3_resource.Object("dpa-metro-predictions", "year={}/month={}/{}.csv".format(str(year), str(month).zfill(2), str(year)+str(month).zfill(2)))

        file_content = obj.get()['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(file_content))
        return df
