from models.db import local_engine
from flask import Flask
from flask_restful import Resource, Api
from flask_jsonpify import jsonify

app = Flask(__name__)
api = Api(app)


class RemoteMH(Resource):
    """
    Remote MHeard list API
    """
    def get(self):
        conn = local_engine.connect()
        query = conn.execute("SELECT parent_call, remote_call AS call, "
                             "heard_time, ssid, band FROM remote_mh")

        result = {'mheard': [i for i in query.cursor.fetchall()]}
        return jsonify(result)


api.add_resource(RemoteMH, '/mheard')


if __name__ == '__main__':
    app.run(port='5002')