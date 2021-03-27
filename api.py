from flask import Flask
from flask_restful import Resource, Api
from flask_jsonpify import jsonify
from sqlalchemy import create_engine
from common import get_conf

conf = get_conf()
app = Flask(__name__)
api = Api(app)

con_string = f"postgresql://{conf['pg_user']}:{conf['pg_pw']}@" \
             f"{conf['remote_pg_host']}/packetmap"

remote_engine = create_engine(con_string)


class RemoteMH(Resource):
    """
    Remote MHeard list API
    """
    def get(self):
        conn = remote_engine.connect()
        query = conn.execute("SELECT parent_call, remote_call AS call, "
                             "heard_time, ssid, band FROM remote_mh")

        result = {'mheard': [i for i in query.cursor.fetchall()]}
        return jsonify(result)


api.add_resource(RemoteMH, '/mheard')


if __name__ == '__main__':
    app.run(port='5002')