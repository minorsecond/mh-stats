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


@app.route("/")
def hello():
    return "<h1 style='color:blue'>Hello There!</h1>"


class RemoteMH(Resource):
    """
    Remote MHeard list API
    """
    def get(self):
        conn = remote_engine.connect()
        query = conn.execute("select parent_call, remote_call, heard_time,"
                             " ssid, band "
                             "from remote_mh "
                             "order by heard_time desc")

        result = {'mheard': [i for i in query.cursor.fetchall()]}
        return jsonify(result)


class RemoteOperators(Resource):
    """
    Remote operators API
    """
    def get(self):
        conn = remote_engine.connect()
        query = conn.execute("select parent_call, remote_call, lastheard, "
                             "grid, (st_x(geom), st_y(geom)), bands "
                             "from remote_operators "
                             "order by lastheard desc")
        result = {'remote_ops': [i for i in query.cursor.fetchall()]}
        return jsonify(result)


class RemoteDigipeaters(Resource):
    """
    Remote digipeaters API
    """
    def get(self):
        conn = remote_engine.connect()
        query = conn.execute("select call, lastheard, grid, ssid, (st_x(geom), "
                             "st_y(geom)) from remote_digipeaters "
                             "order by lastheard desc")
        result = {'digipeaters': [i for i in query.cursor.fetchall()]}
        return jsonify(result)


class Nodes(Resource):
    """
    NET/ROM nodes API
    """
    def get(self):
        conn = remote_engine.connect()
        query = conn.execute("select call, grid, (st_x(geom), st_y(geom)) "
                             "from nodes "
                             "order by call asc")
        result = {'nodes': [i for i in query.cursor.fetchall()]}
        return jsonify(result)


api.add_resource(RemoteMH, '/mheard')
api.add_resource(RemoteOperators, '/remoteops')
api.add_resource(RemoteDigipeaters, '/digipeaters')
api.add_resource(Nodes, '/nodes')

if __name__ == '__main__':
    app.run(port='5002')
