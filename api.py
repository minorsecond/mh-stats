from flask import Flask, render_template
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


@app.route("/api")
def hello():
    return "<h1 style='color:Black; text-align: center;'>" \
           "Packet Radio Map API</h1>" \
           "<BR>" \
           "<ul>" \
           "<li><a href='/api/mheard'>MHeard Data</a></li>" \
           "  <ul>" \
           "    <li>Parent Call - The station who received the transmission</li>" \
           "    <li>Call - The transmitting station</li>" \
           "    <li>Heard Time - The time that the transmission was received in UTC timezone</li>" \
           "    <li>SSID - The SSID of the transmitting station, if any</li>" \
           "    <li>Band - The band the transmission was heard on</li>" \
           "  </ul>" \
           "<li><a href='/api/remoteops'>Operator Data</a></li>" \
           "  <ul>" \
           "    <li>Parent Call - The last station that heard the operator</li>" \
           "    <li>Call - The station call</li>" \
           "    <li>Last Heard - The last time the station was received in UTC timezone</li>" \
           "    <li>Grid - The station's Maidenhead grid square</li>" \
           "    <li>Coordinates - The X & Y coordinates of the station</li>" \
           "    <li>Bands - Bands that the station has been heard on</li>" \
           "  </ul>" \
           "<li><a href='/api/digipeaters'>Digipeater Data</a></li>" \
           "  <ul>" \
           "    <li>Call - The digipeater callsign</li>" \
           "    <li>Last Heard - The last time the digipeater was received in UTC timezone</li>" \
           "    <li>Grid - The digipeater's Maidenhead grid square</li>" \
           "    <li>SSID - The digipeater's SSID</li>" \
           "    <li>Coordinates - The X & Y coordinates of the digiepater</li>" \
           "  </ul>" \
           "<li><a href='/api/nodes'>Node Data</a></li>" \
           "  <ul>" \
           "    <li>Call - The NET/ROM node callsign</li>" \
           "    <li>Grid - The NET/ROM node Maidenhead grid square</li>" \
           "    <li>Coordinates - The NET/ROM node X & Y coordinates</li>" \
           "  </ul>" \
           "</ul>"


class RemoteMH(Resource):
    """
    Remote MHeard list API
    """
    def get(self):
        conn = remote_engine.connect()
        query = conn.execute("select parent_call, remote_call, "
                             "coalesce(to_char(heard_time, "
                             "'YYYY-MM-DD HH24:MI:SS'), 'NULL'),"
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
        query = conn.execute("select parent_call, remote_call, "
                             "coalesce(to_char(lastheard, "
                             "'YYYY-MM-DD HH24:MI:SS'), 'NULL'), "
                             "grid, (st_x(geom), st_y(geom)), bands "
                             "from remote_operators "
                             "order by lastheard desc")
        result = {'operators': [i for i in query.cursor.fetchall()]}
        return jsonify(result)


class RemoteDigipeaters(Resource):
    """
    Remote digipeaters API
    """
    def get(self):
        conn = remote_engine.connect()
        query = conn.execute("select call, "
                             "coalesce(to_char(lastheard, "
                             "'YYYY-MM-DD HH24:MI:SS'), 'NULL'), grid, ssid, "
                             "(st_x(geom), "
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


api.add_resource(RemoteMH, '/api/mheard')
api.add_resource(RemoteOperators, '/api/remoteops')
api.add_resource(RemoteDigipeaters, '/api/digipeaters')
api.add_resource(Nodes, '/api/nodes')

if __name__ == '__main__':
    app.run()
