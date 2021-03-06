from datetime import datetime

from geoalchemy2 import *
from sqlalchemy import Column, BigInteger, String, DateTime, \
    Integer, Boolean, create_engine, ForeignKey
from sqlalchemy.sql import expression
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from common import get_conf

debug = False

conf = get_conf()

if debug:
    con_string = f"postgresql://{conf['pg_user']}:{conf['pg_pw']}@" \
                 f"{conf['pg_host']}/packetmaptest"

else:
    con_string = f"postgresql://{conf['pg_user']}:{conf['pg_pw']}@" \
                 f"{conf['pg_host']}/{conf['pg_db']}"

local_engine = create_engine(con_string)
Base = declarative_base()

__all__ = ["local_engine", "BadGeocode", "CrawledNode", "Digipeater",
           "LocallyHeardStation", "Node", "Operator", "RemoteDigipeater",
           "RemotelyHeardStation", "RemoteOperator"]


class BadGeocode(Base):
    """
    Stations that couldn't be geocoded
    """
    __tablename__ = 'bad_geocodes'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    last_checked = Column(DateTime, default=datetime.now())
    reason = Column(String, nullable=False)
    node_name = Column(String, nullable=False)
    parent_node = Column(String, nullable=True)


class CrawledNode(Base):
    """
    Nodes that have been crawled
    """
    __tablename__ = 'crawled_nodes'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    node_id = Column(String, nullable=False)
    port = Column(Integer, nullable=False)
    last_crawled = Column(DateTime, default=datetime.now())
    port_name = Column(String, nullable=False)
    needs_check = Column(Boolean, nullable=False)
    uid = Column(String, nullable=False)
    active_port = Column(Boolean, nullable=False)

    received_tx = relationship("RemotelyHeardStation",
                               back_populates="crawled_node",
                               primaryjoin="and_(RemotelyHeardStation"
                                           ".parent_call==CrawledNode"
                                           ".node_id, "
                                           "RemotelyHeardStation.port"
                                           "==CrawledNode.port_name)")
    remote_operator = relationship("RemoteOperator",
                                   back_populates="crawled_node",
                                   primaryjoin="and_(RemoteOperator"
                                               ".parent_call==CrawledNode"
                                               ".node_id, "
                                               "RemoteOperator.port"
                                               "==CrawledNode.port_name)")
    remote_digipeater = relationship("RemoteDigipeater",
                                     back_populates="crawled_node",
                                     primaryjoin="and_(RemoteDigipeater"
                                                 ".parent_call==CrawledNode"
                                                 ".node_id, "
                                                 "RemoteDigipeater.last_port"
                                                 "==CrawledNode.port_name)")


class Digipeater(Base):
    """
    Local digipeater data
    """
    __tablename__ = 'digipeaters'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    call = Column(String, nullable=False)
    lastheard = Column(DateTime, default=datetime.now())
    grid = Column(String, nullable=False)
    geom = Column(Geometry(geometry_type='POINT', srid=4326), nullable=False)
    heard = Column(Boolean, nullable=False)
    ssid = Column(Integer, nullable=True)
    lastcheck = Column(DateTime, default=datetime.now())


class LocallyHeardStation(Base):
    """
    Local MHeard List
    """
    __tablename__ = 'mh_list'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.now())
    call = Column(String, nullable=False)
    digipeaters = Column(String, nullable=True)
    op_call = Column(String, nullable=False)
    ssid = Column(Integer, nullable=True)


class Node(Base):
    """
    Nodes I am connected to
    """
    __tablename__ = 'nodes'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    call = Column(String, nullable=False)
    parent_call = Column(String, nullable=False)
    last_check = Column(DateTime, default=datetime.now())
    geom = Column(Geometry(geometry_type='POINT', srid=4326), nullable=False)
    ssid = Column(Integer, nullable=True)
    path = Column(String, nullable=False)
    level = Column(Integer, nullable=False)
    grid = Column(String, nullable=False)
    node_name = Column(String, nullable=True)
    bpq = Column(Boolean, nullable=True, default=expression.null())


class Operator(Base):
    """
    Locally heard operators
    """
    __tablename__ = 'operators'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    call = Column(String, nullable=False)
    lastheard = Column(DateTime, default=datetime.now())
    geom = Column(Geometry(geometry_type='POINT', srid=4326), nullable=False)
    grid = Column(String, nullable=False)
    lastcheck = Column(DateTime, default=datetime.now())


class RemoteDigipeater(Base):
    """
    Remotely-heard digipeater
    """
    __tablename__ = 'remote_digipeaters'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    parent_call = Column(String, ForeignKey("crawled_nodes.node_id"),
                         nullable=False)
    crawled_node = relationship(CrawledNode,
                                back_populates="remote_digipeater")
    call = Column(String, nullable=False)
    lastheard = Column(DateTime, default=datetime.now())
    grid = Column(String, nullable=False)
    heard = Column(Boolean, nullable=False)
    ssid = Column(Integer, nullable=True)
    geom = Column(Geometry(geometry_type='POINT', srid=4326), nullable=False)
    last_port = Column(String, nullable=False)
    uid = Column(String, nullable=False)
    ports = Column(String, nullable=False)
    lastcheck = Column(DateTime, default=datetime.now())


class RemotelyHeardStation(Base):
    """
    Remotely-heard station
    """
    __tablename__ = 'remote_mh'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    parent_call = Column(String, ForeignKey("crawled_nodes.node_id"),
                         nullable=False)
    crawled_node = relationship(CrawledNode, back_populates="received_tx")
    remote_call = Column(String, nullable=False)
    heard_time = Column(DateTime, default=datetime.now())
    ssid = Column(Integer, nullable=True)
    update_time = Column(DateTime, default=datetime.now())
    port = Column(String, nullable=False)
    band = Column(String, nullable=True)
    uid = Column(String, nullable=False)
    digis = Column(String, nullable=True)


class RemoteOperator(Base):
    """
    Store remotely-heard operator data
    """
    __tablename__ = 'remote_operators'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    parent_call = Column(String, ForeignKey("crawled_nodes.node_id"),
                         nullable=False)
    crawled_node = relationship(CrawledNode,
                                back_populates="remote_operator")
    remote_call = Column(String, nullable=False)
    lastheard = Column(DateTime, default=datetime.now())
    grid = Column(String, nullable=False)
    geom = Column(Geometry(geometry_type='POINT', srid=4326))
    port = Column(String, nullable=False)
    bands = Column(String, nullable=True)
    uid = Column(String, nullable=False)
    lastcheck = Column(DateTime, default=datetime.now())

BadGeocode.__table__.create(local_engine, checkfirst=True)
CrawledNode.__table__.create(local_engine, checkfirst=True)
Digipeater.__table__.create(local_engine, checkfirst=True)
LocallyHeardStation.__table__.create(local_engine, checkfirst=True)
Node.__table__.create(local_engine, checkfirst=True)
Operator.__table__.create(local_engine, checkfirst=True)
RemoteDigipeater.__table__.create(local_engine, checkfirst=True)
RemotelyHeardStation.__table__.create(local_engine, checkfirst=True)
RemoteOperator.__table__.create(local_engine, checkfirst=True)

Base.metadata.create_all(local_engine, checkfirst=True)
