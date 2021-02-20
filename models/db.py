from datetime import datetime

from geoalchemy2 import *
from sqlalchemy import Column, BigInteger, String, DateTime, \
    Integer, Boolean, create_engine
from sqlalchemy.ext.declarative import declarative_base

engine = create_engine(
    'postgresql://rwardrup:Rward0232@192.168.3.65/packetmap')
Base = declarative_base()

__all__ = ["BadGeocode", "CrawledNode", "Digipeater", "LocallyHeardStation",
           "Node", "Operator", "RemoteDigipeater", "RemotelyHeardStation",
           "RemoteOperator"]


class BadGeocode(Base):
    """
    Stations that couldn't be geocoded
    """
    __tablename__ = 'bad_geocodes'
    id = Column(BigInteger, primary_key=True)
    last_checked = Column(DateTime, default=datetime.now())
    reason = Column(String, nullable=False)
    node_name = Column(String, nullable=False)


class CrawledNode(Base):
    """
    Nodes that have been crawled
    """
    __tablename__ = 'crawled_nodes'
    id = Column(BigInteger, primary_key=True)
    node_id = Column(String, nullable=False)
    port = Column(Integer, nullable=False)
    last_crawled = Column(DateTime, default=datetime.now())
    port_name = Column(String, nullable=False)
    needs_check = Column(Boolean, nullable=False)


class Digipeater(Base):
    """
    Local digipeater data
    """
    __tablename__ = 'digipeaters'
    id = Column(BigInteger, primary_key=True)
    call = Column(String, nullable=False)
    lastheard = Column(DateTime, default=datetime.now())
    grid = Column(String, nullable=False)
    geom = Column(Geometry(geometry_type='POINT', srid=4326), nullable=False)
    heard = Column(Boolean, nullable=False)
    ssid = Column(Integer, nullable=True)


class LocallyHeardStation(Base):
    """
    Local MHeard List
    """
    __tablename__ = 'mh_list'
    id = Column(BigInteger, primary_key=True)
    timestamp = Column(DateTime, default=datetime.now())
    call = Column(String, nullable=False)
    digipeaters = Column(String, nullable=True)
    op_call = Column(String, nullable=False)


class Node(Base):
    """
    Nodes I am connected to
    """
    __tablename__ = 'nodes'
    id = Column(BigInteger, primary_key=True)
    call = Column(String, nullable=False)
    parent_call = Column(String, nullable=False)
    last_check = Column(DateTime, default=datetime.now())
    geom = Column(Geometry(geometry_type='POINT', srid=4326), nullable=False)
    ssid = Column(Integer, nullable=True)
    path = Column(String, nullable=False)
    level = Column(Integer, nullable=False)
    grid = Column(String, nullable=False)


class Operator(Base):
    """
    Locally heard operators
    """
    __tablename__ = 'operators'
    id = Column(BigInteger, primary_key=True)
    call = Column(String, nullable=False)
    lastheard = Column(DateTime, default=datetime.now())
    geom = Column(Geometry(geometry_type='POINT', srid=4326), nullable=False)
    grid = Column(String, nullable=False)


class RemoteDigipeater(Base):
    """
    Remotely-heard digipeater
    """
    __tablename__ = 'remote_digipeaters'
    id = Column(BigInteger, primary_key=True)
    parent_call = Column(String, nullable=False)
    call = Column(String, nullable=False)
    lastheard = Column(DateTime, default=datetime.now())
    grid = Column(String, nullable=False)
    heard = Column(Boolean, nullable=False)
    ssid = Column(Integer, nullable=True)
    geom = Column(Geometry(geometry_type='POINT', srid=4326), nullable=False)
    port = Column(Integer, nullable=False)


class RemotelyHeardStation(Base):
    """
    Remotely-heard station
    """
    __tablename__ = 'remote_mh'
    id = Column(BigInteger, primary_key=True)
    parent_call = Column(String, nullable=False)
    remote_call = Column(String, nullable=False)
    heard_time = Column(DateTime, default=datetime.now())
    ssid = Column(Integer, nullable=True)
    update_time = Column(DateTime, default=datetime.now())
    port = Column(Integer, nullable=False)
    band = Column(String, nullable=True)


class RemoteOperator(Base):
    """
    Store remotely-heard operator data
    """
    __tablename__ = 'remote_operators'
    id = Column(BigInteger, primary_key=True)
    parent_call = Column(String, nullable=False)
    remote_call = Column(String, nullable=False)
    lastheard = Column(DateTime, default=datetime.now())
    grid = Column(String, nullable=False)
    geom = Column(Geometry(geometry_type='POINT', srid=4326))
    port = Column(Integer, nullable=False)
    bands = Column(String, nullable=True)


BadGeocode.__table__.create(engine, checkfirst=True)
CrawledNode.__table__.create(engine, checkfirst=True)
Digipeater.__table__.create(engine, checkfirst=True)
LocallyHeardStation.__table__.create(engine, checkfirst=True)
Node.__table__.create(engine, checkfirst=True)
Operator.__table__.create(engine, checkfirst=True)
RemoteDigipeater.__table__.create(engine, checkfirst=True)
RemotelyHeardStation.__table__.create(engine, checkfirst=True)
RemoteOperator.__table__.create(engine, checkfirst=True)

Base.metadata.create_all(engine, checkfirst=True)
