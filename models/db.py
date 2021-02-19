from datetime import datetime

from geoalchemy2 import *
from sqlalchemy import MetaData, engine, Column, BigInteger, String, DateTime, \
    Integer, Boolean
from sqlalchemy.ext.declarative import declarative_base

metadata = MetaData(engine)
Base = declarative_base(metadata=metadata)


class BadGeocodes(Base):
    """
    Stations that couldn't be geocoded
    """
    id = Column(BigInteger, primary_key=True)
    last_checked = Column(DateTime, default=datetime.now())
    reason = Column(String, nullable=False)
    node_name = Column(String, nullable=False)


class CrawledNodes(Base):
    """
    Nodes that have been crawled
    """
    id = Column(BigInteger, primary_key=True)
    node_id = Column(String, nullable=False)
    port = Column(Integer, nullable=False)
    last_crawled = Column(DateTime, default=datetime.now())


class Digipeaters(Base):
    """
    Local digipeater data
    """
    id = Column(BigInteger, primary_key=True)
    call = Column(String, nullable=False)
    lastheard = Column(DateTime, default=datetime.now())
    grid = Column(String, nullable=False)
    geom = Column(Geometry(geometry_type='POINT', srid=4326), nullable=False)
    heard = Column(Boolean, nullable=False)
    ssid = Column(Integer, nullable=True)


class MHList(Base):
    """
    Local MHeard List
    """
    id = Column(BigInteger, primary_key=True)
    timestamp = Column(DateTime, default=datetime.now())
    call = Column(String, nullable=False)
    digipeaters = Column(String, nullable=True)
    op_call = Column(String, nullable=False)


class Nodes(Base):
    """
    Nodes I am connected to
    """
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
    id = Column(BigInteger, primary_key=True)
    call = Column(String, nullable=False)
    lastheard = Column(DateTime, default=datetime.now())
    geom = Column(Geometry(geometry_type='POINT', srid=4326), nullable=False)
    grid = Column(String, nullable=False)


class RemoteDigipeater(Base):
    """
    Remotely-heard digipeater
    """
    id = Column(BigInteger, primary_key=True)
    parent_call = Column(String, nullable=False)
    call = Column(String, nullable=False)
    lastheard = Column(DateTime, default=datetime.now())
    grid = Column(String, nullable=False)
    heard = Column(Boolean, nullable=False)
    ssid = Column(Integer, nullable=True)
    geom = Column(Geometry(geometry_type='POINT', srid=4326), nullable=False)
    port = Column(Integer, nullable=False)


class RemoteMH(Base):
    """
    Remotely-heard station
    """
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
