#!/bin/python3
# Synchronize local PRM database with remote

from sqlalchemy.orm import sessionmaker

from models.db import local_engine, remote_engine, CrawledNode, \
    RemoteOperator, RemoteDigipeater, RemotelyHeardStation, BadGeocode, \
    LocallyHeardStation, Digipeater, Node, Operator

# Connect to PG
LocalSession = sessionmaker(bind=local_engine)
local_session = LocalSession()
RemoteSession = sessionmaker(bind=remote_engine)
remote_session = RemoteSession()

bad_geocodes_res = remote_session.query(BadGeocode.id).all()

bad_geocode_ids = []
for bad_geocode_item in bad_geocodes_res:
    bad_geocode_ids.append(bad_geocode_item[0])

new_bad_geocode_res = local_session.query(BadGeocode). \
    filter(BadGeocode.id.notin_(bad_geocode_ids)).all()

new_bg_counter = 0
for new_bad_geocode in new_bad_geocode_res:
    # local_session.expunge(new_bad_geocode)
    remote_session.merge(new_bad_geocode)
    new_bg_counter += 1

remote_crawled_node_id_res = remote_session.query(CrawledNode.id).all()
remote_crawled_node_ids = []
for id in remote_crawled_node_id_res:
    remote_crawled_node_ids.append(id[0])

new_remote_crawled_node_res = local_session.query(CrawledNode). \
    filter(CrawledNode.id.notin_(remote_crawled_node_ids)).all()

new_crawled_node_counter = 0
for new_crawled_node in new_remote_crawled_node_res:
    # local_session.expunge(new_crawled_node)
    remote_session.merge(new_crawled_node)
    new_crawled_node_counter += 1

remote_digipeater_res = remote_session.query(Digipeater.id).all()
existing_digi_ids = []
for digipeater_item in remote_digipeater_res:
    existing_digi_ids.append(digipeater_item[0])

new_digi_res = local_session.query(Digipeater). \
    filter(Digipeater.id.notin_(existing_digi_ids)).all()

new_digi_counter = 0
for new_digi_item in new_digi_res:
    # local_session.expunge(new_digi_item)
    remote_session.merge(new_digi_item)
    new_digi_counter += 1

local_mh_list_res = remote_session.query(LocallyHeardStation.id).all()
local_mh_list_ids = []
for id in local_mh_list_res:
    local_mh_list_ids.append(id[0])
new_mh_list_res = local_session.query(LocallyHeardStation). \
    filter(LocallyHeardStation.id.notin_(local_mh_list_ids)).all()

new_mh_counter = 0
for new_mh_item in new_mh_list_res:
    # local_session.expunge(new_mh_item)
    remote_session.merge(new_mh_item)
    new_mh_counter += 1

node_res = remote_session.query(Node.id).all()
node_ids = []
for id in node_res:
    node_ids.append(id[0])
new_node_res = local_session.query(Node).filter(Node.id.notin_(node_ids)).all()

new_node_counter = 0
for new_node_item in new_node_res:
    # local_session.expunge(new_node_item)
    remote_session.merge(new_node_item)
    new_node_counter += 1

local_operator_res = remote_session.query(Operator.id).all()
local_operator_ids = []
for id in local_operator_res:
    local_operator_ids.append(id[0])

new_local_op_res = local_session.query(Operator). \
    filter(Operator.id.notin_(local_operator_ids)).all()

new_op_counter = 0
for new_operator_item in new_local_op_res:
    # local_session.expunge(new_operator_item)
    remote_session.merge(new_operator_item)
    new_op_counter += 1

remote_digi_res = remote_session.query(RemoteDigipeater.id).all()
remote_digi_ids = []
for id in remote_digi_res:
    remote_digi_ids.append(id[0])

new_remote_digi_res = local_session.query(RemoteDigipeater). \
    filter(RemoteDigipeater.id.notin_(remote_digi_ids)).all()

new_remote_digi_counter = 0
for new_remote_digi_item in new_remote_digi_res:
    # local_session.expunge(new_remote_digi_item)
    remote_session.merge(new_remote_digi_item)
    new_remote_digi_counter += 1

remote_mh_res = remote_session.query(RemotelyHeardStation.id).all()
remote_mh_ids = []
for id in remote_mh_res:
    remote_mh_ids.append(id[0])

new_remote_mh_res = local_session.query(RemotelyHeardStation). \
    filter(RemotelyHeardStation.id.notin_(remote_mh_ids)).all()

new_remote_mh_counter = 0
for new_remote_mh_item in new_remote_mh_res:
    # local_session.expunge(new_remote_mh_item)
    remote_session.merge(new_remote_mh_item)
    new_remote_mh_counter += 1

remote_op_res = remote_session.query(RemoteOperator.id).all()
remote_op_ids = []
for id in remote_op_res:
    remote_op_ids.append(id[0])

new_remote_op_res = local_session.query(RemoteOperator). \
    filter(RemoteOperator.id.notin_(remote_op_ids)).all()

new_remote_op_counter = 0
for new_remote_op_item in new_remote_op_res:
    # local_session.expunge(new_remote_op_item)
    remote_session.merge(new_remote_op_item)
    new_remote_op_counter += 1

remote_session.commit()
remote_session.close()
local_session.close()

print(f"Added {new_bg_counter} bad geocodes\n"
      f"Added {new_crawled_node_counter} crawled nodes\n"
      f"Added {new_digi_counter} digipeaters\n"
      f"Added {new_mh_counter} local MH entries\n"
      f"Added {new_node_counter} nodes\n"
      f"Added {new_op_counter} operators\n"
      f"Added {new_remote_digi_counter} remote digipeaters\n"
      f"Added {new_remote_mh_counter} remote MH entries\n"
      f"Added {new_remote_op_counter} remote operators")
