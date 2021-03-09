import datetime

from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from common import get_conf
from models.db import engine, ops_to_crawl

conf = get_conf()
year = datetime.date.today().year
Session = sessionmaker(bind=engine)
session = Session()

refresh_time = datetime.datetime.utcnow().replace(microsecond=0) - \
               datetime.timedelta(days=7)

# Get an operator to crawl
try:

    for r in session.query(ops_to_crawl):
        print(r.id, r.remote_call)

    """
    # Get a node port that doesn't need check and is active
    op_to_crawl = session.query(CrawledNode).filter(
        CrawledNode.last_crawled < refresh_time). \
        filter(CrawledNode.needs_check == false(),
               CrawledNode.active_port == true()). \
        order_by(func.random()).limit(1).one_or_none()
    if op_to_crawl:
        node_to_crawl_info = {
            op_to_crawl.node_id: (
                op_to_crawl.id,
                op_to_crawl.port,
                op_to_crawl.last_crawled,
                op_to_crawl.port_name
            )
        }
    """
except NoResultFound:
    print("Nothing to crawl")
    exit()
