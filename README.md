# mh-stats
A collection of scripts for converting packet radio MHeard & node lists into spatial data & storing them in a Postgresql (PostGIS) database.

# Getting Started
## Requirements
- PostgreSQL database with PostGIS extension enabled
- Python 3.8 with the following modules (it is best if you use Anaconda/Miniconda)
  - SQLAlchemy
  - Geoalchemy2
  - Shapely
  - configparser
  - requests

## Configuration File
You must first set up a configuration file, named settings.cfg in the root project directory, as follows:

[qrz]
username=YOUR_QRZ_USERNAME

password=YOUR_QRZ_PASSWORD

[telnet]
ip=YOUR_BPQ_NODE_IP_ADDRESS

port=YOUR_BPQ_TELNET_PORT

username=YOUR_BPQ_USERNAME

password=YOUR_BPQ_PASSWORD

[postgres]
host=YOUR_PG_HOST_IP

port=YOUR_PG_PORT

username=YOUR_PG_USERNAME

password=YOUR_PG_PASSWORD

db=PG_DB_NAME

[mhcrawler]
info_method=QRZ_OR_HAMDB

for info_method, enter either hamdb if you want the mainly US only, but free, hamdb.org database. 
If you are a QRZ.com subscriber, you can use the QRZ API by entering 'qrz' in the text. Either
qrz or hamdb must be lowercase. Note that if you choose hamdb, you may omit the qrz section of
the config file.
