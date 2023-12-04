# pgdb2 - a psycopg2 DB wrapper
pgdb2 reads of json config files for setting of database parameters, allowing per box configuration management of postgresql connections. 

The default config file is pgdb.json
The location can be set using a PGDB_HOME environment variable to set the directory location.  When the PGDB_HOME environment variable is not set, the location defaults to the user's home directory

different boxes can be configured using additional config files with them ending in the hostname (uname -n)

ie pgdb.json.myhostname, the hostname is picked up in the backround and does not need to be spefically called on wrapper initiation

it also has prepared query mixins built in to allow for server side prepared statements, something that psycopg2 does not support natively.
###### usage
import pgdb2
db = pgdb2.database()

(conn, cursor) = db.getConnCursor()

or

(engine, conn, cursor) = db.getEngineConnCursor()

or

engine = db.getEngine()

conn = db.getConn()

cursor = db.getCursor()
