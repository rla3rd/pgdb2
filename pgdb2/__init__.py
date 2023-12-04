import re
import os
import inspect
import socket
import json
import psycopg2
import sqlalchemy as sa
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy import exc as sa_exc
from typing import Optional, Dict, List
from psycopg2.extras import DictCursor
from psycopg2.extensions import adapt
import sys
import traceback
import warnings

warnings.simplefilter("ignore", category=sa_exc.SAWarning)

def errorDetails():
    error = sys.exc_info()[0]
    details = traceback.format_exc()
    sys.stderr.write(f"{error}, {details}")


class PrepareCursor(object):
    '''
    mix in with dbapi cursor class

    formatRe fishes out all format specifiers for a given paramstyle
    this one works with paramstyles 'qmark', 'format' or 'pyformat'
    '''
    formatRe = re.compile('(\%s|\%\([\w\.]+\)s)', re.DOTALL)

    def __init__(self, *a, **kw):
        super(PrepareCursor, self).__init__(*a, **kw)

        self.prepCache = {}

    def execPrepared(self, cmd, args=None):
        '''
        execute a command using a prepared statement.
        '''
        prepStmt = self.prepCache.get(cmd)
        if prepStmt is None:
            cmdId = f"ps_{len(self.prepCache) + 1}"
            # unique name for new prepared statement
            prepStmt = self.prepCache[cmd] = \
                self.prepare(cmd, cmdId)

        self.execute(prepStmt, args)

    def prepare(self, cmd, cmdId):
        '''
        translate a sql command into its corresponding
        prepared statement, and execute the declaration.
        '''
        specifiers = []

        def replaceSpec(mo):
            specifiers.append(mo.group())
            return f"${len(specifiers)}"

        replacedCmd = self.formatRe.sub(replaceSpec, cmd)
        prepCmd = f"prepare {cmdId} as {replacedCmd}"

        if len(specifiers) == 0:    # no variable arguments
            execCmd = f"execute {cmdId}"

        else:       # set up argument slots in prep statement
            execCmd = f"execute {cmdId}({', '.join(specifiers)})"

        self.execute(prepCmd)
        self.prepCache[execCmd] = execCmd

        return execCmd

    def execManyPrepared(self, cmd, seq_of_parameters):
        '''
        prepared statement version of executemany.
        '''
        for p in seq_of_parameters:
            self.execPrepared(cmd, p)

        # Don't want to leave the value of the last execute() call
        try:
            self.rowcount = -1
        except TypeError:   # fooks with psycopg
            pass


class Cursor(PrepareCursor, DictCursor):
    pass


class database(object):

    def __repr__(self):
        return " ".join([
            f"Host: {self.host}",
            f"Port: {self.port}",
            f"Database: {self.database}",
            f"User: {self.user}",
            f"Password: {self.password}",
            f"Application_name: {self.appname}"])

    def __init__(self, mode='rw', configFile='pgdb.json'):
        try:
            self.readonly = 'RO' in mode.upper()
            self.appname = '%s.%s.%s.%s' % (
                socket.gethostname(),
                os.getpid(),
                os.environ.get('UNIQUE_ID', ''),
                os.path.basename(inspect.stack()[-1][1]))
            self.adapt = adapt
            db_url = os.getenv(f"PGDB_{mode.upper()}")
            if db_url is None:
                home = os.path.expanduser("~")
                cfgPath = os.environ.get("PGDB_HOME", home)
                boxName = os.uname()[1]

                if os.path.exists(f"{cfgPath}/{configFile}.{boxName}"):
                    configFile = f"{configFile}.{boxName}"

                print(f"{cfgPath}/{configFile}")
                file = open(f"{cfgPath}/{configFile}", "rb")
                option = json.loads(file.read())
                file.close()
                self.host = option.get('host')
                self.port = option.get('port')
                self.database = option.get('database')
                self.user = option.get('user')
                self.password = option.get('password')
            else:
                db_url = db_url.replace("postgres://", "postgresql://")
                db_url = db_url.replace("pgsql://", "postgresql://")
                pattern = re.compile(
                r"""
                    postgresql://
                    (?:
                        (?P<user>[^:/]*)
                        (?::(?P<password>[^@]*))?
                    @)?
                    (?:(?P<host>[^/:\?]+))?
                    (?::(?P<port>[^/\?]*))?
                    (?:/(?P<database>[^\?]*))?
                    """,
                    re.X)

                m = pattern.match(db_url)
                if m is not None:
                    db_cfg = m.groupdict()
                    self.host = db_cfg.pop("host")
                    self.port = db_cfg.pop("port")
                    self.database = db_cfg.pop("database")
                    self.user = db_cfg.pop("user")
                    self.password = db_cfg.pop("password")
                
            connString = "".join([
                f"postgresql://{self.user}",
                f":{self.password}@",
                f"{self.host}:",
                f"{self.port}/",
                f"{self.database}?",
                f"application_name={self.appname}"])
            
            db = create_engine(
                connString,
                poolclass=NullPool,
                connect_args={'connect_timeout': 10})
            self.engine = db.engine
            self.metadata = MetaData()
            self.metadata.reflect(self.engine)
            self.conn = db.engine.raw_connection()
            # the connection starts in transaction mode
            # that needs rolled back in order
            # to set the session in autocommit mode
            self.conn.rollback()
            self.conn.set_session(readonly=self.readonly, autocommit=True)
            self.cursor = self.conn.cursor(cursor_factory=Cursor)
            self.available = True
            self.cursor.execute("set statement_timeout='10min'")

        except sa.exc.OperationalError:
            self.available = False
            self.conn = None
            self.cursor = None
            self.engine = None
            self.metadata = None

    def autocommit(self, auto):
        if auto is False:
            self.conn.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
        if auto is True:
            self.conn.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    def getEngineConnCursor(self):
        return (self.engine, self.conn, self.cursor)

    def getConnCursor(self):
        return (self.conn, self.cursor)

    def getConn(self):
        return self.conn

    def getCursor(self):
        return self.cursor

    def getEngine(self):
        return self.engine


def create_upsert_method(
    meta: sa.MetaData,
    update_cols: Optional[Dict[str, str]] = None,
    skip_cols: Optional[List[str]] = []
):
    """
    Create upsert method that satisfied the pandas's to_sql API.
    we pass in the full metadata object here because pandas.to_sql
    API uses a tablename parameter to look up the data from the metadata
    object under the hood, so we do the same here, and yes its kind of
    wierd that the update_cols and skip_cols are table specific, but
    it is what it is
    """
    def method(table, conn, keys, data_iter):
        # select table that data is being inserted to (from pandas's context)
        sql_table = sa.Table(table.name, meta)
        # sqlalchemy and pandas to_sql does not like serial columns on upsert
        # so we remove the column definition from the Table metatable object
        # prior to upsert, this allows postgres to manage the serial
        for col in skip_cols:
            skip_columns = [col for col in sql_table._columns if col.name == col]
            if len(skip_columns) > 0:
                skip_instance = skip_columns[0]
                sql_table._columns.remove(skip_instance)
          
        # list of dictionaries {col_name: value} of data to insert
        dict_vals = [dict(zip(keys, data)) for data in data_iter]
        # we also must remove the serial column from the array of dicts
        rows = []
        for row in dict_vals:
            for col in skip_cols:
                del row[col]
            rows.append(row)

        # create insert statement
        insert_stmt = sa.dialects.postgresql.insert(sql_table).values(rows)

        # create update statement for excluded fields on conflict
        # skipping the serial column in skip_cols
        update_stmt = {
            exc_k.key: exc_k for exc_k in insert_stmt.excluded 
            if exc_k.key not in skip_cols}
        # update_cols is a dict representing a key/value of strings
        # to do things like set foo = now() on the pg side of things
        if update_cols:
            update_stmt.update(update_cols)
        
        # create upsert statement. 
        upsert_stmt = insert_stmt.on_conflict_do_update(
            # index elements are primary keys of a table
            index_elements=sql_table.primary_key.columns, 
            # the SET part of an INSERT statement
            set_=update_stmt 
        )
        
        # execute upsert statement
        conn.execute(upsert_stmt)

    return method
