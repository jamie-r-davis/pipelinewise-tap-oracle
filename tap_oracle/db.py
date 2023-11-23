import singer
import cx_Oracle

LOGGER = singer.get_logger()

def fully_qualified_column_name(schema, table, column):
    return '"{}"."{}"."{}"'.format(schema, table, column)

def make_dsn(config):
   return cx_Oracle.makedsn(config["host"], config["port"], config["sid"])

def open_connection(config):
    if 'dsn' in config:
        LOGGER.info("dsn: %s", config['dsn'])
        conn = cx_Oracle.connect(user=config['user'], password=config['password'], dsn=config['dsn'])
    else:
        LOGGER.info("dsn: %s", make_dsn(config))
        conn = cx_Oracle.connect(config["user"], config["password"], make_dsn(config))
    return conn
