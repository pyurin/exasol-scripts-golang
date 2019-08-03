import pyexasol
import pprint
import os

dsn = os.environ.get('EXAHOST', 'localhost:8563')
user = os.environ.get('EXAUID', 'SYS')
password = os.environ.get('EXAPWD', 'exasol')
schema = os.environ.get('EXASCHEMA', 'test')

printer = pprint.PrettyPrinter(indent=4, width=140)

# Basic connect
C = pyexasol.connect(dsn=dsn, user=user, password=password, schema=schema)

# Create schema
stmt = C.execute("CREATE SCHEMA IF NOT EXISTS test")
printer.pprint(stmt.fetchall())

# Basic query
stmt = C.execute("SELECT * FROM users ORDER BY user_id LIMIT 5")
printer.pprint(stmt.fetchall())

# Disconnect
C.close()