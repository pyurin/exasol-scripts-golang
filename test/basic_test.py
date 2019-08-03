import pyexasol
import pprint
import os

printer = pprint.PrettyPrinter(indent=4, width=140)

# Basic connect
C = pyexasol.connect(dsn='localhost:8563', user='SYS', password='exasol', schema='test')

# Create schema
stmt = C.execute("CREATE SCHEMA IF NOT EXISTS test")
printer.pprint(stmt.fetchall())

# Basic query
stmt = C.execute("SELECT now()")
printer.pprint(stmt.fetchall())


# Basic query
stmt = C.execute("SELECT * FROM users ORDER BY user_id LIMIT 5")
printer.pprint(stmt.fetchall())

# Disconnect
C.close()