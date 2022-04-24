import datetime

#
# PEP 249 module globals
#
apilevel = '2.0'
threadsafety = 1  # Threads may share the module, but not connections.
paramstyle = 'pyformat'  # Python extended format codes, e.g. ...WHERE name=%(name)s

#
# Type Objects and Constructors
#

class DBAPITypeObject(object):
    def __init__(self, *values):
        self.values = values

    def __eq__(self, other):
        return other in self.values

STRING = DBAPITypeObject('string')
BINARY = DBAPITypeObject('binary')
NUMBER = DBAPITypeObject('boolean', 'tinyint', 'smallint', 'int', 'bigint',
                         'float', 'double', 'decimal')
DATETIME = DBAPITypeObject('timestamp')
DATE = DBAPITypeObject('date')
ROWID = DBAPITypeObject()

Date = datetime.date
Timestamp = datetime.datetime

def DateFromTicks(ticks):
    return Date(*time.localtime(ticks)[:3])

def TimestampFromTicks(ticks):
    return Timestamp(*time.localtime(ticks)[:6])
