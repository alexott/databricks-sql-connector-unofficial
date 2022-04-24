from __future__ import absolute_import
from __future__ import unicode_literals

# Make all DB-API interface objects visible in this module.
from databricks.sql.dbapi import *
# Make all exceptions visible in this module per DB-API
from databricks.sql.exc import *

__version__ = "1.0.0"
USER_AGENT_NAME = "PyDatabricksSqlConnector"

def connect(
        server_hostname,
        http_path,
        access_token,
        **kwargs
):
    """Connect to a Databricks SQL endpoint or a Databricks cluster.

    :param server_hostname: Databricks instance host name.
    :param http_path: Http path either to a DBSQL endpoint (e.g. /sql/1.0/endpoints/1234567890abcdef)
           or to a DBR interactive cluster (e.g. /sql/protocolv1/o/1234567890123456/1234-123456-slid123)
    :param access_token: Http Bearer access token, e.g. Databricks Personal Access Token.

    :returns: a :py:class:`Connection` object.
    """
    from databricks.sql.client import Connection
    return Connection(server_hostname, http_path, access_token, **kwargs)
