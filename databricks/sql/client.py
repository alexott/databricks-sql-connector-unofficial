"""DB-API implementation backed by Databricks.

See http://www.python.org/dev/peps/pep-0249/

Many docstrings in this file are based on the PEP, which is in the public domain.
"""

import base64
from collections import namedtuple, OrderedDict
import datetime
import math
import re
import time
import threading
from decimal import Decimal
from ssl import CERT_NONE, CERT_OPTIONAL, CERT_REQUIRED, create_default_context


from databricks.sql import common, USER_AGENT_NAME, __version__
from databricks.sql.types import Row
from databricks.sql.exc import *
from databricks.sql.TCLIService import TCLIService, ttypes
from builtins import range
from future.utils import iteritems
import logging
import sys
import thrift.transport.THttpClient
import thrift.protocol.TBinaryProtocol
import thrift.transport.TSocket
import thrift.transport.TTransport

_logger = logging.getLogger(__name__)

_TIMESTAMP_PATTERN = re.compile(r'(\d+-\d+-\d+ \d+:\d+:\d+(\.\d{,6})?)')

ssl_cert_parameter_map = {
    "none": CERT_NONE,
    "optional": CERT_OPTIONAL,
    "required": CERT_REQUIRED,
}

# see Connection.__init__ for parameter descriptions.
# - Min/Max avoids unsustainable configs (sane values are far more constrained)
# - 900s attempts-duration lines up w ODBC/JDBC drivers (for cluster startup > 10 mins)
_retry_policy = {                          # (type, default, min, max)
    "_retry_delay_min":                     (float, 1, 0.1, 60),
    "_retry_delay_max":                     (float, 60, 5, 3600),
    "_retry_stop_after_attempts_count":     (int, 30, 1, 60),
    "_retry_stop_after_attempts_duration":  (float, 900, 1, 86400),
}


THRIFT_ERROR_MESSAGE_HEADER = "x-thriftserver-error-message"
DATABRICKS_ERROR_OR_REDIRECT_HEADER = "x-databricks-error-or-redirect-message"
DATABRICKS_REASON_HEADER = "x-databricks-reason-phrase"


def _parse_timestamp(value):
    if value:
        match = _TIMESTAMP_PATTERN.match(value)
        if match:
            if match.group(2):
                format = '%Y-%m-%d %H:%M:%S.%f'
                # use the pattern to truncate the value
                value = match.group()
            else:
                format = '%Y-%m-%d %H:%M:%S'
            value = datetime.datetime.strptime(value, format)
        else:
            raise Exception(
                'Cannot convert "{}" into a datetime'.format(value))
    else:
        value = None
    return value


def _parse_date(value):
    if value:
        format = '%Y-%m-%d'
        value = datetime.datetime.strptime(value, format).date()
    else:
        value = None
    return value


def _bound(min_x, max_x, x):
    """Bound x by [min_x, max_x]

    min_x or max_x being None means unbounded in that respective side.
    """
    if min_x is None and max_x is None:
        return x
    if min_x is None:
        return min(max_x, x)
    if max_x is None:
        return max(min_x, x)
    return min(max_x, max(min_x, x))


TYPES_CONVERTER = {"decimal": Decimal,
                   "timestamp": _parse_timestamp,
                   "date": _parse_date}


class _HiveParamEscaper(common.ParamEscaper):
    def escape_string(self, item):
        # backslashes and single quotes need to be escaped
        # TODO verify against parser
        # Need to decode UTF-8 because of old sqlalchemy.
        # Newer SQLAlchemy checks dialect.supports_unicode_binds before encoding Unicode strings
        # as byte strings. The old version always encodes Unicode as byte strings, which breaks
        # string formatting here.
        if isinstance(item, bytes):
            item = item.decode('utf-8')
        return "'{}'".format(
            item
                .replace('\\', '\\\\')
                .replace("'", "\\'")
                .replace('\r', '\\r')
                .replace('\n', '\\n')
                .replace('\t', '\\t')
        )


_escaper = _HiveParamEscaper()


class Connection(object):
    """Wraps a Thrift session"""

    def __init__(
            self,
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
        """

        # Internal arguments in **kwargs:
        # _user_agent_entry
        #   Tag to add to User-Agent header. For use by partners.
        # _username, _password
        #   Username and password Basic authentication (no official support)
        #
        # TLS
        # _tls_no_verify
        #   Set to True (Boolean) to completely disable SSL verification.
        # _tls_verify_hostname
        #   Set to False (Boolean) to disable SSL hostname verification, but check certificate.
        # _tls_trusted_ca_file
        #   Set to the path of the file containing trusted CA certificates for server certificate
        #   verification. If not provide, uses system truststore.
        # _tls_client_cert_file, _tls_client_cert_key_file, _tls_client_cert_key_password
        #   Set client SSL certificate.
        #   See https://docs.python.org/3/library/ssl.html#ssl.SSLContext.load_cert_chain
        #
        # _connection_uri
        #   Overrides server_hostname and http_path.
        #
        # RETRY/ATTEMPT POLICY
        # _retry_delay_min                      (default: 1)
        # _retry_delay_max                      (default: 60)
        #   {min,max} pre-retry delay bounds
        # _retry_stop_after_attempts_count      (default: 30)
        #   total max attempts during retry sequence
        # _retry_stop_after_attempts_duration   (default: 900)
        #   total max wait duration during retry sequence
        #   (Note this will stop _before_ intentionally exceeding; thus if the
        #   next calculated pre-retry delay would go past
        #   _retry_stop_after_attempts_duration, stop now.)
        #

        port = 443
        if kwargs.get("_connection_uri"):
            uri = kwargs.get("_connection_uri")
        elif server_hostname and http_path:
            uri = "https://{host}:{port}/{path}".format(
                host=server_hostname, port=port, path=http_path.lstrip("/"))
        else:
            raise ValueError("No valid connection settings.")

        self._request_lock = threading.RLock()

        # Configure retries & timing: use user-settings or defaults, and bound
        # by policy. Log.warn when given param gets restricted.
        for (key, (type_, default, min, max)) in _retry_policy.items():
            given_or_default = type_(kwargs.get(key, default))
            bound = _bound(min, max, given_or_default)
            setattr(self, key, bound)
            _logger.debug('retry parameter: {} given_or_default {}'.format(key, given_or_default))
            if bound != given_or_default:
                _logger.warn('Override out of policy retry parameter: ' +
                    '{} given {}, restricted to {}'.format(key, given_or_default, bound))

        # Fail on retry delay min > max; consider later adding fail on min > duration?
        if self._retry_stop_after_attempts_count > 1 \
                and self._retry_delay_min > self._retry_delay_max:
            raise ValueError("Invalid configuration enables retries with retry delay min(={}) > max(={})"
                .format(self._retry_delay_min, self._retry_delay_max))

        # Configure tls context
        ssl_context = create_default_context(cafile=kwargs.get("_tls_trusted_ca_file"))
        if kwargs.get("_tls_no_verify") is True:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = CERT_NONE
        elif kwargs.get("_tls_verify_hostname") is False:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = CERT_REQUIRED
        else:
            ssl_context.check_hostname = True
            ssl_context.verify_mode = CERT_REQUIRED

        tls_client_cert_file = kwargs.get("_tls_client_cert_file")
        tls_client_cert_key_file = kwargs.get("_tls_client_cert_key_file")
        tls_client_cert_key_password = kwargs.get("_tls_client_cert_key_password")
        if tls_client_cert_file:
            ssl_context.load_cert_chain(
                certfile=tls_client_cert_file,
                keyfile=tls_client_cert_key_file,
                password=tls_client_cert_key_password
            )

        self._transport = thrift.transport.THttpClient.THttpClient(
            uri_or_host=uri,
            ssl_context=ssl_context,
        )

        if kwargs.get("_username") and kwargs.get("_password"):
            auth_credentials = "{username}:{password}".format(
                username=kwargs.get("_username"), password=kwargs.get("_password")
            ).encode("UTF-8")
            auth_credentials_base64 = base64.standard_b64encode(auth_credentials).decode(
                "UTF-8"
            )
            authorization_header = "Basic {}".format(auth_credentials_base64)
        elif access_token:
            authorization_header = "Bearer {}".format(access_token)
        else:
            raise ValueError("No valid authentication settings.")

        if not kwargs.get("_user_agent_entry"):
            useragent_header = "{}/{}".format(USER_AGENT_NAME, __version__)
        else:
            useragent_header = "{}/{} ({})".format(
                USER_AGENT_NAME, __version__, kwargs.get("_user_agent_entry"))

        self._transport.setCustomHeaders({
            "Authorization": authorization_header,
            "User-Agent": useragent_header
        })
        protocol = thrift.protocol.TBinaryProtocol.TBinaryProtocol(self._transport)
        self._client = TCLIService.Client(protocol)
        # oldest version that still contains features we care about
        # "V6 uses binary type for binary payload (was string) and uses columnar result set"
        protocol_version = ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6

        try:
            self._transport.open()
            open_session_req = ttypes.TOpenSessionReq(
                client_protocol=protocol_version
            )
            response = self._make_request(self._client.OpenSession, open_session_req)
            _check_status(response)
            assert response.sessionHandle is not None, "Expected a session from OpenSession"
            self._sessionHandle = response.sessionHandle
            assert response.serverProtocolVersion == protocol_version, \
                "Unable to handle protocol version {}".format(response.serverProtocolVersion)
        except:
            self._transport.close()
            raise

    def __enter__(self):
        """Transport should already be opened by __init__"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Call close"""
        self.close()

    def close(self):
        """Close the underlying session and Thrift transport"""
        req = ttypes.TCloseSessionReq(sessionHandle=self._sessionHandle)
        response = self._make_request(self._client.CloseSession, req)
        self._transport.close()
        _check_status(response)

    def commit(self):
        """No-op because Databricks does not support transactions"""
        pass

    def cursor(self, *args, **kwargs):
        """Return a new :py:class:`Cursor` object using the connection."""
        return Cursor(self, *args, **kwargs)

    def rollback(self):
        raise NotSupportedError("Databricks does not have transactions")

    @staticmethod
    def _extract_error_message_from_headers(headers):
        err_msg = None
        if THRIFT_ERROR_MESSAGE_HEADER in headers:
            err_msg = headers[THRIFT_ERROR_MESSAGE_HEADER]
        if DATABRICKS_ERROR_OR_REDIRECT_HEADER in headers:
            if err_msg:  # We don't expect both to be set, but log both here just in case
                err_msg = "Thriftserver message: {}, Databricks message: {}".format(err_msg, headers[DATABRICKS_ERROR_OR_REDIRECT_HEADER])
            else:
                err_msg = headers[DATABRICKS_ERROR_OR_REDIRECT_HEADER]
            if DATABRICKS_REASON_HEADER in headers:
                err_msg += ": " + headers[DATABRICKS_REASON_HEADER]
        return err_msg

    def _handle_request_error(self, error_info, attempt, elapsed):
        max_attempts = self._retry_stop_after_attempts_count
        max_duration_s = self._retry_stop_after_attempts_duration

        if error_info.retry_delay is not None and elapsed + error_info.retry_delay > max_duration_s:
            no_retry_reason = common.NoRetryReason.OUT_OF_TIME
        elif error_info.retry_delay is not None and attempt >= max_attempts:
            no_retry_reason = common.NoRetryReason.OUT_OF_ATTEMPTS
        elif error_info.retry_delay is None:
            no_retry_reason = common.NoRetryReason.NOT_RETRYABLE
        else:
            no_retry_reason = None

        full_error_info_str = error_info.full_info_logging_str(no_retry_reason, attempt,
                                                               max_attempts, elapsed,
                                                               max_duration_s)

        if no_retry_reason is not None:
            user_friendly_error_message = error_info.user_friendly_error_message(
                no_retry_reason, attempt, elapsed)
            _logger.info("{}: {}".format(user_friendly_error_message, full_error_info_str))

            raise OperationalError(user_friendly_error_message, error_info.error)

        _logger.info("Retrying request after error in {} seconds: {}".format(
            error_info.retry_delay, full_error_info_str))
        time.sleep(error_info.retry_delay)

    def _make_request(self, method, request):
        """Execute given request, attempting retries when receiving HTTP 429/503.

        For delay between attempts, honor the given Retry-After header, but with bounds.
        Use lower bound of expontial-backoff based on _retry_delay_min,
        and upper bound of _retry_delay_max.
        Will stop retry attempts if total elapsed time + next retry delay would exceed
        _retry_stop_after_attempts_duration.
        """
        # basic strategy: build range iterator rep'ing number of available
        # retries. bounds can be computed from there. iterate over it with
        # retries until success or final failure achieved.

        t0 = time.time()

        def get_elapsed():
            return time.time() - t0

        def extract_retry_delay(attempt):
            # encapsulate retry checks, returns None || delay-in-secs
            # Retry IFF 429/503 code + Retry-After header set
            http_code = getattr(self._transport, "code", None)
            retry_after = getattr(self._transport, "headers", {}).get("Retry-After")
            if http_code in [429, 503] and retry_after:
                # bound delay (seconds) by [min_delay*1.5^(attempt-1), max_delay]
                delay = int(retry_after)
                delay = max(delay, self._retry_delay_min * math.pow(1.5, attempt - 1))
                delay = min(delay, self._retry_delay_max)
                return delay
            return None

        def attempt_request(attempt):
            # splits out lockable attempt, from delay & retry loop
            try:
                _logger.debug("Sending request: {}".format(request))
                response = method(request)
                _logger.debug("Received response: {}".format(response))
                return response
            except Exception as error:
                retry_delay = extract_retry_delay(attempt)
                error_message = Connection._extract_error_message_from_headers(getattr(self._transport, "headers", {}))
                return common.RequestErrorInfo(error=error,
                                               error_message=error_message,
                                               retry_delay=retry_delay,
                                               http_code=getattr(self._transport, "code", None),
                                               method=method.__name__,
                                               request=request)

        # The real work:
        # - for each available attempt:
        #       lock-and-attempt
        #       return on success
        #       if available: bounded delay and retry
        #       if not: raise error
        max_attempts = self._retry_stop_after_attempts_count
        max_duration_s = self._retry_stop_after_attempts_duration

        # use index-1 counting for logging/human consistency
        for attempt in range(1, max_attempts + 1):
            # We have a lock here because .cancel can be called from a separate thread.
            # We do not want threads to be simultaneously sharing the Thrift Transport
            # because we use its state to determine retries
            with self._request_lock:
                response_or_error_info = attempt_request(attempt)
            elapsed = get_elapsed()

            # conditions: success, non-retry-able, no-attempts-left, no-time-left, delay+retry
            if not isinstance(response_or_error_info, common.RequestErrorInfo):
                # log nothing here, presume that main request logging covers
                return response_or_error_info

            error_info = response_or_error_info
            # The error handler will either sleep or throw an exception
            self._handle_request_error(error_info, attempt, elapsed)


class Cursor(common.DBAPICursor):
    """These objects represent a database cursor, which is used to manage the context of a fetch
    operation.

    Cursors are not isolated, i.e., any changes done to the database by a cursor are immediately
    visible by other cursors or connections.
    """

    def __init__(self, connection, arraysize=10000):
        self._operationHandle = None
        super(Cursor, self).__init__()
        self._arraysize = arraysize
        self._connection = connection

    def _reset_state(self):
        """Reset state about the previous query in preparation for running another query"""
        super(Cursor, self)._reset_state()
        self._description = None
        if self._operationHandle is not None:
            request = ttypes.TCloseOperationReq(self._operationHandle)
            try:
                conn = self._connection
                response = conn._make_request(conn._client.CloseOperation, request)
                _check_status(response)
            finally:
                self._operationHandle = None

    @property
    def arraysize(self):
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value):
        """Array size cannot be None, and should be an integer"""
        default_arraysize = 10000
        try:
            self._arraysize = int(value) or default_arraysize
        except TypeError:
            self._arraysize = default_arraysize

    @property
    def description(self):
        """This read-only attribute is a sequence of 7-item sequences.

        Each of these sequences contains information describing one result column:

        - name
        - type_code
        - display_size (None in current implementation)
        - internal_size (None in current implementation)
        - precision (None in current implementation)
        - scale (None in current implementation)
        - null_ok (always True in current implementation)

        This attribute will be ``None`` for operations that do not return rows or if the cursor has
        not had an operation invoked via the :py:meth:`execute` method yet.

        The ``type_code`` can be interpreted by comparing it to the Type Objects specified in the
        section below.
        """
        if self._operationHandle is None or not self._operationHandle.hasResultSet:
            return None
        if self._description is None:
            req = ttypes.TGetResultSetMetadataReq(self._operationHandle)
            response = self._connection._make_request(self._connection._client.GetResultSetMetadata, req)
            _check_status(response)
            columns = response.schema.columns
            self._description = []
            for col in columns:
                primary_type_entry = col.typeDesc.types[0]
                if primary_type_entry.primitiveEntry is None:
                    # All fancy stuff maps to string
                    type_code = ttypes.TTypeId._VALUES_TO_NAMES[ttypes.TTypeId.STRING_TYPE]
                else:
                    type_id = primary_type_entry.primitiveEntry.type
                    type_code = ttypes.TTypeId._VALUES_TO_NAMES[type_id]
                # Stripping "_TYPE" and converting to lowercase makes TTypeIds consistent with
                # Spark types.
                if type_code.endswith("_TYPE"):
                    type_code = type_code[:-5]
                type_code = type_code.lower()

                self._description.append((
                    col.columnName.decode('utf-8') if sys.version_info[0] == 2 else col.columnName,
                    type_code.decode('utf-8') if sys.version_info[0] == 2 else type_code,
                    None, None, None, None, None
                ))
        return self._description

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Close the operation handle"""
        self._reset_state()

    def execute(self, operation, parameters=None):
        """Prepare and execute a database operation (query or command).

        Return values are not defined.
        """
        # Prepare statement
        if parameters is None:
            sql = operation
        else:
            sql = operation % _escaper.escape_args(parameters)

        self.__prepare_state_for_execution()

        req = ttypes.TExecuteStatementReq(self._connection._sessionHandle,
                                          sql, runAsync=True)
        response = self._connection._make_request(self._connection._client.ExecuteStatement, req)

        self.__handle_execute_response(response)

    def catalogs(self):
        """Get all available catalogs."""
        self.__prepare_state_for_execution()

        req = ttypes.TGetCatalogsReq(
            sessionHandle=self._connection._sessionHandle)
        response = self._connection._make_request(self._connection._client.GetCatalogs, req)

        self.__handle_execute_response(response)

    def schemas(self, catalog_name=None, schema_name=None):
        """Get schemas corresponding to the catalog_name and schema_name.

        Names can contain % wildcards.
        """
        self.__prepare_state_for_execution()

        req = ttypes.TGetSchemasReq(
            sessionHandle=self._connection._sessionHandle,
            catalogName=catalog_name,
            schemaName=schema_name)
        response = self._connection._make_request(self._connection._client.GetSchemas, req)

        self.__handle_execute_response(response)

    def tables(self, catalog_name=None, schema_name=None, table_name=None, table_types=None):
        """Get tables corresponding to the catalog_name, schema_name and table_name.
        table_types is an optional list of table types to match e.g. "TABLE", "VIEW", etc.
        Names can contain % wildcards.
        """
        if table_types is None:
            table_types = []

        self.__prepare_state_for_execution()

        req = ttypes.TGetTablesReq(
            sessionHandle=self._connection._sessionHandle,
            catalogName=catalog_name,
            schemaName=schema_name,
            tableName=table_name,
            tableTypes=table_types)
        response = self._connection._make_request(self._connection._client.GetTables, req)

        self.__handle_execute_response(response)

    def columns(self, catalog_name=None, schema_name=None, table_name=None, column_name=None):
        """Get columns corresponding to the catalog_name, schema_name, table_name and column_name.
        Names can contain % wildcards.
        """
        self.__prepare_state_for_execution()
        
        req = ttypes.TGetColumnsReq(
               sessionHandle=self._connection._sessionHandle,
               catalogName=catalog_name,
               schemaName=schema_name,
               tableName=table_name,
               columnName=column_name)
        response = self._connection._make_request(self._connection._client.GetColumns, req)
        
        self.__handle_execute_response(response)

    def cancel(self):
        req = ttypes.TCancelOperationReq(
            operationHandle=self._operationHandle,
        )
        response = self._connection._make_request(self._connection._client.CancelOperation, req)
        _check_status(response)

    def _fetch_more(self):
        """Send another TFetchResultsReq and update state"""
        assert(self._state == self._STATE_RUNNING), "Should be running when in _fetch_more"
        assert(self._operationHandle is not None), "Should have an op handle in _fetch_more"
        if not self._operationHandle.hasResultSet:
            raise ProgrammingError("No result set")
        req = ttypes.TFetchResultsReq(
            operationHandle=self._operationHandle,
            orientation=ttypes.TFetchOrientation.FETCH_NEXT,
            maxRows=self.arraysize,
        )
        response = self._connection._make_request(self._connection._client.FetchResults, req)
        _check_status(response)
        schema = self.description
        assert not response.results.rows, 'expected data in columnar format'
        columns = [_unwrap_column(col, col_schema[1]) for col, col_schema in
                   zip(response.results.columns, schema)]
        
        ResultRow = Row(*[c[0] for c in self.description])
        new_data = [ResultRow(*r) for r in zip(*columns)]

        self._data += new_data
        # response.hasMoreRows seems to always be False, so we instead check the number of rows
        # https://github.com/apache/hive/blob/release-1.2.1/service/src/java/org/apache/hive/service/cli/thrift/ThriftCLIService.java#L678
        # if not response.hasMoreRows:
        if not new_data:
            self._state = self._STATE_FINISHED

    def __prepare_state_for_execution(self):
        self._reset_state()
        self._state = self._STATE_RUNNING

    def __handle_execute_response(self, response):
        _check_status(response)
        self._operationHandle = response.operationHandle
        self.__wait_for_query_completion()

    def __wait_for_query_completion(self):
        state = ttypes.TOperationState.INITIALIZED_STATE
        while state in [ttypes.TOperationState.INITIALIZED_STATE, ttypes.TOperationState.PENDING_STATE, ttypes.TOperationState.RUNNING_STATE]:
            resp = self.__poll()
            state = resp.operationState

        if state in [ttypes.TOperationState.ERROR_STATE,
                     ttypes.TOperationState.UKNOWN_STATE,
                     ttypes.TOperationState.CANCELED_STATE,
                     ttypes.TOperationState.TIMEDOUT_STATE]:
            raise OperationalError("Query execution failed. State: {}; Error code: {}; SQLSTATE: {}; Error message: {}"
                                   .format(ttypes.TOperationState._VALUES_TO_NAMES[state],
                                           resp.errorCode,
                                           resp.sqlState,
                                           resp.errorMessage))

    def __poll(self, get_progress_update=True):
        """Poll for and return the raw status data provided by the Hive Thrift REST API.
        :returns: ``ttypes.TGetOperationStatusResp``
        :raises: ``ProgrammingError`` when no query has been started
        .. note::
            This is not a part of DB-API.
        """
        if self._state == self._STATE_NONE:
            raise ProgrammingError("No query yet")

        req = ttypes.TGetOperationStatusReq(
            operationHandle=self._operationHandle,
            getProgressUpdate=get_progress_update,
        )
        response = self._connection._make_request(self._connection._client.GetOperationStatus, req)
        _check_status(response)

        return response

#
# Private utilities
#


def _unwrap_column(col, type_=None):
    """Return a list of raw values from a TColumn instance."""
    for attr, wrapper in iteritems(col.__dict__):
        if wrapper is not None:
            result = wrapper.values
            nulls = wrapper.nulls  # bit set describing what's null
            assert isinstance(nulls, bytes)
            for i, char in enumerate(nulls):
                byte = ord(char) if sys.version_info[0] == 2 else char
                for b in range(8):
                    if byte & (1 << b):
                        result[i * 8 + b] = None
            converter = TYPES_CONVERTER.get(type_, None)
            if converter and type_:
                result = [converter(row) if row else row for row in result]
            return result
    raise DataError("Got empty column value {}".format(col))  # pragma: no cover


def _check_status(response):
    """Raise an OperationalError if the status is not success"""
    _logger.debug(response)
    if response.status.statusCode != ttypes.TStatusCode.SUCCESS_STATUS:
        raise OperationalError(response)
