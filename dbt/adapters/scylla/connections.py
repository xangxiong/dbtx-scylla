from contextlib import contextmanager

import cassandra
from cassandra import type_codes
from cassandra.auth import PlainTextAuthenticator
from cassandra.cluster import Cluster

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import AdapterResponse
from dbt.events import AdapterLogger
from dbt.events.contextvars import get_node_info
from dbt.events.functions import fire_event
from dbt.events.types import SQLCommit

from dbt.helper_types import Port
from dataclasses import dataclass
from typing import Optional
from typing_extensions import Annotated
from mashumaro.jsonschema.annotations import Maximum, Minimum


logger = AdapterLogger("Scylla")


@dataclass
class ScyllaCredentials(Credentials):
    host: str
    user: str
    # Annotated is used by mashumaro for jsonschema generation
    port: Annotated[Port, Minimum(0), Maximum(65535)]
    password: str  # on Scylla the password is mandatory
    connect_timeout: int = 10
    sslmode: Optional[str] = None
    sslcert: Optional[str] = None
    application_name: Optional[str] = "dbt"
    retries: int = 1

    _ALIASES = {"pass": "password"}

    @property
    def type(self):
        return "Scylla"

    @property
    def unique_field(self):
        return self.host

    def _connection_keys(self):
        return (
            "host",
            "port",
            "user",
            "keyspace",
            "connect_timeout",
            "sslmode",
            "sslcert",
            "application_name",
            "retries"
        )


class ScyllaConnectionManager(SQLConnectionManager):
    TYPE = "Scylla"

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except cassandra.DriverException as e:
            logger.debug("Scylla error: {}".format(str(e)))

            try:
                self.rollback_if_open()
            except cassandra.DriverException:
                logger.debug("Failed to release connection!")
                pass

            raise dbt.exceptions.DbtDatabaseError(str(e).strip()) from e

        except Exception as e:
            logger.debug("Error running SQL: {}", sql)
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            if isinstance(e, dbt.exceptions.DbtRuntimeError):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise dbt.exceptions.DbtRuntimeError(e) from e

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = cls.get_credentials(connection.credentials)
        kwargs = {}

        # if credentials.sslmode:
        #     kwargs["sslmode"] = credentials.sslmode

        # if credentials.sslcert is not None:
        #     kwargs["sslcert"] = credentials.sslcert

        if credentials.application_name:
            kwargs["application_name"] = credentials.application_name

        def connect():
            cluster = Cluster(
                contact_points=[credentials.host],
                port=credentials.port,
                load_balancing_policy=None,
                auth_provider=PlainTextAuthenticator(
                    username=credentials.user,
                    password=credentials.password
                ),
                **kwargs
            )
            
            session = cluster.connect(credentials.keyspace)
            
            return session

        retryable_exceptions = [
            cassandra.ConnectionError,
        ]

        def exponential_backoff(attempt: int):
            return attempt * attempt

        return cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retry_limit=credentials.retries,
            retry_timeout=exponential_backoff,
            retryable_exceptions=retryable_exceptions,
        )

    def cancel(self, connection):
        connection.handle.shutdown()

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        # TODO: need to validate if this is correct
        message = str(cursor.statusmessage)
        rows = cursor.rowcount
        status_message_parts = message.split() if message is not None else []
        status_messsage_strings = [part for part in status_message_parts if not part.isdigit()]
        code = " ".join(status_messsage_strings)
        return AdapterResponse(_message=message, code=code, rows_affected=rows)

    @classmethod
    def data_type_code_to_name(cls, type_code: int) -> str:
        type_mapping = {
            type_codes.BytesType: 'BLOB',
            type_codes.UTF8Type: 'TEXT',
            type_codes.VarcharType: 'VARCHAR'
        }
        if type_mapping.get(type_code, None):
            return type_mapping[type_code]
        else:
            return f"unknown type_code {type_code}"

    def begin(self):
        connection = self.get_thread_connection()
        if connection.transaction_open is True:
            raise dbt.exceptions.DbtInternalError(
                'Tried to begin a new transaction on connection "{}", but '
                "it already had one open!".format(connection.name)
            )

        connection.transaction_open = True
        return connection

    def commit(self):
        connection = self.get_thread_connection()
        if connection.transaction_open is False:
            raise dbt.exceptions.DbtInternalError(
                'Tried to commit transaction on connection "{}", but '
                "it does not have one open!".format(connection.name)
            )

        fire_event(SQLCommit(conn_name=connection.name, node_info=get_node_info()))
        self.add_commit_query()

        connection.transaction_open = False

        return connection
