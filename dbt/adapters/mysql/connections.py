from contextlib import contextmanager

import mysql.connector
import mysql.connector.constants

import dbt_common.exceptions
from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.contracts.connection import AdapterResponse, Connection, Credentials
from dbt.adapters.events.logging import AdapterLogger
from dataclasses import dataclass
from typing import Optional, Union

logger = AdapterLogger("mysql")


@dataclass(init=False)
class MySQLCredentials(Credentials):
    server: str = ""
    unix_socket: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None  # type: ignore[assignment]
    schema: str = ""
    username: Optional[str] = None
    password: Optional[str] = None
    charset: Optional[str] = None
    collation: Optional[str] = None

    _ALIASES = {
        "UID": "username",
        "user": "username",
        "PWD": "password",
        "host": "server",
    }

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
            self.database = None

    def __post_init__(self):
        # mysql classifies database and schema as the same thing
        if self.database is not None and self.database != self.schema:
            raise dbt_common.exceptions.DbtRuntimeError(
                f"    schema: {self.schema} \n"
                f"    database: {self.database} \n"
                f"On MySQL, database must be omitted or have the same value as"
                f" schema."
            )

    @property
    def type(self):
        return "mysql"

    @property
    def unique_field(self):
        return self.schema

    def _connection_keys(self):
        """
        Returns an iterator of keys to pretty-print in 'dbt debug'
        """
        return (
            "server",
            "unix_socket",
            "port",
            "database",
            "schema",
            "user",
        )


class MySQLConnectionManager(SQLConnectionManager):
    TYPE = "mysql"

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = cls.get_credentials(connection.credentials)
        kwargs = {}

        kwargs["user"] = credentials.username
        kwargs["passwd"] = credentials.password
        kwargs["buffered"] = True

        if credentials.server:
            kwargs["host"] = credentials.server
        elif credentials.unix_socket:
            kwargs["unix_socket"] = credentials.unix_socket

        if credentials.port:
            kwargs["port"] = credentials.port

        if credentials.charset:
            kwargs["charset"] = credentials.charset

        if credentials.collation:
            kwargs["collation"] = credentials.collation

        try:
            connection.handle = mysql.connector.connect(**kwargs)
            connection.state = "open"
        except mysql.connector.Error:
            try:
                logger.debug(
                    "Failed connection without supplying the `database`. "
                    "Trying again with `database` included."
                )

                # Try again with the database included
                kwargs["database"] = credentials.schema

                connection.handle = mysql.connector.connect(**kwargs)
                connection.state = "open"
            except mysql.connector.Error as e:
                logger.debug(
                    "Got an error when attempting to open a mysql " "connection: '{}'".format(e)
                )

                connection.handle = None
                connection.state = "fail"

                raise dbt_common.exceptions.ConnectionError(str(e))

        return connection

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    def cancel(self, connection: Connection):
        connection.handle.close()

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except mysql.connector.DatabaseError as e:
            logger.debug("MySQL error: {}".format(str(e)))

            try:
                self.rollback_if_open()
            except mysql.connector.Error:
                logger.debug("Failed to release connection!")
                pass

            raise dbt_common.exceptions.DbtDatabaseError(str(e).strip()) from e

        except Exception as e:
            logger.debug("Error running SQL: {}", sql)
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            if isinstance(e, dbt_common.exceptions.DbtRuntimeError):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise dbt_common.exceptions.DbtRuntimeError(e) from e

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        code = "SUCCESS"
        num_rows = 0

        if cursor is not None and cursor.rowcount is not None:
            num_rows = cursor.rowcount

        # There's no real way to get the status from the
        # mysql-connector-python driver.
        # So just return the default value.
        return AdapterResponse(
            _message="{} {}".format(code, num_rows), rows_affected=num_rows, code=code
        )

    @classmethod
    def data_type_code_to_name(cls, type_code: Union[int, str]) -> str:
        field_type_values = mysql.connector.constants.FieldType.desc.values()
        mapping = {code: name for (code, name) in field_type_values}
        return mapping[type_code]
