#!/usr/bin/env python
# encoding: utf-8
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any
from typing import Generator
from typing import Optional
from typing import Union

import mysql.connector
from dbt_common.exceptions import DbtDatabaseError
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.helper_types import Port
from mashumaro.jsonschema.annotations import Maximum
from mashumaro.jsonschema.annotations import Minimum
from typing_extensions import Annotated

from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.contracts.connection import Connection
from dbt.adapters.contracts.connection import Credentials
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.sql import SQLConnectionManager


logger = AdapterLogger("Doris")


@dataclass
class DorisCredentials(Credentials):
    host: str = "127.0.0.1"
    port: Annotated[Port, Minimum(1), Maximum(65535)] = 9030
    username: str = "root"
    password: str = ""
    database: Optional[str] = None  # type: ignore[assignment]
    schema: Optional[str] = None

    _ALIASES = {
        "uid": "username",
        "user": "username",
        "pwd": "password",
        "pass": "password",
        "dbname": "schema",
    }

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
            self.database = None

    @property
    def type(self):
        return "doris"

    @property
    def unique_field(self) -> Optional[str]:
        return self.schema

    def _connection_keys(self):
        return "host", "port", "user", "schema"

    def __post_init__(self):
        if self.database is not None and self.database != self.schema:
            raise DbtRuntimeError(
                f"    schema: {self.schema} \n"
                f"    database: {self.database} \n"
                f"On Doris, database must be omitted or have the same value as"
                f" schema."
            )


class DorisConnectionManager(SQLConnectionManager):
    TYPE = "doris"

    @contextmanager
    def exception_handler(self, sql: str) -> Generator[Any, None, None]:
        try:
            yield

        except mysql.connector.DatabaseError as e:
            logger.debug(f"Doris database error: {e}, sql: {sql}")

            try:
                self.rollback_if_open()
            except mysql.connector.Error:
                logger.debug("Failed to release connection!")

            raise DbtDatabaseError(str(e).strip()) from e

        except Exception as e:
            logger.debug(f"Error running SQL: {sql}")
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            if isinstance(e, DbtRuntimeError):
                # During a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise e

            raise DbtRuntimeError(str(e)) from e

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open")
            return connection

        credentials = cls.get_credentials(connection.credentials)
        kwargs = {
            "host": credentials.host,
            "port": credentials.port,
            "user": credentials.username,
            "password": credentials.password,
            "buffered": True,
            "charset": "utf8",
            "get_warnings": True,
        }

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

                raise DbtDatabaseError(str(e))

        return connection

    @classmethod
    def cancel(cls, connection: Connection):
        connection.handle.close()

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def get_response(cls, cursor) -> Union[AdapterResponse, str]:
        code = "SUCCESS"
        num_rows = 0

        if cursor is not None and cursor.rowcount is not None:
            num_rows = cursor.rowcount

        # There's no real way to get the status from the
        # mysql-connector-python driver.
        # So just return the default value.
        return AdapterResponse(
            code=code,
            _message=f"{num_rows} rows affected",
            rows_affected=num_rows,
        )

    def begin(self):
        """
        https://doris.apache.org/docs/data-operate/import/import-scenes/load-atomicity/
        Doris's inserting always transaction, ignore it
        """
        pass

    def commit(self):
        """
        https://doris.apache.org/docs/data-operate/import/import-scenes/load-atomicity/
        Doris's inserting always transaction, ignore it
        """
        pass
