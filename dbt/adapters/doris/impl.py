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
from concurrent.futures import Future
from enum import Enum
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple

import agate
from dbt_common.clients.agate_helper import table_from_rows
from dbt_common.contracts.constraints import ConstraintType
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.utils import executor

import dbt.exceptions
from dbt.adapters.base.impl import _expect_row_value
from dbt.adapters.base.impl import catch_as_completed
from dbt.adapters.base.impl import ConstraintSupport
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.base.relation import InformationSchema
from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.doris.column import DorisColumn
from dbt.adapters.doris.connections import DorisConnectionManager
from dbt.adapters.doris.doris_column_item import DorisColumnItem
from dbt.adapters.doris.relation import DorisRelation
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.protocol import AdapterConfig
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.sql.impl import LIST_RELATIONS_MACRO_NAME
from dbt.adapters.sql.impl import LIST_SCHEMAS_MACRO_NAME
from dbt.contracts.graph.manifest import Manifest


logger = AdapterLogger("Doris")


class Engine(str, Enum):
    olap = "olap"
    mysql = "mysql"
    elasticsearch = "elasticsearch"
    hive = "hive"
    iceberg = "iceberg"


class PartitionType(str, Enum):
    list = "LIST"
    range = "RANGE"


class DorisConfig(AdapterConfig):
    engine: Engine
    duplicate_key: Tuple[str]
    partition_by: Tuple[str]
    partition_type: PartitionType
    partition_by_init: List[str]
    distributed_by: Tuple[str]
    buckets: int
    properties: Dict[str, str]


class DorisAdapter(SQLAdapter):
    ConnectionManager = DorisConnectionManager
    Relation = DorisRelation
    AdapterSpecificConfigs = DorisConfig
    Column = DorisColumn

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.ENFORCED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.ENFORCED,
        ConstraintType.primary_key: ConstraintSupport.ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_SUPPORTED,
    }

    @classmethod
    def date_function(cls) -> str:
        return "current_date()"

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "datetime"

    @classmethod
    def quote(cls, identifier: str) -> str:
        return "`{}`".format(identifier)

    def list_relations_without_caching(
        self, schema_relation: DorisRelation
    ) -> List[DorisRelation]:
        kwargs = {"schema_relation": schema_relation}
        try:
            results = self.execute_macro(LIST_RELATIONS_MACRO_NAME, kwargs=kwargs)
        except DbtRuntimeError as e:
            raise DbtRuntimeError(
                f"Error while retrieving information about relations in {schema_relation}: {str(e)}"
            ) from e

        relations = []
        for row in results:
            if len(row) != 4:
                raise dbt.exceptions.DbtRuntimeError(
                    f"Invalid value from 'show table extended ...', "
                    f"got {len(row)} values, expected 4"
                )
            _, name, schema, type_info = row
            relation_type = (
                RelationType.View if "view" in type_info else RelationType.Table
            )
            relation = self.Relation.create(
                database=None,
                schema=schema,
                identifier=name,
                type=relation_type,
            )
            relations.append(relation)

        return relations

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "string"

    def check_schema_exists(self, database, schema):
        results = self.execute_macro(
            LIST_SCHEMAS_MACRO_NAME, kwargs={"database": database}
        )

        exists = True if schema in [row[0] for row in results] else False
        return exists

    def get_relation(self, database: Optional[str], schema: str, identifier: str):
        if not self.Relation.get_default_include_policy().database:
            database = None

        return super().get_relation(database, schema, identifier)

    def drop_schema(self, relation: BaseRelation):
        relations = self.list_relations(
            database=relation.database, schema=relation.schema
        )
        for relation in relations:
            self.drop_relation(relation)
        super().drop_schema(relation)

    def get_catalog(self, manifest):
        schema_map = self._get_catalog_schemas(manifest)

        with executor(self.config) as tpe:
            futures: List[Future[agate.Table]] = []
            for info, schemas in schema_map.items():
                for schema in schemas:
                    futures.append(
                        tpe.submit_connected(
                            self,
                            schema,
                            self._get_one_catalog,
                            info,
                            [schema],
                            manifest,
                        )
                    )
            catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions

    @classmethod
    def _catalog_filter_schemas(cls, manifest: Manifest) -> Callable[[agate.Row], bool]:
        schemas = frozenset((None, s.lower()) for d, s in manifest.get_used_schemas())

        def _(row: agate.Row) -> bool:
            table_database = _expect_row_value("table_database", row)
            table_schema = _expect_row_value("table_schema", row)
            if table_schema is None:
                return False
            return (table_database, table_schema.lower()) in schemas

        return _

    @classmethod
    def _catalog_filter_table(
        cls, table: agate.Table, manifest: Manifest
    ) -> agate.Table:
        table = table_from_rows(
            table.rows,
            table.column_names,
            text_only_columns=["table_schema", "table_name"],
        )
        return table.where(cls._catalog_filter_schemas(manifest))

    def _get_one_catalog(
        self,
        information_schema: InformationSchema,
        schemas: Set[str],
        manifest: Manifest,
    ) -> agate.Table:
        if len(schemas) != 1:
            dbt.exceptions.raise_compiler_error(
                f"Expected only one schema in Doris _get_one_catalog, found "
                f"{schemas}"
            )

        return super()._get_one_catalog(information_schema, schemas, manifest)

    # Methods used in adapter tests
    def timestamp_add_sql(
        self, add_to: str, number: int = 1, interval: str = "hour"
    ) -> str:
        # for backwards compatibility, we're compelled to set some sort of
        # default. A lot of searching has lead me to believe that the
        # '+ interval' syntax used in Doris/redshift is relatively common
        # and might even be the SQL standard's intention.
        return f"{add_to} + interval {number} {interval}"

    @classmethod
    def render_raw_columns_constraints(
        cls, raw_columns: Dict[str, Dict[str, Any]]
    ) -> List:
        rendered_column_constraints = []
        for v in raw_columns.values():
            cols_name = cls.quote(v["name"]) if v.get("quote") else v["name"]
            data_type = v.get("data_type")
            comment = v.get("description")

            column = DorisColumnItem(cols_name, data_type, comment, "")
            rendered_column_constraints.append(column)

        return rendered_column_constraints

    def valid_incremental_strategies(self):
        """The set of standard builtin strategies which this adapter supports out-of-the-box.
        Not used to validate custom strategies defined by end users.
        """
        return ["append", "insert_overwrite", "microbatch"]
