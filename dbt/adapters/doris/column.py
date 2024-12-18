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
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import Optional
from typing import TypeVar

from dbt.adapters.base.column import Column

Self = TypeVar("Self", bound="DorisColumn")


@dataclass
class DorisColumn(Column):
    TYPE_LABELS = {
        "TIMESTAMP": "DATETIME",
        "BOOL": "BOOLEAN",
    }
    table_database: Optional[str] = None
    table_schema: Optional[str] = None
    table_name: Optional[str] = None
    table_type: Optional[str] = None
    table_owner: Optional[str] = None
    table_stats: Optional[Dict[str, Any]] = None
    column_index: Optional[int] = None

    @property
    def quoted(self) -> str:
        return "`{}`".format(self.column)

    def __repr__(self) -> str:
        return f"<DorisColumn {self.name} ({self.data_type})>"
