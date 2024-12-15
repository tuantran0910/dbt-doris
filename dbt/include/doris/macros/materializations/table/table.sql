-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements. See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership. The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License. You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied. See the License for the
-- specific language governing permissions and limitations
-- under the License.

{% materialization table, adapter='doris' %}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') %}
  {%- set intermediate_relation =  make_intermediate_relation(target_relation) -%}
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}

  -- Grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}

  -- Drop the temp relations if they exist already in the database
  {{ doris__drop_relation(preexisting_intermediate_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- Build model
  {% call statement('main') -%}
    {{ get_create_table_as_sql(False, intermediate_relation, sql) }}
  {%- endcall %}

  -- Cleanup
  {% if existing_relation -%}
    {% do exchange_relation(target_relation, intermediate_relation, True) %}
  {% else %}
    {{ adapter.rename_relation(intermediate_relation, target_relation) }}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  -- Alter relation comment
  {% do persist_docs(target_relation, model) %}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  -- Finally, drop the existing/backup relation after the commit
  {{ doris__drop_relation(intermediate_relation) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
