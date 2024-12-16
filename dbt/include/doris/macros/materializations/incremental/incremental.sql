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

{% materialization incremental, adapter='doris', supported_languages=['sql'] %}
    {% set unique_key = config.get('unique_key', validator=validation.any[list]) %}
    {% set full_refresh_mode = (should_full_refresh()) %}
    {% set target_relation = this.incorporate(type='table') %}
    {% set existing_relation = load_relation(this) %}
    {% set tmp_relation = make_temp_relation(this) %}

    {# Modify tmp_relation name. E.g: from `raw`.`sessions__dbt_tmp_20241201 00:00:00+00:00` to `raw`.`sessions__dbt_tmp_20241201_00_00_00_00_00` #}
    {% set new_tmp_relation_name = tmp_relation.identifier | replace(' ', '_') | replace(':', '_') | replace('+', '_') %}
    {% set tmp_relation = tmp_relation.incorporate(path={"identifier": new_tmp_relation_name}) %}

    {% set strategy = dbt_doris_validate_get_incremental_strategy(config) %}
    {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}
    {% set incremental_predicates = [] if config.get('incremental_predicates') is none else config.get('incremental_predicates') %}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}
    {% set to_drop = [] %}

    {#-- Append or no unique key --#}
    {% if not unique_key or strategy == 'append'  %}
        {#-- Create table first --#}
        {% if existing_relation is none  %}
            {% set build_sql = doris__create_table_as(False, target_relation, sql) %}
        {% elif existing_relation.is_view or full_refresh_mode %}
            {#-- Backup table is new table, exchange table backup and old table #}
            {% set backup_identifier = existing_relation.identifier ~ "__dbt_backup" %}
            {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}
            {% do adapter.drop_relation(backup_relation) %}
            {% set run_sql = doris__create_table_as(False, backup_relation, sql) %}
            {% call statement("run_sql") %}
                {{ run_sql }}
            {% endcall %}
            {% do exchange_relation(target_relation, backup_relation, True) %}
            {% set build_sql = "select 'hello doris'" %}
        {% else %}
            {#-- Append data --#}
            {% do to_drop.append(tmp_relation) %}
            {% do run_query(create_table_as(True, tmp_relation, sql)) %}
            {% set build_sql = tmp_insert(tmp_relation, target_relation, unique_key=none) %}
        {% endif %}
    {#-- Insert overwrite --#}
    {% elif strategy == 'insert_overwrite' %}
        {#-- Create table first --#}
        {% if existing_relation is none  %}
            {% set build_sql = doris__create_unique_table_as(False, target_relation, sql) %}
        {#-- Insert data refresh --#}
        {% elif existing_relation.is_view or full_refresh_mode %}
            {#-- Backup table is new table ,exchange table backup and old table #}
            {% set backup_identifier = existing_relation.identifier ~ "__dbt_backup" %}
            {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}
            {% do adapter.drop_relation(backup_relation) %} {#-- likes 'drop table if exists ... ' --#}
            {% set run_sql = doris__create_unique_table_as(False, backup_relation, sql) %}
            {% call statement("run_sql") %}
                {{ run_sql }}
            {% endcall %}
            {% do exchange_relation(target_relation, backup_relation, True) %}
            {% set build_sql = "select 'hello doris'" %}
        {% else %}
            {#-- Append data --#}
            {#-- Check doris unique table  --#}
            {% if not is_unique_model(target_relation) %}
                {% do exceptions.raise_compiler_error("doris table:"~ target_relation ~ ", model must be 'UNIQUE'" ) %}
            {% endif %}
            {#-- Create temp duplicate table for this incremental task  --#}
            {% do run_query(create_table_as(True, tmp_relation, sql)) %}
            {% do to_drop.append(tmp_relation) %}
            {% do adapter.expand_target_column_types(
                from_relation=tmp_relation,
                to_relation=target_relation
            ) %}
            {% set build_sql = tmp_insert(tmp_relation, target_relation, unique_key=unique_key) %}
        {% endif %}
    {#-- Microbatch --#}
    {% elif strategy == 'microbatch' %}
        {#-- Create table first --#}
        {% if existing_relation is none %}
            {% set run_sql = doris__create_unique_table_as(False, target_relation, sql) %}
            {% call statement("run_sql") %}
                {{ run_sql }}
            {% endcall %}
        {% endif %}

        {#-- Check doris unique table  --#}
        {% if not is_unique_model(target_relation) %}
            {% do exceptions.raise_compiler_error("doris table:"~ target_relation ~ ", model must be 'UNIQUE'" ) %}
        {% endif %}

        {% set event_time = model.config.event_time %}
        {% set start_time = model.config.get("__dbt_internal_microbatch_event_time_start") %}
        {% set end_time = model.config.get("__dbt_internal_microbatch_event_time_end") %}
        {% if start_time %}
            {% do incremental_predicates.append(event_time ~ " >= '" ~ start_time ~ "'") %}
        {% endif %}
        {% if end_time %}
            {% do incremental_predicates.append(event_time ~ " < '" ~ end_time ~ "'") %}
        {% endif %}

        {#-- Create temp duplicate table for this incremental task  --#}
        {% do run_query(create_table_as(True, tmp_relation, sql)) %}
        {% do to_drop.append(tmp_relation) %}
        {% do adapter.expand_target_column_types(
            from_relation=tmp_relation,
            to_relation=target_relation
        ) %}
        {% set build_sql = tmp_insert(tmp_relation, target_relation, unique_key=unique_key, predicates=incremental_predicates) %}
    {% else %}
        {#-- Never  --#}
    {% endif %}

    {% call statement("main") %}
        {{ build_sql }}
    {% endcall %}

    {% do persist_docs(target_relation, model) %}
    {{ run_hooks(post_hooks, inside_transaction=True) }}
    {% do adapter.commit() %}
    {% for rel in to_drop %}
        {% do doris__drop_relation(rel) %}
    {% endfor %}
    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
