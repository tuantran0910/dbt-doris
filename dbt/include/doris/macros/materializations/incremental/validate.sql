{% macro dbt_doris_validate_microbatch_config(config) %}
    {% set required_config_keys = ['unique_key', 'event_time', 'begin', 'batch_size'] %}
    {% for key in required_config_keys %}
        {% if not config.get(key) %}
            {% do exceptions.raise_compiler_error("The 'microbatch' incremental strategy requires the '" ~ key ~ "' configuration to be set.") %}
        {% endif %}
    {% endfor %}
{% endmacro %}

{% macro dbt_doris_validate_get_incremental_strategy(config) %}
    {#-- Find and validate the incremental strategy #}
    {% set strategy = config.get("incremental_strategy") or 'insert_overwrite' %}

    {% set invalid_strategy_msg %}
        Invalid incremental strategy provided: {{ strategy }}
        Expected one of: 'append', 'insert_overwrite', 'microbatch'
    {% endset %}
    {% if strategy not in ['append', 'insert_overwrite', 'microbatch'] %}
        {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
    {% endif %}

    {% if strategy == 'microbatch' %}
        {% do dbt_doris_validate_microbatch_config(config) %}
    {% endif %}

    {% do return(strategy) %}
{% endmacro %}
