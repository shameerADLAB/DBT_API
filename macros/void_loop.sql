{% macro run_void_merge_loop() %}
{% set dates_query %}
    SELECT reporting_date
    FROM ADLAB_DEV.WORKSPACE.SALES_REPORTING_VIEW
    WHERE reporting_date < DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 day' 
    GROUP BY reporting_date
    ORDER BY reporting_date DESC
    LIMIT 4
{% endset %}

{% set results = run_query(dates_query).columns[0].values() %}

{% for date in results %}
    {{ log("Running MERGE for reporting_date: " ~ date, info=True) }}
    {% set sql = generate_void_merge_sql(date) %}
    {% do run_query(sql) %}
{% endfor %}

{{ void_lost_sales() }}

{% endmacro %}