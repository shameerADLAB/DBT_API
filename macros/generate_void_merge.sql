{% macro generate_void_merge_sql(reporting_date) %}
{% set sql %}
MERGE INTO ADLAB_DEV.WORKSPACE.VOID_HISTORICAL_LATESTFILE T
USING (
    WITH LatestPlanoStartDate AS (
        SELECT
            FORMATTED_CHAIN_NAME AS RETAIL_CHAIN_NAME,
            MAX(PLANOGRAM_START_DATE) AS max_start_date
        FROM ADLAB_DEV.WORKSPACE.CONSOLIDATED_PLANOGRAM
        WHERE PLANOGRAM_START_DATE <= DATE('{{ reporting_date }}')
        GROUP BY FORMATTED_CHAIN_NAME
    ),
    sales AS (
        SELECT DISTINCT
            sales.STORE_ITEM_ID,
            sales.STORE_ID,
            sales.CHAIN AS RETAIL_CHAIN_NAME
        FROM ADLAB_DEV.WORKSPACE.SALES_REPORTING_VIEW sales
        WHERE sales.SALE_DATE BETWEEN DATEADD(DAY, -27, DATE('{{ reporting_date }}'))
                                  AND DATE('{{ reporting_date }}')
          AND sales.DOLLAR_SALES > 0
    ),
    sales_last_sold AS (
        SELECT
            sales.STORE_ID,
            sales.CHAIN AS RETAIL_CHAIN_NAME,
            sales.STORE_ITEM_ID,
            MAX(sales.SALE_DATE) AS last_sold_date
        FROM ADLAB_DEV.WORKSPACE.SALES_REPORTING_VIEW sales
        WHERE sales.DOLLAR_SALES > 0
          AND sales.SALE_DATE <= DATE('{{ reporting_date }}')
        GROUP BY sales.STORE_ID, sales.CHAIN, sales.STORE_ITEM_ID
    ),
    rt_chain AS (
        SELECT DISTINCT CHAIN 
        FROM ADLAB_DEV.WORKSPACE.SALES_REPORTING_VIEW sales
        WHERE sales.reporting_date = DATE('{{ reporting_date }}')
          AND sales.DOLLAR_SALES > 0
    ),
    planogram AS (
        SELECT  FORMATTED_CHAIN_NAME,
                PLANOGRAM_START_DATE,
                FORMATTED_CHAIN_DIVISION_CODE,
                FORMATTED_CHAIN_STORE_NUMBER,
                ITEM_CODE,
                STORE_ID_WITHIN_CHAIN,
                STORE_ID,
                STORE_ITEM_ID
        FROM ADLAB_DEV.WORKSPACE.CONSOLIDATED_PLANOGRAM pog
        INNER JOIN LatestPlanoStartDate pog_start
            ON pog.FORMATTED_CHAIN_NAME = pog_start.RETAIL_CHAIN_NAME
            AND pog.PLANOGRAM_START_DATE = pog_start.max_start_date
        INNER JOIN rt_chain
            ON pog.FORMATTED_CHAIN_NAME = rt_chain.CHAIN
    ),
    void_sales AS (
        SELECT
            plano.FORMATTED_CHAIN_NAME AS RETAIL_CHAIN_NAME,
            plano.STORE_ID,
            plano.STORE_ITEM_ID,
            plano.ITEM_CODE,
            plano.PLANOGRAM_START_DATE,
            plano.FORMATTED_CHAIN_DIVISION_CODE,
            plano.FORMATTED_CHAIN_STORE_NUMBER,
            COALESCE(DATEDIFF(DAY, sales_last_sold.last_sold_date, DATE('{{ reporting_date }}')), 100) AS days_since_last_sold,
            CASE 
                WHEN sales_last_sold.RETAIL_CHAIN_NAME = 'ABSCO' THEN 0 
                WHEN COALESCE(DATEDIFF(DAY, sales_last_sold.last_sold_date, DATE('{{ reporting_date }}')), 100) >= 7 THEN 1 
                ELSE 0 
            END AS one_week_void,
            CASE WHEN COALESCE(DATEDIFF(DAY, sales_last_sold.last_sold_date, DATE('{{ reporting_date }}')), 100) >= 14 THEN 1 ELSE 0 END AS two_week_void,
            CASE WHEN COALESCE(DATEDIFF(DAY, sales_last_sold.last_sold_date, DATE('{{ reporting_date }}')), 100) >= 21 THEN 1 ELSE 0 END AS three_week_void,
            DATE('{{ reporting_date }}') AS as_of_date,
            CASE
                WHEN sales.STORE_ITEM_ID IS NULL THEN 1
                ELSE 0
            END AS four_week_void
        FROM planogram plano
        LEFT JOIN sales ON plano.STORE_ITEM_ID = sales.STORE_ITEM_ID
        LEFT JOIN sales_last_sold ON plano.STORE_ITEM_ID = sales_last_sold.STORE_ITEM_ID
    )
    SELECT
        void.as_of_date,
        void.RETAIL_CHAIN_NAME,
        void.STORE_ID,
        sales.STORE_ZIP,
        void.STORE_ITEM_ID,
        void.ITEM_CODE AS poppi_item_id,
        void.PLANOGRAM_START_DATE,
        void.FORMATTED_CHAIN_DIVISION_CODE,
        void.FORMATTED_CHAIN_STORE_NUMBER,
        NULL AS flavour_type,
        sales.STORE_ADDRESS AS address,
        void.one_week_void,
        void.two_week_void,
        void.three_week_void,
        void.four_week_void,
        void.days_since_last_sold
    FROM void_sales void
    LEFT JOIN ADLAB_DEV.WORKSPACE.SALES_REPORTING_VIEW sales
        ON void.STORE_ITEM_ID = sales.STORE_ITEM_ID
    QUALIFY ROW_NUMBER() OVER (PARTITION BY void.STORE_ITEM_ID ORDER BY void.as_of_date DESC) = 1
) S
ON DATE(T.AS_OF_DATE) = DATE(S.AS_OF_DATE)
AND T.STORE_ITEM_ID = S.STORE_ITEM_ID
WHEN MATCHED THEN
    UPDATE SET
        T.AS_OF_DATE = S.AS_OF_DATE,
        T.RETAIL_CHAIN_NAME = S.RETAIL_CHAIN_NAME,
        T.RETAIL_STORE_ID = S.STORE_ID,
        T.STORE_ZIP_CD = S.STORE_ZIP,
        T.STORE_ITEM_ID = S.STORE_ITEM_ID,
        T.POPPI_ITEM_ID = S.poppi_item_id,
        T.PLANOGRAM_START_DATE = S.PLANOGRAM_START_DATE,
        T.DIVISION_NUMBER = S.FORMATTED_CHAIN_DIVISION_CODE,
        T.STORE_NUMBER = S.FORMATTED_CHAIN_STORE_NUMBER,
        T.ADDRESS = S.address,
        T."1-WEEK-VOID" = S.one_week_void,
        T."2-WEEK-VOID" = S.two_week_void,
        T."3-WEEK-VOID" = S.three_week_void,
        T."4-WEEK-VOID" = S.four_week_void,
        T.DAYS_SINCE_LAST_SOLD = S.days_since_last_sold,
        T.UPDATE_DTTM = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        AS_OF_DATE, RETAIL_CHAIN_NAME, RETAIL_STORE_ID, STORE_ZIP_CD,
        STORE_ITEM_ID, POPPI_ITEM_ID, PLANOGRAM_START_DATE, DIVISION_NUMBER,
        STORE_NUMBER, ADDRESS, "1-WEEK-VOID", "2-WEEK-VOID", "3-WEEK-VOID", 
        "4-WEEK-VOID", DAYS_SINCE_LAST_SOLD, CREATE_DTTM, UPDATE_DTTM
    )
    VALUES (
        S.AS_OF_DATE, S.RETAIL_CHAIN_NAME, S.STORE_ID, S.STORE_ZIP,
        S.STORE_ITEM_ID, S.poppi_item_id, S.PLANOGRAM_START_DATE, 
        S.FORMATTED_CHAIN_DIVISION_CODE, S.FORMATTED_CHAIN_STORE_NUMBER, 
        S.address, S.one_week_void, S.two_week_void, S.three_week_void,
        S.four_week_void, S.days_since_last_sold, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    )
{% endset %}
{{ return(sql) }}
{% endmacro %}