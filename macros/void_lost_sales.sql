{% macro void_lost_sales() %}

{% set merge_query %}
    MERGE INTO ADLAB_DEV.WORKSPACE.VOID_HISTORICAL_LOST_SALES target
    USING (
        WITH AVG_SALES_CHAIN AS (
            SELECT 
                CHAIN,
                REPORTING_DATE,
                ITEM_CODE,
                SUM(UNIT_SALES) AS TOTAL_SALE,
                COUNT(DISTINCT STORE_ID) AS NO_STORE,
                CASE 
                    WHEN (SUM(UNIT_SALES) / COUNT(DISTINCT STORE_ID)) - FLOOR(SUM(UNIT_SALES) / COUNT(DISTINCT STORE_ID)) < 0.5 
                    THEN FLOOR(SUM(UNIT_SALES) / COUNT(DISTINCT STORE_ID))
                    ELSE CEIL(SUM(UNIT_SALES) / COUNT(DISTINCT STORE_ID))
                END AS EXPECTED_LOST_QUANTITY
            FROM ADLAB_DEV.WORKSPACE.SALES_REPORTING_VIEW
            WHERE UNIT_SALES > 0
            GROUP BY 1, 2, 3
        ),
        VOID AS (
            SELECT *, DAYS_SINCE_LAST_SOLD / 7 AS WEEKS_SINCE_LAST_SOLD 
            FROM ADLAB_DEV.WORKSPACE.VOID_HISTORICAL_LATESTFILE
            WHERE "4-WEEK-VOID" = 1 OR "4-WEEK-VOID" = 0
        ),
        VOID_WITH_SALES AS (
            SELECT 
                VOID.*,
                AVG_SALES.EXPECTED_LOST_QUANTITY
            FROM VOID
            INNER JOIN AVG_SALES_CHAIN AVG_SALES
                ON VOID.RETAIL_CHAIN_NAME = AVG_SALES.CHAIN
                AND VOID.POPPI_ITEM_ID = AVG_SALES.ITEM_CODE
                AND VOID.AS_OF_DATE = AVG_SALES.REPORTING_DATE
        ),
        FILTERED AS (
            SELECT *
            FROM VOID_WITH_SALES
        ),
        ORDERED AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY RETAIL_CHAIN_NAME, RETAIL_STORE_ID, POPPI_ITEM_ID ORDER BY AS_OF_DATE) AS RN
            FROM FILTERED
        ),
        RECURSIVE_COMM_SALE AS (
            SELECT 
                o.*,
                CASE WHEN "4-WEEK-VOID" = 1 THEN EXPECTED_LOST_QUANTITY ELSE 0 END AS COMM_LOST_QUANTITY
            FROM ORDERED o
            WHERE RN = 1
            
            UNION ALL
            
            SELECT 
                curr.*,
                CASE 
                    WHEN curr."4-WEEK-VOID" = 1 THEN prev.COMM_LOST_QUANTITY + curr.EXPECTED_LOST_QUANTITY
                    ELSE 0
                END AS COMM_LOST_QUANTITY
            FROM ORDERED curr
            JOIN RECURSIVE_COMM_SALE prev
              ON curr.RN = prev.RN + 1
              AND curr.RETAIL_CHAIN_NAME = prev.RETAIL_CHAIN_NAME
              AND curr.RETAIL_STORE_ID = prev.RETAIL_STORE_ID
              AND curr.POPPI_ITEM_ID = prev.POPPI_ITEM_ID
        )
        SELECT 
            R.AS_OF_DATE,
            R.RETAIL_CHAIN_NAME,
            R.RETAIL_STORE_ID,
            R.STORE_ZIP_CD,
            R.STORE_ITEM_ID,
            R.POPPI_ITEM_ID,
            P.ITEM_CODE,
            R.PLANOGRAM_START_DATE,
            R.DIVISION_NUMBER,
            R.STORE_NUMBER,
            R.ADDRESS,
            R."1-WEEK-VOID",
            R."2-WEEK-VOID",
            R."3-WEEK-VOID",
            R."4-WEEK-VOID",
            R.DAYS_SINCE_LAST_SOLD,
            R.WEEKS_SINCE_LAST_SOLD,
            R.EXPECTED_LOST_QUANTITY,
            ROUND((R.EXPECTED_LOST_QUANTITY * P.CASE_EQUIVALENT_144OZ) * 12.56, 2) AS EXPECTED_DOLLAR_LOST_SALES,
            ROUND(R."2-WEEK-VOID" * (R.EXPECTED_LOST_QUANTITY * P.CASE_EQUIVALENT_144OZ * 12.56), 2) AS "2-WEEK-LOST-SALES",
            ROUND(R."4-WEEK-VOID" * (R.EXPECTED_LOST_QUANTITY * P.CASE_EQUIVALENT_144OZ * 12.56), 2) AS "4-WEEK-LOST-SALES",
            R.COMM_LOST_QUANTITY,
            ROUND((R.COMM_LOST_QUANTITY*P.CASE_EQUIVALENT_144OZ)*12.56,2) AS COMM_DOLLARS_LOST_SALES
        FROM RECURSIVE_COMM_SALE R
        LEFT JOIN POPPI_BI.CORE.DIM_SCANNED_PRODUCTS P 
            ON R.POPPI_ITEM_ID = P.ITEM_CODE
    ) source
    ON target.AS_OF_DATE = source.AS_OF_DATE
       AND target.RETAIL_CHAIN_NAME = source.RETAIL_CHAIN_NAME
       AND target.RETAIL_STORE_ID = source.RETAIL_STORE_ID
       AND target.POPPI_ITEM_ID = source.POPPI_ITEM_ID
    WHEN MATCHED THEN
        UPDATE SET
            target.STORE_ZIP_CD = source.STORE_ZIP_CD,
            target.STORE_ITEM_ID = source.STORE_ITEM_ID,
            target.ITEM_CODE = source.ITEM_CODE,
            target.PLANOGRAM_START_DATE = source.PLANOGRAM_START_DATE,
            target.DIVISION_NUMBER = source.DIVISION_NUMBER,
            target.STORE_NUMBER = source.STORE_NUMBER,
            target.ADDRESS = source.ADDRESS,
            target."1-WEEK-VOID" = source."1-WEEK-VOID",
            target."2-WEEK-VOID" = source."2-WEEK-VOID",
            target."3-WEEK-VOID" = source."3-WEEK-VOID",
            target."4-WEEK-VOID" = source."4-WEEK-VOID",
            target.DAYS_SINCE_LAST_SOLD = source.DAYS_SINCE_LAST_SOLD,
            target.WEEKS_SINCE_LAST_SOLD = source.WEEKS_SINCE_LAST_SOLD,
            target.EXPECTED_LOST_QUANTITY = source.EXPECTED_LOST_QUANTITY,
            target.EXPECTED_DOLLAR_LOST_SALES = source.EXPECTED_DOLLAR_LOST_SALES,
            target."2-WEEK-LOST-SALES" = source."2-WEEK-LOST-SALES",
            target."4-WEEK-LOST-SALES" = source."4-WEEK-LOST-SALES",
            target.COMM_LOST_QUANTITY = source.COMM_LOST_QUANTITY,
            target.COMM_DOLLARS_LOST_SALES = source.COMM_DOLLARS_LOST_SALES
    WHEN NOT MATCHED THEN
        INSERT (
            AS_OF_DATE, RETAIL_CHAIN_NAME, RETAIL_STORE_ID, STORE_ZIP_CD, STORE_ITEM_ID, 
            POPPI_ITEM_ID, ITEM_CODE, PLANOGRAM_START_DATE, DIVISION_NUMBER, STORE_NUMBER, 
            ADDRESS, "1-WEEK-VOID", "2-WEEK-VOID", "3-WEEK-VOID", "4-WEEK-VOID", 
            DAYS_SINCE_LAST_SOLD, WEEKS_SINCE_LAST_SOLD, EXPECTED_LOST_QUANTITY, 
            EXPECTED_DOLLAR_LOST_SALES, "2-WEEK-LOST-SALES", "4-WEEK-LOST-SALES",
            COMM_LOST_QUANTITY, COMM_DOLLARS_LOST_SALES
        )
        VALUES (
            source.AS_OF_DATE, source.RETAIL_CHAIN_NAME, source.RETAIL_STORE_ID, source.STORE_ZIP_CD, 
            source.STORE_ITEM_ID, source.POPPI_ITEM_ID, source.ITEM_CODE, source.PLANOGRAM_START_DATE, 
            source.DIVISION_NUMBER, source.STORE_NUMBER, source.ADDRESS, source."1-WEEK-VOID", 
            source."2-WEEK-VOID", source."3-WEEK-VOID", source."4-WEEK-VOID", source.DAYS_SINCE_LAST_SOLD, 
            source.WEEKS_SINCE_LAST_SOLD, source.EXPECTED_LOST_QUANTITY, source.EXPECTED_DOLLAR_LOST_SALES,
            source."2-WEEK-LOST-SALES", source."4-WEEK-LOST-SALES", source.COMM_LOST_QUANTITY,
            source.COMM_DOLLARS_LOST_SALES
        )
{% endset %}

{% do run_query(merge_query) %}

{% endmacro %}