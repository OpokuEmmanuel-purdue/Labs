
--Daily sales aggregation
SELECT
        DATE(`Order Date`) as order_date,
        SUM(Sales) as daily_sales
    FROM
        `mgmt599-emmanuel-opoku-lab1.pipeline_processed_data.superstore_transformed`
    GROUP BY 1
    ORDER BY 1;

--Weekly sales aggregation
  SELECT
        DATE_TRUNC(DATE(`Order Date`), WEEK) as order_week,
        SUM(Sales) as weekly_sales
    FROM
        `mgmt599-emmanuel-opoku-lab1.pipeline_processed_data.superstore_transformed`
    GROUP BY 1
    ORDER BY 1;

-- Monthly sales aggregation
    SELECT
        DATE_TRUNC(DATE(`Order Date`), MONTH) as order_month,
        SUM(Sales) as monthly_sales
    FROM
        `mgmt599-emmanuel-opoku-lab1.pipeline_processed_data.superstore_transformed`
    GROUP BY 1
    ORDER BY 1;



--Daily sales with moving averages and anomalies
WITH DailySales AS (
        SELECT
            DATE(`Order Date`) as order_date,
            SUM(Sales) as daily_sales
        FROM
            `mgmt599-emmanuel-opoku-lab1.pipeline_processed_data.superstore_transformed`
        GROUP BY 1
    ),
    MovingAverages AS (
        SELECT
            order_date,
            daily_sales,
            AVG(daily_sales) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as moving_avg_7day,
            AVG(daily_sales) OVER (ORDER BY order_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as moving_avg_30day,
            AVG(daily_sales) OVER (ORDER BY order_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) - 2 * STDDEV(daily_sales) OVER (ORDER BY order_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as lower_bound,
            AVG(daily_sales) OVER (ORDER BY order_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) + 2 * STDDEV(daily_sales) OVER (ORDER BY order_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as upper_bound
        FROM DailySales
    )
    SELECT
        order_date,
        daily_sales,
        moving_avg_7day,
        moving_avg_30day,
        CASE
            WHEN daily_sales < lower_bound OR daily_sales > upper_bound THEN 'Anomaly'
            ELSE 'Normal'
        END as anomaly_flag
    FROM MovingAverages
    ORDER BY order_date