--cohort_analysis_query
WITH CustomerFirstPurchase AS (
    SELECT
        `Customer ID`,
        MIN(DATE_TRUNC(DATE(`Order Date`), MONTH)) as first_purchase_month
    FROM
        `mgmt599-emmanuel-opoku-lab1.pipeline_processed_data.superstore_transformed`
    GROUP BY
        `Customer ID`
),
CohortSales AS (
    SELECT
        cfp.first_purchase_month,
        DATE_TRUNC(DATE(t.`Order Date`), MONTH) as purchase_month,
        COUNT(DISTINCT t.`Customer ID`) as monthly_customers,
        SUM(t.Sales) as monthly_revenue
    FROM
        `mgmt599-emmanuel-opoku-lab1.pipeline_processed_data.superstore_transformed` as t
    JOIN
        CustomerFirstPurchase as cfp ON t.`Customer ID` = cfp.`Customer ID`
    GROUP BY
        1, 2
),
CohortMetrics AS (
    SELECT
        first_purchase_month,
        purchase_month,
        EXTRACT(YEAR FROM purchase_month) * 12 + EXTRACT(MONTH FROM purchase_month) - (EXTRACT(YEAR FROM first_purchase_month) * 12 + EXTRACT(MONTH FROM first_purchase_month)) as cohort_age_months,
        monthly_customers,
        monthly_revenue
    FROM
        CohortSales
),
CohortSize AS (
    SELECT
        first_purchase_month,
        COUNT(DISTINCT `Customer ID`) as total_cohort_customers
    FROM
        CustomerFirstPurchase
    GROUP BY
        1
)
SELECT
    cm.first_purchase_month,
    cm.cohort_age_months,
    cs.total_cohort_customers,
    cm.monthly_customers,
    SAFE_DIVIDE(cm.monthly_customers, cs.total_cohort_customers) as retention_rate,
    cm.monthly_revenue,
    SAFE_DIVIDE(cm.monthly_revenue, cm.monthly_customers) as average_purchase_value,
    SAFE_DIVIDE(cm.monthly_revenue, cs.total_cohort_customers) as revenue_per_customer
FROM
    CohortMetrics as cm
JOIN
    CohortSize as cs ON cm.first_purchase_month = cs.first_purchase_month
ORDER BY
    cm.first_purchase_month, cm.cohort_age_months
