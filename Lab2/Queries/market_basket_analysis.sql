--market basket analysis
WITH OrderProducts AS (
    SELECT DISTINCT
        `Order ID`,
        `Product Name`,
        Sales
    FROM
        `mgmt599-emmanuel-opoku-lab1.pipeline_processed_data.superstore_transformed`
),
ProductPairs AS (
    SELECT
        op1.`Order ID`,
        op1.`Product Name` as product_a,
        op2.`Product Name` as product_b,
        op1.Sales as sales_a,
        op2.Sales as sales_b
    FROM
        OrderProducts op1
    JOIN
        OrderProducts op2 ON op1.`Order ID` = op2.`Order ID` AND op1.`Product Name` < op2.`Product Name`
),
PairCounts AS (
    SELECT
        product_a,
        product_b,
        COUNT(DISTINCT `Order ID`) as pair_count,
        SUM(sales_a + sales_b) as pair_revenue
    FROM
        ProductPairs
    GROUP BY 1, 2
),
ProductCounts AS (
    SELECT
        `Product Name`,
        COUNT(DISTINCT `Order ID`) as product_count
    FROM
        OrderProducts
    GROUP BY 1
),
TotalOrders AS (
    SELECT
        COUNT(DISTINCT `Order ID`) as total_orders
    FROM
        OrderProducts
)
SELECT
    pc.product_a,
    pc.product_b,
    pc.pair_count,
    pa.product_count as product_a_count,
    pb.product_count as product_b_count,
    t.total_orders, -- Changed alias from 'to' to 't'
    SAFE_DIVIDE(pc.pair_count, t.total_orders) as support, -- Changed alias from 'to' to 't'
    SAFE_DIVIDE(pc.pair_count, pa.product_count) as confidence_a_to_b,
    SAFE_DIVIDE(pc.pair_count, pb.product_count) as confidence_b_to_a,
    SAFE_DIVIDE(SAFE_DIVIDE(pc.pair_count, pa.product_count), SAFE_DIVIDE(pb.product_count, t.total_orders)) as lift_a_to_b, -- Changed alias from 'to' to 't'
    SAFE_DIVIDE(SAFE_DIVIDE(pc.pair_count, pb.product_count), SAFE_DIVIDE(pa.product_count, t.total_orders)) as lift_b_to_a, -- Changed alias from 'to' to 't'
    pc.pair_revenue
FROM
    PairCounts pc
JOIN
    ProductCounts pa ON pc.product_a = pa.`Product Name`
JOIN
    ProductCounts pb ON pc.product_b = pb.`Product Name`
CROSS JOIN
    TotalOrders t -- Changed alias from 'to' to 't'
ORDER BY
    support DESC
LIMIT 20

