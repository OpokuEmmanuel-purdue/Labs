
# Lab 2 Summary: Pipeline Analytics

## Pipeline Configuration
- Pipeline Name: mgmt599-emmanuel-pipeline
- Source: gs://mgmt599-emmanuel-opoku-data-lake/pipeline_input/Sample - Superstore.csv
- Destination: = 
- Schedule: 6Am Daily

## Key Technical Achievements
1. Successfully built Dataflow pipeline processing 9994 records
2. Reduced processing time from 337ms to 264ms
3. Created 3 analytical views for reporting

## Business Insights from Pipeline Data
1. The time series analysis provided insights into the daily, weekly, and monthly sales trends. The moving averages helped smooth out daily fluctuations, 
and the anomaly detection flagged days with unusually high or low sales, which could warrant further investigation.

2.The cohort analysis allows tracking the behavior of customer groups acquired at different times. Metrics like retention rate and revenue per customer by cohort age provide insights into customer loyalty
 and the long-term value of different customer cohorts. The cohort analysis DataFrame has 894 rows.

3. The product association analysis identified pairs of products frequently bought together, along with metrics like support, confidence, and lift. 
These insights can be valuable for merchandising, recommendations, and promotional strategies. 
The `pair_count` column indicates how many times each pair appeared together in an order. For the displayed top 20 pairs, the `pair_count` was 2.

## DIVE Analysis Summary
- Question: "What insights emerge from cleaned/transformed data?".
- Key Finding: The processed data immediately reveals aggregated patterns like sales trends over time (daily, weekly, monthly), 
  customer behavior grouped by acquisition time (cohorts), and product relationships across many orders.
- Business Impact: Having cleaned and transformed data readily available from a pipeline should shift business operations from being reactive to proactive 
  and data-driven. Decisions can be based on current trends and insights rather than historical data or intuition alone. This can impact areas like inventory 
  management, marketing campaign timing, sales strategy adjustments, and customer service interventions.

## Cost Analysis
- Dataflow job: ~$0.0524288 per run
- BigQuery storage: ~$0.01048576GB per month
- Total within university credits: Yes

## Challenges & Solutions
1. Challenge:
    Character Encoding: The source file was not standard UTF-8. This is a common issue when dealing with files from different systems.
    Data-Type Mismatches: The raw CSV data had dates and numbers stored as strings.
    Error Handling: The source data contained malformed rows.
2. Solution: [how you solved it]
    Leveraged a custom Latin1Coder to read the data without errors
    Explicit conversion to DATE, INTEGER, and FLOAT was necessary to match the BigQuery schema and enable proper analysis.
    Implementing a try-except block was essential to gracefully skip these rows and log them, rather than letting the entire pipeline fail.

## Next Steps
- Additional pipelines to build:
  Integrate data from other sources: Combine sales data with marketing campaign data, website traffic data, customer support interactions, or external market data for a more holistic view.
  Generate more advanced features: Create pipelines to calculate customer lifetime value (CLTV) estimates, predict customer churn risk, or forecast demand at a more granular level (e.g., by product or region).
  Automate reporting and dashboards: Create pipelines that feed directly into business intelligence tools for automated, up-to-date dashboards on key metrics.
  Enable personalized recommendations: Develop pipelines that power real-time personalized product recommendations for customers based on their browsing and purchase history.

- Optimizations identified:
  Adjusting inventory levels based on daily or weekly sales trends and forecasts.
  Triggering targeted marketing campaigns based on customer cohort behavior or recent purchases (identified through product association).
  Responding quickly to sales anomalies, investigating potential issues (like a sudden drop) or capitalizing on opportunities (like a sudden spike).
  Optimizing product placement or recommendations on a website or in a store based on frequently bought together items.
