CREATE MATERIALIZED VIEW `gcp-test-project-395421.ecommerce.Materialised_Top_Customers_LTV`
PARTITION BY DATE(_PARTITIONTIME)
CLUSTER BY CustomerID
AS
WITH CUSTOMER_LTV AS (
  SELECT
    CustomerID,
    SUM(Amount) AS Lifetime_Value
  FROM `gcp-test-project-395421.ecommerce.transactions`
  GROUP BY CustomerID
),
RANKED_CUSTOMERS AS (
  SELECT
    *,
    PERCENT_RANK() OVER (ORDER BY Lifetime_Value DESC) AS Percentile
  FROM CUSTOMER_LTV
)
SELECT
  CustomerID,
  Lifetime_Value
FROM RANKED_CUSTOMERS
WHERE Percentile <= 0.05;

