SELECT CUSTOMERID, 
       FORMAT_TIMESTAMP('%Y-%m',timestamp(TransactionDate)) AS MONTH,
       COUNT(*) AS TRANSACTION_COUNT,
       SUM(AMOUNT) AS TOTAL_SPEND, 
       AVG(AMOUNT) AS AVG_SPEND
FROM   `gcp-test-project-395421.ecommerce.transactions`
GROUP BY CUSTOMERID, MONTH
