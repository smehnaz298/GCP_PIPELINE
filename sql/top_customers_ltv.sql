WITH CUSTOMER_LTV AS 
(SELECT CUSTOMERID,
SUM(AMOUNT) AS LIFETIME_VALUE
FROM  `gcp-test-project-395421.ecommerce.transactions`
GROUP BY CUSTOMERID
),
RANKED_CUSTOMERS AS 
(SELECT *,
PERCENT_RANK() OVER (ORDER BY LIFETIME_VALUE DESC) AS PERCENTILE
FROM CUSTOMER_LTV)
SELECT CUSTOMERID, 
LIFETIME_VALUE
FROM RANKED_CUSTOMERS
WHERE PERCENTILE <=0.05
