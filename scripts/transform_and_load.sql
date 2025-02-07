USE SalesDW;
GO

-- Step 0: Deduplicate staging tables

-- Deduplicate staging_products
WITH Deduplicated AS (
    SELECT
        product_id,
        product_name,
        category,
        price,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS row_num
    FROM staging_products
)
DELETE FROM staging_products
WHERE product_id IN (
    SELECT product_id FROM Deduplicated WHERE row_num > 1
);
GO

-- Deduplicate staging_customers
WITH Deduplicated AS (
    SELECT
        customer_id,
        name,
        region,
        signup_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id) AS row_num
    FROM staging_customers
)
DELETE FROM staging_customers
WHERE customer_id IN (
    SELECT customer_id FROM Deduplicated WHERE row_num > 1
);
GO

-- Deduplicate staging_sales
WITH Deduplicated AS (
    SELECT
        transaction_id,
        product_id,
        customer_id,
        amount,
        timestamp,
        ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_id) AS row_num
    FROM staging_sales
)
DELETE FROM staging_sales
WHERE transaction_id IN (
    SELECT transaction_id FROM Deduplicated WHERE row_num > 1
);
GO

-- Step 1: Load Data into DimProducts
MERGE DimProducts AS target
USING staging_products AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN
    UPDATE SET 
        target.product_name = source.product_name,
        target.category = source.category,
        target.price = source.price
WHEN NOT MATCHED THEN
    INSERT (product_id, product_name, category, price)
    VALUES (source.product_id, source.product_name, source.category, source.price);
GO

-- Step 2: Load Data into DimCustomers
MERGE DimCustomers AS target
USING staging_customers AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
    UPDATE SET
        target.name = source.name,
        target.region = source.region,
        target.signup_date = source.signup_date
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, region, signup_date, valid_from, valid_to)
    VALUES (source.customer_id, source.name, source.region, source.signup_date, GETDATE(), NULL);
GO

-- Step 3: Load Data into FactSales
INSERT INTO FactSales (transaction_id, product_id, customer_id, amount, timestamp)
SELECT 
    staging_sales.transaction_id,
    staging_sales.product_id,
    staging_sales.customer_id,
    staging_sales.amount,
    staging_sales.timestamp
FROM staging_sales
WHERE NOT EXISTS (
    SELECT 1 FROM FactSales WHERE FactSales.transaction_id = staging_sales.transaction_id
);
GO
