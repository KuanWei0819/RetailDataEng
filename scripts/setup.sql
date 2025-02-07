-- Create the SalesDW database
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'SalesDW')
BEGIN
    CREATE DATABASE SalesDW;
END;
GO

-- Switch to the SalesDW database
USE SalesDW;
GO

-- Create staging tables
CREATE TABLE staging_sales (
    transaction_id INT PRIMARY KEY,
    product_id INT,
    customer_id INT,
    amount FLOAT,
    timestamp DATETIME
);

CREATE TABLE staging_products (
    product_id INT PRIMARY KEY,
    product_name NVARCHAR(100),
    category NVARCHAR(50),
    price FLOAT
);

CREATE TABLE staging_customers (
    customer_id INT PRIMARY KEY,
    name NVARCHAR(100),
    region NVARCHAR(50),
    signup_date DATE
);

-- Create fact and dimension tables
CREATE TABLE DimProducts (
    product_id INT PRIMARY KEY,
    product_name NVARCHAR(100),
    category NVARCHAR(50),
    price FLOAT
);

CREATE TABLE DimCustomers (
    customer_id INT PRIMARY KEY,
    name NVARCHAR(100),
    region NVARCHAR(50),
    signup_date DATE,
    valid_from DATE,
    valid_to DATE
);

CREATE TABLE FactSales (
    transaction_id INT PRIMARY KEY,
    product_id INT,
    customer_id INT,
    amount FLOAT,
    timestamp DATETIME
);
