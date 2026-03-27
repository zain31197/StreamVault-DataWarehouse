-- drop database dw_project;
CREATE DATABASE IF NOT EXISTS walmart_warehouse;
drop database walmart_warehouse;
USE walmart_warehouse;


-- Drop tables in correct dependency order (Fact first)
-- DROP TABLE IF EXISTS fact_sales;
-- DROP TABLE IF EXISTS dim_customer;
-- DROP TABLE IF EXISTS dim_product;
-- DROP TABLE IF EXISTS dim_store;
-- DROP TABLE IF EXISTS dim_supplier;
-- DROP TABLE IF EXISTS dim_time;

-- =========================
-- Dimension Tables
-- =========================

-- Customer Dimension
CREATE TABLE dim_customer (
    CustomerID VARCHAR(15) PRIMARY KEY,
    Gender CHAR(1),
    Age VARCHAR(10),
    Occupation VARCHAR(50),
    City_Category VARCHAR(20),
    StayInCurrentCityYears VARCHAR(5),
    MaritalStatus INT
);



-- Product Dimension
CREATE TABLE dim_product (
    ProductID VARCHAR(15) PRIMARY KEY,
    ProductCategory VARCHAR(50),
    Price FLOAT
);

-- Store Dimension
CREATE TABLE dim_store (
    StoreID VARCHAR(10) PRIMARY KEY,
    StoreName VARCHAR(50)
);

-- Supplier Dimension
CREATE TABLE dim_supplier (
    SupplierID VARCHAR(10) PRIMARY KEY,
    SupplierName VARCHAR(50)
);

-- Time Dimension
CREATE TABLE dim_time (
    DateKey DATE PRIMARY KEY,
    DayOfWeek VARCHAR(10),
    Month VARCHAR(10),
    Quarter VARCHAR(5),
    Year INT
);



-- =========================
-- Fact Table
-- =========================




CREATE TABLE fact_sales (
    SalesID INT AUTO_INCREMENT PRIMARY KEY,
    OrderID VARCHAR(20),
    CustomerID VARCHAR(15),
    ProductID VARCHAR(15),
    StoreID VARCHAR(10),
    SupplierID VARCHAR(10),
    DateKey DATE,
    Quantity INT,
    TotalAmount FLOAT,
    FOREIGN KEY (CustomerID) REFERENCES dim_customer(CustomerID),
    FOREIGN KEY (ProductID) REFERENCES dim_product(ProductID),
    FOREIGN KEY (StoreID) REFERENCES dim_store(StoreID),
    FOREIGN KEY (SupplierID) REFERENCES dim_supplier(SupplierID),
    FOREIGN KEY (DateKey) REFERENCES dim_time(DateKey),
    INDEX idx_fs_customer (CustomerID),
    INDEX idx_fs_product (ProductID),
    INDEX idx_fs_store (StoreID),
    INDEX idx_fs_supplier (SupplierID),
    INDEX idx_fs_date (DateKey),
    INDEX idx_fs_order (OrderID)
);



-- Query1

WITH ProductSales AS (
  SELECT
    fs.ProductID,
    dp.ProductCategory,
    dt.Year,
    dt.Month,
    CASE
      WHEN dt.DayOfWeek IN ('Saturday', 'Sunday') THEN 'Weekend'
      ELSE 'Weekday'
    END AS DayType,
    SUM(fs.TotalAmount) AS Revenue
  FROM fact_sales fs
  JOIN dim_time dt ON fs.DateKey = dt.DateKey
  JOIN dim_product dp ON fs.ProductID = dp.ProductID
  -- Specify the year you want to analyze
  WHERE dt.Year = 2017 
  GROUP BY fs.ProductID, dp.ProductCategory, dt.Year, dt.Month, DayType
),
RankedSales AS (
  SELECT
    ProductID,
    ProductCategory,
    Year,
    Month,
    DayType,
    Revenue,
    -- Rank products within each month and day type
    ROW_NUMBER() OVER(
      PARTITION BY Year, Month, DayType
      ORDER BY Revenue DESC
    ) as rn
  FROM ProductSales
)
SELECT
  Year,
  Month,
  DayType,
  ProductID,
  ProductCategory,
  Revenue
FROM RankedSales
WHERE rn <= 5
ORDER BY Year, STR_TO_DATE(CONCAT('01-', Month, '-', Year), '%d-%M-%Y'), DayType, Revenue DESC;



-- Query2

SELECT 
  dc.City_Category,
  dc.Gender,
  dc.Age,
  SUM(fs.TotalAmount) AS Total_Purchase_Amount
FROM fact_sales fs
JOIN dim_customer dc ON fs.CustomerID = dc.CustomerID
GROUP BY dc.City_Category, dc.Gender, dc.Age
ORDER BY dc.City_Category, dc.Gender, dc.Age;


-- Query3

SELECT 
  dp.ProductCategory,
  dc.Occupation,
  SUM(fs.TotalAmount) AS Total_Sales
FROM fact_sales fs
JOIN dim_product dp ON fs.ProductID = dp.ProductID
JOIN dim_customer dc ON fs.CustomerID = dc.CustomerID
GROUP BY dp.ProductCategory, dc.Occupation
ORDER BY dp.ProductCategory, dc.Occupation;

-- Query4

SELECT 
  dc.Gender,
  dc.Age,
  dt.Quarter,
  dt.Year,
  SUM(fs.TotalAmount) AS Total_Purchases
FROM fact_sales fs
JOIN dim_customer dc ON fs.CustomerID = dc.CustomerID
JOIN dim_time dt ON fs.DateKey = dt.DateKey
-- Specify the year you want to analyze
WHERE dt.Year = 2017
GROUP BY dc.Gender, dc.Age, dt.Quarter, dt.Year
ORDER BY dt.Year, dt.Quarter, dc.Gender, dc.Age;


-- Query5


WITH OccupationSales AS (
  SELECT 
    dp.ProductCategory,
    dc.Occupation,
    SUM(fs.TotalAmount) AS Category_Sales,
    -- Rank occupations within each product category
    ROW_NUMBER() OVER (
      PARTITION BY dp.ProductCategory 
      ORDER BY SUM(fs.TotalAmount) DESC
    ) AS rank_num
  FROM fact_sales fs
  JOIN dim_product dp ON fs.ProductID = dp.ProductID
  JOIN dim_customer dc ON fs.CustomerID = dc.CustomerID
  GROUP BY dp.ProductCategory, dc.Occupation
)
SELECT 
  ProductCategory,
  Occupation,
  Category_Sales
FROM OccupationSales 
WHERE rank_num <= 5
ORDER BY ProductCategory, Category_Sales DESC;



-- Query6
SELECT 
  dc.City_Category,
  dc.MaritalStatus,
  dt.Year,
  dt.Month,
  SUM(fs.TotalAmount) AS Purchase_Amount
FROM fact_sales fs
JOIN dim_customer dc ON fs.CustomerID = dc.CustomerID
JOIN dim_time dt ON fs.DateKey = dt.DateKey
-- Specify the 6-month period you want to analyze.
-- Using Jan-June 2017 as an example.
WHERE fs.DateKey BETWEEN '2017-01-01' AND '2017-06-30'
GROUP BY dc.City_Category, dc.MaritalStatus, dt.Year, dt.Month
ORDER BY dc.City_Category, dc.MaritalStatus, dt.Year, STR_TO_DATE(CONCAT('01-', Month, '-', Year), '%d-%M-%Y');

-- Query7
SELECT 
  dc.StayInCurrentCityYears,
  dc.Gender,
  AVG(fs.TotalAmount) AS Avg_Purchase_Amount
FROM fact_sales fs
JOIN dim_customer dc ON fs.CustomerID = dc.CustomerID
GROUP BY dc.StayInCurrentCityYears, dc.Gender
ORDER BY dc.StayInCurrentCityYears, dc.Gender;
-- Query8
WITH CityCategoryRevenue AS (
  SELECT 
    dp.ProductCategory,
    dc.City_Category,
    SUM(fs.TotalAmount) AS Revenue,
    ROW_NUMBER() OVER (
      PARTITION BY dp.ProductCategory
      ORDER BY SUM(fs.TotalAmount) DESC
    ) AS rn
  FROM fact_sales fs
  JOIN dim_product dp ON fs.ProductID = dp.ProductID
  JOIN dim_customer dc ON fs.CustomerID = dc.CustomerID
  GROUP BY dp.ProductCategory, dc.City_Category
)
SELECT 
  ProductCategory,
  City_Category,
  Revenue
FROM CityCategoryRevenue
WHERE rn <= 5
ORDER BY ProductCategory, Revenue DESC;
-- Query9
WITH MonthlySales AS (
  SELECT
    dp.ProductCategory,
    dt.Year,
    dt.Month,
    -- Order by a real date to ensure correct LAG
    STR_TO_DATE(CONCAT('01-', Month, '-', Year), '%d-%M-%Y') as MonthDate,
    SUM(fs.TotalAmount) AS TotalSales
  FROM fact_sales fs
  JOIN dim_time dt ON fs.DateKey = dt.DateKey
  JOIN dim_product dp ON fs.ProductID = dp.ProductID
  -- Specify the year
  WHERE dt.Year = 2017 
  GROUP BY dp.ProductCategory, dt.Year, dt.Month
),
LaggedSales AS (
  SELECT
    ProductCategory,
    Year,
    Month,
    MonthDate,
    TotalSales,
    -- Get the previous month's sales within the same category and year
    LAG(TotalSales) OVER (
      PARTITION BY ProductCategory, Year 
      ORDER BY MonthDate
    ) AS PreviousMonthSales
  FROM MonthlySales
)
SELECT
  ProductCategory,
  Year,
  Month,
  TotalSales,
  PreviousMonthSales,
  -- Calculate growth percentage, handling division by zero
  ROUND(
    (TotalSales - PreviousMonthSales) / PreviousMonthSales * 100,
    2
  ) AS GrowthPercentage
FROM LaggedSales
ORDER BY ProductCategory, Year, MonthDate;
-- Query10
SELECT 
  dc.Age,
  -- Use CASE statements to pivot sales into two columns
  SUM(CASE 
    WHEN dt.DayOfWeek IN ('Saturday', 'Sunday') THEN fs.TotalAmount 
    ELSE 0 
  END) AS WeekendSales,
  SUM(CASE 
    WHEN dt.DayOfWeek NOT IN ('Saturday', 'Sunday') THEN fs.TotalAmount 
    ELSE 0 
  END) AS WeekdaySales
FROM fact_sales fs
JOIN dim_customer dc ON fs.CustomerID = dc.CustomerID
JOIN dim_time dt ON fs.DateKey = dt.DateKey
-- Specify the year
WHERE dt.Year = 2017
GROUP BY dc.Age
ORDER BY dc.Age;
-- Query11
WITH ProductSales AS (
  SELECT
    fs.ProductID,
    dp.ProductCategory,
    dt.Year,
    dt.Month,
    CASE
      WHEN dt.DayOfWeek IN ('Saturday', 'Sunday') THEN 'Weekend'
      ELSE 'Weekday'
    END AS DayType,
    SUM(fs.TotalAmount) AS Revenue
  FROM fact_sales fs
  JOIN dim_time dt ON fs.DateKey = dt.DateKey
  JOIN dim_product dp ON fs.ProductID = dp.ProductID
  -- Specify the year
  WHERE dt.Year = 2017 
  GROUP BY fs.ProductID, dp.ProductCategory, dt.Year, dt.Month, DayType
),
RankedSales AS (
  SELECT
    ProductID,
    ProductCategory,
    Year,
    Month,
    DayType,
    Revenue,
    ROW_NUMBER() OVER(
      PARTITION BY Year, Month, DayType
      ORDER BY Revenue DESC
    ) as rn
  FROM ProductSales
)
SELECT
  Year,
  Month,
  DayType,
  ProductID,
  ProductCategory,
  Revenue
FROM RankedSales
WHERE rn <= 5
ORDER BY Year, STR_TO_DATE(CONCAT('01-', Month, '-', Year), '%d-%M-%Y'), DayType, Revenue DESC;

-- Query12
WITH StoreQuarterRevenue AS (
  SELECT 
    dt.Year,
    dt.Quarter,
    fs.StoreID,
    ds.StoreName,
    SUM(fs.TotalAmount) AS StoreRevenue
  FROM fact_sales fs
  JOIN dim_time dt ON fs.DateKey = dt.DateKey
  JOIN dim_store ds ON fs.StoreID = ds.StoreID
  WHERE dt.Year = 2017 
  GROUP BY dt.Year, dt.Quarter, fs.StoreID, ds.StoreName
),
StoreGrowth AS (
  SELECT 
    StoreID,
    StoreName,
    Quarter,
    StoreRevenue,
    -- Get the previous quarter's revenue for the same store
    LAG(StoreRevenue) OVER (
      PARTITION BY StoreID 
      ORDER BY Quarter
    ) AS PrevQuarterRev
  FROM StoreQuarterRevenue
)
SELECT 
  StoreID,
  StoreName,
  Quarter,
  StoreRevenue,
  PrevQuarterRev,
  -- Calculate growth percentage, handling division by zero
  ROUND(
    CASE 
      WHEN PrevQuarterRev > 0 THEN ((StoreRevenue - PrevQuarterRev) / PrevQuarterRev) * 100 
      ELSE NULL 
    END, 
    2
  ) AS GrowthRatePct
FROM StoreGrowth
ORDER BY StoreID, Quarter;
-- Query13
SELECT 
  fs.StoreID,
  ds.StoreName,
  fs.SupplierID,
  dsu.SupplierName,
  fs.ProductID,
  dp.ProductCategory, 
  SUM(fs.TotalAmount) AS SupplierProductSales
FROM fact_sales fs
JOIN dim_store ds ON fs.StoreID = ds.StoreID
JOIN dim_supplier dsu ON fs.SupplierID = dsu.SupplierID
JOIN dim_product dp ON fs.ProductID = dp.ProductID
GROUP BY fs.StoreID, ds.StoreName, fs.SupplierID, dsu.SupplierName, fs.ProductID, dp.ProductCategory
ORDER BY StoreName, SupplierName, SupplierProductSales DESC;
-- Query14
SELECT
  dp.ProductID,
  dp.ProductCategory,
  -- Use CASE statement to define seasons based on Month
  CASE 
    WHEN dt.Month IN ('March', 'April', 'May') THEN 'Spring'
    WHEN dt.Month IN ('June', 'July', 'August') THEN 'Summer'
    WHEN dt.Month IN ('September', 'October', 'November') THEN 'Fall'
    WHEN dt.Month IN ('December', 'January', 'February') THEN 'Winter'
    ELSE 'Unknown' 
  END AS Season,
  SUM(fs.TotalAmount) AS TotalSales
FROM fact_sales fs
JOIN dim_time dt ON fs.DateKey = dt.DateKey
JOIN dim_product dp ON fs.ProductID = dp.ProductID
GROUP BY dp.ProductID, dp.ProductCategory, Season
ORDER BY dp.ProductCategory, Season, TotalSales DESC;
-- Query15
WITH MonthlyStoreSupplierRevenue AS (
  SELECT 
    fs.StoreID,
    fs.SupplierID,
    dt.Year, 
    dt.Month,
    -- Create a date object to ensure correct monthly ordering
    STR_TO_DATE(CONCAT('01-', dt.Month, '-', dt.Year), '%d-%M-%Y') as MonthDate,
    SUM(fs.TotalAmount) AS MonthlyRevenue
  FROM fact_sales fs
  JOIN dim_time dt ON fs.DateKey = dt.DateKey
  GROUP BY fs.StoreID, fs.SupplierID, dt.Year, dt.Month
),
RevenueVolatility AS (
  SELECT 
    StoreID,
    SupplierID,
    Year, 
    Month, 
    MonthDate,
    MonthlyRevenue,
    -- Get the previous month's revenue for the same store/supplier
    LAG(MonthlyRevenue) OVER (
      PARTITION BY StoreID, SupplierID 
      ORDER BY MonthDate
    ) AS PrevMonthRevenue
  FROM MonthlyStoreSupplierRevenue
)
SELECT 
  StoreID,
  SupplierID,
  Year,
  Month,
  MonthlyRevenue,
  PrevMonthRevenue,
  -- Calculate volatility percentage, handling division by zero
  ROUND(
    CASE 
      WHEN PrevMonthRevenue > 0 THEN ((MonthlyRevenue - PrevMonthRevenue) / PrevMonthRevenue) * 100 
      ELSE NULL 
    END, 
    2
  ) AS RevenueVolatilityPercent
FROM RevenueVolatility
ORDER BY StoreID, SupplierID, MonthDate;
-- Query16
WITH ProductPairs AS (
  -- Find all unique pairs of products (Product1, Product2) that appear in the same OrderID
  SELECT DISTINCT
    f1.OrderID,
    f1.ProductID AS Product1,
    f2.ProductID AS Product2
  FROM fact_sales f1
  -- Join the table to itself on OrderID
  JOIN fact_sales f2 ON f1.OrderID = f2.OrderID
  -- Use < to get unique pairs (e.g., A,B) and avoid duplicates (B,A) and self-pairs (A,A)
  WHERE f1.ProductID < f2.ProductID
)
SELECT
  pp.Product1,
  p1.ProductCategory AS Product1_Category,
  pp.Product2,
  p2.ProductCategory AS Product2_Category,
  -- Count how many distinct orders this pair appears in
  COUNT(DISTINCT pp.OrderID) AS Frequency
FROM ProductPairs pp
-- Join to get product details
JOIN dim_product p1 ON pp.Product1 = p1.ProductID
JOIN dim_product p2 ON pp.Product2 = p2.ProductID
GROUP BY pp.Product1, p1.ProductCategory, pp.Product2, p2.ProductCategory
ORDER BY Frequency DESC
LIMIT 5;



-- Query17
SELECT 
  dt.Year,
  ds.StoreName,
  dsu.SupplierName,
  dp.ProductCategory,
  SUM(fs.TotalAmount) AS TotalRevenue
FROM fact_sales fs
JOIN dim_time dt ON fs.DateKey = dt.DateKey
LEFT JOIN dim_store ds ON fs.StoreID = ds.StoreID
LEFT JOIN dim_supplier dsu ON fs.SupplierID = dsu.SupplierID
LEFT JOIN dim_product dp ON fs.ProductID = dp.ProductID
-- Group by names for a more readable rollup
GROUP BY dt.Year, ds.StoreName, dsu.SupplierName, dp.ProductCategory WITH ROLLUP
ORDER BY 
  dt.Year,
  ds.StoreName,
  dsu.SupplierName,
  dp.ProductCategory;
-- Query18
SELECT
  dt.Year,
  dp.ProductID,
  dp.ProductCategory,
  -- H1 (First Half) Revenue and Volume
  SUM(CASE 
    WHEN dt.Month IN ('January', 'February', 'March', 'April', 'May', 'June') THEN fs.TotalAmount 
    ELSE 0 
  END) AS H1_Revenue,
  SUM(CASE 
    WHEN dt.Month IN ('January', 'February', 'March', 'April', 'May', 'June') THEN fs.Quantity 
    ELSE 0 
  END) AS H1_Volume,
  -- H2 (Second Half) Revenue and Volume
  SUM(CASE 
    WHEN dt.Month IN ('July', 'August', 'September', 'October', 'November', 'December') THEN fs.TotalAmount 
    ELSE 0 
  END) AS H2_Revenue,
  SUM(CASE 
    WHEN dt.Month IN ('July', 'August', 'September', 'October', 'November', 'December') THEN fs.Quantity 
    ELSE 0 
  END) AS H2_Volume,
  -- Yearly Totals
  SUM(fs.TotalAmount) AS Yearly_Revenue,
  SUM(fs.Quantity) AS Yearly_Volume
FROM fact_sales fs
JOIN dim_time dt ON fs.DateKey = dt.DateKey
JOIN dim_product dp ON fs.ProductID = dp.ProductID
GROUP BY dt.Year, dp.ProductID, dp.ProductCategory
ORDER BY dt.Year, Yearly_Revenue DESC;
-- Query19
WITH DailyProductSales AS (
  -- First, get the total sales for each product on each day
  SELECT
    fs.ProductID,
    fs.DateKey,
    SUM(fs.TotalAmount) AS DailySales
  FROM fact_sales fs
  GROUP BY fs.ProductID, fs.DateKey
),
ProductSalesStats AS (
  -- Next, calculate the average daily sales for each product
  SELECT
    ProductID,
    AVG(DailySales) AS AvgDailySales
  FROM DailyProductSales
  GROUP BY ProductID
)
-- Finally, join the stats back to the daily sales and filter
SELECT
  dps.ProductID,
  dp.ProductCategory,
  dps.DateKey,
  dps.DailySales,
  pss.AvgDailySales
FROM DailyProductSales dps
JOIN ProductSalesStats pss ON dps.ProductID = pss.ProductID
JOIN dim_product dp ON dps.ProductID = dp.ProductID
-- The outlier condition: daily sales are more than double the product's average
WHERE dps.DailySales > (pss.AvgDailySales * 2)
ORDER BY dps.ProductID, dps.DateKey;
-- Query20
CREATE OR REPLACE VIEW store_quarterly_sales AS
SELECT 
  fs.StoreID, 
  ds.StoreName,
  dt.Year, 
  dt.Quarter, 
  SUM(fs.TotalAmount) AS QuarterlySales
FROM fact_sales fs
JOIN dim_store ds ON fs.StoreID = ds.StoreID
JOIN dim_time dt ON fs.DateKey = dt.DateKey
GROUP BY fs.StoreID, ds.StoreName, dt.Year, dt.Quarter
ORDER BY ds.StoreName, dt.Year, dt.Quarter;