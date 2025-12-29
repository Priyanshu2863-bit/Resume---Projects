
-- DATA WAREHOUSE ANALYTICS PROJECT – SQL SERVER

-- Description: Sales, Customer & Product Analytics 
-- Using Fact & Dimension Tables (gold schema)

-- TABLES USED
-- gold.fact_sales
-- gold.dim_products
-- gold.dim_customers

--

-- Change Over Time (Monthly Sales Trend)

SELECT 
    YEAR(order_date) AS Order_Year,
    MONTH(order_date) AS Order_Month,
    SUM(sales_amount) AS Total_Sales,
    COUNT(DISTINCT customer_key) AS Total_Customers,
    SUM(quantity) AS Total_Quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY Order_Year, Order_Month;


-- Monthly Trend Using DATETRUNC

SELECT 
    DATETRUNC(MONTH, order_date) AS Order_Month,
    SUM(sales_amount) AS Total_Sales,
    COUNT(DISTINCT customer_key) AS Total_Customers,
    SUM(quantity) AS Total_Quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(MONTH, order_date)
ORDER BY Order_Month;


--Monthly Trend Using FORMAT()

SELECT 
    FORMAT(order_date, 'yyyy-MMM') AS Month_Name,
    SUM(sales_amount) AS Total_Sales,
    COUNT(DISTINCT customer_key) AS Total_Customers,
    SUM(quantity) AS Total_Quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY FORMAT(order_date, 'yyyy-MMM')
ORDER BY Month_Name;


-- Cumulative Sales and Moving Average (Running Total)

SELECT 
    Order_Date,
    Total_Sales,
    SUM(Total_Sales) OVER (ORDER BY Order_Date) AS Running_Total_Sales,
    AVG(Avg_Price) OVER (ORDER BY Order_Date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS Moving_Avg_Price
FROM (
    SELECT 
        DATETRUNC(MONTH, order_date) AS Order_Date,
        SUM(sales_amount) AS Total_Sales,
        AVG(price) AS Avg_Price
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATETRUNC(MONTH, order_date)
) t
ORDER BY Order_Date;


-- Year-over-Year & Above/Below Average Performance

WITH yearly_product_sales AS (
    SELECT 
        YEAR(f.order_date) AS Order_Year,
        p.product_name,
        SUM(f.sales_amount) AS Current_Sales
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p 
        ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
    GROUP BY YEAR(f.order_date), p.product_name
)
SELECT 
    Order_Year,
    product_name,
    Current_Sales,
    AVG(Current_Sales) OVER(PARTITION BY product_name) AS Avg_Sales,
    Current_Sales - AVG(Current_Sales) OVER(PARTITION BY product_name) AS Diff_Avg,
    CASE 
        WHEN Current_Sales > AVG(Current_Sales) OVER(PARTITION BY product_name) 
            THEN 'Above Average'
        ELSE 'Below Average'
    END AS Performance,
    LAG(Current_Sales) OVER(PARTITION BY product_name ORDER BY Order_Year) AS Previous_Year_Sales,
    Current_Sales - LAG(Current_Sales) OVER(PARTITION BY product_name ORDER BY Order_Year) AS YoY_Change
FROM yearly_product_sales
ORDER BY product_name, Order_Year;


-- Category Contribution (Part-to-Whole Analysis)

WITH category_sales AS (
    SELECT 
        p.category,
        SUM(f.sales_amount) AS Total_Sales
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON f.product_key = p.product_key
    GROUP BY p.category
)
SELECT 
    category,
    Total_Sales,
    SUM(Total_Sales) OVER() AS Overall_Sales,
    CAST((Total_Sales * 100.0 / SUM(Total_Sales) OVER()) AS DECIMAL(10,2)) AS Percentage_Contribution
FROM category_sales
ORDER BY Total_Sales DESC;


-- Product Segmentation by Cost Ranges

WITH product_segment AS (
    SELECT 
        product_key,
        product_name,
        cost,
        CASE 
            WHEN cost < 100 THEN 'Below 100'
            WHEN cost BETWEEN 100 AND 500 THEN '100 - 500'
            WHEN cost BETWEEN 500 AND 1000 THEN '500 - 1000'
            ELSE 'Above 1000'
        END AS Cost_Range
    FROM gold.dim_products
)
SELECT 
    Cost_Range,
    COUNT(product_key) AS Total_Products
FROM product_segment
GROUP BY Cost_Range
ORDER BY Total_Products DESC;


-- Customer Segmentation (VIP, Regular, New)

WITH customer_spending AS (
    SELECT 
        c.customer_key,
        SUM(f.sales_amount) AS Total_Spending,
        MIN(order_date) AS First_Order,
        MAX(order_date) AS Last_Order,
        DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS Life_Span
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c
        ON f.customer_key = c.customer_key
    GROUP BY c.customer_key
)
SELECT 
    CASE
        WHEN Life_Span >= 12 AND Total_Spending > 5000 THEN 'VIP'
        WHEN Life_Span >= 12 AND Total_Spending <= 5000 THEN 'Regular'
        ELSE 'New'
    END AS Customer_Segment,
    COUNT(customer_key) AS Total_Customers
FROM customer_spending
GROUP BY 
    CASE
        WHEN Life_Span >= 12 AND Total_Spending > 5000 THEN 'VIP'
        WHEN Life_Span >= 12 AND Total_Spending <= 5000 THEN 'Regular'
        ELSE 'New'
    END
ORDER BY Total_Customers DESC;


-- Full Customer Profile Report

WITH base_query AS (
    SELECT 
        f.order_number,
        f.product_key,
        f.order_date,
        f.sales_amount,
        f.quantity,
        c.customer_key,
        c.customer_number,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        c.birthdate,
        DATEDIFF(YEAR, c.birthdate, GETDATE()) AS Age
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c
        ON f.customer_key = c.customer_key
    WHERE f.order_date IS NOT NULL
),
customer_aggregation AS (
    SELECT 
        customer_key,
        customer_number,
        customer_name,
        Age,
        COUNT(DISTINCT order_number) AS Total_Orders,
        SUM(sales_amount) AS Total_Sales,
        SUM(quantity) AS Total_Quantity,
        MAX(order_date) AS Last_Order_Date,
        DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS Life_Span
    FROM base_query
    GROUP BY 
        customer_key,
        customer_number,
        customer_name,
        Age
)
SELECT 
    customer_key,
    customer_number,
    customer_name,
    Age,
    CASE 
        WHEN Age < 20 THEN 'Under 20'
        WHEN Age BETWEEN 20 AND 29 THEN '20-29'
        WHEN Age BETWEEN 30 AND 39 THEN '30-39'
        WHEN Age BETWEEN 40 AND 49 THEN '40-49'
        ELSE '50+'
    END AS Age_Group,
    CASE
        WHEN Life_Span >= 12 AND Total_Sales > 5000 THEN 'VIP'
        WHEN Life_Span >= 12 AND Total_Sales <= 5000 THEN 'Regular'
        ELSE 'New'
    END AS Customer_Segment,
    Last_Order_Date,
    DATEDIFF(MONTH, Last_Order_Date, GETDATE()) AS Recency,
    Total_Orders,
    Total_Sales,
    Total_Quantity,
    CASE 
        WHEN Total_Orders = 0 THEN 0 
        ELSE Total_Sales / Total_Orders
    END AS Avg_Order_Value,
    CASE 
        WHEN Life_Span = 0 THEN Total_Sales
        ELSE Total_Sales / Life_Span
    END AS Avg_Monthly_Spending
FROM customer_aggregation;









