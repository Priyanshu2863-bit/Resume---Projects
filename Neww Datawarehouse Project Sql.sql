
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








-- Rough Work ---------Rough Work-------------



--Change Over Time Analysis
--CREATE VIEW gold.Prepare_Customer_report
select year(order_date) as Order_year, month(order_date) as Order_Month,
sum(sales_amount) as Total_sales,
count(distinct customer_key) as Total_customers,
sum(quantity) as Total_Quantity 
from gold.fact_sales
where order_date is not null 
group by year(order_date), month(order_date)
order by year(order_date), Order_Month;


select DATETRUNC(MONTH, order_date) as order_date,sum(sales_amount) as Total_sales,
count(distinct customer_key) as Total_customers,
sum(quantity) as Total_Quantity 
from gold.fact_sales
where order_date is not null 
group by DATETRUNC(MONTH, order_date)
order by DATETRUNC(MONTH, order_date)


select FORMAT(order_date, 'yyyy-MMM') as order_date,sum(sales_amount) as Total_sales,
count(distinct customer_key) as Total_customers,
sum(quantity) as Total_Quantity 
from gold.fact_sales
where order_date is not null 
group by FORMAT(order_date, 'yyyy-MMM')
order by FORMAT(order_date, 'yyyy-MMM')

--- Cumulative Analysis

-- calculate total sales per month 
-- and the running total of sales over time 

select 
order_date, total_sales,
sum(total_sales) over (order by order_date) as running_total_sales,  -- (partition by order_date order by order_date)
Avg(avg_price) over (order by order_date) as moving_Avg_price
from
(
select DATETRUNC(year, order_date) as Order_date,
sum(sales_amount) as Total_sales,
AVG(price) as avg_price
from gold.fact_sales
where order_date is not null 
group by DATETRUNC(year, order_date)
--order by DATETRUNC(month, order_date)
) t 


-- Performance Analysis

-- Analyze yearly performance of Products by comapring sales  
-- and Average Sales of Product of Current year and Previous year 

SELECT 
    Order_year, 
    product_name, 
    Current_sales,
    AVG(Current_sales) OVER (PARTITION BY product_name) AS Avg_sales,
    Current_sales - AVG(Current_sales) OVER (PARTITION BY product_name) AS diff_avg,
    CASE 
        WHEN Current_sales - AVG(Current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above_Avg'
        WHEN Current_sales - AVG(Current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below_Avg'
        ELSE 'Avg'
    END AS Avg_change,
	-- year over year analysis 
	lag(current_sales) over (partition by product_name order by Order_year)  as previous_year_sales,
	Current_sales -  lag(current_sales) over (partition by product_name order by Order_year) as diff_pre_year,
	case 
		when Current_sales -  lag(current_sales) over (partition by product_name order by Order_year) > 0 then 'Increase'
		when Current_sales -  lag(current_sales) over (partition by product_name order by Order_year) < 0 then 'Decrease'
		else 'No change'
	end previous_year_change
FROM (
    SELECT 
        YEAR(f.order_date) AS Order_year,
        p.product_name, 
        SUM(f.sales_amount) AS Current_sales
    FROM gold.fact_sales f 
    LEFT JOIN gold.dim_products p 
        ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL 
    GROUP BY YEAR(f.order_date), p.product_name
) AS yearly_product_sales;


-- Part to Whole Analysis

-- Which categories contributr the most of overall sales  

WITH category_sales AS (
    SELECT 
        p.category, 
        SUM(f.sales_amount) AS total_sales
    FROM gold.fact_sales AS f
    LEFT JOIN gold.dim_products AS p 
        ON f.product_key = p.product_key
    GROUP BY p.category
)
SELECT category, total_sales, sum(total_sales) over () overall_sales,
concat(round((cast(total_sales as FLOAT) / sum(total_sales) over ()) * 100, 2), '%') as percentage_of_sales  
FROM category_sales
order by total_sales desc																			

---- Data segmentation 

-- segments products into cost ranges and 
-- count how many products fall  into each segment /

with product_segment as
(
select product_key, product_name, cost,
case 
	when cost < 100 then 'Below 100'
	when cost between 100 and 500 then '100 - 500'
	when cost between 500 and 1000 then '500 - 1000'
	else 'Above 1000'
end as cost_range 
from gold.dim_products
)
select cost_range, count(product_key) as total_product
from product_segment
group by cost_range
order by total_product


-- Group customer into three segments based on their spending behaviour:
	-- Vip customer atleast 12 months history spending more than $5000
	-- Regular customer atleast 12 months history spending less than or equal $5000
	-- New customer lifespan less than 12 months 
	-- and find the total number of customer by each group

with customer_spending as(
select c.customer_key, sum(f.sales_amount) as total_spending,
min(order_date) as first_order,
max(order_date) as last_order,
DATEDIFF(month, min(order_date), max(order_date)) as Life_span
from gold.fact_sales as f 
Left join gold.dim_customers as c on f.customer_key = c.customer_key
group by c.customer_key
)

select customer_segment, count(customer_key) as total_customer
from (
select customer_key, 
case
	when Life_span >= 12 and total_spending > 5000 then 'Vip'
	when Life_span >= 12 and total_spending <= 5000 then 'Regular'
	else 'New'
end as customer_segment
from customer_spending
) as gg
group by customer_segment 
order by total_customer desc;
-------------------------------------------
WITH yearly_product_sales AS (
    SELECT 
        YEAR(f.order_date) AS Order_year,
        p.product_name, 
        SUM(f.sales_amount) AS Current_sales
    FROM gold.fact_sales AS f
    LEFT JOIN gold.dim_products AS p 
        ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL 
    GROUP BY YEAR(f.order_date), p.product_name
)

SELECT 
    Order_year, 
    product_name, 
    Current_sales,
    AVG(Current_sales) OVER (PARTITION BY product_name) AS Avg_sales
FROM yearly_product_sales
ORDER BY Order_year, product_name


-- Build Customer Report 

-- Base Query: Retrieve more columns from tables 

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
    FROM gold.fact_sales AS f 
    LEFT JOIN gold.dim_customers AS c 
        ON f.customer_key = c.customer_key
    WHERE f.order_date IS NOT NULL 
),
customer_aggregation AS (         ---  Customer Aggregation: Summarizes key metrics at the customer level 
    SELECT  
        customer_key, 
        customer_number, 
        customer_name, 
        Age,
        COUNT(DISTINCT order_number) AS total_order,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        MAX(order_date) AS last_order_date,
        DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS Life_span
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
        ELSE '50 and above'
    END AS age_group,
    CASE 
        WHEN Life_span >= 12 AND total_sales > 5000 THEN 'Vip'
        WHEN Life_span >= 12 AND total_sales <= 5000 THEN 'Regular'
        ELSE 'New'
    END AS customer_segment,
	last_order_date,
	DATEDIFF(month, last_order_date, getdate()) as Recency, 
    total_order,
    total_sales,
    total_quantity,
    last_order_date,
    Life_span,
             -- Compute Average order value (AVO) 
case
	when total_sales = 0 then 0 
	else  total_sales / total_order  
end as Average_order_value,
                     -- Compute Average monthly spending      
case
	when Life_span = 0 then total_sales
	else total_sales /  Life_span
end as Avg_monthly_spending 
FROM customer_aggregation;



















































--select * from yearly_product_sales  With cte

																		--WITH yearly_product_sales AS (
																				  --  SELECT 
																					--    YEAR(f.order_date) AS Order_year,
																					  --  p.product_name, 
																						--SUM(f.sales_amount) AS Current_sales
																					--FROM gold.fact_sales AS f
																					--LEFT JOIN gold.dim_products AS p 
																					  --  ON f.product_key = p.product_key
																					--WHERE f.order_date IS NOT NULL 
																							--GROUP BY YEAR(f.order_date), p.product_name
																						--)

																						--SELECT 
																						  --  Order_year, 
																							--product_name, 
																							--Current_sales,
																							--AVG(Current_sales) OVER (PARTITION BY product_name) AS Avg_sales
																						--FROM yearly_product_sales
																						--ORDER BY Order_year, product_name


