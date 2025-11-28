use Pizza_DB;

select * from pizza_sales

-- 1. Total Revenue from all pizza sales

select round(sum(total_price),2) as total_revenue from pizza_sales;

-- 2. Average Order Value (Revenue per Order)
select sum(total_price)/count(distinct order_id) as Avg_order_value from pizza_sales;

-- 3. Total Number of Pizzas Sold
select sum(quantity) as Total_pizza_sold from pizza_sales

-- 4. Total Number of Orders
select  count(distinct order_id) as Total_orders from pizza_sales

-- 5. Average Number of Pizzas per Order
select cast(cast(sum(quantity) as decimal(10, 2)) / cast(count(distinct order_id) as decimal(10, 2))  as decimal(10, 2)) as Avg_pizza_per_order from pizza_sales

-- 6. Total Orders by Day of the Week
select DATENAME(DW, order_date) as order_day, count(distinct order_id) as Total_orders 
from pizza_sales
group by DATENAME(DW, order_date) 

-- 7. Total Orders by Month
select DATENAME(MONTH, order_date) as Month_Name, count(distinct order_id)as total_order 
from pizza_sales
group by DATENAME(MONTH, order_date)

-- 8. Total Sales and Category-wise Percentage for January
select pizza_category, sum(total_price) as Total_Sales, sum(total_price)* 100 / (select sum(total_price) from pizza_sales 
where MONTH(order_date) = 1
)
from pizza_sales as percentage_
where MONTH(order_date) = 1
group by pizza_category

-- 9. Total Sales and Size-wise Percentage for Quarter 1
SELECT pizza_size, round(sum(total_price), 2) AS Total_Sales, round(sum(total_price) * 100.0 / 
(SELECT SUM(total_price) from pizza_sales 
where DATEPART(quarter, order_date) = 1),
 2) AS percentage_
FROM pizza_sales
WHERE DATEPART(quarter, order_date) = 1
GROUP BY pizza_size;

-- 10. Top 5 Pizza Names by Revenue
select Top 5 pizza_name, round(sum(total_price),2) as Total_revenue 
from pizza_sales
group by pizza_name
order by round(sum(total_price),2) desc

LAPTOP-TK3HEK8B



-- Sales by Hour of the Day
SELECT 
    DATEPART(HOUR, order_time) AS Hour_of_Day,
    SUM(total_price) AS Total_Sales,
    COUNT(DISTINCT order_id) AS Total_Orders
FROM pizza_sales
GROUP BY DATEPART(HOUR, order_time)
ORDER BY Hour_of_Day;


-- Monthly Trend – Revenue + Orders

SELECT 
    DATENAME(MONTH, order_date) AS Month_Name,
    SUM(total_price) AS Total_Revenue,
    COUNT(DISTINCT order_id) AS Total_Orders
FROM pizza_sales
GROUP BY DATENAME(MONTH, order_date)
ORDER BY MIN(order_date);


-- % of Sales by Category – Revenue + Quantity

SELECT 
    pizza_category,
    SUM(total_price) AS Revenue,
    SUM(quantity) AS Quantity_Sold,
    ROUND(SUM(total_price) * 100.0 / (SELECT SUM(total_price) FROM pizza_sales), 2) AS Pct_Revenue,
    ROUND(SUM(quantity) * 100.0 / (SELECT SUM(quantity) FROM pizza_sales), 2) AS Pct_Quantity
FROM pizza_sales
GROUP BY pizza_category
ORDER BY Revenue DESC;


-- % Sales by Pizza Size & Category (Revenue + Quantity)

SELECT 
    pizza_size,
    pizza_category,
    SUM(total_price) AS Revenue,
    SUM(quantity) AS Quantity,
    ROUND(SUM(total_price) * 100.0 / (SELECT SUM(total_price) FROM pizza_sales), 2) AS Pct_Revenue
FROM pizza_sales
GROUP BY pizza_size, pizza_category
ORDER BY pizza_size, Revenue DESC;


-- Total Pizzas Sold by Pizza Category

SELECT 
    pizza_category,
    SUM(quantity) AS Total_Pizzas_Sold
FROM pizza_sales
GROUP BY pizza_category
ORDER BY Total_Pizzas_Sold DESC;


-- Bottom 5 Pizzas by Revenue

SELECT 
    pizza_category,
    SUM(quantity) AS Total_Pizzas_Sold
FROM pizza_sales
GROUP BY pizza_category
ORDER BY Total_Pizzas_Sold Asc;


