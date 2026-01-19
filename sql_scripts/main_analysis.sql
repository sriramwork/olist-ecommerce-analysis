
/* PROJECT: OLIST E-COMMERCE END-TO-END DATA ANALYSIS
--------------------------------------------------
SECTION 1: Data Engineering (Schema & ETL)
SECTION 2: Descriptive Analytics (Sales & Logistics KPIs)
SECTION 3: CRM Analytics (RFM Segmentation & Pareto)
SECTION 4: Retention Analytics (Cohort Heatmaps)
*/

-- PHASE 1 DATA IMPORT

-- creating database
create database ecommerce_analysis;
USE ecommerce_analysis;

-- creating 2 schemas to store a copy of raw data
CREATE SCHEMA raw;
CREATE SCHEMA analytics;

-- creating tables and importing data
-- orders table 
CREATE TABLE raw.orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    order_status VARCHAR(20),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME
);
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist data/olist_orders_dataset.csv'
INTO TABLE raw.orders
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(order_id, customer_id, order_status, @v_purchase, @v_approved, @v_carrier, @v_delivery, @v_estimated)
SET 
    order_purchase_timestamp = STR_TO_DATE(NULLIF(@v_purchase, ''), '%d-%m-%Y %H:%i'),
    order_approved_at = STR_TO_DATE(NULLIF(@v_approved, ''), '%d-%m-%Y %H:%i'),
    order_delivered_carrier_date = STR_TO_DATE(NULLIF(@v_carrier, ''), '%d-%m-%Y %H:%i'),
    order_delivered_customer_date = STR_TO_DATE(NULLIF(@v_delivery, ''), '%d-%m-%Y %H:%i'),
    order_estimated_delivery_date = STR_TO_DATE(NULLIF(@v_estimated, ''), '%d-%m-%Y %H:%i');
 
 -- checking if all the data is loaded
select count(*) from raw.orders;

-- 1. Customers Table
CREATE TABLE raw.customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix INT,
    customer_city VARCHAR(50),
    customer_state VARCHAR(10)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist data/olist_customers_dataset.csv'
INTO TABLE raw.customers
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- 2. Order Payments Table
CREATE TABLE raw.order_payments (
    order_id VARCHAR(50),
    payment_sequential INT,
    payment_type VARCHAR(20),
    payment_installments INT,
    payment_value DECIMAL(10, 2)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist data/olist_order_payments_dataset.csv'
INTO TABLE raw.order_payments
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- 3. Order Items Table (Crucial for Revenue)
CREATE TABLE raw.order_items (
    order_id VARCHAR(50),
    order_item_id INT,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date DATETIME,
    price DECIMAL(10, 2),
    freight_value DECIMAL(10, 2)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist data/olist_order_items_dataset.csv'
INTO TABLE raw.order_items
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(order_id, order_item_id, product_id, seller_id, @v_shipping_limit, price, freight_value)
SET shipping_limit_date = NULLIF(@v_shipping_limit, '');



-- 4. Products Table
CREATE TABLE raw.products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist data/olist_products_dataset.csv'
INTO TABLE raw.products
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(product_id, product_category_name, @v_name_len, @v_desc_len, @v_photos, @v_weight, @v_length, @v_height, @v_width)
SET 
    product_name_length = NULLIF(@v_name_len, ''),
    product_description_length = NULLIF(@v_desc_len, ''),
    product_photos_qty = NULLIF(@v_photos, ''),
    product_weight_g = NULLIF(@v_weight, ''),
    product_length_cm = NULLIF(@v_length, ''),
    product_height_cm = NULLIF(@v_height, ''),
    product_width_cm = NULLIF(@v_width, '');

-- making first analysis table only including delivered orders
CREATE TABLE analytics.fact_sales AS 
SELECT 
    o.order_id, 
    o.customer_id, 
    o.order_purchase_timestamp, 
    i.product_id, 
    i.price, 
    i.freight_value,
    (i.price + i.freight_value) AS total_order_cost -- calculated column
FROM raw.orders as o
INNER JOIN raw.order_items as i ON o.order_id = i.order_id
WHERE o.order_status = 'delivered';

select * from analytics.fact_sales
limit 5;

-- extracting the most expensive products and date time they are sold
SELECT product_id, price, order_purchase_timestamp
from analytics.fact_sales
order by price desc
limit 5;

-- importing translation table 
CREATE TABLE raw.product_category_name_translation (
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100)
);

-- Load the Translation Data
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist data/product_category_name_translation.csv'
INTO TABLE raw.product_category_name_translation
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- creating analytics table with english product category names
CREATE TABLE analytics.dim_products AS
SELECT 
    p.product_id,
    t.product_category_name_english AS category,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
FROM raw.products AS p
LEFT JOIN raw.product_category_name_translation AS t 
    ON p.product_category_name = t.product_category_name;

-- identifying the most unique products listed categories 
SELECT category, COUNT(*) 
FROM analytics.dim_products 
GROUP BY category 
ORDER BY 2 DESC 
LIMIT 5;

-- creating a table with distinct customer data
CREATE TABLE analytics.dim_customers AS SELECT DISTINCT customer_unique_id, customer_city, customer_state FROM raw.customers;

-- creating one big analytics table with clean data
CREATE TABLE analytics.master_sales_report AS
SELECT 
    f.order_id,
    f.order_purchase_timestamp,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    p.category,
    f.price,
    f.freight_value,
    f.total_order_cost
FROM analytics.fact_sales f
JOIN analytics.dim_products p ON f.product_id = p.product_id
JOIN raw.customers c ON f.customer_id = c.customer_id;


-- PHASE 2: DESCRIPTIVE ANALYSIS


-- checking monthly revenue
SELECT 
    DATE_FORMAT(order_purchase_timestamp, '%Y-%m') AS sales_month, 
    ROUND(SUM(total_order_cost), 2) AS monthly_revenue, 
    COUNT(order_id) AS total_orders
FROM analytics.master_sales_report
GROUP BY 1  -- Refers to the first column (sales_month)
ORDER BY 1;
-- The month of 2017-11 shows revenue of 1,153,364.20, which is nearly a 53% increase from the previous month. This is the Black Friday effect.

-- finding top 5 states by revenue (potential to open new warehouses and branches)
SELECT 
	customer_state,
    ROUND(SUM(total_order_cost), 2)
FROM analytics.master_sales_report
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5;

-- looking at the most popular payment method
SELECT 
	payment_type, 
    count(*) as no_of_transactions,
    ROUND(SUM(payment_value), 2) as total_paid
From raw.order_payments
GROUP BY 1
ORDER BY 3 DESC;

-- identifying top 10 product categories
SELECT
	category,
    ROUND(SUM(total_order_cost), 2)
FROM analytics.master_sales_report
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;

-- calculating average delivery time
SELECT 
    AVG(datediff(order_delivered_customer_date, order_purchase_timestamp))
FROM raw.orders
WHERE order_status = 'delivered';
-- the average delivery time is 12.5 days

-- checking no of repeat buyers to observe customer loyalty
SELECT
	customer_unique_id,
    count(order_id) as no_of_purchases
FROM analytics.master_sales_report
GROUP BY 1
HAVING no_of_purchases > 1
ORDER BY 2 DESC;

-- Executive summary (looking at the big numbers)
SELECT
	SUM(total_order_cost) as total_revenue,
    count(DISTINCT order_id) as total_no_of_orders,
    count(DISTINCT customer_unique_id) as no_of_customers
FROM analytics.master_sales_report;

-- calculating average order value to understand customer spending behaviour
SELECT
	customer_state,
    ROUND(SUM(total_order_cost) /count(DISTINCT order_id), 2) as average_order_value
FROM analytics.master_sales_report
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;

-- identifying the top 5 sellers
SELECT 
	i.seller_id,
    SUM(i.price + i.freight_value)
FROM raw.order_items as i JOIN raw.orders as o ON i.order_id = o.order_id
WHERE order_status = 'delivered'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5;

-- identifying the delivery gap
SELECT 
    AVG(DATEDIFF(order_estimated_delivery_date, order_delivered_customer_date)) AS avg_buffer_days,
    MIN(DATEDIFF(order_estimated_delivery_date, order_delivered_customer_date)) AS max_delay_days,
    MAX(DATEDIFF(order_estimated_delivery_date, order_delivered_customer_date)) AS max_early_days
FROM raw.orders
WHERE order_status = 'delivered';

-- percentage of late vs on-time deliveries
SELECT 
    ROUND(AVG(CASE WHEN order_delivered_customer_date <= order_estimated_delivery_date THEN 1 ELSE 0 END) * 100, 2) AS on_time_percentage,
    ROUND(100 - (AVG(CASE WHEN order_delivered_customer_date <= order_estimated_delivery_date THEN 1 ELSE 0 END) * 100), 2) AS late_percentage
FROM raw.orders
WHERE order_status = 'delivered';


-- PHASE 3: ADVANCED CUSTOMER SEGMENTATION (RFM)


-- 1. Calculate Raw RFM Metrics
CREATE TABLE analytics.rfm_raw AS
SELECT 
    customer_unique_id,
    DATEDIFF((SELECT MAX(order_purchase_timestamp) FROM analytics.master_sales_report), 
             MAX(order_purchase_timestamp)) AS recency,
    COUNT(DISTINCT order_id) AS frequency,
    ROUND(SUM(total_order_cost), 2) AS monetary
FROM analytics.master_sales_report
GROUP BY customer_unique_id;

-- 2. Create Final Segmented Table (The "Source of Truth")
-- We do the math once here and save it.
CREATE TABLE analytics.final_customer_segments AS
WITH rfm_scores AS (
    SELECT 
        *,
        NTILE(5) OVER (ORDER BY recency DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency ASC) AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC) AS m_score
    FROM analytics.rfm_raw
)
SELECT 
    *,
    CASE 
        WHEN r_score >= 4 AND f_score >= 4 THEN 'Champions'
        WHEN r_score >= 4 AND f_score < 4 THEN 'New Customers'
        WHEN r_score <= 2 AND f_score >= 4 THEN 'At Risk / Loyal'
        WHEN r_score <= 2 AND f_score <= 2 THEN 'Hibernating'
        ELSE 'General'
    END AS customer_segment
FROM rfm_scores;

-- 3. Executive Summary Report
-- Now we just query the final table!
SELECT 
    customer_segment,
    COUNT(*) AS total_customers,
    ROUND(SUM(monetary), 2) AS total_revenue,
    ROUND(AVG(monetary), 2) AS avg_spend,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM analytics.final_customer_segments), 2) AS pct_of_base
FROM analytics.final_customer_segments
GROUP BY 1
ORDER BY total_revenue DESC;

-- 4. Revenue Concentration 
SELECT 
    customer_segment,
    ROUND(SUM(monetary) * 100 / (SELECT SUM(monetary) FROM analytics.final_customer_segments), 2) AS pct_revenue
FROM analytics.final_customer_segments
GROUP BY 1
ORDER BY 2 DESC;

-- 5. Top Product Categories per Segment
WITH segment_product_totals AS (
    SELECT 
        s.customer_segment,
        m.category,
        COUNT(*) AS category_purchase_count,
        RANK() OVER (PARTITION BY s.customer_segment ORDER BY COUNT(*) DESC) AS category_rank
    FROM analytics.final_customer_segments s
    JOIN analytics.master_sales_report m ON s.customer_unique_id = m.customer_unique_id
    GROUP BY 1, 2
)
SELECT customer_segment, category, category_purchase_count
FROM segment_product_totals
WHERE category_rank <= 3
ORDER BY customer_segment, category_purchase_count DESC;

-- PHASE 4: COHORT ANALYSIS (Retention Heatmap)

-- Find the first purchase month for every customer
WITH customer_first_purchase AS (
    SELECT 
        customer_unique_id,
        MIN(DATE_FORMAT(order_purchase_timestamp, '%Y-%m-01')) AS cohort_month
    FROM analytics.master_sales_report
    GROUP BY 1
),

-- Join back to all orders to see the time gap (diff) between purchases
cohort_retention AS (
    SELECT 
        c.cohort_month,
        PERIOD_DIFF(
            DATE_FORMAT(m.order_purchase_timestamp, '%Y%m'), 
            DATE_FORMAT(c.cohort_month, '%Y%m')
        ) AS month_number,
        COUNT(DISTINCT m.customer_unique_id) AS returning_customers
    FROM analytics.master_sales_report m
    JOIN customer_first_purchase c ON m.customer_unique_id = c.customer_unique_id
    GROUP BY 1, 2
)

-- View the retention counts
SELECT 
    cohort_month,
    month_number,
    returning_customers
FROM cohort_retention
WHERE month_number <= 12 -- Look at the first year of life
ORDER BY cohort_month, month_number;

-- PHASE 4B: COHORT RETENTION PERCENTAGES 

WITH customer_first_purchase AS (
    SELECT 
        customer_unique_id,
        MIN(DATE_FORMAT(order_purchase_timestamp, '%Y-%m-01')) AS cohort_month
    FROM analytics.master_sales_report
    GROUP BY 1
),

cohort_retention AS (
    SELECT 
        c.cohort_month,
        PERIOD_DIFF(
            DATE_FORMAT(m.order_purchase_timestamp, '%Y%m'), 
            DATE_FORMAT(c.cohort_month, '%Y%m')
        ) AS month_number,
        COUNT(DISTINCT m.customer_unique_id) AS customers
    FROM analytics.master_sales_report m
    JOIN customer_first_purchase c 
        ON m.customer_unique_id = c.customer_unique_id
    GROUP BY 1, 2
),

cohort_size AS (
    SELECT 
        cohort_month,
        MAX(customers) AS cohort_customers
    FROM cohort_retention
    WHERE month_number = 0
    GROUP BY 1
)

SELECT 
    r.cohort_month,
    r.month_number,
    r.customers,
    ROUND(r.customers / s.cohort_customers * 100, 2) AS retention_pct
FROM cohort_retention r
JOIN cohort_size s 
    ON r.cohort_month = s.cohort_month
WHERE r.month_number <= 12
ORDER BY r.cohort_month, r.month_number;

-- PHASE 4C: COHORT PIVOT VIEW (Management Heatmap with Percentages)
-- Pivoting cohort retention data into wide format for clear visualization (months 0–6)

WITH cohort_data AS (
    -- Calculate retention percentages for each cohort and month
    SELECT 
        r.cohort_month,
        r.month_number,
        ROUND(r.customers / s.cohort_customers * 100, 2) AS retention_pct
    FROM (
        -- Count returning customers per cohort per month
        SELECT 
            c.cohort_month,
            PERIOD_DIFF(
                DATE_FORMAT(m.order_purchase_timestamp, '%Y%m'), 
                DATE_FORMAT(c.cohort_month, '%Y%m')
            ) AS month_number,
            COUNT(DISTINCT m.customer_unique_id) AS customers
        FROM analytics.master_sales_report m
        JOIN (
            -- Find first purchase month per customer
            SELECT 
                customer_unique_id, 
                MIN(DATE_FORMAT(order_purchase_timestamp, '%Y-%m-01')) AS cohort_month 
            FROM analytics.master_sales_report 
            GROUP BY 1
        ) c 
            ON m.customer_unique_id = c.customer_unique_id
        GROUP BY 1, 2
    ) r
    JOIN (
        -- Count total customers in each cohort (month 0)
        SELECT 
            cohort_month, 
            COUNT(DISTINCT customer_unique_id) AS cohort_customers 
        FROM (
            SELECT 
                customer_unique_id, 
                MIN(DATE_FORMAT(order_purchase_timestamp, '%Y-%m-01')) AS cohort_month 
            FROM analytics.master_sales_report 
            GROUP BY 1
        ) t 
        GROUP BY 1
    ) s 
        ON r.cohort_month = s.cohort_month
)

-- Pivot the long cohort data into wide format (0–6 months)
SELECT 
    cohort_month,

    COALESCE(MAX(CASE WHEN month_number = 0 THEN retention_pct END), 0) AS month_0,  -- First month
    COALESCE(MAX(CASE WHEN month_number = 1 THEN retention_pct END), 0) AS month_1,
    COALESCE(MAX(CASE WHEN month_number = 2 THEN retention_pct END), 0) AS month_2,
    COALESCE(MAX(CASE WHEN month_number = 3 THEN retention_pct END), 0) AS month_3,
    COALESCE(MAX(CASE WHEN month_number = 4 THEN retention_pct END), 0) AS month_4,
    COALESCE(MAX(CASE WHEN month_number = 5 THEN retention_pct END), 0) AS month_5,
    COALESCE(MAX(CASE WHEN month_number = 6 THEN retention_pct END), 0) AS month_6

FROM cohort_data
GROUP BY cohort_month
ORDER BY cohort_month;
