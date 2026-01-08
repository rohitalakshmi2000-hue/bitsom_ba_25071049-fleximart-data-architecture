-- Query 1: Monthly Sales Drill-Down
 use fleximart_dw;
SELECT 
    d.year,
    d.quarter,
    d.month_name AS month,
    SUM(f.total_amount) AS total_sales,
    SUM(f.quantity_sold) AS total_quantity
FROM fact_sales f
JOIN dim_date d 
    ON f.date_key = d.date_key
WHERE d.year = 2024
GROUP BY 
    d.year,
    d.quarter,
    d.month,
    d.month_name
ORDER BY 
    d.year,
    d.quarter,
    d.month;

  -- Query 2: Top 10 Products by Revenue

SELECT 
    p.product_name,
    p.category,
    SUM(f.quantity_sold) AS units_sold,
    SUM(f.total_amount) AS revenue,
    ROUND(
        (SUM(f.total_amount) / 
        (SELECT SUM(total_amount) FROM fact_sales)) * 100, 
        2
    ) AS revenue_percentage
FROM fact_sales f
JOIN dim_product p
    ON f.product_key = p.product_key
GROUP BY p.product_name, p.category
ORDER BY revenue DESC

  -- Query 3: Customer Segmentation

SELECT
    customer_segment,
    COUNT(*) AS customer_count,
    SUM(total_spent) AS total_revenue,
    AVG(total_spent) AS avg_revenue
FROM (
    SELECT
        c.customer_key,
        SUM(f.total_amount) AS total_spent,
        CASE
            WHEN SUM(f.total_amount) > 50000 THEN 'High Value'
            WHEN SUM(f.total_amount) BETWEEN 20000 AND 50000 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS customer_segment
    FROM dim_customer c
    JOIN fact_sales f
        ON c.customer_key = f.customer_key
    GROUP BY c.customer_key
) customer_totals
GROUP BY customer_segment
ORDER BY total_revenue DESC;


