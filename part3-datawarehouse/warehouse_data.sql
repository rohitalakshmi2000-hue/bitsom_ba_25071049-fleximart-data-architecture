-- =====================================================
-- INSERT DATA INTO dim_date (30 dates: Jan–Feb 2024)
-- =====================================================

INSERT INTO dim_date
(date_key, full_date, day_of_week, day_of_month, month, month_name, quarter, year, is_weekend)
VALUES
(20240101,'2024-01-01','Monday',1,1,'January','Q1',2024,0),
(20240102,'2024-01-02','Tuesday',2,1,'January','Q1',2024,0),
(20240103,'2024-01-03','Wednesday',3,1,'January','Q1',2024,0),
(20240104,'2024-01-04','Thursday',4,1,'January','Q1',2024,0),
(20240105,'2024-01-05','Friday',5,1,'January','Q1',2024,0),
(20240106,'2024-01-06','Saturday',6,1,'January','Q1',2024,1),
(20240107,'2024-01-07','Sunday',7,1,'January','Q1',2024,1),
(20240108,'2024-01-08','Monday',8,1,'January','Q1',2024,0),
(20240109,'2024-01-09','Tuesday',9,1,'January','Q1',2024,0),
(20240110,'2024-01-10','Wednesday',10,1,'January','Q1',2024,0),
(20240111,'2024-01-11','Thursday',11,1,'January','Q1',2024,0),
(20240112,'2024-01-12','Friday',12,1,'January','Q1',2024,0),
(20240113,'2024-01-13','Saturday',13,1,'January','Q1',2024,1),
(20240114,'2024-01-14','Sunday',14,1,'January','Q1',2024,1),
(20240115,'2024-01-15','Monday',15,1,'January','Q1',2024,0),

(20240201,'2024-02-01','Thursday',1,2,'February','Q1',2024,0),
(20240202,'2024-02-02','Friday',2,2,'February','Q1',2024,0),
(20240203,'2024-02-03','Saturday',3,2,'February','Q1',2024,1),
(20240204,'2024-02-04','Sunday',4,2,'February','Q1',2024,1),
(20240205,'2024-02-05','Monday',5,2,'February','Q1',2024,0),
(20240206,'2024-02-06','Tuesday',6,2,'February','Q1',2024,0),
(20240207,'2024-02-07','Wednesday',7,2,'February','Q1',2024,0),
(20240208,'2024-02-08','Thursday',8,2,'February','Q1',2024,0),
(20240209,'2024-02-09','Friday',9,2,'February','Q1',2024,0),
(20240210,'2024-02-10','Saturday',10,2,'February','Q1',2024,1),
(20240211,'2024-02-11','Sunday',11,2,'February','Q1',2024,1),
(20240212,'2024-02-12','Monday',12,2,'February','Q1',2024,0),
(20240213,'2024-02-13','Tuesday',13,2,'February','Q1',2024,0),
(20240214,'2024-02-14','Wednesday',14,2,'February','Q1',2024,0);

-- =====================================================
-- INSERT DATA INTO dim_product (15 products, 3 categories)
-- =====================================================

INSERT INTO dim_product
(product_id, product_name, category, subcategory, unit_price)
VALUES
('P001','Samsung Galaxy S21','Electronics','Mobile',45999),
('P002','iPhone 13','Electronics','Mobile',69999),
('P003','HP Laptop','Electronics','Laptop',52999),
('P004','Dell Monitor','Electronics','Monitor',12999),
('P005','Boat Earbuds','Electronics','Accessories',1499),

('P006','Nike Running Shoes','Fashion','Footwear',3499),
('P007','Levis Jeans','Fashion','Clothing',2999),
('P008','Puma Sneakers','Fashion','Footwear',4599),
('P009','Adidas T-Shirt','Fashion','Clothing',1299),
('P010','Reebok Trackpants','Fashion','Clothing',1899),

('P011','Basmati Rice 5kg','Groceries','Grains',650),
('P012','Organic Honey','Groceries','Organic',450),
('P013','Masoor Dal 1kg','Groceries','Pulses',120),
('P014','Organic Almonds','Groceries','Dry Fruits',899),
('P015','Cooking Oil 1L','Groceries','Essentials',180);

-- =====================================================
-- INSERT DATA INTO dim_customer (12 customers, 4 cities)
-- =====================================================

INSERT INTO dim_customer
(customer_id, customer_name, city, state, customer_segment)
VALUES
('C001','Rahul Sharma','Bangalore','Karnataka','Gold'),
('C002','Priya Patel','Mumbai','Maharashtra','Silver'),
('C003','Amit Kumar','Delhi','Delhi','Gold'),
('C004','Sneha Reddy','Hyderabad','Telangana','Silver'),

('C005','Vikram Singh','Jaipur','Rajasthan','Bronze'),
('C006','Anjali Mehta','Ahmedabad','Gujarat','Silver'),
('C007','Ravi Verma','Pune','Maharashtra','Gold'),
('C008','Pooja Iyer','Chennai','Tamil Nadu','Bronze'),

('C009','Karthik Nair','Kochi','Kerala','Silver'),
('C010','Deepa Gupta','Delhi','Delhi','Gold'),
('C011','Arjun Rao','Bangalore','Karnataka','Silver'),
('C012','Neha Shah','Mumbai','Maharashtra','Bronze');

-- =====================================================
-- INSERT DATA INTO fact_sales (40 transactions)
-- =====================================================

INSERT INTO fact_sales
(date_key, product_key, customer_key, quantity_sold, unit_price, discount_amount, total_amount)
VALUES
(20240101,1,1,1,45999,0,45999),
(20240102,6,2,2,3499,0,6998),
(20240103,11,3,3,650,0,1950),
(20240104,2,4,1,69999,2000,67999),
(20240105,7,5,2,2999,0,5998),

(20240106,12,6,4,450,0,1800),
(20240106,3,7,1,52999,0,52999),
(20240107,9,8,3,1299,0,3897),
(20240107,5,9,2,1499,0,2998),
(20240108,14,10,5,899,0,4495),

(20240109,1,11,1,45999,0,45999),
(20240110,8,12,2,4599,0,9198),
(20240111,10,1,2,1899,0,3798),
(20240112,15,2,6,180,0,1080),
(20240113,4,3,1,12999,0,12999),

(20240114,6,4,3,3499,0,10497),
(20240115,13,5,8,120,0,960),
(20240201,2,6,1,69999,3000,66999),
(20240202,11,7,4,650,0,2600),
(20240203,5,8,3,1499,0,4497),

(20240203,1,9,1,45999,0,45999),
(20240204,12,10,2,450,0,900),
(20240205,3,11,1,52999,0,52999),
(20240206,7,12,2,2999,0,5998),
(20240207,8,1,2,4599,0,9198),

(20240208,9,2,3,1299,0,3897),
(20240209,10,3,2,1899,0,3798),
(20240210,15,4,6,180,0,1080),
(20240210,14,5,3,899,0,2697),
(20240211,4,6,1,12999,0,12999),

(20240211,1,7,1,45999,0,45999),
(20240212,6,8,2,3499,0,6998),
(20240212,11,9,4,650,0,2600),
(20240213,2,10,1,69999,0,69999),
(20240213,13,11,6,120,0,720),

(20240214,3,12,1,52999,0,52999),
(20240214,5,1,3,1499,0,4497),
(20240214,8,2,2,4599,0,9198),
(20240214,9,3,2,1299,0,2598),
(20240214,10,4,2,1899,0,3798);