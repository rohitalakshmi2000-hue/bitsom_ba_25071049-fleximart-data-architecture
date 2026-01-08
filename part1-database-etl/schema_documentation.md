
Fleximart Documentation Schema

 1. Entity-Relationship Description

1.1  ENTITY: customers ;

Purpose:
Stores customer personal and contact information.

Attributes:

customer_id (Primary Key): Unique identifier for each customer

first_name: Customer’s first name

last_name: Customer’s last name

email: Customer’s email address (unique)

phone: Customer’s phone number in standardized format

city: City where the customer resides

registration_date: Date when the customer registered

Relationships:

One customer can place many orders
(1 : M relationship with orders table).

1.2. ENTITY: products ;

Purpose:
Stores product catalog and inventory details.

Attributes:

product_id (Primary Key): Unique identifier for each product

product_name: Name of the product

category: Product category (standardized)

price: Unit price of the product

stock_quantity: Available inventory quantity

Relationships:

One product can appear in many order items
(1 : M relationship with order_items table).

1.3. ENTITY: orders ;

Purpose:
Stores customer order-level information.

Attributes:

order_id (Primary Key): Unique identifier for each order

customer_id (Foreign Key): References customers.customer_id

order_date: Date when the order was placed

total_amount: Total monetary value of the order

status: Current order status (Completed, Pending, Cancelled)

Relationships:

Each order belongs to one customer

One order can contain many order items.

1.4. ENTITY: order_items

Purpose:
Stores detailed product-level information for each order.

Attributes:

order_item_id (Primary Key): Unique identifier for each order item

order_id (Foreign Key): References orders.order_id

product_id (Foreign Key): References products.product_id

quantity: Number of units ordered

unit_price: Price per unit at time of purchase

subtotal: Calculated as quantity × unit_price

Relationships:

Many order items belong to one order

Many order items reference one product.
    
 
            2. Normalization Explanation (3NF)

The FlexiMart database design follows Third Normal Form (3NF) to ensure data integrity, reduce redundancy, and prevent anomalies.

  ->Third Normal Form (3NF):

The Third Normal Form (3NF) builds on the First (1NF) and Second (2NF) Normal Forms. Achieving 3NF ensures that the database structure is free of transitive dependencies, reducing the chances of data anomalies. Even though tables in 2NF have reduced redundancy compared to 1NF, they may still encounter issues like update anomalies.

A relation is in Third Normal Form (3NF) if it satisfies the following two conditions:

1. It is in Second Normal Form (2NF): This means the table has no partial dependencies (i.e., no non-prime attribute is dependent on a part of a candidate key).

2. There is no transitive dependency for non-prime attributes: In simpler terms, no non-key attribute should depend on another non-key attribute. Instead, all non-key attributes should depend directly on the primary key.

There are no transitive dependencies.A transitive dependency occurs when one non-prime attribute depends on another non-prime attribute rather than depending directly on the primary key. This can create redundancy and inconsistencies in the database.For example, if we have the following relationship between attributes:
A -> B (A determines B)
B -> C (B determines C) 
For example, customer details (name, email, phone) are stored only in the customers table and not repeated in the orders table. Similarly, product price and category are stored only in the products table.

Functional Dependencies:

customer_id → first_name, last_name, email, phone, city

product_id → product_name, category, price, stock_quantity

order_id → customer_id, order_date, total_amount, status

order_item_id → order_id, product_id, quantity, unit_price, subtotal

This design avoids:

Update anomalies by storing each fact in one place

Insert anomalies by allowing independent insertion of customers or products

Delete anomalies by preserving related data even if orders are removed.


                     3. Sample Data Representation

customers ;
customer_id	  first_name	last_name	          email	                 city
1	            Rahul	      Sharma	   rahul.sharma@gmail.com       Bangalore
2	            Priya	      Patel	       priya.patel@yahoo.com        Mumbai

products ;
product_id     product_name	       category	             price	stock_quantity
1	        Samsung Galaxy S21	  Electronics	         45999	  150
2	            Nike             Running Shoes Fashion	  3499	  80

orders ;
order_id	customer_id	    order_date	   total_amount	       status
1	           1	         2024-01-15	       45999	       Completed
2	           2	         2024-01-16	       5998	           Completed

order_items ;
order_item_id	order_id	product_id	quantity	unit_price	subtotal
1	               1	      1	           1	       45999	  45999
2	               2	      2	           2	       2999	      5998

