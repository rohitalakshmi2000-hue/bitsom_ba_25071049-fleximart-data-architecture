NoSQL Justification Report – FlexiMart Product Catalog

                           //  SECTION - A //

Introduction ;

FlexiMart is planning to expand its product catalog to include a wide variety of products such as electronics, clothing, footwear, accessories, groceries, and more. Each product category has its own unique attributes, and the platform also wants to store customer reviews, ratings, and feedback efficiently.

Currently, FlexiMart uses a relational database (RDBMS) like MySQL, which follows a rigid schema design. As the diversity and complexity of product data increases, it becomes important to evaluate whether a NoSQL database like MongoDB would be more suitable.

This document analyzes the limitations of RDBMS, explains the benefits of MongoDB, and discusses the trade-offs involved in switching from MySQL to MongoDB.

Section A: Limitations of RDBMS ;

Relational databases are highly reliable and structured, but they face several challenges when handling highly diverse and evolving data like a modern e-commerce product catalog.

1. Difficulty Handling Products with Different Attributes

In an RDBMS, all records in a table must follow the same schema. This becomes a major limitation when products have category-specific attributes.

Example:

A laptop requires attributes like:

RAM

Processor

Storage

Battery life

A shoe requires attributes like:

Size

Color

Material

Gender

In MySQL, this would force FlexiMart to:

Either create many nullable columns (most of which remain unused)

Or create separate tables for each product type, increasing complexity

Both approaches lead to:

Wasted storage

Poor readability

Complex joins during querying

2. Frequent Schema Changes for New Product Types

As FlexiMart adds new product categories, the relational schema must be altered frequently.

Example:

If FlexiMart adds:

Smartwatches → requires sensors, OS, battery type

Furniture → requires dimensions, weight, material

In MySQL, this means:

ALTER TABLE products ADD COLUMN sensor_type VARCHAR(50);
ALTER TABLE products ADD COLUMN dimensions VARCHAR(100);


Frequent schema changes cause:

Downtime during schema migrations

Increased risk of errors

Difficulty maintaining backward compatibility

This makes RDBMS less agile for fast-growing product catalogs.

3. Poor Support for Nested Data (Customer Reviews)

Customer reviews naturally form nested and hierarchical data.

Each product can have:

Multiple reviews

Each review with user details, rating, comment, and timestamp

In MySQL, this requires:

A separate reviews table

Foreign key relationships

JOIN operations to fetch product + reviews

Problems:

Complex queries

Performance overhead from joins

Harder data modeling for deeply nested structures

4. Scalability Challenges

Relational databases typically scale vertically (adding more CPU/RAM to a single server).

For FlexiMart:

Increasing product catalog

High read/write traffic

Growing review data

Vertical scaling:

Is expensive

Has hardware limits

Causes performance bottlenecks under heavy load

Section B: Benefits of MongoDB ;

MongoDB is a NoSQL document-based database designed to handle flexible, large-scale, and evolving data structures.
 
                     //  SECTION - B //

1. Flexible Schema (Document-Oriented Model)

MongoDB stores data as JSON-like documents, allowing each product to have its own structure.

Example:
{
  "product_name": "Dell Inspiron",
  "category": "Laptop",
  "ram": "16GB",
  "processor": "Intel i7",
  "storage": "512GB SSD"
}

{
  "product_name": "Nike Running Shoes",
  "category": "Footwear",
  "size": 9,
  "color": "Black",
  "material": "Mesh"
}


Benefits:

No fixed schema

No unused fields

Easy addition of new product types

Faster development and deployment

2. Embedded Documents for Reviews

MongoDB allows embedding related data inside a single document.

Example:
{
  "product_name": "iPhone 15",
  "category": "Mobile",
  "price": 80000,
  "reviews": [
    {
      "customer": "Rama",
      "rating": 5,
      "comment": "Excellent performance",
      "date": "2025-12-01"
    },
    {
      "customer": "Anil",
      "rating": 4,
      "comment": "Good camera quality",
      "date": "2025-12-05"
    }
  ]
}


Advantages:

No joins required

Faster read performance

Natural data representation

Easy retrieval of product + reviews in one query

3. Easy Schema Evolution

MongoDB allows adding new fields without modifying existing documents.

Example:

Add warranty_period only for electronics

Add fabric_type only for clothing

No ALTER commands required, making MongoDB ideal for frequent changes.

4. Horizontal Scalability

MongoDB supports horizontal scaling (sharding).

Benefits:

Data distributed across multiple servers

Handles high traffic efficiently

Improved fault tolerance

Cost-effective scaling as FlexiMart grows

This makes MongoDB well-suited for large e-commerce platforms.

Section C: Trade-offs of Using MongoDB ;

While MongoDB offers flexibility, it also comes with certain disadvantages.

                      //  SECTION - C //

1. Limited Transaction Support Compared to MySQL

Although MongoDB supports transactions, MySQL is still stronger for:

Complex multi-table transactions

Financial operations

Strong ACID compliance

For operations like:

Order payments

Inventory accounting

MySQL may be more reliable.

2. Data Duplication and Storage Overhead

Because MongoDB embeds data:

Some information may be duplicated

Updates must be handled carefully

Example:

Customer name stored in multiple review documents

Requires updating in multiple places if data changes

This can increase storage usage and maintenance complexity.



