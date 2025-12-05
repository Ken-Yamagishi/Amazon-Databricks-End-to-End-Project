--
--
--	Amazon End-to-End Project using Databricks (Free Edition)
--
--


--
--	Creating Table Query
--

--create table customer
CREATE TABLE customer(
  customer_id varchar(255) PRIMARY KEY,
  customer_name varchar(255)
);

--create table product
CREATE TABLE product(
  product_id varchar(255) PRIMARY KEY,
  product_name varchar(255),
  category varchar(255),
  brand varchar(255)
);

--create table location
CREATE TABLE location(
  location_id varchar(255) PRIMARY KEY,
  city varchar(255),
  state varchar(255)
);

--create table payment method
CREATE TABLE payment_method(
  payment_method_id varchar(255) PRIMARY KEY,
  payment_method_name varchar(255)
);

--create table order status
CREATE TABLE order_status(
  order_status_id varchar(255) PRIMARY KEY,
  order_status_name varchar(255)
);

--create seller table
CREATE TABLE seller(
  seller_id varchar(255) PRIMARY KEY
);

--create table order
CREATE TABLE order(
  order_id varchar(255) PRIMARY KEY,
  order_date DATE,
  customer_id VARCHAR(255),
  seller_id VARCHAR(255),
  location_id VARCHAR(255),
  payment_method_id VARCHAR(255),
  order_status_id VARCHAR(255),
  FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
  FOREIGN KEY (location_id) REFERENCES location(location_id),
  FOREIGN KEY (payment_method_id) REFERENCES payment_method(payment_method_id),
  FOREIGN KEY (order_status_id) REFERENCES order_status(order_status_id),
  FOREIGN KEY (seller_id) REFERENCES seller(seller_id)
);


--create order_items table
CREATE TABLE order_items(
  order_item_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  order_id VARCHAR(255),
  product_id VARCHAR(255),
  quantity int,
  unit_price int,
  discount int,
  tax int,
  shipment_cost int,
  total_amount int,
  FOREIGN KEY (order_id) REFERENCES order(order_id),
  FOREIGN KEY (product_id) REFERENCES product(product_id)
);

--creating location table with auto increment
CREATE TABLE location (
  location_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  city VARCHAR(255),
  state VARCHAR(255)
);

--creating payment method table with auto increment
CREATE TABLE payment_method (
  payment_method_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  payment_method_name VARCHAR(255)
);

--creating order statu table with auto increment
CREATE TABLE order_status (
  order_status_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  order_status_name VARCHAR(255)
);


--
--	Insert Data Query
--

--insert customers data to amazon_raw table
INSERT INTO customer (customer_id, customer_name)
SELECT DISTINCT CustomerID, CustomerName
FROM amazon_raw;

--insert product data to amazon_raw table
INSERT INTO product (product_id, product_name,category, brand)
SELECT DISTINCT ProductID, ProductName, Category, Brand
FROM amazon_raw

--insert payment_method data to amazon_raw table
INSERT INTO payment_method (payment_method_name)
SELECT DISTINCT PaymentMethod
FROM amazon_raw

--insert order_status data to amazon_raw table
INSERT INTO order_status (order_status_name)
SELECT DISTINCT OrderStatus
FROM amazon_raw

--insert seller data to amazon_raw table
INSERT INTO seller (seller_id)
SELECT DISTINCT SellerID
FROM amazon_raw

--insert location data tp amazon raw table
INSERT INTO location (city, state, country)
SELECT DISTINCT City, State, Country
FROM amazon_raw

--insert data to order table from amazon raw and normalized tables
INSERT INTO order (
    order_id,
    order_date,
    customer_id,
    seller_id,
    location_id,
    payment_method_id,
    order_status_id
)
SELECT
    a.OrderID,
    a.OrderDate,
    a.CustomerID,     -- already normalized
    a.SellerID,       -- already normalized
    l.location_id,
    pm.payment_method_id,
    os.order_status_id
FROM amazon_raw a
JOIN location l
    ON a.City = l.city
   AND a.State = l.state
   AND a.Country = l.country
JOIN payment_method pm
    ON a.PaymentMethod = pm.payment_method_name
JOIN order_status os
    ON a.OrderStatus = os.order_status_name;

--inster date to order_item from amazon raw and normalized tables
INSERT INTO order_items (
    order_id,
    product_id,
    quantity,
    unit_price,
    discount,
    tax,
    shipment_cost,
    total_amount
)

SELECT
    o.order_id,
    a.ProductID,      -- already normalized
    a.Quantity,
    a.UnitPrice,
    a.Discount,
    a.Tax,
    a.ShippingCost,
    a.TotalAmount
FROM amazon_raw a
JOIN order o
    ON a.OrderID = o.order_id
JOIN product p
    ON a.ProductID = p.product_id;
  

--
--	Alter Table Query
--

--add country column to location table
ALTER TABLE location ADD COLUMN country STRING;

--delete table location, order status, payment method
DROP TABLE location
DROP TABLE order_status
DROP TABLE payment_method

--delet table order items
DROP TABLE order_items


--
--	Data Analysis Query
--

--SELECT amazon raw
SELECT *
FROM amazon_raw
LIMIT 10;