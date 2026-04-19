# STEP 0

# SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database (assuming 'classicmodels.db' is in the same directory)
conn = sqlite3.connect('classicmodels.db')

# Query for df_boston: Join employees and offices, filter by city using WHERE
query_boston = """
SELECT e.firstName, e.lastName
FROM employees e
JOIN offices o ON e.officeCode = o.officeCode
WHERE o.city = 'Boston'
"""
df_boston = pd.read_sql(query_boston, conn)

# Query for df_zero_emp: Group by officeCode, filter aggregates using HAVING (returns 0 rows as no office has 0 employees)
query_zero_emp = """
SELECT officeCode, COUNT(employeeNumber) as num_emp
FROM employees
GROUP BY officeCode
HAVING num_emp = 0
"""
df_zero_emp = pd.read_sql(query_zero_emp, conn)

# Query for df_employee: Left join employees and offices, retain all from employees
query_employee = """
SELECT e.firstName, e.lastName, e.employeeNumber, o.city
FROM employees e
LEFT JOIN offices o ON e.officeCode = o.officeCode
ORDER BY e.firstName
"""
df_employee = pd.read_sql(query_employee, conn)

# Query for df_contacts: Left join customers and orders, filter for customers with no orders
query_contacts = """
SELECT c.contactFirstName, c.contactLastName, c.customerNumber, c.country
FROM customers c
LEFT JOIN orders o ON c.customerNumber = o.customerNumber
WHERE o.orderNumber IS NULL
ORDER BY c.contactFirstName
"""
df_contacts = pd.read_sql(query_contacts, conn)

# Query for df_payment: Cast amount to REAL for proper sorting
query_payment = """
SELECT c.contactFirstName, p.checkNumber, CAST(p.amount AS REAL) as amount, p.paymentDate
FROM payments p
JOIN customers c ON p.customerNumber = c.customerNumber
ORDER BY CAST(p.amount AS REAL) DESC
"""
df_payment = pd.read_sql(query_payment, conn)

# Query for df_credit: Join, group, filter with HAVING, sort
query_credit = """
SELECT c.contactFirstName as firstName, c.contactLastName as lastName, c.creditLimit, COUNT(o.orderNumber) as num_orders
FROM customers c
JOIN orders o ON c.customerNumber = o.customerNumber
GROUP BY c.customerNumber
HAVING COUNT(o.orderNumber) > 5
ORDER BY c.creditLimit DESC
"""
df_credit = pd.read_sql(query_credit, conn)

# Query for df_product_sold: Join, group, aggregate, sort
query_product_sold = """
SELECT p.productCode, p.productName, SUM(od.quantityOrdered) as totalunits
FROM products p
JOIN orderdetails od ON p.productCode = od.productCode
GROUP BY p.productCode
ORDER BY totalunits DESC
"""
df_product_sold = pd.read_sql(query_product_sold, conn)

# Query for df_total_customers: Multiple joins, distinct, group, sort
query_total_customers = """
SELECT p.productCode, p.productName, COUNT(DISTINCT c.customerNumber) as numpurchasers
FROM products p
JOIN orderdetails od ON p.productCode = od.productCode
JOIN orders o ON od.orderNumber = o.orderNumber
JOIN customers c ON o.customerNumber = c.customerNumber
GROUP BY p.productCode
ORDER BY numpurchasers DESC
"""
df_total_customers = pd.read_sql(query_total_customers, conn)

# Query for df_customers: Group by country, count customers
query_customers = """
SELECT country, COUNT(customerNumber) as n_customers
FROM customers
GROUP BY country
ORDER BY n_customers DESC
"""
df_customers = pd.read_sql(query_customers, conn)

# Query for df_under_20: Subquery to filter customers who ordered products with buyPrice < 20
query_under_20 = """
SELECT c.customerNumber, c.contactFirstName as firstName, c.contactLastName, c.city, c.country
FROM customers c
WHERE c.customerNumber IN (
    SELECT o.customerNumber
    FROM orders o
    WHERE o.orderNumber IN (
        SELECT od.orderNumber
        FROM orderdetails od
        WHERE od.productCode IN (
            SELECT p.productCode
            FROM products p
            WHERE p.buyPrice < 20
        )
    )
)
ORDER BY c.contactFirstName
"""
df_under_20 = pd.read_sql(query_under_20, conn)

# Close the connection
conn.close()