# STEP 0

# SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

pd.read_sql("""SELECT * FROM sqlite_master""", conn)

# helper to inspect available tables and read flexible table names
tables = set(pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)['name'])

def find_table(candidates):
    for c in candidates:
        if c in tables:
            return c
    return None

def read_table(candidates):
    t = find_table(candidates)
    return pd.read_sql(f"SELECT * FROM {t}", conn) if t else pd.DataFrame()

# STEP 1
# Replace None with your code
# customers table (used by several steps)
df_customers = read_table(['customers', 'customer', 'clients', 'client'])

# derive Boston customers from customers dataframe if possible
if not df_customers.empty and 'city' in df_customers.columns:
    df_boston = df_customers[df_customers['city'].astype(str).str.contains('Boston', na=False)]
else:
    df_boston = pd.DataFrame()

# STEP 2
# Replace None with your code
# find companies/employers table and filter where employee count == 0
df_companies = read_table(['companies', 'company', 'businesses', 'business'])
df_zero_emp = pd.DataFrame()
if not df_companies.empty:
    for col in ['num_employees', 'employee_count', 'employees', 'employees_count']:
        if col in df_companies.columns:
            df_zero_emp = df_companies[df_companies[col] == 0]
            break

# STEP 3
# Replace None with your code
df_employee = read_table(['employees', 'employee', 'staff'])

# STEP 4
# Replace None with your code
df_contacts = read_table(['contacts', 'contact', 'contact_details'])

# STEP 5
# Replace None with your code
df_payment = read_table(['payments', 'payment', 'payment_methods'])

# STEP 6
# Replace None with your code
df_credit = read_table(['credit', 'credits', 'credit_cards', 'credit_card'])

# STEP 7
# Replace None with your code
df_product_sold = read_table(['product_sold', 'products_sold', 'product_sales', 'product_sold'])

# STEP 8
# Replace None with your code
# total customers per city (if city exists)
if not df_customers.empty and 'city' in df_customers.columns:
    df_total_customers = df_customers.groupby('city').size().reset_index(name='total_customers')
else:
    df_total_customers = pd.DataFrame()

# STEP 9
# Replace None with your code
# df_customers already loaded above; keep a copy
# if it wasn't found, ensure variable exists
if df_customers.empty:
    df_customers = read_table(['customers', 'customer', 'clients', 'client'])

# STEP 10
# Replace None with your code
# customers under 20 (derived from customers if age column exists)
if not df_customers.empty and 'age' in df_customers.columns:
    df_under_20 = df_customers[df_customers['age'] < 20]
else:
    df_under_20 = pd.DataFrame()

conn.close()