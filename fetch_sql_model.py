import json
import pandas as pd
import sqlite3
import datetime

# Load Data
try:
    with open("receipts.json", "r") as receipts_file:
        receipts_data = json.load(receipts_file)

    with open("brands.json", "r") as brands_file:
        brands_data = json.load(brands_file)

    with open("users.json", "r") as users_file:
        user_data = json.load(users_file)

    print("JSON files successfully loaded.")
except FileNotFoundError as e:
    print("Error loading files: ", e)
    exit()

receipts_df = pd.json_normalize(receipts_data)
brands_df = pd.json_normalize(brands_data)
users_df = pd.json_normalize(user_data)

print("Data successfully loaded into Pandas DataFrames.")
print("Samples Receipts Data:\n", receipts_df.head())
print()
print("Samples Brands Data:\n", brands_df.head())
print()
print("Samples Users Data:\n", users_df.head())

if "dateScanned.$date" in receipts_df.columns:
    receipts_df["dateScanned"] = receipts_df["dateScanned.$date"]
    receipts_df.drop(columns=["dateScanned.$date"], inplace=True)
else:
    print("Column 'dateScanned.$date' not found; check JSON structure.")

# Drop rows without valid timestamps and check for correct data type
receipts_df = receipts_df.dropna(subset=["dateScanned"])
receipts_df["dateScanned"] = receipts_df["dateScanned"].astype(int)

# Flatten nested list columns
if "rewardsReceiptItemList" in receipts_df.columns:
    receipts_df["rewardsReceiptItemList"] = receipts_df[
        "rewardsReceiptItemList"
    ].astype(str)
else:
    print("Column 'rewardsReceiptItemList' not found, skipping...")


# Connect to SQLite database
conn = sqlite3.connect("data.db")
receipts_df.to_sql("receipts", conn, if_exists="replace", index=False)
brands_df.to_sql("brands", conn, if_exists="replace", index=False)
users_df.to_sql("users", conn, if_exists="replace", index=False)
print("Data successfully written to SQLite database")

# Finding the most recent month from the receipts db. Must convert timestamps for them to be readable.
query_recent_month = """
SELECT MAX(STRFTIME('%Y-%m', dateScanned / 1000, 'unixepoch')) AS latest_month
FROM receipts;
"""

try:
    result = pd.read_sql_query(query_recent_month, conn)
    print("Query result:\n", result)  # This will show what the query is returning
    latest_month = result.iloc[0]["latest_month"]
    print(f"Most recent month: {latest_month}")
except Exception as e:
    print("Error running query:", e)
    conn.close()
    exit()

# # Bring together the receipts and brands table
query_join = f"""
SELECT
    r.'_id.$oid' AS receipt_id,
    r.dateScanned,
    r.totalSpent,
    b.name AS brand_name,
    b.category
FROM receipts r
JOIN brands b
    ON r.'rewardsReceiptItemList' LIKE '%' || b.barcode || '%'
WHERE STRFTIME('%Y-%m', r.dateScanned / 1000, 'unixepoch') = '{latest_month}';
"""

# Query execution and store results in a dataframe
try:
    result_df = pd.read_sql_query(query_join, conn)
    print("Data successfully joined and retrieved")
except Exception as e:
    print("Error executing join query:", e)
    conn.close()
    exit()

# columns_query = "PRAGMA table_info(receipts);"
# try:
#     columns_result = pd.read_sql_query(columns_query, conn)
#     print("Columns in receipts table:", columns_result)
# except Exception as e:
#     print("Error retrieving columns:", e)

# Display results and save to a CSV file
# print("Joined Data:\n", result_df)
# output_file = "joined_data.csv"
# result_df.to_csv(output_file, index=False)
# print(f"Data successfully written to {output_file}.")

# When tables were joined, no result was given because there aren't any commonalities between the receipts file and the brand file to bring them together. Realizing that I would maybe have to manually count the brands of the receipts scanned for the latest month then count how many times each brand showed up I moved on to other queries.


# Average spend from receipts with 'rewardReceiptStatus' of 'accepted' or 'rejected'
# accepted in this case would be the same as 'finished'
# Rationale: wouldn't be able to finish if it wasn't accepted

avg_spend_amnt_query = """
SELECT 
    rewardsReceiptStatus,
    AVG(CAST(NULLIF(totalSpent, '') AS FLOAT)) AS average_spend  -- Calculate the average spend
FROM receipts
WHERE 
    rewardsReceiptStatus IN ('FINISHED', 'REJECTED')   
    AND totalSpent IS NOT NULL  
    AND totalSpent != ''  
GROUP BY rewardsReceiptStatus; 
"""

try:
    result = conn.execute(avg_spend_amnt_query).fetchall()
    print()
    for row in result:
        status, avg_spend = row
        print(f"Status: {status}, Average Spend: ${avg_spend:.2f}")
except Exception as e:
    print("Error executing query:", e)


# Total number of items purchase by those receipts with either 'Accepted' or 'rejected' status.
# accepted in this case would be the same as 'finished'

total_items_purchased_query = """
SELECT 
    rewardsReceiptStatus, 
    SUM(CAST(NULLIF(purchasedItemCount, 0) AS INTEGER)) AS total_items_purchased
FROM receipts
WHERE 
    rewardsReceiptStatus IN ('FINISHED', 'REJECTED') 
    AND purchasedItemCount IS NOT NULL
GROUP BY rewardsReceiptStatus;
"""

try:
    result = conn.execute(total_items_purchased_query).fetchall()
    print()
    for row in result:
        status, total_items = row
        print(f"Status: {status}, Total Items Purchased: {total_items}")
except Exception as e:
    print("Error executing query:", e)

# finally:
#     conn.close()

# Checks for Data quality issues

## Missing values check
missing_val_query = """
SELECT
    COUNT(CASE WHEN totalSpent IS NULL or totalSpent = '' THEN 1 END) AS missing_totalSpent,
    COUNT(CASE WHEN dateScanned IS NULL THEN 1 END) AS missing_dateScanned,
    COUNT(CASE WHEN rewardsReceiptStatus IS NULL or rewardsReceiptStatus = '' THEN 1 END) AS missing_status
FROM receipts;
"""

result = pd.read_sql_query(missing_val_query, conn)
print()
print("Missing Values:\n", result)


# Check for Duplicates in the id column
duplicate_query = """
SELECT '_id.$oid', COUNT(*) AS count
FROM receipts
GROUP BY '_id.$oid'
HAVING COUNT(*) > 1;
"""

duplicates = pd.read_sql_query(duplicate_query, conn)
print()
print("Duplicate Records:\n", duplicates)


# Check for missing or invalid dates
invalid_dates_query = """
SELECT '_id.$oid', dateScanned
FROM receipts
WHERE dateScanned iS NULL OR dateScanned < 0;
"""

invalid_dates = pd.read_sql_query(invalid_dates_query, conn)
print()
print("Invalid Dates:\n", invalid_dates)

# No invalid dates seen

# Ran checks for missing values, duplicate records and invalid dates.

# E-mail to
