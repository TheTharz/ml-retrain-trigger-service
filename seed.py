import pandas as pd
import mysql.connector
from mysql.connector import errorcode
import os

# Database connection
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "userpass")
DB_NAME = os.getenv("DB_NAME", "bookings_db")

try:
    conn = mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD
    )
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    cursor.execute(f"USE {DB_NAME}")
except mysql.connector.Error as err:
    print(f"Error: {err}")
    exit(1)

# Function to seed a CSV into a table
def seed_table(csv_file, table_name, columns):
    df = pd.read_csv(csv_file)
    for _, row in df.iterrows():
        values = tuple(row[col] if not pd.isna(row[col]) else None for col in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        cursor.execute(query, values)
    print(f"Seeded {len(df)} rows into {table_name}")

# Define columns for each table
seed_table("./data/bookings_train.csv", "bookings", [
    "booking_id","citizen_id","booking_date","appointment_date","appointment_time",
    "check_in_time","check_out_time","task_id","num_documents","queue_number","satisfaction_rating"
])

seed_table("./data/staffing_train.csv", "staffing", [
    "date","section_id","employees_on_duty","total_task_time_minutes"
])

seed_table("./data/tasks.csv", "tasks", [
    "task_id","task_name","section_id","section_name"
])

cursor.close()
conn.close()
print("Seeding completed successfully!")
