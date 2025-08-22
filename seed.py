import pandas as pd
import mysql.connector
from mysql.connector import errorcode
import os
import time

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
        password=DB_PASSWORD,
        autocommit=False  # Disable autocommit for better batch performance
    )
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    cursor.execute(f"USE {DB_NAME}")
    conn.commit()  # Commit the database creation/selection
except mysql.connector.Error as err:
    print(f"Error: {err}")
    exit(1)

# Function to seed a CSV into a table with batch processing
def seed_table(csv_file, table_name, columns, batch_size=1000):
    start_time = time.time()
    print(f"\n{'='*50}")
    print(f"Loading data from {csv_file}")
    
    try:
        df = pd.read_csv(csv_file)
        total_rows = len(df)
        print(f"Starting to seed {total_rows} rows into {table_name} with batch size {batch_size}")
        
        # Prepare the query template for batch insert
        placeholders = ", ".join(["%s"] * len(columns))
        query_template = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES "
        
        batches_processed = 0
        for start_idx in range(0, total_rows, batch_size):
            batch_start_time = time.time()
            end_idx = min(start_idx + batch_size, total_rows)
            batch_df = df.iloc[start_idx:end_idx]
            
            # Prepare batch data
            batch_values = []
            value_placeholders = []
            
            for _, row in batch_df.iterrows():
                values = tuple(row[col] if not pd.isna(row[col]) else None for col in columns)
                batch_values.extend(values)
                value_placeholders.append(f"({placeholders})")
            
            # Execute batch insert
            if batch_values:
                query = query_template + ", ".join(value_placeholders)
                cursor.execute(query, batch_values)
                conn.commit()
                
                batches_processed += 1
                rows_processed = end_idx
                batch_time = time.time() - batch_start_time
                elapsed_time = time.time() - start_time
                
                print(f"Batch {batches_processed}: {rows_processed}/{total_rows} rows ({(rows_processed/total_rows*100):.1f}%) "
                      f"- Batch time: {batch_time:.2f}s, Total time: {elapsed_time:.2f}s")
        
        total_time = time.time() - start_time
        print(f"✓ Successfully seeded {total_rows} rows into {table_name} in {batches_processed} batches")
        print(f"  Total time: {total_time:.2f}s ({total_rows/total_time:.0f} rows/sec)")
        
    except Exception as e:
        print(f"✗ Error seeding table {table_name}: {e}")
        conn.rollback()
        raise

# Define columns for each table and seed with appropriate batch sizes
print("Starting data seeding process...")
overall_start_time = time.time()

try:
    # Bookings table (likely the largest) - use smaller batch size
    seed_table("./data/bookings_train.csv", "bookings", [
        "booking_id","citizen_id","booking_date","appointment_date","appointment_time",
        "check_in_time","check_out_time","task_id","num_documents","queue_number","satisfaction_rating"
    ], batch_size=500)

    # Staffing table - moderate batch size
    seed_table("./data/staffing_train.csv", "staffing", [
        "date","section_id","employees_on_duty","total_task_time_minutes"
    ], batch_size=1000)

    # Tasks table (smallest) - larger batch size
    seed_table("./data/tasks.csv", "tasks", [
        "task_id","task_name","section_id","section_name"
    ], batch_size=1000)

    overall_time = time.time() - overall_start_time
    print(f"\n{'='*50}")
    print(f"🎉 All seeding completed successfully!")
    print(f"Total execution time: {overall_time:.2f} seconds ({overall_time/60:.1f} minutes)")

except Exception as e:
    print(f"\n❌ Seeding failed: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()
    print("Database connection closed.")
