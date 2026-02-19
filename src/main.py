import pandas as pd
import sqlite3
from datetime import datetime

# Get the current date and time
current_time = datetime.now()

# Format the time into a string suitable for a filename
# Format: YYYYMMDD_HHMMSS (e.g., "20260118_191830")
timestamp_str = current_time.strftime("%Y%m%d_%H%M%S")
# Specify the path to your JSON file
sqlite_conn = sqlite3.connect("sample.db")
# cursor = sqlite_conn.cursor()
# cursor.execute("CREATE TABLE IF NOT EXISTS logs (message text)")



# Read the JSON file into a DataFrame
df = pd.read_sql_query("SELECT * from users limit 10", sqlite_conn)
sqlite_conn.close()

# Output DataFrame to CSV
output_csv_file = f'/tmp/output_{timestamp_str}.csv'
#s3_path = f's3://kethan-gp-artifact/airflow_data/output_{timestamp_str}.csv'
#df.to_csv(s3_path, index=False)
df.to_csv(output_csv_file, index=False)

