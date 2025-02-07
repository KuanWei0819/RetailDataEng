import pyodbc
import pandas as pd
import logging 


def get_db_connection():
    """Establishes and returns a database connection."""
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost,1433;"
        "DATABASE=SalesDW;"
        "UID=sa;"
        "PWD=StrongP@ssword123;"
    )

def test_odbc_connection():
    """Tests if the ODBC connection to SQL Server is successful."""
    try:
        with get_db_connection() as conn:
            print("✅ ODBC Connection successful!")
    except Exception as e:
        print(f"❌ ODBC Connection failed: {e}")

def execute_sql(sql):
    """Executes a given SQL query on SQL Server."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                conn.commit()
    except Exception as e:
        print(f"❌ SQL Execution failed: {e}")

def load_table(csv_file, table_name, pk_column):
    """Loads data from a CSV file into a SQL Server table, avoiding duplicate key errors."""
    try:
        data = pd.read_csv(csv_file)

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                for _, row in data.iterrows():
                    # Check if record exists (avoids duplicate key error)
                    check_query = f"SELECT COUNT(*) FROM {table_name} WHERE {pk_column} = ?"
                    cursor.execute(check_query, row[pk_column])
                    exists = cursor.fetchone()[0]

                    if exists:
                        # If record exists, update it (or ignore)
                        update_query = f"""
                        UPDATE {table_name} 
                        SET {', '.join([f"{col} = ?" for col in row.index if col != pk_column])} 
                        WHERE {pk_column} = ?
                        """
                        cursor.execute(update_query, tuple(row[col] for col in row.index if col != pk_column) + (row[pk_column],))
                    else:
                        # If record does not exist, insert it
                        columns = ", ".join(row.index)
                        placeholders = ", ".join(["?" for _ in row.index])
                        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                        cursor.execute(insert_query, tuple(row))

            conn.commit()
        logging.info(f"✅ Data loaded into {table_name} from {csv_file}.")
    except Exception as e:
        logging.error(f"❌ Failed to load {table_name}: {e}")