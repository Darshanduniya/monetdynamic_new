import psycopg2

# Connect to the Airflow metadata database
conn = psycopg2.connect(
    host="your_host",
    port="your_port",
    database="your_database",
    user="your_user",
    password="your_password"
)

dag_id = "your_dag_id"

# Query the dag_run table
query = """
    SELECT execution_date
    FROM dag_run
    WHERE dag_id = %s
    ORDER BY execution_date DESC
    LIMIT 1
"""

cur = conn.cursor()
cur.execute(query, (dag_id,))
result = cur.fetchone()

if result:
    last_execution_date = result[0]
    print("Last Execution Date:", last_execution_date)
else:
    print("No execution found for the DAG.")

cur.close()
conn.close()
