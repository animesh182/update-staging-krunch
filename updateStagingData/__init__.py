import logging
import datetime
import azure.functions as func
import psycopg2
import psycopg2.extras
from psycopg2 import sql
from params import production, staging

table_date_columns = {
    'Predictions_employeeinfo':  ['created_at'],
    'Predictions_avgemployeecost':  ['created_at'],
    'Predictions_employeecostandhoursinfo':  ['date'],
    'Predictions_holidayparameters':  ['date', 'created_at'],
    'Predictions_hourlyavgemployeecost':  ['created_at'],
    'Predictions_hourlyemployeecostandhoursinfo':  ['date'],
    'DailyHistoricalMasterTable': ['gastronomic_day'],
    'Predictions_alcoholprediction':  ['date'] ,
    'Predictions_typeprediction':  ['date', 'created_at'],
    'DailyGroupAggregation': ['gastronomic_day'],  
    'HistoricalMasterTable':  ['gastronomic_day', 'date'],
    'Predictions_predictions':   ['date', 'created_at'],
    'Predictions_predictionsbyhour':   ['date', 'created_at'],
    'Predictions_productmixprediction':   ['date', 'created_at'],
    'Predictions_supergroupprediction':   ['date', 'created_at'],
    'SalesData': ['date', 'gastronomic_day']
}

# Assume all tables are to be processed without date-based filtering
tables_to_process = [
    'Predictions_employeeinfo',
    # 'Predictions_avgemployeecost',
    'Predictions_employeecostandhoursinfo',
    'Predictions_holidayparameters',
    # 'Predictions_hourlyavgemployeecost',
    'Predictions_hourlyemployeecostandhoursinfo',
    'DailyHistoricalMasterTable',
    'Predictions_alcoholprediction',
    'Predictions_typeprediction',
    'DailyGroupAggregation',  
    'HistoricalMasterTable',
    'Predictions_predictions',
    'Predictions_predictionsbyhour',
    # 'Predictions_productmixprediction',
    'Predictions_supergroupprediction',
    'SalesData',
]

def main(myTimer: func.TimerRequest) -> None:
    logging.info("Function execution started.")

    prod_conn = psycopg2.connect(**production)
    staging_conn = psycopg2.connect(**staging)
    prod_cur = prod_conn.cursor()
    staging_cur = staging_conn.cursor()

    # Determine the cutoff date for recent data
    latest_date = datetime.datetime.utcnow() - datetime.timedelta(days=1)

    for table_name in tables_to_process:
        process_table(prod_cur, staging_cur, table_name, latest_date)

    prod_cur.close()
    staging_cur.close()
    prod_conn.close()
    staging_conn.close()
    logging.info("Data synchronization completed successfully.")

def process_table(prod_cur, staging_cur, table_name, latest_date):
    logging.info(f"Processing table: {table_name}")

    # Determine the date column to use for the current table
    date_columns = table_date_columns.get(table_name, [])
    date_column = None
    for col in date_columns:
        prod_cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s AND column_name = %s", (table_name, col))
        if prod_cur.fetchone():
            date_column = col
            break

    # If a date column is identified, use it to filter the data; otherwise, fetch all data
    if date_column:
        query = sql.SQL("SELECT * FROM public.{table} WHERE {date_column} >= %s ORDER BY {date_column} DESC").format(
            table=sql.Identifier(table_name), date_column=sql.Identifier(date_column))
        prod_cur.execute(query, (latest_date,))
    else:
        logging.info(f"No specific date column found for table: {table_name}, fetching all available data.")
        query = sql.SQL("SELECT * FROM public.{table}").format(table=sql.Identifier(table_name))
        prod_cur.execute(query)

    rows = prod_cur.fetchall()

    if rows:
        columns = [desc[0] for desc in prod_cur.description]
        # Prepare data for execute_batch: convert each row into a tuple that matches the placeholders in the query
        data_for_insert = [tuple(row) for row in rows]

        # Construct the INSERT query
        insert_query = sql.SQL("INSERT INTO public.{table} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING").format(
            table=sql.Identifier(table_name),
            columns=sql.SQL(', ').join(map(sql.Identifier, columns)),
            placeholders=sql.SQL(', ').join(sql.Placeholder() * len(columns))
        )

        # Execute the batch insert operation
        psycopg2.extras.execute_batch(staging_cur, insert_query.as_string(staging_cur.connection), data_for_insert)
        staging_cur.connection.commit()
