# -*- coding: utf-8 -*-
"""
Created on Thu Mar 30 03:53:01 2023

@author: Ivan
"""

import pandas as pd
import psycopg2

filename='SpaceNK_2.0.xlsx'

def load_excel_data_to_postgres(excel_file_path, db_host, db_port, db_name, db_user, db_password):
    # Read data from the first sheet in Excel file
    df = pd.read_excel(excel_file_path, sheet_name=0, skiprows=5)
    # drop empty columns 
    df.dropna(how='all', axis=1, inplace=True)
    # remove last (Total row)
    df=df[:-1]

    

    #df = df.where(pd.notnull(df), None)

    # Connect to the Postgres database
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password
    )

    # Create a cursor object
    cur = conn.cursor()

    # Create the table (if it doesn't already exist)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS last_week (
            store_no VARCHAR(255) PRIMARY KEY,
            store VARCHAR(255),
            ty_units FLOAT,
            ly_units FLOAT,
            tw_sales FLOAT,
            lw_sales FLOAT,
            lw_var FLOAT,
            ly_sales FLOAT,
            ly_var FLOAT,
            ytd_sales FLOAT,
            lytd_sales FLOAT,
            lytd_var FLOAT
        );
    """)

    # Load the data into the table
    for index, row in df.iterrows():
        cur.execute("""
            INSERT INTO last_week (store_no, store, ty_units, ly_units, tw_sales, lw_sales, lw_var, ly_sales, ly_var, ytd_sales, lytd_sales, lytd_var)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[7],
            row[8],
            row[9],
            row[10],
            row[11]
        ))

    # Commit the changes and close the connection
    conn.commit()
    cur.close()
    conn.close()
    
load_excel_data_to_postgres(
    excel_file_path=filename,
    db_host='localhost',
    db_port=5432,
    db_name='postgres',
    db_user='postgres',
    db_password='1234'
)

