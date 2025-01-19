import csv
import sqlite3
import pandas as pd


def create_db(db_file):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS data
    ''')

def insert_data_0(csv_file, db_file):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    df = pd.read_csv(csv_file)

    products = df['product'].unique()
    for product in products: 
        c.execute('INSERT OR IGNORE INTO product (name) VALUES (?)', (product,))

    conn.commit()

    product_id = {row[1]: row[0] for row in c.execute('SELECT id, name FROM product').fetchall()}

    for _, row in df.iterrows():
        c.execute('''
                  INSERT INTO shipment (quantity, origin, destination)
            VALUES (?, ?, ?, ?)
        ''', (
            product_id[row['product_quantity']], 
            row['origin_warehouse'], 
            row['date'],
            row['destination_store']
        ))
    
    conn.commit()
    conn.close()
    print('Data 0 done')

def insert_data_1_2(csv_file_1, csv_file_2, db_file):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    df1 = pd.read_csv(csv_file_1)
    df2 = pd.read_csv(csv_file_2)

    group_df1 = df1.groupby(['shipment_identifier', 'product'], as_index=False).agg({
        'quantity': 'sum'
    })

    merged_df = pd.merge(group_df1, df2, on='shipment_identifier', how='inner')

    products = merged_df['product'].unique()
    for product in products: 
        c.execute('INSERT OR IGNORE INTO product (name) VALUES (?)', (product,))

    conn.commit()

    product_map = {row[1]: row[0] for row in c.execute('SELECT id, name FROM product').fetchall()}

    for _, row in merged_df.iterrows():
        c.execute('''
            INSERT INTO shipments (shipment_identifier, Product)
            VALUES (?, ?, ?)
        ''', (
            row['shipment_identifier'],
            row['product']
        ))





