import csv
import sqlite3
import pandas as pd



def insert_data_0(csv_file, db_file):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    df = pd.read_csv(csv_file)

    products = df['product'].unique()
    for product in products:
        c.execute('INSERT OR IGNORE INTO product (name) VALUES (?)', (product,))

    conn.commit()

    product_id_map = {row[1]: row[0] for row in c.execute('SELECT id, name FROM product').fetchall()}

    for _, row in df.iterrows():
        c.execute('''
            INSERT INTO shipment (product_id, quantity, origin, destination)
            VALUES (?, ?, ?, ?)
        ''', (
            product_id_map[row['product']],  # Map product name to product_id
            row['product_quantity'],        # Quantity
            row['origin_warehouse'],        # Origin warehouse
            row['destination_store']        # Destination store
        ))

    conn.commit()
    conn.close()




def insert_data_1_2(csv_file_1, csv_file_2, db_file):

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    df1 = pd.read_csv(csv_file_1)
    df2 = pd.read_csv(csv_file_2)

    grouped_df1 = df1.groupby(['shipment_identifier', 'product'], as_index=False).agg({'on_time': 'count'})
    grouped_df1.rename(columns={'on_time': 'quantity'}, inplace=True)

    merged_data = pd.merge(grouped_df1, df2, on='shipment_identifier', how='inner')

    products = merged_data['product'].unique()
    for product in products:
        cursor.execute('INSERT OR IGNORE INTO product (name) VALUES (?)', (product,))

    conn.commit()

    product_map = {row[1]: row[0] for row in cursor.execute('SELECT id, name FROM product').fetchall()}

    # Insert data into the shipment table
    for _, row in merged_data.iterrows():
        cursor.execute('''
            INSERT INTO shipment (product_id, quantity, origin, destination)
            VALUES (?, ?, ?, ?)
        ''', (
            product_map[row['product']],
            row['quantity'],
            row['origin_warehouse'],
            row['destination_store']
        ))
    
    conn.commit()
    conn.close()
    print(f"Data from Table 1 and Table 2 processed and inserted into {db_file}.")

db_file = 'shipment_database.db'
spreadsheet_0 = 'data/shipping_data_0.csv'
spreadsheet_1 = 'data/shipping_data_1.csv'
spreadsheet_2 = 'data/shipping_data_2.csv'


insert_data_0(spreadsheet_0, db_file)

insert_data_1_2(spreadsheet_1, spreadsheet_2, db_file)






