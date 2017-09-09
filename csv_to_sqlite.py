import sqlite3
import csv

# this script processes the csv's

connection = sqlite3.connect('ups_fedex_analyzed.db')
connection.text_factory = str
cursor = connection.cursor()

def export_analyzed_sqlite3(file_name, table_name):
    query_columns = ''' (
        tracking_number, 
        ship_date,
        delivery_date,
        origin_zip3, 
        recipient_zip3,
        transit_days,
        carrier
    )'''
    query = 'CREATE TABLE ' + table_name + query_columns
    cursor.execute(query)

# with takes care of opening and closing files
# with replaces try/catch loops  --> simply opens
# a file and processes tis contents

    with open(file_name, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            query_type = 'INSERT INTO '
            fields = ' VALUES (?, ?, ?, ?, ?, ?, ?)'
            query = query_type + table_name + fields
            cursor.executemany(query, (row,))

export_analyzed_sqlite3('FedEx_analyzed.csv', 'fedex_analyzed')
export_analyzed_sqlite3('UPS_analyzed.csv', 'ups_analyzed')

# # test function for printing all rows in DB
# def print_all_rows(table_name):
#     for row in cursor.execute('SELECT * from ' + table_name):
#         print(row)

# print_all_rows('fedex_analyzed')
# print_all_rows('ups_analyzed')

connection.commit()
connection.close()