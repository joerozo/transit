import sqlite3

import csv

# glob is a regex filepath processor (returns files which match a regex expression)
import glob



# Goals: 1) Prediction algorithm

# 2) 



connection = sqlite3.connect('canals.db')

connection.text_factory = str

cursor = connection.cursor()

# class FedEx:



# class UPS:



class Carrier:

    def __init__(self, file_path):

        lines = []

        if '.txt' in file_path:

            for file_name in glob.iglob(file_path):

                with open(file_name) as textfile:

                    for line in textfile:

                        split_row = line.split('","')

                        lines.append(split_row)

        else:

            with open(file_path, 'r') as csvfile:

                reader = csv.reader(csvfile, delimiter=',')

                for line in reader:

                    lines.append(line)

        self.raw_data = lines



    def convert_raw_data(self, properties_of_interest):

        self.properties_of_interest = properties_of_interest

        # for key in properties_of_interest:

        #     print('The property ' + key + ' is in index ' + str(properties_of_interest))



        # This variable will be a 2D list with each item being another list

        # that stores the filtered data without unnecessary columns

        self.filtered_data = []

        for i in range(len(self.raw_data)):

            filtered_row = []

            for key in properties_of_interest:

                column_of_interest = properties_of_interest[key]

                cell = self.raw_data[i][column_of_interest]

                filtered_row.append(cell)

            self.filtered_data.append(filtered_row)



    # refactor this function for other carriers

    def create_db_table(self, table_name):

        self.table_name = table_name

        query_columns = ''' (

            tracking_number, 

            delivery_date,

            origin_city, 

            origin_state, 

            origin_zip, 

            ship_date_time, 

            recipient_city, 

            recipient_state, 

            recipient_zip,

            service_type

        )'''

        query = 'CREATE TABLE ' + table_name + query_columns

        cursor.execute(query)

    

    def send_to_db(self, service_type):

        query_type = 'INSERT INTO '
        fields = ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, "' + service_type + '")'
        for i in range(len(self.filtered_data)):

            query = query_type + self.table_name + fields

            # query = 'INSERT INTO ngst_packages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "Newgistics Parcel Select")'

            cursor.executemany(query, (self.filtered_data[i],))



newgistics = Carrier('./Newgistics/*.txt')

fedex = Carrier('./Fedex.csv')

ups = Carrier('./UPS.csv')



ngst_property_mapper = {

    'tracking_number':  2,

    'delivery_date':    9,

    'origin_city':      14,

    'origin_state':     15,

    'origin_zip':       16,

    'ship_date_time':   20,

    'recipient_city':   25,

    'recipient_state':  26,

    'recipient_zip':    27

}



fedex_property_mapper = {

    'ship_date':        1,

    'tracking_number':  2,

    'origin_city':      3,

    'origin_state':     4,

    'origin_zip':       6,

    'recipient_city':   7,

    'recipient_state':  8,

    'recipient_zip':    10,

    'delivery_date':    16

}



ups_property_mapper = {

    'ship_date':        1,

    'tracking_number':  2,

    'origin_city':      3,

    'origin_state':     4,

    'origin_zip':       6,

    'recipient_city':   7,

    'recipient_state':  8,

    'recipient_zip':    10,

    'delivery_date':    15

}





fedex.convert_raw_data(fedex_property_mapper)

ups.convert_raw_data(ups_property_mapper)



fedex.create_db_table('fedex_packages')

ups.create_db_table('ups_packages')



fedex.send_to_db('Fedex Smartpost')

ups.send_to_db('UPS Surepost')



newgistics.convert_raw_data(ngst_property_mapper)



newgistics.create_db_table('ngst_packages') # comment this out after creating table

newgistics.send_to_db("Newgistics Parcel Select")


# for i in range(len(newgistics.filtered_data)):

#     query = 'INSERT INTO ngst_packages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "Newgistics Parcel Select")'

#     cursor.executemany(query, (newgistics.filtered_data[i],))



connection.commit()

connection.close()
