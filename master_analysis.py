import sqlite3
import statistics
import csv

ngst_connection = sqlite3.connect('./newgistics_analyzed.db')
ups_fedex_connection = sqlite3.connect('./ups_fedex_analyzed.db')
ngst_connection.text_factory = str
ups_fedex_connection.text_factory = str

ngst_cursor = ngst_connection.cursor()
ups_fedex_cursor = ups_fedex_connection.cursor()


# possible table names:
# Newgistics - ngst_analyzed
# UPS - ups_packages
# FedEx - fedex_packages
def fetch_avg_transit_days(origin_zip3, destination_zip3, db_cursor, table_name):
    matched_transit_days = [] # store all packages with matched zip codes' transit days
    for row in db_cursor.execute('SELECT * from ' + table_name):
        package_origin_zip = row[3]
        package_destination_zip = row[4]
        package_transit_days = row[5]

        # Find packages that match origin zip and destination zip
        if (package_origin_zip == origin_zip3) and (package_destination_zip == destination_zip3):
            # add matched packages to the matched_transit_days variable
            matched_transit_days.append(int(package_transit_days))
    # Calculate the 'n' 
    num_matched_packages = len(matched_transit_days)
    # calculate the average transit days for matched packages
    average_transit_days = statistics.mean(matched_transit_days)

    return {'avg_transit_days': average_transit_days, 'n': num_matched_packages}

# newgistics_brk_la = fetch_avg_transit_days('112', '072', ngst_cursor, 'ngst_analyzed')
# fedex_sfo_nc = fetch_avg_transit_days('940', '828', ups_fedex_cursor, 'fedex_analyzed')
# fedex_penn_dallas = fetch_avg_transit_days('171', '750', ups_fedex_cursor, 'fedex_analyzed')
# fedex_940_945 = fetch_avg_transit_days('940', '945', ups_fedex_cursor, 'fedex_analyzed')
# ups_940_945 = fetch_avg_transit_days('940', '945', ups_fedex_cursor, 'ups_analyzed')
# ups_171_300 = fetch_avg_transit_days('171', '300', ups_fedex_cursor, 'ups_analyzed')
# fedex_171_300 = fetch_avg_transit_days('171', '300', ups_fedex_cursor, 'fedex_analyzed')

# print(newgistics_brk_la)
# print(fedex_sfo_nc)
# print(fedex_penn_dallas)

# print('Fedex packages from 940 to 945:')
# print(fedex_940_945)
# print('UPS packages from 940 to 945:')
# print(ups_940_945)
# print('Fedex packages from 171 to 300:')
# print(fedex_171_300) 
# print('UPS packages from 171 to 300:')
# print(ups_171_300)



# top 10 sources and destinations for packages for each database
# Find average transit days for each source-destination combination
# Distribution of packages' transit days over 10 days with real data
def raw_transit_days(db_cursor, table_name):
    zip_master = {}
    for row in db_cursor.execute('SELECT * from ' + table_name):
        package_origin_zip = row[3]
        package_destination_zip = row[4]
        package_transit_days = row[5]

        zip_pair = package_origin_zip + package_destination_zip
        if zip_pair in zip_master:
            zip_master[zip_pair]['transit_days'].append(int(package_transit_days))
        else:
            try:
                zip_master[zip_pair] = {
                    'transit_days': [int(package_transit_days)]
                }
            except ValueError:
                continue

    return zip_master

fedex_zips = raw_transit_days(ups_fedex_cursor, 'fedex_analyzed')
ups_zips = raw_transit_days(ups_fedex_cursor, 'ups_analyzed')
newgistics_zips = raw_transit_days(ngst_cursor, 'ngst_analyzed')

# print(fedex_zips)
# print(ups_zips)
# print(newgistics_zips)



def populate_avg_transit_days(master_dictionary):
    for zip_pair in master_dictionary:
        transit_days_list = master_dictionary[zip_pair]['transit_days']
        master_dictionary[zip_pair]['avg_transit_days'] = statistics.mean(transit_days_list)
        master_dictionary[zip_pair]['n'] = len(transit_days_list)
    
    return master_dictionary



def add_transit_day_counter(master_dictionary):
    for zip_pair in master_dictionary:
        transit_days_list = master_dictionary[zip_pair]['transit_days']
        for i in range(40):
            master_dictionary[zip_pair][i] = 0
        for i in range(len(transit_days_list)):
            package_transit_days = transit_days_list[i]
            try:
                master_dictionary[zip_pair][package_transit_days] += 1
            except KeyError:
                continue

    return master_dictionary



def export_avg_transit_days(master_dictionary, file_name):
    with open(file_name, 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=",")
        writer.writerow(['Zip Pair', 'Number of Packages', 'Average Transit Days'])
        for zip_pair in master_dictionary:
            writer.writerow([zip_pair, master_dictionary[zip_pair]['n'], master_dictionary[zip_pair]['avg_transit_days']])



def export_transit_days_distribution(master_dictionary, file_name):
    with open(file_name, 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=",")
        first_row = ['Zip Pair']
        for i in range(40):
            first_row.append(str(i) + ' days')
        writer.writerow(first_row)
        # repeat for each row
        for zip_pair in master_dictionary:
            # start with the first element in the first column, which is the zip pair
            next_row = [zip_pair]
            for i in range(40):
                # There is a relationship between the column number and the number of days
                # take advantage of that relationship between column number and number of days
                # to populate the column according to number of packages delivered in i days.
                # The first element of the row (aka the first column) is always filled
                # with the zip pair
                next_row.append(master_dictionary[zip_pair][i])
            writer.writerow(next_row)


'''
Master Dictionary Structure

master_dictionary = {
    'zip_pair_1': {
        'n':                    total packages,
        'avg_transit_days':     average transit days,
        0:                      NUMBER of packages delivered in 0 days,
        1:                      NUMBER of packages delivered in 1 day,
        2:                      NUMBER of packages delivered in 2 days,
        3:                      ...,
        4:                      ...,
        ...
        39:                     NUMBER of packages delivered in 39 days
    },
    'zip_pair_2': {
        'n':                    total packages,
        'avg_transit_days':     average transit days,
        0:                      NUMBER of packages delivered in 0 days,
        1:                      NUMBER of packages delivered in 1 day,
        2:                      NUMBER of packages delivered in 2 days,
        3:                      ...,
        4:                      ...,
        ...
        39
    },
    'zip_pair_3': {
        'n':                    c
        4:                      ...,
        ...
        39
    },
    ...for all zip pairs
}
'''


export_avg_transit_days(populate_avg_transit_days(fedex_zips), 'fedex_transit_days.csv')
export_avg_transit_days(populate_avg_transit_days(ups_zips), 'ups_transit_days.csv')
export_avg_transit_days(populate_avg_transit_days(newgistics_zips), 'newgistics_transit_days.csv')

export_transit_days_distribution(add_transit_day_counter(fedex_zips), 'fedex_delivery_distribution.csv')
export_transit_days_distribution(add_transit_day_counter(ups_zips), 'ups_delivery_distribution.csv')
export_transit_days_distribution(add_transit_day_counter(newgistics_zips), 'newgistics_delivery_distribution.csv')


