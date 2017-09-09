import sqlite3
import datetime
import math
import csv
import statistics
import numpy as np
import matplotlib.pyplot as plt

connection = sqlite3.connect('canals.db')
connection.text_factory = str
cursor = connection.cursor()

properties_of_interest = [
    'tracking_number',
    'ref_number',
    'delivery_date',
    'origin_address',
    'origin_city',
    'origin_state',
    'origin_zip',
    'ship_date_time',
    'recipient_city',
    'recipient_state',
    'recipient_zip'
]

# Uncomment the following lines to debug and make sure 
# number of rows matches expected number of rows in DB

# counter = 0

# # make sure the data in DB loaded properly
# for row in cursor.execute('SELECT * FROM ngst_packages'):
#     counter += 1

# print(counter)



def convert_to_datetime(date_string):
    date_list = date_string.split('-')
    try:
        dtdate = datetime.date(int(date_list[0]), int(date_list[1]), int(date_list[2]))
        return dtdate
    except:
        # print(date_list)
        pass

def holiday_in_transit(delivery_dt, ship_dt):
    # not taking into account Thanksgiving or Labor day, which shift each year
    list_of_holidays_2017 = {
        'new_years_day':    datetime.date(2017, 1, 1),
        'mlk':              datetime.date(2017, 1, 16),
        'presidents_day':   datetime.date(2017, 2, 20),
        'memorial_day':     datetime.date(2017, 5, 29),
        'independence_day': datetime.date(2017, 7, 4),
        'labor_day':        datetime.date(2017, 9, 4),
        'columbus_day':     datetime.date(2017, 10, 9),
        'veterans_day':     datetime.date(2017, 11, 10),
        'christmas':        datetime.date(2017, 12, 25)
    }

    # for holidays in list of holidays
    # check if a holiday falls in the same month
    # check if that same holiday falls between the delivery and ship dates
    # if it does, return true. If not, return false
    for holiday_name in list_of_holidays_2017:
        # holiday falls in between the ship date and the delivery date
        try:
            if ship_dt <= list_of_holidays_2017[holiday_name] <= delivery_dt:
                return True
                # rest of the function will stop executing
            else:
                continue
        except TypeError:
            continue
    # the default case is to return False
    return False


# Need to be able to account for whether there is an "extra_sunday" during shipment --> ex: say the
# package is in transit for 3 weeks and it got shipped out on a friday and was delivered on a monday...
# 3 weeks in transit total_sundays=3 yet there is still an unaccounted 4th sunday that is not being processed
# def extra_sunday calculates the difference it delivery and ship and if it is negative that means there was
# an extra unaccounted for Sunday - because min_total_sundays accounts for total days in transit where as
# extra_sunday accounts for difference in days of week

def extra_sunday(delivery_weekday, ship_weekday):
    if (delivery_weekday - ship_weekday) > 0:
        return 0
    else:
        return 1

def how_many_transit_days(delivery_dt, ship_dt):
    try:
        total_days = delivery_dt - ship_dt
        day_delivered = delivery_dt.weekday()
        day_shipped = delivery_dt.weekday()
        min_total_sundays = math.floor(total_days.days / 7)
        total_sundays = min_total_sundays + extra_sunday(day_delivered, day_shipped)
        return total_days.days - total_sundays
    except (TypeError or AttributeError):
        pass
    # total_days.days
    # Monday 0
    # Tuesday 1
    # Wednesday 2
    # Thursday 3
    # Friday 4
    # Saturday 5
    # Sunday 6
        # no sunday

def calculate_transit_days(delivery_date, ship_date_time):

    dt_delivery_date = convert_to_datetime(delivery_date)
    dt_ship_date_time = convert_to_datetime(ship_date_time)

    # switch dates to account for bad data so there are no 'negative' transit days
    try:
        if dt_delivery_date < dt_ship_date_time:
            temp_date = dt_delivery_date
            dt_delivery_date = dt_ship_date_time
            dt_ship_date_time = temp_date
    except TypeError:
        pass

    # account for number of Sundays
    transit_days = how_many_transit_days(dt_delivery_date, dt_ship_date_time)

    # account for holiday
    if holiday_in_transit(dt_delivery_date, dt_ship_date_time) is True:
        transit_days -= 1

    if transit_days is None or transit_days < 0:
        pass
    else:
        return transit_days

# calculate_transit_days('2017-12-29', '2017-12-20-00:32:00.0000000-05:00')
# calculate_transit_days('2017-06-01', '2017-05-10-00:32:00.0000000-05:00')
# calculate_transit_days('2017-04-29', '2017-04-20-00:32:00.0000000-05:00')
# calculate_transit_days('2017-03-27', '2017-03-12-00:32:00.0000000-05:00')



zip_code_counter = {
    # '900': {
        # 'sent':     int,
        # 'received': int
    # },
    # 
}

# Iteration algorithms through DB

# Analyzer function for exporting zip codes into a separate file

# def analyzer():
#     for row in cursor.execute('SELECT * FROM ngst_packages'):

#         origin_zip3 = row[6][:3]
#         destination_zip3 = row[10][:3]

#         # check if origin zip exists
#         if origin_zip3 in zip_code_counter:
#             # increment packages sent by 1
#             zip_code_counter[origin_zip3]['sent'] += 1
#         else:
#             # create unique zip code in dictionary with one package sent
#             zip_code_counter[origin_zip3] = {
#                 'sent':     1,
#                 'received': 0   
#             }

#         # check if destination zip exists
#         if destination_zip3 in zip_code_counter:
#             # increment packages received by 1
#             zip_code_counter[destination_zip3]['received'] += 1
#         else:
#             # create unique zip code and receive one package
#             zip_code_counter[destination_zip3] = {
#                 'sent':     0,
#                 'received': 1
#             }

    # # print(zip_code_counter)

    #     # if isinstance(transit_days, int):
    #     #     print(transit_days)
    #     # else:
    #     #     continue

    # export to csv
    # with open('zip_codes.csv', 'w') as csvfile:
    #     zipwriter = csv.writer(csvfile, delimiter=',')
    #     zipwriter.writerow(['First 3 Zip Digits', 'Packages Sent', 'Packages Received'])
    #     # loop through zip code dictionary
    #     for zip3d in zip_code_counter:
    #         zipwriter.writerow([zip3d, zip_code_counter[zip3d]['sent'], zip_code_counter[zip3d]['received']])

# analyzer()

top_sources = ['112', '606', '945', '300', '750']
top_dests = ['900', '460', '072', '303', '894']

top_zip_pairs = {
    '112900': {
        'n': 5934,
        'trans_days': []
        }, 
    '112460': {
        'n': 1476,
        'trans_days': []
        }, 
    '112072': {
        'n': 6336,
        'trans_days': []
        }, 
    '112894': {
        'n': 491,
        'trans_days': []
        }, 
    '606900': {
        'n': 5447,
        'trans_days': []
        }, 
    '606460': {
        'n': 1917,
        'trans_days': []
        }, 
    '606894': {
        'n': 559,
        'trans_days': []
        }, 
    '945900': {
        'n': 9672,
        'trans_days': []
        }, 
    '300900': {
        'n': 5160,
        'trans_days': []
        }, 
    '300303': {
        'n': 5285,
        'trans_days': []
        }, 
    '750900': {
        'n': 5564,
        'trans_days': []
        }
}

# SQLite queries for top zip codes sent + received
# Append transit days to top_zip_pairs list for each zip pair
def zip_filter():
    new_connection = sqlite3.connect('newgistics_analyzed.db')
    new_connection.text_factory = str
    new_cursor = new_connection.cursor()
    query_columns = ''' (
        tracking_number, 
        ship_date,
        delivery_date,
        origin_zip3, 
        recipient_zip3,
        transit_days,
        carrier
    )'''
    query = 'CREATE TABLE ngst_analyzed' + query_columns
    new_cursor.execute(query)

    master_dictionary = {}

    for row in cursor.execute('SELECT * FROM ngst_packages'):
        # can print row for testing purposes
        # print(row)
        # master index. Change if DB changes
        origin_zip3 = row[0][:3]
        destination_zip3 = row[8][:3]
        dv_date = row[3]
        ship_date = row[5]
        tracking_number = row[2]
        # End master index definition
        transit_days = calculate_transit_days(dv_date, ship_date)
        if transit_days is not None:
            # Data cleaning logic for Newgistics
            if tracking_number in master_dictionary:
                # sum transit days to get the total transit days
                old_dv_date = master_dictionary[tracking_number]['delivery_date']
                old_ship_date = master_dictionary[tracking_number]['ship_date']
                master_dictionary[tracking_number]['transit_days'] += transit_days
                # Test which ship date is sooner
                if convert_to_datetime(old_ship_date) > convert_to_datetime(ship_date):
                    # set the stored ship date to the ship date that is older / previous
                    master_dictionary[tracking_number]['ship_date'] = ship_date
                    master_dictionary[tracking_number]['origin_zip3'] = origin_zip3
                # Test which delivery date is later
                if convert_to_datetime(old_dv_date) < convert_to_datetime(dv_date):
                    # set the stored delivery date to the delivery date that is later / recent
                    master_dictionary[tracking_number]['delivery_date'] = dv_date
                    master_dictionary[tracking_number]['destination_zip3'] = destination_zip3
            else:
                master_dictionary[tracking_number] = {
                    'ship_date': ship_date,
                    'delivery_date': dv_date,
                    'origin_zip3': origin_zip3,
                    'recipient_zip3': destination_zip3,
                    'transit_days': transit_days,
                    'carrier': 'Newgistics'
                }
        else:
            continue
    
    for unique_tracking_number in master_dictionary:
        package = master_dictionary[unique_tracking_number]
        filtered_row = [unique_tracking_number, package['ship_date'], package['delivery_date'], package['origin_zip3'], package['recipient_zip3'], package['transit_days'], package['carrier']]
        query_type = 'INSERT INTO ngst_analyzed'
        fields = ' VALUES (?, ?, ?, ?, ?, ?, ?)'
        query = query_type + fields
        new_cursor.executemany(query, (filtered_row,))
    
    # # test to make sure analyzed data uploaded properly
    # for row in new_cursor.execute('SELECT * FROM ngst_analyzed'):
    #     print(row)
    
    new_connection.commit()
    new_connection.close()
        # if (origin_zip3 in top_sources) and (destination_zip3 in top_dests) and (origin_zip3 + destination_zip3 in top_zip_pairs):
            # top_zip_pairs[origin_zip3 + destination_zip3]
            # calculate transit days here
            # calculate transit days based on dv_date and ship_date from DB
            # append transit days to corresponding zip_pair
            # if transit_days is not None:
            #     top_zip_pairs[origin_zip3+destination_zip3]['trans_days'].append(transit_days)

    # # Test to make sure length of 'trans_days' matches n
    # for zip_pair in top_zip_pairs:
    #     print(len(top_zip_pairs[zip_pair]['trans_days']))

zip_filter()

def calculate_average_transit_days(zip_pair_dict):
    # Loop through passed-in dictionary
    for zip_pair in zip_pair_dict:
        # call list of transit days
        transit_days_list = zip_pair_dict[zip_pair]['trans_days']
        zip_pair_dict[zip_pair]['avg_trans_days'] = statistics.mean(transit_days_list)
    return zip_pair_dict

# calculate_average_transit_days(top_zip_pairs)

def poisson(zip_pair_dict):
    for zip_pair in zip_pair_dict:
        print('Average transit days for ', zip_pair, zip_pair_dict[zip_pair]['avg_trans_days'])
        dist = np.random.poisson(zip_pair_dict[zip_pair]['avg_trans_days'], zip_pair_dict[zip_pair]['n'])
        count, bins, ignored = plt.hist(dist, 20, normed=True)
        plt.savefig(zip_pair + '.pdf')
# # Run Poisson distribution by uncommenting the following 
# poisson(top_zip_pairs)


# take the zip pair dictionary and add new key representing "days in transit" for each package
def cumulative_distribution_zips(zip_pair_dict):
    for zip_pair in zip_pair_dict:
        for i in range(len(zip_pair_dict[zip_pair]['trans_days'])):
            package_days = zip_pair_dict[zip_pair]['trans_days'][i]
            if package_days in zip_pair_dict[zip_pair]:
                zip_pair_dict[zip_pair][package_days] += 1
            else:
                zip_pair_dict[zip_pair][package_days] = 1

    # test function to make sure dictionary has 'days' properties
    # for test_zip_pair in zip_pair_dict:
    #     for key in zip_pair_dict[test_zip_pair]:
    #         if type(key) is int:
    #             print(str(zip_pair_dict[test_zip_pair][key]) + 'packages delivered in ' + str(key) + ' days')
    #         else:
    #             continue

# cumulative_distribution_zips(top_zip_pairs)


# export data to csv
def export_trans_days_distribution(zip_pairs):
    with open('transit_days.csv', 'w') as csvfile:
        datawriter = csv.writer(csvfile, delimiter=',')
        for pair_of_zips in zip_pairs:
            # section data by zip pairs
            datawriter.writerow(pair_of_zips)
            datawriter.writerow(['Transit Days', 'Number of Packages'])
            # write number of transit days and number of packages
            for key in zip_pairs[pair_of_zips]:
                if type(key) is int:
                    datawriter.writerow([key, zip_pairs[pair_of_zips][key]])
                else:
                    continue

# export_trans_days_distribution(top_zip_pairs)