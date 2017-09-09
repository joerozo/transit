import sqlite3
import datetime
import math
import csv
# import statistics

connection = sqlite3.connect('canals.db')
connection.text_factory = str
cursor = connection.cursor()

holidays = {
    'new_years_day':    '01/01',
    'mlk':              '01/16',
    'presidents_day':   '02/20',
    'memorial_day':     '05/29',
    'independence_day': '07/04',
    'columbus_day':     '10/09',
    'veterans_day':     '11/10',
    'christmas':        '12/25'
}

def extra_sunday(delivery_weekday, ship_weekday):
    if (delivery_weekday - ship_weekday) > 0:
        return 0
    else:
        return 1

def holiday_between(month_start, day_start, month_end, day_end, year):
    for holiday in holidays:
        holiday_month = int(holidays[holiday][0:2])
        holiday_day = int(holidays[holiday][-2:])
        if (holiday_month == month_start) or (holiday_month == month_end):
            # check if holiday in between start and end date
            # does not support edge case of package shipped in December
            # and not arriving until January the next 'year'
            start_datetime = datetime.date(day_start, month_start, year)
            holiday_datetime = datetime.date(holiday_day, holiday_month, year)
            end_datetime = datetime.date(day_end, month_end, year)
            if (start_datetime <= holiday_datetime <= end_datetime):
                return True
            else:
                continue
        else:
            continue
    return False # only called after looping through all holidays

def calculate_total_sundays(days_delta):
    return math.floor(math.fabs(days_delta) / 7)

def reformat_date_string(string_date):
    month_day = string_date[:-2]
    year = '20' + string_date[-2:]
    return month_day + year

def calculate_transit_days(deliv_date_string, ship_date_string, delimiter):
    if (delimiter in deliv_date_string) and (delimiter in ship_date_string):
        formatted_delivery_string = reformat_date_string(deliv_date_string)
        formatted_ship_string = reformat_date_string(ship_date_string)
        deliv_date_object = datetime.datetime.strptime(formatted_delivery_string, '%m' + delimiter + '%d' + delimiter + '%Y')
        ship_date_object = datetime.datetime.strptime(formatted_ship_string, '%m' + delimiter + '%d' + delimiter + '%Y')
        # # test to print weekday
        # print(deliv_date_object.weekday())
        # print(ship_date_object.weekday())

        # uncomment below code to make sure that delivery date is always
        # after the ship date (should always return True)
        # print(ship_date_object < deliv_date_object)

        total_days = (deliv_date_object - ship_date_object).days
        # check that the total days makes sense
        # print(total_days)
        min_sundays = calculate_total_sundays(total_days)

        # # calculate total sundays
        total_sundays = min_sundays + extra_sunday(deliv_date_object.weekday(), ship_date_object.weekday())
        # check if extra sunday matches up
        # print(total_sundays) 
        if holiday_between(ship_date_object.month, ship_date_object.day, deliv_date_object.month, deliv_date_object.day, ship_date_object.year) is True:
            transit_days = total_days - total_sundays - 1
        else:
            transit_days = total_days - total_sundays

        return transit_days

    else:
        pass

def analyzer(table_name):
    package_dictionary = {} # store package data inside this dictionary

    ### Define index numbers from database ###
    # change the following numbers in case data columns change
    # the index numbers are column-name-independent
    ship_date_index = 6
    delivery_date_index = 3
    origin_zip_index = 0
    recipient_zip_index = 8
    tracking_number_index = 2
    ### End column index definitions ###

    for row in cursor.execute('SELECT * from ' + table_name):
        ### organize data by zip pairs ###
        origin_zip3 = row[origin_zip_index][:3]
        destination_zip3 = row[recipient_zip_index][:3]

        try:
            total_transit_days = calculate_transit_days(row[delivery_date_index], row[ship_date_index], '/')
            # test to make sure transit days prints properly
            # print(total_transit_days)

        # handle exception for edge case b/w Dec to Jan of next year
        except ValueError:
            continue
        
        ### start formatting data for export ###
        tracking_number = row[tracking_number_index]
        # calculate carrier based on table name
        # edit following code block to add new carrier
        if table_name == 'ups_packages':
            carrier = 'UPS'
        elif table_name == 'fedex_packages':
            carrier = 'FedEx'
        else:
            carrier = 'Unknown Carrier'
        # end carrier calculation

        # add tracking number as unique key to dictionary
        package_dictionary[tracking_number] = {
            'ship_date': row[ship_date_index],
            'delivery_date': row[delivery_date_index],
            'origin_zip3': origin_zip3,
            'recipient_zip3': destination_zip3,
            'transit_days': total_transit_days,
            'carrier': carrier
        }

    # function has done its job
    # note that it doesn't account for edge case for packages sent in December
    # and delivered in January
    # edit the 'holiday_between' function to solve this exception
    return package_dictionary

ups_analyzed = analyzer('ups_packages')
fedex_analyzed = analyzer('fedex_packages')

def export_to_csv(package_dictionary):
    # optionally, print the dictionary for data validation
    # print(package_dictionary)
    first_key = list(package_dictionary)[0]

    with open(package_dictionary[first_key]['carrier'] + '_analyzed.csv', 'w') as csvfile:
        filewriter = csv.writer(csvfile, delimiter=',')
        filewriter.writerow(['Tracking Number', 'Ship Date', 'Delivery Date', 'Origin 3-Digit Zip', 'Recipient 3-Digit Zip', 'Transit Days', 'Carrier'])

        # map individual properties below from dictionary to csv
        # 'ship_date'
        # 'delivery_date'
        # 'origin_zip3'
        # 'recipient_zip3'
        # 'transit_days'
        # 'carrier'
        for tracking_number in package_dictionary:
            package = package_dictionary[tracking_number]
            filewriter.writerow([tracking_number, package['ship_date'], package['delivery_date'], package['origin_zip3'], package['recipient_zip3'], package['transit_days'], package['carrier']])

export_to_csv(ups_analyzed)
export_to_csv(fedex_analyzed)

# print(zips_sending)
# print(zips_receiving)

# table names
# ups_packages
# fedex_packages

connection.close()