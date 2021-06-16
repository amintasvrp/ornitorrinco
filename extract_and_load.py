""" 
Importing dependencies
"""

import json
import boto3
import datetime
import requests
import pandas as pd
from time import sleep
from tabulate import tabulate
from mysql.connector import connect
from configparser import ConfigParser
from botocore.exceptions import NoCredentialsError

""" 
Creating settings.ini file 
"""

SETTINGS = ConfigParser()

SETTINGS['S3_AUTH'] = {'ACCESS_KEY': '', 'SECRET_KEY': ''}
SETTINGS['RDS_SETTINGS'] = {
    'HOST': '',
    'USER': '',
    'PASSWORD': '',
    'DATABASE': ''
}

with open('settings.ini', 'w') as file:
    SETTINGS.write(file)

SETTINGS.read("settings.ini")

""" 
Getting data from external API COVID-19
"""

url = 'https://api.covid19api.com/countries'
countries = requests.request("GET", url).json()

measurements = []
print('Getting Measurements')
for country in countries:
    if country['Slug'] != 'united-states':
        url = f"https://api.covid19api.com/country/{country['Slug']}?from=2020-01-01T00:00:00Z&to=2021-06-09T00:00:00Z"
        data = requests.request("GET", url).json()
        sleep(1)
        print(f"{country['Slug']} - {len(data)}")
        measurements += data

# Getting US data
week_days = pd.date_range(start='2020-01-01', end='2021-06-09', freq='7d')
for week_day in week_days:
    week_ago = week_day - datetime.timedelta(days=6)
    url = f"https://api.covid19api.com/country/united-states?from={week_ago}&to={week_day}"
    data = requests.request("GET", url).json()
    sleep(1)
    measurements += filter(
        lambda measurement: measurement['Province'] == "", data)
    print(f"united-states - {len(data)}")


print('Measurements obtained')

""" 
Writing data in measurements.json
"""

with open('measurements.json', 'w') as outfile:
    json.dump(measurements, outfile)

""" 
Uploading measurements to AWS S3 
"""

ACCESS_KEY = SETTINGS["S3_AUTH"]["ACCESS_KEY"]
SECRET_KEY = SETTINGS["S3_AUTH"]["SECRET_KEY"]
S3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                  aws_secret_access_key=SECRET_KEY)


def upload_to_aws(local_file, bucket, s3_file):
    try:
        S3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        print(f'Uploaded file: {local_file}')
        print(f'Upload to bucket: {bucket}')
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")


local_file_name = '/content/measurements.json'
bucket_name = 'ornitorrinco'
s3_file_name = 'measurements.json'
upload_to_aws(local_file_name, bucket_name, s3_file_name)

""" 
Creating tables in AWS RDS - MySQL
"""

mydb = connect(
    host=SETTINGS["RDS_SETTINGS"]["HOST"],
    user=SETTINGS["RDS_SETTINGS"]["USER"],
    password=SETTINGS["RDS_SETTINGS"]["PASSWORD"],
    database=SETTINGS["RDS_SETTINGS"]["DATABASE"]
)
mycursor = mydb.cursor()

# Creating tables queries

mycursor.execute("""CREATE TABLE IF NOT EXISTS `Locations` (
	`Country` VARCHAR(255) NOT NULL,
	`CountryCode` VARCHAR(255) NOT NULL,
	`Lat` VARCHAR(255) NOT NULL,
	`Lon` VARCHAR(255) NOT NULL,
  `City` VARCHAR(255) NOT NULL,
	`CityCode` VARCHAR(255) NOT NULL,
	`Province` VARCHAR(255) NOT NULL,
	PRIMARY KEY (`Country`)
);""")

mycursor.execute("""CREATE TABLE IF NOT EXISTS `Measurements` (
	`ID` INT NOT NULL AUTO_INCREMENT,
	`Country` VARCHAR(255) NOT NULL,
	`Deaths` INT NOT NULL,
	`Confirmed` INT NOT NULL,
	`Recovered` INT NOT NULL,
	`Active` INT NOT NULL,
	`Date` TIMESTAMP NOT NULL,
  `RegisterId` VARCHAR(255) NOT NULL,
	PRIMARY KEY (`ID`),
  CONSTRAINT `Measurements_fk0` FOREIGN KEY (`Country`)
	REFERENCES `Locations`(`Country`)
);""")

""" 
Migrating data from AWS S3 to AWS RDS
"""

ACCESS_KEY = SETTINGS["S3_AUTH"]["ACCESS_KEY"]
SECRET_KEY = SETTINGS["S3_AUTH"]["SECRET_KEY"]
S3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                  aws_secret_access_key=SECRET_KEY)

mydb = connect(
    host=SETTINGS["RDS_SETTINGS"]["HOST"],
    user=SETTINGS["RDS_SETTINGS"]["USER"],
    password=SETTINGS["RDS_SETTINGS"]["PASSWORD"],
    database=SETTINGS["RDS_SETTINGS"]["DATABASE"]
)
mycursor = mydb.cursor()


def download_from_aws(local_file, bucket, s3_file):
    try:
        S3.download_file(bucket, s3_file, local_file)
        print("Download Successful")
        print(f'Downloaded file: {s3_file}')
        print(f'Download from bucket: {bucket}')
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")


local_file_name = '/content/s3_measurements.json'
bucket_name = 'ornitorrinco'
file_name = 'measurements.json'
download_from_aws(local_file_name, bucket_name, file_name)

measurements = json.load(open(local_file_name))


queryInsertCountry = f"""INSERT IGNORE INTO Locations 
  (Country, CountryCode, Lat, Lon, City, CityCode, Province) 
  VALUES (%s, %s, %s, %s, %s, %s, %s);"""

queryInsertMeasurement = f"""INSERT INTO Measurements 
  (Country, Deaths, Confirmed, Recovered, Active, Date, RegisterId) 
  VALUES (%s, %s, %s, %s, %s, %s, %s);"""

print('Uploading Data')

locations_data = []
measurements_data = []
for measurement in measurements:
    country = measurement['Country']
    country_code = measurement['CountryCode']
    lat = measurement['Lat']
    lon = measurement['Lon']
    city = measurement['City']
    city_code = measurement['CityCode']
    province = measurement['Province']
    register_id = measurement['ID']
    deaths = measurement['Deaths']
    confirmed = measurement['Confirmed']
    recovered = measurement['Recovered']
    active = measurement['Active']
    date = measurement['Date']

    locations_data.append(
        (country, country_code, lat, lon, city, city_code, province))
    measurements_data.append(
        (country, deaths, confirmed, recovered, active, date, register_id))

    if len(locations_data) == 100:
        mycursor.executemany(queryInsertCountry, locations_data)
        mycursor.executemany(queryInsertMeasurement, measurements_data)
        mydb.commit()
        locations_data = []
        measurements_data = []

mycursor.executemany(queryInsertCountry, locations_data)
mycursor.executemany(queryInsertMeasurement, measurements_data)
mydb.commit()
print("Upload Successful")

""" 
COVID-19 Daily Report
"""

# Getting Data

mydb = connect(
    host=SETTINGS["RDS_SETTINGS"]["HOST"],
    user=SETTINGS["RDS_SETTINGS"]["USER"],
    password=SETTINGS["RDS_SETTINGS"]["PASSWORD"],
    database=SETTINGS["RDS_SETTINGS"]["DATABASE"]
)
mycursor = mydb.cursor()


def relate_last_500_days(param, label):
    query = f"""
  SELECT Country, SUM({param}) AS {param}, Date FROM Measurements
  WHERE Country = (
    SELECT Country FROM Measurements
    WHERE Date = '2021-06-09 00:00:00'
    GROUP BY Country, Date
    ORDER BY Date DESC, SUM({param}) DESC
    LIMIT 1
  )
  GROUP BY Country, Date
  ORDER BY Date DESC LIMIT 500;
  """
    mycursor.execute(query)
    data = mycursor.fetchall()

    country = data[0][0]
    print(f'\nMost {label}: {country}')
    print(tabulate([(str(cases), date.strftime('%d/%m/%Y'))
          for (_, cases, date) in data], headers=[param, 'Date']))


def relate_top_10(param, label):
    query = f"""
  SELECT Country, SUM({param}) AS {param} FROM Measurements
  WHERE Date = '2021-06-09 00:00:00'
  GROUP BY Country, Date
  ORDER BY Date DESC, SUM({param}) DESC
  LIMIT 10;
  """
    mycursor.execute(query)
    data = mycursor.fetchall()

    print(f'\nTop 10 Countries with Most {label}')
    print(tabulate([(country, str(cases))
          for (country, cases) in data], headers=['Country', param]))


# Reporting COVID-19 Confirmed Cases and Deaths

relate_last_500_days('Confirmed', 'Confirmed Cases')

relate_last_500_days('Deaths', 'Deaths')

relate_top_10('Confirmed', 'Confirmed Cases')

relate_top_10('Deaths', 'Deaths')
