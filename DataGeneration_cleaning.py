
#1 Generation phase:
"""
Program should generate N=~5000 JSON files on disk in /tmp/flights/%MM-YY%-%origin_city%-flights.json or similar 
folder structure where each file is a JSON array of random size M = [50 - 100] of randomly generated flights data between cities.
Total set of cities is K=[100-200]. Flight record is an object containing  {date, origin_city, destination_city, flight_duration_secs, 
# of passengers of board}. Some records, with probability L=[0.5%-0.1%] should have NULL in any of the flight record properties.
"""

"""
#2 Analysis & Cleaning phase:

Program should process those files in the most optimal way and produce the following result:

 - #count of total records processed, #count of dirty records and total run duration.

 - AVG and P95 (95th percentile) of flight duration for Top 25 destination cities.

 - Assuming cities had originally 0 passengers, find two cities with MAX passengers arrived and left.
"""

import json
import random
import os
import datetime
import shutil
import pandas as pd
import numpy as np


# Constants
number_files = 5000  # Number of files
records_per_file = (50, 100)  # Range of records per file

total_cities = 150  # Total cities
prob_null = 0.001  # Probability of null values
dir_name = "tmp/flights"

# City names generating like city_1, city_2 etc
cities = [f"city_{i}" for i in range(total_cities)]

def generate_flight_record():
    """
    It is to generate file record in json file 
    """
    record = {
        "date": datetime.date.today().isoformat(),
        "origin_city": random.choice(cities),
        "destination_city": random.choice(cities),
        "flight_duration_secs": random.randint(3600, 7200),  # 1-2 hours , args are in secs
        "num_passengers": random.randint(50, 200)
    }

    # Setting null values for probability of 0.001
    if random.random() < prob_null:
        null_field = random.choice(list(record.keys()))
        record[null_field] = None
    return record

def generate_files():
    """
    It is to generate json files with flight records 
    """
    folder_name = f"{dir_name}" # Folder will contain json files

    try:
        if os.path.exists(folder_name): 
            print(f"{folder_name} exists")
            shutil.rmtree(folder_name)            
            print(f"{folder_name} has been removed successfully")
        
        os.makedirs(folder_name)   # Create directory
        print(f"{folder_name} created successfully")   

        print(f"Generating files")   
        # Creating json files 
        for i in range(number_files):
            
            # Generating files naming as  %m-%Y-%H-%M-%S-%f-city_<number>-flights.json  Example:  11-24-17-55-54-170961-city_50-flights
            filename = f"{folder_name}/{datetime.datetime.today().strftime('%m-%y-%H-%M-%S-%f')}-{random.choice(cities)}-flights.json"
            
            #Generate Records 
            records = [generate_flight_record() for _ in range(random.randint(*records_per_file))]
            
            # Dump records into json file 
            with open(filename, "w") as file:
                json.dump(records, file)
        
        print(f"Generating files finished in path : {folder_name}")   
    except OSError as error:
        print(error)
        print(f"{folder_name} exists, can not be removed")
        raise OSError(f"{folder_name} exists, can not be removed")

def process_files():
    """
    This function process files 
    """
    total_records = 0
    dirty_records = 0    
    
    all_records = []

    start_time = pd.Timestamp.now()

    # Iterating over directory for files 
    for root, dirs, files in os.walk(dir_name):
        for file in files:
            with open(os.path.join(root, file), "r") as f:
                records = json.load(f)
                
                total_records += len(records)      # Calculate total records 
                for record in records:
                    # Checking NaN values 
                    if any(value is None for value in record.values()):
                        dirty_records += 1
                        continue
                
                all_records.extend(records)                     

    # Create pandas DataFrame
    df = pd.DataFrame(all_records)

    # Calculate avg and P95 of flight duration for Top 25 destination cities
    top_25_cities = df['destination_city'].value_counts().nlargest(25).index.tolist()
    
    # crate dataframe with that top 25 cities 
    df_top_25 = df[df['destination_city'].isin(top_25_cities)]   
           
    avg_p95 = df_top_25.groupby('destination_city')['flight_duration_secs'].agg(['mean', lambda x: np.percentile(x.dropna(), 95) if len(x.dropna()) > 0 else 0]).reset_index()

    avg_p95.columns = ['Destination City', 'Average Flight Duration (secs)', 'P95 Flight Duration (secs)']

    # Find two cities with MAX passengers arrived and left
    df_arrived = df.groupby('destination_city')['num_passengers'].sum().reset_index()
    df_arrived.columns = ['Destination City', 'Passengers Arrived']
    
    max_arrived = df_arrived.loc[df_arrived['Passengers Arrived'].idxmax()]['Destination City']
    
    df_left = df.groupby('origin_city')['num_passengers'].sum().reset_index()
    df_left.columns = ['Origin City', 'Passengers Left']
    max_left = df_left.loc[df_left['Passengers Left'].idxmax()]['Origin City']

    end_time = pd.Timestamp.now()
    run_duration = (end_time - start_time).total_seconds()

    print(f"Total records processed: {total_records}")
    print(f"Dirty records: {dirty_records}")
    print(f"Run duration: {run_duration:.2f} seconds")
    print("AVG and P95 of flight duration for Top 25 destination cities:")
    print(avg_p95)
    print(f"City with most passengers arrived: {max_arrived} ({df_arrived.loc[df_arrived['Destination City'] == max_arrived, 'Passengers Arrived'].values[0]})")
    print(f"City with most passengers left: {max_left} ({df_left.loc[df_left['Origin City'] == max_left, 'Passengers Left'].values[0]})")


def main():
    """
    It is main function to generate and cleaning of data 
    """
    try:
        # Generate files 
        generate_files() 

        # Analysis 
        process_files()

    except OSError as e:
        print(f"Error : {e}")

    except Exception as e:
        print(f"Exception occured , can not proceed further ")

if __name__ == "__main__":
    main()


