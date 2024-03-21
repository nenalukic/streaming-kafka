from kafka import KafkaProducer
import pandas as pd
import time 

# Record the start time
start_time = time.time()

# Create a KafkaProducer instance
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Define the topic name
topic = 'green-trips'

# Define the data types for each column
dtype_dict = {
    'lpep_pickup_datetime': 'str',
    'lpep_dropoff_datetime': 'str',
    'PULocationID': 'int',
    'DOLocationID': 'int',
    'passenger_count': 'float',
    'trip_distance': 'float',
    'tip_amount': 'float'
}

# Define the columns you want to keep
columns_to_keep = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount'
]

# Read the CSV file into a DataFrame, specifying the dtype parameter
df_green = pd.read_csv('green_tripdata_2019-10.csv', usecols=columns_to_keep, dtype=dtype_dict, na_values=[''])

# Fill missing values in integer columns with zeros
df_green['passenger_count'] = df_green['passenger_count'].fillna(0).astype(int)

#df_green['lpep_pickup_datetime'] = pd.to_datetime(df_green['lpep_pickup_datetime'])
#df_green['lpep_dropoff_datetime'] = pd.to_datetime(df_green['lpep_dropoff_datetime'])



# Iterate over each row of the DataFrame
for row in df_green.itertuples(index=False):
    # Extract the row values as a dictionary
    row_dict = {col: getattr(row, col) for col in row._fields}
    
    # Filter the dictionary to keep only the specified columns
    row_dict_filtered = {key: value for key, value in row_dict.items() if key in columns_to_keep}
    
    # Convert the dictionary to JSON and encode as bytes
    message = pd.Series(row_dict_filtered).to_json().encode('utf-8')
    
    # Send the message to the Kafka topic
    producer.send(topic, value=message)
    
# Close the producer
producer.close()

# Record the end time
end_time = time.time()

# Calculate the execution time
execution_time = end_time - start_time

# Print the execution time
print(f"Script execution completed in {execution_time:.2f} seconds.")