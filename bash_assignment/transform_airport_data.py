import pandas as pd

source_file = 'airports.dat'
transformed_file = 'transformed_airport_data.csv'
columns_to_select = ['airport_id', 'airport_name', 'airport_city']

df = pd.read_csv(source_file, names=[
    'airport_id', 'airport_name', 'airport_city', 'country', 'iata', 'icao', 
    'latitude', 'longitude', 'altitude', 'timezone', 'dst', 'tz', 'type', 'source'
])[columns_to_select]

df.to_csv(transformed_file, index=False)