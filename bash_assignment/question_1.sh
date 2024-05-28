
CSV_URL="https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
TEMP_CSV="temp_covid_data.csv"
TRANSFORMED_CSV="transformed_covid_data.csv"
SNOWFLAKE_DB="YANSHEN_DIT"
SNOWFLAKE_SCHEMA="BASH_ETL"
SNOWFLAKE_TABLE="COVID_TBL"
SNOWSQL_CMD="snowsql -c my_connection -d $SNOWFLAKE_DB -s $SNOWFLAKE_SCHEMA"

# Download CSV data
echo "Downloading CSV file..."
curl -o $TEMP_CSV $CSV_URL

# extract (1, 2, 4, 5)
echo "Extracting..."
cut -d ',' -f 1,2,4,5 $TEMP_CSV > $TRANSFORMED_CSV

# Create table
echo "Creating table..."
$SNOWSQL_CMD -q "CREATE OR REPLACE TABLE $SNOWFLAKE_TABLE (
    iso_code VARCHAR(100),
    continent VARCHAR(100),
    date VARCHAR(100),
    total_cases NUMBER
);"

# Copy into Snowflake
echo "Loading to Snowflake..."
$SNOWSQL_CMD -q "PUT file://$PWD/$TRANSFORMED_CSV @%$SNOWFLAKE_TABLE"
$SNOWSQL_CMD -q "COPY INTO $SNOWFLAKE_TABLE
                 FROM @%$SNOWFLAKE_TABLE/$TRANSFORMED_CSV
                 FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);"


# Clear temporary file
echo "Cleaning file..."
rm $TEMP_CSV $TRANSFORMED_CSV

echo "Done"
