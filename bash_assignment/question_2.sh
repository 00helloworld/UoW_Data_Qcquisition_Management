
SOURCE_URL="https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
TEMP_CSV="airports.dat"
TRANSFORMED_CSV="transformed_airport_data.csv"
SNOWFLAKE_DB="YANSHEN_DIT"
SNOWFLAKE_SCHEMA="BASH_ETL"
SNOWFLAKE_TABLE="AIRPORT_TBL"
SNOWSQL_CMD="snowsql -c my_connection -d $SNOWFLAKE_DB -s $SNOWFLAKE_SCHEMA"

run_python_transformation() {
    echo "Python transformation..."
    python3 transform_airport_data.py
}


# Download source data
echo "Downloading source data..."
curl -o $TEMP_CSV $SOURCE_URL

# Python transformation
run_python_transformation

# Create table
echo "Creating table..."
$SNOWSQL_CMD -q "CREATE OR REPLACE TABLE $SNOWFLAKE_TABLE (
    airport_id NUMBER,
    airport_name VARCHAR(100),
    airport_city VARCHAR(100)
);"

# Copy into Snowflake
echo "Loading to Snowflake..."
$SNOWSQL_CMD -q "PUT file://$PWD/$TRANSFORMED_CSV @%$SNOWFLAKE_TABLE"
$SNOWSQL_CMD -q "COPY INTO $SNOWFLAKE_TABLE
                 FROM @%$SNOWFLAKE_TABLE/$TRANSFORMED_CSV
                 FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 ESCAPE = NONE FIELD_OPTIONALLY_ENCLOSED_BY = '\"') ;"

# Clean up temporary files
echo "Cleaning up temporary files..."
rm $TEMP_CSV $TRANSFORMED_CSV

echo "Done"
