import pandas as pd
from sqlalchemy import create_engine

# Load data from Google Sheets
def load_raw_data():
    messages_url = 'https://docs.google.com/spreadsheets/d/1XC0YaSQ4WjLwhzCB96RxF23-NjFga1Fisr-9lX_7hmk/gviz/tq?tqx=out:csv&gid=1033608769'
    statuses_url = 'https://docs.google.com/spreadsheets/d/1XC0YaSQ4WjLwhzCB96RxF23-NjFga1Fisr-9lX_7hmk/gviz/tq?tqx=out:csv&gid=966707183'

    messages_df = pd.read_csv(messages_url)
    statuses_df = pd.read_csv(statuses_url)

    messages_df.to_csv("data/message.csv")
    statuses_df.to_csv("data/statues.csv")

    return messages_df, statuses_df

def insert_data_to_stg(messages_df,statuses_df):
# Connect to PostgreSQL
    engine = create_engine('postgresql://sachin:sachin@localhost:5432/noora')

# Load data into PostgreSQL
    messages_df.to_sql('messages', engine, if_exists='append', index=False, schema='public')
    statuses_df.to_sql('statuses', engine, if_exists='append', index=False, schema='public')

def validate_data(messages_df,statuses_df):

    print(messages_df.columns)

    # Define the expected schema
    expected_messages_columns = ['id', 'message_type', 'masked_addressees', 'masked_author', 'content',
       'author_type', 'direction', 'external_id', 'external_timestamp',
       'masked_from_addr', 'is_deleted', 'last_status',
       'last_status_timestamp', 'rendered_content', 'source_type', 'uuid',
       'inserted_at', 'updated_at']
    
    print(statuses_df.columns)

    expected_statuses_columns = ['id', 'status', 'timestamp', 'uuid', 'message_uuid','message_id','number_id', 
    'inserted_at', 'updated_at']



    # Validate the schema of the messages DataFrame
    assert list(messages_df.columns) == expected_messages_columns, \
    "Messages DataFrame columns do not match expected schema"

# Validate the schema of the statuses DataFrame
    assert list(statuses_df.columns) == expected_statuses_columns, \
    "Statuses DataFrame columns do not match expected schema"
   
    print(statuses_df.columns)


    missing_values_in_messages = messages_df.isnull().sum()
    print("Missing values in each column:\n", missing_values_in_messages)

    # Data type validation and conversion if needed
    messages_df['external_timestamp'] = pd.to_datetime(messages_df['external_timestamp'], errors='coerce')
    messages_df['inserted_at'] = pd.to_datetime(messages_df['inserted_at'], errors='coerce')
    messages_df['updated_at'] = pd.to_datetime(messages_df['updated_at'], errors='coerce')
    messages_df['last_status_timestamp'] = pd.to_datetime(messages_df['last_status_timestamp'], errors='coerce')
    
      # Ensure all values in 'masked_from_addr' are strings
    messages_df['masked_from_addr'] = messages_df['masked_from_addr'].astype(str)
    # Remove non-numeric characters from `masked_from_addr`
    messages_df['masked_from_addr'] = messages_df['masked_from_addr'].str.replace(r'[^0-9]', '', regex=True)

    messages_df['is_deleted'].fillna(False, inplace=True)
    
    messages_df['masked_addressees'] = messages_df['masked_addressees'].astype(str).str.replace(r'[^0-9]', '', regex=True)

    # Data type validation and conversion if needed
    statuses_df['inserted_at'] = pd.to_datetime(statuses_df['inserted_at'], errors='coerce')
    statuses_df['updated_at'] = pd.to_datetime(statuses_df['updated_at'], errors='coerce')
    statuses_df['timestamp'] = pd.to_datetime(statuses_df['timestamp'], errors='coerce')
    
def main():
    messages_df, statuses_df = load_raw_data()
    validate_data(messages_df,statuses_df)
    insert_data_to_stg(messages_df, statuses_df)

if __name__ == "__main__":
    main()