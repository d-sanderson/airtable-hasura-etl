import requests
import psycopg2
import yaml
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Load mapping configuration from config.yaml
with open('config.yaml') as file:
    config = yaml.safe_load(file)

# Airtable API configuration
with open('.env') as env_file:
    env_vars = dict(line.strip().split('=') for line in env_file if line.strip() and not line.startswith('#'))

AIRTABLE_API_KEY = env_vars.get('AIRTABLE_API_KEY')
AIRTABLE_BASE_ID = env_vars.get('AIRTABLE_BASE_ID')
HEADERS = {
    'Authorization': f'Bearer {AIRTABLE_API_KEY}'
}

# Hasura Postgres database configuration
HASURA_DB = {
    'dbname': env_vars.get('HASURA_DB_NAME'),
    'user': env_vars.get('HASURA_DB_USER'),
    'password': env_vars.get('HASURA_DB_PASSWORD'),
    'host': env_vars.get('HASURA_DB_HOST'),
    'port': env_vars.get('HASURA_DB_PORT')
}

# Get data from Airtable
def get_airtable_data(table_name):
    url = f'https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table_name}'
    records = []
    offset = None
    while True:
        params = {}
        if offset:
            params['offset'] = offset
        response = requests.get(url, headers=HEADERS, params=params)
        data = response.json()
        records.extend(data['records'])
        if 'offset' not in data:
            break
        offset = data['offset']
    return records

# Transform data based on the mapping configuration
def transform_data(table_name, records):
    transformed_records = []
    mapping = config['tables'].get(table_name)
    if not mapping:
        raise ValueError(f'Mapping not found for table: {table_name}')

    for record in records:
        transformed = {}
        for airtable_field, postgres_field in mapping.items():
            transformed[postgres_field] = record['fields'].get(airtable_field)
        transformed_records.append(transformed)
    return transformed_records

# Create tables if they don't exist
def create_table_if_not_exists(table_name):
    mapping = config['tables'].get(table_name)
    if not mapping:
        raise ValueError(f'Mapping not found for table: {table_name}')

    columns = ', '.join([f"{postgres_field} TEXT" for postgres_field in mapping.values()])
    create_query = f'CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY, {columns})'

    conn = psycopg2.connect(**HASURA_DB)
    cur = conn.cursor()
    try:
        cur.execute(create_query)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

# Insert data into Hasura Postgres database
def insert_into_postgres(table_name, records):
    columns = config['tables'][table_name].values()
    column_list = ', '.join(columns)
    placeholders = ', '.join(['%s' for _ in columns])
    insert_query = f'INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})'

    conn = psycopg2.connect(**HASURA_DB)
    cur = conn.cursor()
    try:
        for record in records:
            cur.execute(insert_query, tuple(record.values()))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

# Run migration
def migrate():
    for table_name in config['tables'].keys():
        print(f'Creating table if not exists: {table_name}')
        create_table_if_not_exists(table_name)
        print(f'Migrating table: {table_name}')
        records = get_airtable_data(table_name)
        transformed_records = transform_data(table_name, records)
        insert_into_postgres(table_name, transformed_records)

if __name__ == '__main__':
    migrate()
