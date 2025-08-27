import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    database = event.get('database')
    tables = event.get('tables')
    
    logger.info(f"Event: {event}")

    # There should only ever be one table so get the first table from the list
    table = tables[0]
    table_type = None
    
    if table.startswith('full_load_'):
        table_type = 'full_load'
    elif table.startswith('cdc_'):
        table_type = 'cdc'
    else:
        logger.error(f"Table name '{table}' does not start with 'full_load_' or 'cdc_'")
        raise Exception(f"Table name '{table}' does not start with 'full_load_' or 'cdc_'")
    
    clean_table_name = table.replace('full_load_', '').replace('cdc_', '')
        
    return {
        'statusCode': 200,
        'body': 'Processed table creation event',
        'database': database,
        'table': clean_table_name,
        'full_load_table': 'full_load_' + clean_table_name,
        'event_table_type': table_type
    }
