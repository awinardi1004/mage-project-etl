from mage_ai.io.file import FileIO
import pandas as pd
import os
from datetime import datetime

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

BACKFILL_START_DATE = datetime(2021, 1, 1)  # Start date for backfill
BACKFILL_END_DATE = datetime(2021, 5, 10)    # End date for backfill

def get_file_modification_timestamp(filepath):
    """
    Get the last modification timestamp of a file.
    """
    if os.path.exists(filepath):
        timestamp = os.path.getmtime(filepath)
        return datetime.fromtimestamp(timestamp)
    return datetime.min

@data_loader
def load_backfill_data_from_file(*args, **kwargs):
    """
    Load backfilled data from filesystem.
    """
    # Define file path for the Excel file
    filepath = '/home/src/data/resto.xlsx'
    
    # Load the Excel file
    excel_file = pd.ExcelFile(filepath)
    
    # Define the sheets and corresponding DataFrame names
    sheets = {
        'df_menu': 'Menu',
        'df_orders': 'Order',
        'df_promotions': 'Promotion'
    }
    
    datasets = {}
    
    for key, sheet_name in sheets.items():
        # Load the dataset from the sheet
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        
        
        # Convert relevant date columns to datetime
        if key == 'df_menu' and 'effective_date' in df.columns:
            df['effective_date'] = pd.to_datetime(df['effective_date'], errors='coerce')
            # Backfill from BACKFILL_START_DATE to BACKFILL_END_DATE
            backfill_df = df[(df['effective_date'] >= BACKFILL_START_DATE) & (df['effective_date'] <= BACKFILL_END_DATE)]
            datasets[key] = backfill_df.drop_duplicates()
        
        elif key == 'df_orders' and 'sales_date' in df.columns:
            df['sales_date'] = pd.to_datetime(df['sales_date'], errors='coerce')
            # Backfill from BACKFILL_START_DATE to BACKFILL_END_DATE
            backfill_df = df[(df['sales_date'] >= BACKFILL_START_DATE) & (df['sales_date'] <= BACKFILL_END_DATE)]
            datasets[key] = backfill_df.drop_duplicates()
        
        elif key == 'df_promotions' and 'start_date' in df.columns:
            df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
            # Backfill from BACKFILL_START_DATE to BACKFILL_END_DATE
            backfill_df = df[(df['start_date'] >= BACKFILL_START_DATE) & (df['start_date'] <= BACKFILL_END_DATE)]
            datasets[key] = backfill_df.drop_duplicates()
        
        else:
            datasets[key] = df
        
    return datasets

@test
def test_backfill_output(output, *args) -> None:
    """
    Template code for testing the backfill output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert isinstance(output, dict), 'The output should be a dictionary'
    
    # Define expected keys for backfilled datasets
    expected_keys = [
        'df_menu', 'df_orders', 'df_promotions'
    ]
    
    for key in expected_keys:
        assert key in output, f'{key} is missing'
        assert not output[key].empty, f'The {key} table is empty'
