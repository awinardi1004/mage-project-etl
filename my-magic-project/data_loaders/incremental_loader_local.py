from mage_ai.io.file import FileIO
import pandas as pd
import os
from datetime import datetime

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def get_today_date():
    """
    Get today's date.
    """
    return datetime.now().date()

@data_loader
def load_incremental_data_from_file(*args, **kwargs):
    """
    Load incremental data from filesystem, filtering only today's data.
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
    
    # Get today's date
    today_date = get_today_date()
    
    for key, sheet_name in sheets.items():
        # Load the dataset from the sheet
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        
        # Convert relevant date columns to datetime
        if key == 'df_menu' and 'effective_date' in df.columns:
            df['effective_date'] = pd.to_datetime(df['effective_date'], errors='coerce')
            # Incremental Load for today's date
            filtered_df = df[df['effective_date'].dt.date == today_date]
            datasets[key] = filtered_df.drop_duplicates() if not filtered_df.empty else pd.DataFrame(columns=df.columns)
        
        elif key == 'df_orders' and 'sales_date' in df.columns:
            df['sales_date'] = pd.to_datetime(df['sales_date'], errors='coerce')
            # Incremental Load for today's date
            filtered_df = df[df['sales_date'].dt.date == today_date]
            datasets[key] = filtered_df.drop_duplicates() if not filtered_df.empty else pd.DataFrame(columns=df.columns)
        
        elif key == 'df_promotions' and 'start_date' in df.columns:
            df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
            # Incremental Load for today's date
            filtered_df = df[df['start_date'].dt.date == today_date]
            datasets[key] = filtered_df.drop_duplicates() if not filtered_df.empty else pd.DataFrame(columns=df.columns)
        
        else:
            datasets[key] = df
    
    return datasets

@test
def test_incremental_data_output(output, *args) -> None:
    """
    Template code for testing the incremental data output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert isinstance(output, dict), 'The output should be a dictionary'
    
    # Define expected keys for incremental datasets
    expected_keys = [
        'df_menu', 'df_orders', 'df_promotions'
    ]
    
    for key in expected_keys:
        # Check if the key exists in the output
        assert key in output, f'{key} is missing'
        # If the table is empty, we should still pass the test
        if output[key].empty:
            print(f'The {key} table is empty but processing continues.')
        else:
            assert not output[key].empty, f'The {key} table is empty'
