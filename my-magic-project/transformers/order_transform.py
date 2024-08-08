import pandas as pd
from datetime import datetime
import pytz


if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, data_2,*args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Get the current timestamp in UTC and convert to Jakarta time
    utc_now = datetime.now(pytz.utc)
    jakarta_tz = pytz.timezone('Asia/Jakarta')
    now = utc_now.astimezone(jakarta_tz)
    
    # Specify your transformation logic here
    df_orders_b = data['df_orders']
    df_orders_i = data_2['df_orders']
    df_orders = pd.concat([df_orders_b, df_orders_i], ignore_index=True)

    df_orders['sales_date'] = pd.to_datetime(df_orders['sales_date'], format="%Y/%m/%d")
    # identify order_id that has more than one sales_date
    order_sales_date_counts = df_orders.groupby(['order_id', 'sales_date']).size().reset_index(name='counts')
    order_max_sales_date = order_sales_date_counts.loc[order_sales_date_counts.groupby('order_id')['counts'].idxmax()]

    # Recombine to ensure each order_id has the same sales_date
    df_orders = pd.merge(df_orders.drop('sales_date', axis=1), order_max_sales_date[['order_id', 'sales_date']], on='order_id', how='left')

    # Add created_at and updated_at columns
    df_orders['created_at'] = now
    df_orders['updated_at'] = now

    # Drop duplicate
    df_orders = df_orders.drop_duplicates(inplace=False)

    return df_orders


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
