import pandas as pd

# Transformer block

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def transform(data, *args, **kwargs):
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
    df_menu = data['df_menu']
    df_orders = data['df_orders']
    df_promotions = data['df_promotions']
    
    # Change data type
    df_menu['effective_date'] = pd.to_datetime(df_menu['effective_date'], format="%Y-%m-%d")
    df_orders['sales_date'] = pd.to_datetime(df_orders['sales_date'], format="%Y/%m/%d")
    df_promotions['start_date'] = pd.to_datetime(df_promotions['start_date'], format="%Y-%m-%d")
    df_promotions['end_date'] = pd.to_datetime(df_promotions['end_date'], format="%Y-%m-%d")

    # identify order_id that has more than one sales_date
    order_sales_date_counts = df_orders.groupby(['order_id', 'sales_date']).size().reset_index(name='counts')
    order_max_sales_date = order_sales_date_counts.loc[order_sales_date_counts.groupby('order_id')['counts'].idxmax()]

    # Recombine to ensure each order_id has the same sales_date
    df_orders = pd.merge(df_orders.drop('sales_date', axis=1), order_max_sales_date[['order_id', 'sales_date']], on='order_id', how='left')

    # Drop duplicate
    df_orders = df_orders.drop_duplicates(inplace=False)


    # Combining df_orders and df_menu based on 'menu_id'
    df_merged = pd.merge(df_orders, df_menu, on='menu_id', how='left')

    # Determines price based on effective_date
    df_merged = df_merged[df_merged['sales_date'] >= df_merged['effective_date']]

    # Retrieve the latest price based on effective_date
    df_merged = df_merged.loc[df_merged.groupby(['order_id', 'menu_id'])['effective_date'].idxmax()]

    # Merge the results with df_orders to ensure all rows are present
    df_merged = pd.merge(df_orders, df_merged[['order_id', 'menu_id', 'effective_date', 'name', 'price', 'cogs']], on=['order_id', 'menu_id'], how='left')

    # Removed unnecessary columns
    df_merged = df_merged[['order_id', 'sales_date', 'menu_id', 'name', 'quantity', 'price', 'cogs']]

    # Create an active_date column in df_promotions based on the start_date and end_date range
    df_active_dates = pd.DataFrame()

    for _, row in df_promotions.iterrows():
        temp_df = pd.DataFrame({
            'id': row['id'],
            'active_date': pd.date_range(start=row['start_date'], end=row['end_date'])
            })
        df_active_dates = pd.concat([df_active_dates, temp_df])

    # Merge df_active_dates with df_promotions to get disc_value and max_disc columns
    df_active_dates = pd.merge(df_active_dates, df_promotions, on='id')

    # Merge df_merged with df_active_dates based on sales_date and active_date
    df_final = pd.merge(df_merged, df_active_dates, left_on='sales_date', right_on='active_date', how='left')

    # Fills disc_value and max_disc values ​​that do not correspond to 0
    df_final['disc_value'] = df_final['disc_value'].fillna(0)
    df_final['max_disc'] = df_final['max_disc'].fillna(0)

    # Removed unnecessary columns
    df_final = df_final[['order_id', 'sales_date', 'menu_id', 'name', 'quantity', 'price', 'cogs', 'disc_value', 'max_disc']]


    return df_final


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert isinstance(output, pd.DataFrame), 'The output should be a DataFrame'
    assert not output.empty, 'The output DataFrame is empty'