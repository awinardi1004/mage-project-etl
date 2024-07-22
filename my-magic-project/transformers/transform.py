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
    
    df_menu['effective_date'] = pd.to_datetime(df_menu['effective_date'], format="%Y-%m-%d")
    df_orders['sales_date'] = pd.to_datetime(df_orders['sales_date'], format="%Y/%m/%d")
    df_promotions['start_date'] = pd.to_datetime(df_promotions['start_date'], format="%Y-%m-%d")
    df_promotions['end_date'] = pd.to_datetime(df_promotions['end_date'], format="%Y-%m-%d")

    # Drop duplicate
    df_orders = df_orders.drop_duplicates(inplace=False)

    df_merged = pd.merge(df_orders, df_menu, on='menu_id', how='left')

    # Menentukan harga berdasarkan effective_date
    df_merged = df_merged[df_merged['sales_date'] >= df_merged['effective_date']]

    # Ambil harga terbaru berdasarkan effective_date
    df_merged = df_merged.loc[df_merged.groupby(['order_id', 'menu_id'])['effective_date'].idxmax()]

    # Gabungkan kembali hasil dengan df_orders untuk memastikan semua baris ada
    df_merged = pd.merge(df_orders, df_merged[['order_id', 'menu_id', 'effective_date', 'name','price', 'cogs']], on=['order_id', 'menu_id'], how='left')

    # Menghapus kolom yang tidak perlu
    df_merged = df_merged[['order_id', 'sales_date', 'menu_id', 'name','quantity', 'price', 'cogs']]

    # Buat kunci untuk cross join
    df_promotions['key'] = 1
    df_merged['key'] = 1

    # Gabungkan df_merged dengan df_promotions menggunakan cross join
    df_temp = pd.merge(df_merged, df_promotions, on='key').drop('key', axis=1)

    # Filter berdasarkan start_date dan end_date
    df_temp = df_temp[(df_temp['sales_date'] >= df_temp['start_date']) & (df_temp['sales_date'] <= df_temp['end_date'])]

    # Hilangkan duplikasi dengan mengambil promosi pertama yang cocok
    df_temp = df_temp.drop_duplicates(subset=['order_id', 'menu_id'], keep='first')

    # Ambil kolom yang diperlukan dari df_temp
    df_temp = df_temp[['order_id', 'menu_id', 'disc_value', 'max_disc']]

    # Gabungkan kembali dengan df_merged untuk memastikan semua baris ada
    df_final = pd.merge(df_merged, df_temp, on=['order_id', 'menu_id'], how='left')

    # Mengisi nilai disc_value dan max_disc yang tidak memiliki kecocokan dengan NaN
    df_final['disc_value'] = df_final['disc_value'].fillna(pd.NA)
    df_final['max_disc'] = df_final['max_disc'].fillna(pd.NA)

    return df_final


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert isinstance(output, pd.DataFrame), 'The output should be a DataFrame'
    assert not output.empty, 'The output DataFrame is empty'