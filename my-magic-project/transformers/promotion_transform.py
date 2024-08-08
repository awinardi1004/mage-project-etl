import pandas as pd
from datetime import datetime
import pytz

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@transformer
def transform(data, data_2, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        data_2: The output from another upstream block (if applicable)
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Get the current timestamp in UTC and convert to Jakarta time
    utc_now = datetime.now(pytz.utc)
    jakarta_tz = pytz.timezone('Asia/Jakarta')
    now = utc_now.astimezone(jakarta_tz)

    # Specify your transformation logic here
    df_promotions_b = data['df_promotions']
    df_promotions_i = data_2['df_promotions']
    df_promotions = pd.concat([df_promotions_b, df_promotions_i], ignore_index=True)

    df_promotions['start_date'] = pd.to_datetime(df_promotions['start_date'], format="%Y-%m-%d")
    df_promotions['end_date'] = pd.to_datetime(df_promotions['end_date'], format="%Y-%m-%d")
    df_active_dates = pd.DataFrame()

    # Convert start_date and end_date to datetime if they are not already
    df_promotions['start_date'] = pd.to_datetime(df_promotions['start_date'])
    df_promotions['end_date'] = pd.to_datetime(df_promotions['end_date'])

    for _, row in df_promotions.iterrows():
        temp_df = pd.DataFrame({
            'id': [row['id']] * len(pd.date_range(start=row['start_date'], end=row['end_date'])),
            'active_date': pd.date_range(start=row['start_date'], end=row['end_date'])
            })
        df_active_dates = pd.concat([df_active_dates, temp_df], ignore_index=True)

    # Merge df_active_dates with df_promotions to get disc_value and max_disc columns
    df_promotions = pd.merge(df_active_dates, df_promotions, on='id')
    df_promotions.rename(columns={'active_date':'promo_act_date','id':'promo_id'}, inplace=True)
    df_promotions = df_promotions[['promo_id', 'promo_act_date', 'disc_value', 'max_disc']]

    # Add created_at and updated_at columns
    df_promotions['created_at'] = now
    df_promotions['updated_at'] = now

    return df_promotions

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    # Add more tests if necessary
