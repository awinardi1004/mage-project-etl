from datetime import datetime
import pandas as pd
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
    df_menu_b = data['df_menu']
    df_menu_i = data_2['df_menu']
    df_menu = pd.concat([df_menu_b, df_menu_i], ignore_index=True)
    df_menu['effective_date'] = pd.to_datetime(df_menu['effective_date'], format="%Y-%m-%d")

    df_menu.rename(columns={'name':'menu_name'}, inplace=True )

    # add created_at and updated_at columns
    df_menu['created_at'] = now
    df_menu['updated_at'] = now

    return df_menu

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'