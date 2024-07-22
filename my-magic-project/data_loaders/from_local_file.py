from mage_ai.io.file import FileIO
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_file(*args, **kwargs):
    """
    Template for loading data from filesystem.
    Load data from 1 file or multiple file directories.

    For multiple directories, use the following:
        FileIO().load(file_directories=['dir_1', 'dir_2'])

    Docs: /design/data-loading#fileio
    """
    filepaths = [
        '/home/src/data/menu.csv',
        '/home/src/data/orders.csv',
        '/home/src/data/promotions.csv'
    ]

    # Load each dataset and store them in a dictionary
    datasets = {
        'df_menu': FileIO().load(filepaths[0]),
        'df_orders': FileIO().load(filepaths[1]),
        'df_promotions': FileIO().load(filepaths[2])
    }

    return datasets


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert isinstance(output, dict), 'The output should be a dictionary'
    assert 'df_menu' in output, 'df_menu table is missing'
    assert 'df_orders' in output, 'df_orders table is missing'
    assert 'df_promotions' in output, 'df_promotions table is missing'
    assert not output['df_menu'].empty, 'The df_menu table is empty'
    assert not output['df_orders'].empty, 'The df_orders table is empty'
    assert not output['df_promotions'].empty, 'The df_promotions table is empty'
