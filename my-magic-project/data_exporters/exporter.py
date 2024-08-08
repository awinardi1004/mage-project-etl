from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_menu_data_to_postgres(df_menu: DataFrame, **kwargs) -> None:
    """
    Export df_menu DataFrame to a PostgreSQL table.
    """
    schema_name = 'staging'
    table_name = 'stg_menu'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df_menu,
            schema_name,
            table_name,
            index=False,
            if_exists='replace'
        )

@data_exporter
def export_orders_data_to_postgres(df_orders: DataFrame, **kwargs) -> None:
    """
    Export df_orders DataFrame to a PostgreSQL table.
    """
    schema_name = 'staging'
    table_name = 'stg_orders'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df_orders,
            schema_name,
            table_name,
            index=False,
            if_exists='replace'
        )

@data_exporter
def export_promotions_data_to_postgres(df_promotions: DataFrame, **kwargs) -> None:
    """
    Export df_promotions DataFrame to a PostgreSQL table.
    """
    schema_name = 'stg_staging'
    table_name = 'promotions'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df_promotions,
            schema_name,
            table_name,
            index=False,
            if_exists='replace'
        )
