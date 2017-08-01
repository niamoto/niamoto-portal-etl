# coding: utf-8

from niamoto.data_providers.sql_provider import SQLDataProvider
from niamoto.db.connector import Connector


class TaxaInventoryProvider(SQLDataProvider):

    OCCURRENCE_SQL = \
        """
        SELECT tax.id AS id,
            tax.taxon_id AS taxon_id,
            ST_X(inv.location) AS x,
            ST_Y(inv.location) AS y,
            inv.inventory_date AS inventory_date,
            inv.location_description AS location_description
        FROM public.inventories_taxainventorytaxon AS tax
        INNER JOIN public.inventories_inventory AS inv
            ON tax.taxa_inventory_id = inv.id;
        """

    def __init__(self, name):
        super(TaxaInventoryProvider, self).__init__(
            name, Connector.get_database_url(),
            occurrence_sql=self.OCCURRENCE_SQL,
            plot_sql=None,
            plot_occurrence_sql=None
        )

    @classmethod
    def get_type_name(cls):
        return "TAXA_INVENTORY"
