# coding: utf-8

import pandas as pd

from niamoto.data_publishers.base_fact_table_publisher import \
    BaseFactTablePublisher
from niamoto.api import data_marts_api
from niamoto.db.connector import Connector
from niamoto.db import metadata as meta
from niamoto.conf import settings


class OccurrenceFactTablePublisher(BaseFactTablePublisher):
    """
    Publisher for populating occurrence fact table.
    """
    @classmethod
    def get_key(cls):
        return 'occurrence_fact_table'

    @classmethod
    def get_description(cls):
        return "Publish the occurrence fact table data."

    def _process(self, *args, drop_null_properties=False,
                 **kwargs):
        provinces_dim = data_marts_api.get_dimension('provinces')
        communes_dim = data_marts_api.get_dimension('communes')
        taxon_dim = data_marts_api.get_dimension('taxon_dimension')
        occ_loc_dim = data_marts_api.get_dimension('occurrence_location')
        sql = \
            """
            SELECT COUNT(occ.id) AS occurrence_count,
                provinces.id AS provinces_id,
                taxon.id AS taxon_dimension_id,
                communes.id AS communes_id,
                occ_loc.id AS occurrence_location_id
            FROM {niamoto_schema}.{occ_table} AS occ
            LEFT JOIN {dimensions_schema}.{provinces_table} AS provinces
                ON ST_Intersects(occ.location, provinces.{province_geom})
            LEFT JOIN {dimensions_schema}.{communes_table} AS communes
                ON ST_Intersects(occ.location, communes.{communes_geom})
            LEFT JOIN {dimensions_schema}.{taxon_table} AS taxon
                ON occ.taxon_id = taxon.id
            LEFT JOIN {dimensions_schema}.{occ_loc_table} AS occ_loc
                ON occ.location = occ_loc.location
            GROUP BY taxon.id, provinces.id, communes.id, occ_loc.id;
            """.format(**{
                'niamoto_schema': settings.NIAMOTO_SCHEMA,
                'occ_table': meta.occurrence,
                'dimensions_schema': settings.NIAMOTO_DIMENSIONS_SCHEMA,
                'provinces_table': provinces_dim.name,
                'province_geom': provinces_dim.geom_column_name,
                'communes_table': communes_dim.name,
                'communes_geom': communes_dim.geom_column_name,
                'taxon_table': taxon_dim.name,
                'occ_loc_table': occ_loc_dim.name,
            })
        ns_taxon_sql = \
            """
            SELECT MAX(taxon.id)
            FROM {dimensions_schema}.{taxon_table} AS taxon;
            """.format(**{
                'dimensions_schema': settings.NIAMOTO_DIMENSIONS_SCHEMA,
                'taxon_table': taxon_dim.name,
            })
        ns_provinces_sql = \
            """
            SELECT MAX(provinces.id)
            FROM {dimensions_schema}.{provinces_table} AS provinces;
            """.format(**{
                'dimensions_schema': settings.NIAMOTO_DIMENSIONS_SCHEMA,
                'provinces_table': provinces_dim.name,
            })
        ns_communes_sql = \
            """
            SELECT MAX(communes.id)
            FROM {dimensions_schema}.{communes_table} as communes;
            """.format(**{
                'dimensions_schema': settings.NIAMOTO_DIMENSIONS_SCHEMA,
                'communes_table': communes_dim.name,
            })
        with Connector.get_connection() as connection:
            df = pd.read_sql(sql, connection)
            ns_prov = connection.execute(ns_provinces_sql).fetchone()[0]
            ns_com = connection.execute(ns_communes_sql).fetchone()[0]
            ns_tax = connection.execute(ns_taxon_sql).fetchone()[0]
            df.fillna(
                {
                    'taxon_dimension_id': ns_tax,
                    'provinces_id': ns_prov,
                    'communes_id': ns_com
                }, inplace=True
            )
            df['taxon_dimension_id'] = df['taxon_dimension_id'].astype(int)
            df['provinces_id'] = df['provinces_id'].astype(int)
            df['communes_id'] = df['communes_id'].astype(int)
            return df

    @classmethod
    def get_publish_formats(cls):
        return [cls.CSV, ]
