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
        rainfall_dim = data_marts_api.get_dimension('rainfall')
        elevation_dim = data_marts_api.get_dimension('elevation')
        sql = \
            """
            SELECT COUNT(occ.id) AS occurrence_count,
                provinces.id AS provinces_id,
                taxon.id AS taxon_dimension_id,
                communes.id AS communes_id,
                occ_loc.id AS occurrence_location_id,
                rainfall.id AS rainfall_id,
                elevation.id AS elevation_id
            FROM {niamoto_schema}.{occ_table} AS occ
            LEFT JOIN {dimensions_schema}.{provinces_table} AS provinces
                ON ST_Intersects(occ.location, provinces.{province_geom})
            LEFT JOIN {dimensions_schema}.{communes_table} AS communes
                ON ST_Intersects(occ.location, communes.{communes_geom})
            LEFT JOIN {dimensions_schema}.{taxon_table} AS taxon
                ON occ.taxon_id = taxon.id
            LEFT JOIN {dimensions_schema}.{occ_loc_table} AS occ_loc
                ON occ.location = occ_loc.location
            LEFT JOIN {dimensions_schema}.{rainfall_table} AS rainfall
                ON (occ.properties->>'rainfall')::float = rainfall.rainfall
            LEFT JOIN {dimensions_schema}.{elevation_table} AS elevation
                ON (occ.properties->>'elevation')::float = elevation.elevation
            GROUP BY taxon.id,
                provinces.id,
                communes.id,
                occ_loc.id,
                rainfall.id,
                elevation.id;
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
                'rainfall_table': rainfall_dim.name,
                'elevation_table': elevation_dim.name,
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
        ns_rainfall_sql = \
            """
            SELECT MAX(rainfall.id)
            FROM {dimensions_schema}.{rainfall_table} as rainfall;
            """.format(**{
                'dimensions_schema': settings.NIAMOTO_DIMENSIONS_SCHEMA,
                'rainfall_table': rainfall_dim.name,
            })
        ns_elevation_sql = \
            """
            SELECT MAX(elevation.id)
            FROM {dimensions_schema}.{elevation_table} as elevation;
            """.format(**{
                'dimensions_schema': settings.NIAMOTO_DIMENSIONS_SCHEMA,
                'elevation_table': elevation_dim.name,
            })
        with Connector.get_connection() as connection:
            df = pd.read_sql(sql, connection)
            ns_prov = connection.execute(ns_provinces_sql).fetchone()[0]
            ns_com = connection.execute(ns_communes_sql).fetchone()[0]
            ns_tax = connection.execute(ns_taxon_sql).fetchone()[0]
            ns_rainfall = connection.execute(ns_rainfall_sql).fetchone()[0]
            ns_elevation = connection.execute(ns_elevation_sql).fetchone()[0]
            df.fillna(
                {
                    'taxon_dimension_id': ns_tax,
                    'provinces_id': ns_prov,
                    'communes_id': ns_com,
                    'rainfall_id': ns_rainfall,
                    'elevation_id': ns_elevation,
                }, inplace=True
            )
            df['taxon_dimension_id'] = df['taxon_dimension_id'].astype(int)
            df['provinces_id'] = df['provinces_id'].astype(int)
            df['communes_id'] = df['communes_id'].astype(int)
            df['rainfall_id'] = df['rainfall_id'].astype(int)
            df['elevation_id'] = df['elevation_id'].astype(int)
            return df

    @classmethod
    def get_publish_formats(cls):
        return [cls.CSV, ]
