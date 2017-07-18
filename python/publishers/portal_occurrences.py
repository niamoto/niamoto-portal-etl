# coding: utf-8

from datetime import datetime

from sqlalchemy import select, cast, String
import pandas as pd
import geopandas as gpd

from niamoto.data_publishers.base_data_publisher import BaseDataPublisher
from niamoto.db import metadata as meta
from niamoto.db.connector import Connector


class NiamotoPortalOccurrenceDataPublisher(BaseDataPublisher):
    """
    Publish occurrences at the Niamoto portal format.
    """

    @classmethod
    def get_key(cls):
        return 'portal_occurrences'

    @classmethod
    def get_description(cls):
        return "Publish the occurrence dataframe formatted for " \
               "the Niamoto portal."

    def _process(self, *args, drop_null_properties=False,
                 **kwargs):
        with Connector.get_connection() as connection:
            properties = ['date_observation', 'height', 'stem_nb', 'dbh',
                          'status', 'wood_density', 'bark_thickness',
                          'elevation', 'rainfall']
            keys = properties
            props = [meta.occurrence.c.properties[k].label(k) for k in keys]
            sel = select([
                meta.occurrence.c.id.label('id'),
                meta.occurrence.c.taxon_id.label('taxon_id'),
                cast(meta.occurrence.c.location, String).label('location'),
            ] + props)
            df = gpd.read_postgis(
                sel,
                connection,
                index_col='id',
                geom_col='location',
                crs='+init=epsg:4326'
            )
            df['taxon_id'] = df['taxon_id'].apply(pd.to_numeric)
            df.rename(
                columns={'date_observation': 'date', 'mnt': 'elevation'},
                inplace=True
            )
            df['created_at'] = datetime.now()
            #  Replace None values with nan
            df.fillna(value=pd.np.NAN, inplace=True)
            if drop_null_properties:
                for k in keys:
                    df = df[df[k].notnull()]
            return df, [], {'index_label': 'id'}

    @classmethod
    def get_publish_formats(cls):
        return [cls.CSV, cls.SQL]
