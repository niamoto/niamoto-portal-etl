# coding: utf-8

from datetime import datetime

from sqlalchemy import select, func, cast, String
import pandas as pd
import geopandas as gpd

from niamoto.data_publishers.base_data_publisher import BaseDataPublisher
from niamoto.db import metadata as meta
from niamoto.db.connector import Connector


class NiamotoPortalPlotDataPublisher(BaseDataPublisher):
    """
    Publish plots at the Niamoto portal format.
    """

    @classmethod
    def get_key(cls):
        return 'portal_plots'

    @classmethod
    def get_description(cls):
        return "Publish the plot dataframe formatted for " \
               "the Niamoto portal."

    def _process(self, *args, drop_null_properties=False,
                 **kwargs):
        with Connector.get_connection() as connection:
            properties = ['width', 'height', 'elevation']
            keys = properties
            props = [meta.plot.c.properties[k].label(k) for k in keys]
            sel = select([
                meta.plot.c.id.label('id'),
                meta.plot.c.name.label('name'),
                cast(meta.plot.c.location, String).label('location'),
            ] + props)
            df = gpd.read_postgis(
                sel,
                connection,
                index_col='id',
                geom_col='location',
                crs='+init=epsg:4326'
            )
            #  Replace None values with nan
            df.fillna(value=pd.np.NAN, inplace=True)
            if drop_null_properties:
                for k in keys:
                    df = df[df[k].notnull()]
            return df, [], {'index_label': 'id'}

    @classmethod
    def get_publish_formats(cls):
        return [cls.CSV, cls.SQL]
