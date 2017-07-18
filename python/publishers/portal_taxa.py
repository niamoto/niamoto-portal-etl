# coding: utf-8

from niamoto.data_publishers import TaxonDataPublisher
from niamoto.db import metadata as meta
from niamoto.db.connector import Connector


class NiamotoPortalTaxonDataPublisher(TaxonDataPublisher):
    """
    Publish taxon at the Niamoto portal format.
    """

    @classmethod
    def get_key(cls):
        return 'portal_taxa'

    @classmethod
    def get_description(cls):
        return "Publish the Niamoto taxonomy formatted for the Niamoto " \
               "portal."

    def _process(self, *args, **kwargs):
        df, _args, _kwargs = super(
            NiamotoPortalTaxonDataPublisher,
            self
        )._process(include_mptt=True, include_synonyms=False)
        df.rename(
            columns={
                'mptt_left': 'lft',
                'mptt_right': 'rght',
                'mptt_tree_id': 'tree_id',
                'mptt_depth': 'level',
            },
            inplace=True
        )
        return df, _args, _kwargs

    @classmethod
    def get_publish_formats(cls):
        return [cls.CSV, cls.SQL]
