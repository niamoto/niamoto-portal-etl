# coding: utf-8

from niamoto.data_publishers import PlotOccurrenceDataPublisher


class NiamotoPortalPlotOccurrenceDataPublisher(PlotOccurrenceDataPublisher):
    """
    Publish plots-occurrences at the Niamoto portal format.
    """

    @classmethod
    def get_key(cls):
        return 'portal_plots_occurrences'

    @classmethod
    def get_description(cls):
        return "Publish the plot/occurrence dataframe formatted for " \
               "the Niamoto portal."

    def _process(self, *args, drop_null_properties=False,
                 **kwargs):
        df, _args, _kwargs = super(
            NiamotoPortalPlotOccurrenceDataPublisher,
            self
        )._process()
        df.rename(
            columns={'occurrence_identifier': 'identifier'},
            inplace=True
        )
        return df, _args, _kwargs

    @classmethod
    def get_publish_formats(cls):
        return [cls.CSV, cls.SQL]
