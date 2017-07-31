#!/usr/bin/env bash

# Create schemas. Adapt according to the database host.
psql -U niamoto -d niamoto -c "\
    CREATE SCHEMA niamoto;
    CREATE SCHEMA niamoto_raster;
    CREATE SCHEMA niamoto_vector;
    CREATE SCHEMA niamoto_dimensions;
    CREATE SCHEMA niamoto_fact_tables;"

# Init niamoto db
niamoto init_db

# Add elevation and rainfall rasters
niamoto add_raster elevation mnt10_wgs84_compressed.tif
niamoto add_raster rainfall rainfall_wgs84.tif

# Set taxonomy
niamoto set_taxonomy ~/niamoto/ncpippn_taxonomy.csv

# Add pl@ntnote provider
niamoto add_provider plantnote PLANTNOTE niamoto

# Sync
niamoto sync plantnote NC-PIPPN-12032017.sqlite

# Extract raster values
niamoto all_rasters_to_occurrences
niamoto all_rasters_to_plots

# Publish data to the portal
niamoto publish portal_occurrences sql -d niamoto_data_occurrence --schema public --if_exists truncate --truncate_cascade
niamoto publish portal_plots sql -d niamoto_data_plot --schema public --if_exists truncate --truncate_cascade
niamoto publish portal_plots_occurrences sql -d niamoto_data_plotoccurrences --schema public --if_exists truncate

# Add provinces and communes vectors
niamoto add_vector provinces ~/niamoto/vectors/provinces/provinces.shp
niamoto add_vector communes ~/niamoto/vectors/communes/communes.shp

# Create and populate dimensions
niamoto create_vector_dimension provinces --populate
niamoto create_vector_dimension communes --populate
niamoto create_taxon_dimension --populate
niamoto create_occurrence_location_dimension --populate

# Create the taxon_observed_occurrences fact table
niamoto create_fact_table taxon_observed_occurrences -d provinces -d communes -d taxon_dimension -d occurrence_location -m occurrence_count

# Populate the taxon_observed_occurrences fact table
niamoto populate_fact_table taxon_observed_occurrences occurrence_fact_table
