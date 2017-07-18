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
