#!/bin/bash

# Get the source redshift cluster name and target db name from the user

echo "Enter the source cluster name"
read source_cluster

echo "Enter the target cluster name"
read target_cluster

# Check if cluster directories exist, if not create them

if [ ! -d ./${source_cluster} ]; then
    mkdir ./${source_cluster}
fi

if [ ! -d ./${target_cluster} ]; then
    mkdir ./${target_cluster}
fi

# Get the source db name and target db name from the user

echo "Enter the source db name"
read source_db

echo "Enter the target db name"
read target_db

# Check if db directories exist, if not create them

if [ ! -d ./${source_cluster}/${source_db} ]; then
    mkdir ./${source_cluster}/${source_db}
fi

if [ ! -d ./${target_cluster}/${target_db} ]; then
    mkdir ./${target_cluster}/${target_db}
fi

# Re-write the sql and replace the db names

ls -l ./${source_cluster}/${source_db}/ | awk ' { print $9 } ' | while read sql_file; do sed "s/${source_db}\./${target_db}\./g" ./${source_cluster}/${source_db}/${sql_file} > ./${target_cluster}/${target_db}/${sql_file}; done