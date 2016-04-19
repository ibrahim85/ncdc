#!/usr/bin/env bash

# global parameters
g_temp_folder="ncdc_tmp";
g_data_folder="ncdc_data";
g_url="ftp://ftp.ncdc.noaa.govpub/data/noaa";

# $1: folder_path
function create_folder {
    mkdir -p "$1"
}

# $1: year to download
function download_data {
    local source_url="$g_url/$1"
    wget -r -c -nv --no-parent -P "$g_temp_folder" "$source_url";
}

# $1: year to process
function process_data {
    local year="$1"
    local local_path="$g_temp_folder/$year"
    local tmp_output_file="$g_temp_folder/$year"
    for file in $local_path/*; do
        gunzip -c $file >> "$tmp_output_file"
    done
    dest_file="$g_data_folder/$year"
    mv "$tmp_output_file" "$dest_file"

    rm -rf "$local_path"
}

# $1 - start year
# $2 - finish year
function main {
    local start_year=1901
    if [ -n "$1" ]; then
        start_year=$1
    fi

    local finish_year=$start_year
    if [ -n "$2" ]; then
        finish_year=$2
    fi

    create_folder $g_temp_folder
    create_folder $g_data_folder

    for year in `seq $start_year $finish_year`; do
        download_data $year
        process_data $year
    done

    rm -rf "$g_temp_folder"
}

main $1 $2
