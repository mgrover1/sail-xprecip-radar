#!/bin/bash
#Script to untar all SAIL files within a directory

#Written: Joe O'Brien <obrienj@anl.gov> - 18 Sept 2022

# Define a syntax statement. 
Syntax ()
{
  bold=`tput bold`
  normal=`tput sgr0`
  echo "SYNTAX:  sail_untar.sh output_dir"
  echo ""
  echo "PURPOSE: To use untar any SAIL xprecipradar file and move to output directory"
  echo ""
  exit 1
}

# Process command line parameters.
if [ $# -gt 0 ]; then
  for index in "$@"; do
    if [[ "$index" == "help" || "$index" == "h" || "$index" == "-h" || "$index" == "-help" ]]; then
      Syntax
      exit 1
    fi
    outdir=$index
  done
fi

# Untar all the files
echo "OUTPUT DIRECTORY: " $outdir

# Define all the files 
file_list="`ls  *.tar`"                           #List of all tar files within directory

# Loop through all files and untar to output directory
for file in ${file_list}; do
  tar -xvf $file -C $outdir
done