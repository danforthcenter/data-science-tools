# Data Science Tools

[![Build Status](https://travis-ci.com/danforthcenter/data-science-tools.svg?branch=master)](https://travis-ci.com/danforthcenter/data-science-tools)

A collection of Data Science Facility tools for data management and other tasks.

**Introduction**

This tool has been written to replace [PhenoFront](https://github.com/danforthcenter/PhenoFront) and allow a user to connect to a LemnaTec database and download snapshots without the requirement to use LemnaTec software.

**Instructions**

1. Open database.example-config and adjust parameters in the JSON file to suit your set-up. Save with a new name.

2. Open the command prompt and run `LT-db-extractor.py` with the config file as input (`-c`) and specify an output directory (`-o`) plus experiment (`-e`, your `measurement_label`) to query. For example: `C:/path/to/data-science-tools/LT-db-extractor.py -c T:/path/to/database.config.file -o T:/path/to/my/output/experiment47 -e "Experiment47"`. The script will connect to the database server to query image and experimental metadata. It identifies raw LemnaTec images (in RAWX format) and transfers them to the local machine via SFTP, then it converts them to PNG files. Metadata will be saved to a CSV. Downloaded blob files will be deleted.

3. Utilise [PlantCV](https://github.com/danforthcenter/plantcv) to analyse your data.





