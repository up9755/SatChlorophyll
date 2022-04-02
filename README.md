# SatChlorophyll
This project contains scripts for acquiring, processing, joining and comparing satellite and in-situ chlorophyll data.
These scripts were used for my bachelor's thesis project.
They are provided here for anyone that might find them useful (as a reference or a starting point), but are not guaranteed to work entirely.


## Acquiring data


### Acquiring in-situ data

The in-situ chlorophyll concentration data is downloaded from [cdi.seadatanet.org](https://cdi.seadatanet.org/search), specifically using [these search parameters](https://cdi.seadatanet.org/search/welcome.php?query=1969&query_code={3319AA32-02B1-4133-AFAC-C39A898B7437}). The downloaded file is then extracted and contents placed into the `SatChlorophyll/data/in-situ/raw` folder.


### Acquiring satellite data

The satellite chlorophyll concentration data is acquired from [copernicus.eu](https://resources.marine.copernicus.eu/products), specifically this one: [Mediterranean Sea Reprocessed Surface Chlorophyll Concentration and Phytoplankton Functional Types from Multi Satellite observations](https://resources.marine.copernicus.eu/product-detail/OCEANCOLOUR_MED_CHL_L3_REP_OBSERVATIONS_009_073/INFORMATION).

The data is downloaded using a CLI program called [motuclient](https://pypi.org/project/motu-client/). The script is in `SatChlorophyll/data/sat` folder. Edit the script before running it (provide your Copernicus marine account username and password and the directory to download the files). Running the script will download the satellite chlorophyll concentration data into several files. Each downloaded half-year file is about 500MB in size, all files in total are about 11GB in size.


## Processing data


### Processing in-situ data

In-situ data is processed using a Python script. The script is located in `SatChlorophyll/data/in-situ`. Open the script in a text editor to see available options and their descriptions. You can modify the values in the section marked `USER VARIABLES`.

The script processes the files that are listed in the `filelist.txt` file. Follow the instructions inside this file to properly configure which files you want processed.


### Processing satellite data

Due to the amount of memory that uncompressed satellite data requires, the step of processing satellite data has been moved into the step of joining the two data sets.


## Joining and comparing data

The process of joining and comparing in-situ and satellite data is achieved using a script written in R (RStudio was the IDE used to write, test and run it).

The project is located in `SatChlorophyll/joining/AdriaticSeaChlA_03_REP` folder. The code is contained in `notebook_main.Rmd` file. RStudio can be used to open the `AdriaticSeaChlA_03.Rproj` project first.

The `notebook_main.Rmd` file needs to be edited and configured first.

The block named `user_settings` has all available user-specified settings, along with some file paths and file names that need to be set.

Bathymetry data is useful to have, but not required (set the variable `setting_situ_floor_depth_use` to `FALSE`). If available, it can be placed inside the `SatChlorophyll/data/bathymetry` folder (or anywhere else if the path to it is specified in the R script).

The first half of the notebook file (~1000 lines) contains the initial setup and the steps that join the data and calculate the accuracy. In RStudio, click on `Run all chunks above` option on the right side of the block named `visualize_scatterplot` to join all data again using the parameters specified in the beginning of the script. In the joining steps, the joined data is saved to the disk (`.rds` file containing serialized R data and `.csv` file containing the data in a table using comma separated values format). Once the final values are calculated, the results (along with all user-defined settings) are saved (appended) to a `log.txt` file.

The second half of the notebook contains code to generate various graphs from the data. Each section has a description on what it does and some instructions.
