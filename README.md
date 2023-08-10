# Open Data Cube Importer

*(Python) Tools to download data from predefined sources (e.g. Landsat) and add these to the Open Data Cube index.*

## 1) Open Data Cube

Example for indexing products and datasets from cli:
```commandline
datacube product add metadata/anthroprotect.odc-product.yaml
datacube dataset add metadata/anthroprotect.odc-metadata.yaml
```

## 2) Data sources

### 2.1) AnthroProtect

<details>
<summary>Details</summary>


#### 2.1.1) Folder structure
```
anthroprotect
|_ tiles
  |_ s2
  |_ lcs
  |_ s2_scl
|_ investigative
```

Each of the tiles subfolders contains 23922 files with the same name but different content.
There are names with 4 different patterns:

* anthropo_5.52111-59.46264_0.tif
* wdpa-Ia_6907_98.tif
* wdpa-Ib_654_68.tif
* wdpa-II_907_58.tif

The investigative folder contains 67 Sentinel-2 scenes.

"Images are ﬁltered for the time period of summer 2020 (July 1st to August 30th)."

#### 2.1.2) Open Data Cube products (can be configured differently)

The files are organized into three different ODC products:

* Sentinel-2, default name: 's2'
* Sentinel-2 scene classiﬁcation map scenes, default name: 's2_scl'
* Land cover scenes¹, default name: 'lcs'

¹ Copernicus CORINE Land Cover dataset, MODIS Land Cover Type 1, Copernicus Global Land Service, ESA GlobCover

The files from the folder 'investigative' are part of the Sentinel-2 ODC product.

Instead of the default product names custom names can be defined by setting the environment variable `ANTHROPROTECT_PRODUCT_NAMES`., e.g. `ANTHROPROTECT_PRODUCT_NAMES="s2_anthroprotect s2_scl_anthroprotect lcs_anthroprotect"`

 The names MUST be separated with a space, MUST start with 's2', 's2_scl', 'lcs' and MUST be in the correct order. The first name is used for Sentinel-2 scenes, the second for Sentinel-2 scene classiﬁcation map scenes and the third for land cover scenes.

</details>

### 2.2) Landsat 8

<details>
<summary>Details</summary>
TBD (cf. Testbed-17)
</details>

### 2.3) Copernicus Marine Environment Monitoring System (CMEMS)

<details>
<summary>Details</summary>
The importer contains loaders to import data from CMEMS. Currently, configuration for the following products is provided:

- https://data.marine.copernicus.eu/product/GLOBAL_ANALYSISFORECAST_PHY_001_024/description
- https://data.marine.copernicus.eu/product/GLOBAL_ANALYSISFORECAST_WAV_001_027/description

This includes datasets for ocean currents, physics and waves data. They can be enabled independently using the following environment variables:
- CMEMS_CURRENTS_ENABLED=<True|False>
- CMEMS_PHYSICS_ENABLED=<True|False>
- CMEMS_WAVES_ENABLED=<True|False>

Folders and ODC product names can be configured using
- CMEMS_WAVES_FOLDER and CMEMS_WAVES_PRODUCT_NAME
- CMEMS_CURRENTS_FOLDER and CMEMS_CURRENTS_PRODUCT_NAME
- CMEMS_PHYSICS_FOLDER and CMEMS_PHYSICS_PRODUCT_NAME
</details>

### 2.4) Global Forecast System (GFS)

<details>
<summary>Details</summary>
The importer contains a loader to import weather forecasts from GFS (https://www.ncei.noaa.gov/products/weather-climate-models/global-forecast)

The data source can be enabled using the environment variable GFS_ENABLED=<True|False>. Folder and ODC product name can be changed using the environment variables GFS_FOLDER and GFS_PRODUCT_NAME.
</details>

### 2.5) Water depth data (NOAA)

<details>
<summary>Details</summary>
The importer contains a loader to import water depth data from the National Centers for Environmental Information (NCEI, https://www.ncei.noaa.gov/products/etopo-global-relief-model) of the NOAA.

The data source can be enabled using the environment variable WATER_DEPTH_ENABLED=<True|False>. Folder and ODC product name can be changed using the environment variables WATER_DEPTH_FOLDER and WATER_DEPTH_PRODUCT_NAME. The download url of the NetCDF file and the file name can be changed using the environment variables WATER_DEPTH_URL and WATER_DEPTH_FILE_NAME.
</details>


## 3) Add new data source

Implement a new loader class that inherits from `loader.BasicLoader`. One example is given by `anthroprotect.AnthroprotectLoader`.

## 4) Docker

Pre-built Docker images are available at https://hub.docker.com/r/52north/opendatacube-importer.

### 4.1) Docker image folder structure

Python files:
```
${BASE_FOLDER}/odc_importer/*.py
/odc/odc_importer/*.py                (default)
```

Base folder for data and metadata files:
```
${BASE_FOLDER}/${DATA_FOLDER}
/odc/DATA                             (default)
```

Subfolder for specific data source:
```
${BASE_FOLDER}/${DATA_FOLDER}/<data source>
/odc/DATA/anthroprotect               (example)
```
