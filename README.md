# Open Data Cube Importer

*(Python) Tools to download data from predefined sources (e.g. Landsat) and add these to the Open Data Cube index.*

## 1) Open Data Cube

Example for indexing products and datasets from cli:
```commandline
datacube product add metadata/anthroprotect.odc-product.yaml
datacube dataset add metadata/anthroprotect.odc-metadata.yaml
```

## 2) Datasets

### 2.1) AnthroProtect

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

* s2-anthropo
* s2-wdpa-Ia
* s2-wdpa-Ib
* s2-wdpa-II
* lcs-anthropo
* lcs-wdpa-Ia
* lcs-wdpa-Ib
* lcs-wdpa-II
* s2_scl-anthropo
* s2_scl-wdpa-Ia
* s2_scl-wdpa-Ib
* s2_scl-wdpa-II
* s2_investigative

### 2.2) Landsat 8

TBD (cf. Testbed-17)

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
