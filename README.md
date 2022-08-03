# testbed18-dockerfiles

```commandline
datacube product add metadata/anthroprotect.odc-product.yaml
datacube dataset add metadata/anthroprotect.odc-metadata.yaml
```


# Data

Folder structure:
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

# Open Data Cube

Datacube products:

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