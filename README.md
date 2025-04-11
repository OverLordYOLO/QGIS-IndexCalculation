
# Raster Index Calculator Documentation

## Overview

This documentation describes the functionality and usage of the raster index calculation system built using QGIS and GDAL. The system includes three main components:

- `RasterIndexCalculator`: Manages the entire pipeline.
- `RasterIndexCalculatorTask`: Performs individual raster index computations.
- `RasterSaveTask`: Saves the computed rasters to disk.

---

## RasterIndexCalculator

### Description
Main orchestrator class for calculating raster indices from input raster files using specified formulas.

### Constructor
```python
RasterIndexCalculator(
    input_files: list[str],
    selected_indices: str,
    band_mapping: dict[str, int],
    output_dir: str = "/vsimem/",
    max_memory_usage: int = 1024,
    max_active_tasks: int = 5
)
```

### Example Usage
```python
input_files = ["path/to/raster1.tif", "path/to/raster2.tif"]
selected_indices = "ExG_wernette,ExR_wernette"
band_mapping = {"R": 1, "G": 2, "B": 3}
calculator = RasterIndexCalculator(input_files, selected_indices, band_mapping)
results = calculator.execute()
print(results)
```

---

## RasterIndexCalculatorTask

### Description
Defines a single task for calculating one index using QGIS's `QgsRasterCalculator`.

### Example Usage
```python
task = RasterIndexCalculatorTask(
    description="Calculate ExG",
    raster_layer=raster_layer,
    total_memory_usage=50,
    index="ExG",
    formula="2 * G - R - B",
    band_mapping={"R": 1, "G": 2, "B": 3},
    output_in_memory_file="/vsimem/raster1_ExG.tif",
    output_file="output/raster1_ExG.tif"
)
```

---

## RasterSaveTask

### Description
Handles saving of calculated rasters from memory to disk using GDAL.

### Example Usage
```python
save_task = RasterSaveTask()
save_task.add_task(
    output_file="output.tif",
    output_in_memory_file="/vsimem/output.tif",
    estimated_size=25,
    description="Saving output.tif",
    result={"index": "ExG", "calculation_status": "success", "output_file": None}
)
```

---

## Workflow Summary

1. Instantiate `RasterIndexCalculator`.
2. Call `execute()` to compute and save indices.
3. Review returned results including paths and statuses.

## Environment setup

Execute the code using Python installed in QGIS to be able to use the QGIS modules.

**Debugging in VSCode**
For debugging purposes in VSCode, there is a `.env` file pointing to the installation location of QGIS. Change the OSGEO4W_ROOT and python version to correspond with your setup.
There is also a `.vscode/settings.json` that points VSCode to the `.env` file.