from qgis.analysis import QgsRasterCalculator, QgsRasterCalculatorEntry
from qgis.core import QgsApplication, QgsTask, QgsMessageLog, Qgis, QgsRasterLayer, QgsRasterBandStats
import time, logging, os

class RasterIndexCalculatorTask(QgsTask):
    def __init__(self, description: str, raster_layer: QgsRasterLayer, total_memory_usage:int, index, formula: str, band_mapping: dict[str, int], output_in_memory_file:str, output_file: str):
        super().__init__(description, QgsTask.CanCancel)
        self.raster_layer = raster_layer
        self.index = index
        self.formula = formula
        self.band_mapping = band_mapping
        self.output_in_memory_file = output_in_memory_file
        self.task_output_file = self.output_in_memory_file if self.output_in_memory_file else self.output_file
        self.output_file = output_file
        self.result = None
        self.total_memory_usage = total_memory_usage

    def run(self):
        try:
            logging.debug(f"Starting calculation for index: {self.index}")
            entries = []
            for band_name, band_index in self.band_mapping.items():
                entry = QgsRasterCalculatorEntry()
                entry.ref = f'{band_name}@{band_index}'
                entry.raster = self.raster_layer
                entry.bandNumber = band_index
                entries.append(entry)

            formula_with_bands = self.formula
            for band_name, band_index in self.band_mapping.items():
                formula_with_bands = formula_with_bands.replace(band_name, f'"{band_name}@{band_index}"')

            logging.debug(f"Formula for {self.index}: {formula_with_bands}")
            start_time = time.time()

            calc = QgsRasterCalculator(
                formula_with_bands,
                self.output_in_memory_file,
                "GTiff",
                self.raster_layer.extent(),
                self.raster_layer.width(),
                self.raster_layer.height(),
                entries
            )

            result = calc.processCalculation()
            time_spent = time.time() - start_time

            self.raster_layer = None

            if result != QgsRasterCalculator.Success:
                self.result = {"index": self.index, "calculation_status": "error", "message": calc.lastError(), "output_file": None, "time_spent": time_spent, "saving_status": None}
                logging.warning(f"Failed to calculate index: {self.index}")
            else:
                self.result = {"index": self.index, "calculation_status": "success", "message": None, "output_file": None, "time_spent": time_spent, "saving_status": None}
                logging.info(f"Successfully calculated index: {self.index} in {time_spent:.2f} seconds")
        except Exception as e:
            time_spent = time.time() - start_time
            self.result = {"index": self.index, "calculation_status": "exception", "message": str(e), "output_file": None, "time_spent": time_spent, "saving_status": None}
            logging.critical(f"Error calculating index {self.index}: {e}")
        finally:
            self.setProgress(100)
            return self.result["calculation_status"] == "success"

    def cancel(self):
        logging.warning(f"Task {self.index} was canceled.")
        super().cancel()

    def finished(self, success):
        if success:
            logging.info(f"{self.result}")
        else:
            logging.warning(f"{self.result}")
