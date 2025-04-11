from RasterIndexCalculatorTask import RasterIndexCalculatorTask
from qgis.core import QgsTask
import queue
import logging
import threading
from osgeo import gdal

class RasterSaveTask(QgsTask):
    def __init__(self, description="Raster Save Task"):
        super().__init__(description, QgsTask.CanCancel)
        self.task_queue = queue.Queue()  # Thread-safe queue
        self.running = True  # Control flag
        self.condition = threading.Condition()  # For efficient waiting

        self.__saved_rasters = []  # List of saved rasters
        self.lock = threading.Lock()  # Lock for thread-safe access

    def add_task(self, output_file, output_in_memory_file, estimated_size, description, result):
        """
        Adds a new raster save task to the queue and notifies the waiting thread.
        """
        with self.condition:
            self.task_queue.put((output_file, output_in_memory_file, estimated_size, description, result))
            self.condition.notify()  # Wake up the worker if it's waiting

    def add_tasks(self, tasks: list[RasterIndexCalculatorTask]):
        """
        Adds new raster save tasks to the queue and notifies the waiting thread.
        """
        with self.condition:
            for task in tasks:
                self.task_queue.put((task.output_file, task.output_in_memory_file, task.total_memory_usage, task.description(), task.result))
            
            self.condition.notify()  # Wake up the worker if it's waiting


    def run(self):
        """
        Waits for new raster save tasks and processes them efficiently.
        """
        logging.info("RasterSaveTask started.")
        
        while self.running:
            with self.condition:
                while self.task_queue.empty():
                    if not self.running or self.isCanceled():
                        logging.info("RasterSaveTask was canceled or stopped.")
                        return False
                    self.condition.wait()  # Block the thread until a new task is added

            # Get the next task
            output_file, output_in_memory_file, estimated_size, description, result = self.task_queue.get()

            logging.debug(f"Saving task: {description}")
            try:
                gdal.Translate(output_file, output_in_memory_file)
                logging.info(f"Successfully saved task: {description}")
                result["saving_status"] = "success"

            except Exception as e:
                gdal.Unlink(output_in_memory_file)
                logging.critical(f"Error saving task {description}: {e}")
                result["saving_status"] = f"error - {e}"
            finally:
                # Safely update the saved rasters list
                with self.lock:
                    self.__saved_rasters.append({
                        "total_saved_size": estimated_size,
                        "description": description,
                        "output_file": output_file,
                        "result": result,
                    })

        logging.info("RasterSaveTask stopped.")
        return True

    def cancel(self):
        """
        Stops the task gracefully and wakes up the waiting thread.
        """
        with self.condition:
            self.running = False
            self.condition.notify_all()  # Wake up the waiting thread to allow exit
        super().cancel()

    def get_and_reset_saved_rasters(self):
        """
        Returns a list of saved rasters with metadata and clears the list.
        This method is thread-safe.
        """
        with self.lock:
            saved_rasters_copy = self.__saved_rasters[:]  # Copy the list to return safely
            self.__saved_rasters.clear()  # Reset the list after retrieval
        return saved_rasters_copy
