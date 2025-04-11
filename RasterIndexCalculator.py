from qgis.core import QgsApplication, QgsRasterLayer, QgsProcessingException
from RasterIndexCalculatorTask import RasterIndexCalculatorTask
from RasterSaveTask import RasterSaveTask
from osgeo import gdal
import time, os, logging, re


class RasterIndexCalculator:
    indices_formulas = {
        "Rnorm": "R / func_band_max(R)",
        "Gnorm": "G / func_band_max(G)",
        "Bnorm": "B / func_band_max(B)",
        "Rrefl_stary": "R / (R + G + B)",
        "Grefl_stary": "G / (R + G + B)",
        "Brefl_stary": "B / (R + G + B)",
        "ExR_stary": "MISSING",
        "ExG_stary": "2 * func_index(Grefl_stary) - func_index(Rrefl_stary) - func_index(Brefl_stary)",
        "ExGR_stary": "func_index(ExG_stary) - func_index(ExR_stary)",
        "ExR_wernette": "1.4 * R - G",
        "ExG_wernette": "2 * G - R - B",
        "ExB_wernette": "1.4 * B - G",
        "ExGR_wernette": "func_index(ExG_wernette) - func_index(ExR_wernette)",
        "NGRDI_wernette": "(G - R) / (G + R)",
        "MGRVI_wernette": "(G^2 - R^2) / (G^2 + R^2)",
        "GLI_wernette": "(2 * G - R - B) / (2 * G + R + B)",
        "GLI_stary": "((G - R)+ (G - B)) / (2 * G + R + B)",
        "RGBVI_wernette": "(G - (B * R)) / (G^2 + (B * R))",
        "RGBVI_stary": "(G ^ 2 - (B * R)) / (G ^ 2 + (B * R))",
        "IKAW_wernette": "(R - B) / (R + B)",
        "GLA_wernette": "((G - R) + (G - B)) / ((G + R) + (G + B))",
        "Gperc_stary": "G / (R + G + B)",
        "VARI_stary": "(G - R) / (G + R - B)",
        "TGI_stary": "G - 0.39 * R - 0.61 * B",
        "ExR_george": "1.4 * func_index(r_george) - func_index(g_george)",
        "ExG_george": "2 * func_index(g_george) - func_index(r_george) - func_index(b_george)",
        "ExGR_george": "func_index(ExG_george) - func_index(ExR_george)",
        "r_george": "func_index(Rnorm) / (func_index(Rnorm) + func_index(Gnorm) + func_index(Bnorm))",
        "g_george": "func_index(Gnorm) / (func_index(Rnorm) + func_index(Gnorm) + func_index(Bnorm))",
        "b_george": "func_index(Bnorm) / (func_index(Rnorm) + func_index(Gnorm) + func_index(Bnorm))",
        "NGRDI_stary": "(G - R) / (G + R)",
        "ExGRnorm_george": "(func_index(ExGR_george) + 2.4) / 5.4"
    }

    def __init__(self, input_files: list[str], selected_indices: str, band_mapping: dict[str, int], output_dir: str="/vsimem/", max_memory_usage:int=1024, max_active_tasks:int=5):
        self.input_files = input_files
        self.selected_indices = selected_indices.split(",")
        self.band_mapping = band_mapping
        self.output_dir = output_dir
        self.raster_layers = []
        self.results = []
        self.total_time = 0.0
        self.max_memory_usage = max_memory_usage
        self.max_active_tasks = max_active_tasks
        self.active_tasks:list[RasterIndexCalculatorTask] = []
        self.saving_tasks_queue:list[RasterIndexCalculatorTask] = []
        self.memory_usage = 0
        self.number_of_tasks = len(input_files) * len(self.selected_indices)
        self.progress = 0
        self.progress_step = self.__calculate_progress_step()
        self.raster_save_task = RasterSaveTask()

        gdal.UseExceptions()
        #self.load_raster_layers()

    def load_raster_layers(self):
        for file in self.input_files:
            self.raster_layers.append(RasterIndexCalculator.load_raster_layer(file))

    @staticmethod
    def load_raster_layer(file):
        raster_layer = QgsRasterLayer(file, os.path.basename(file))
        if not raster_layer.isValid():
            logging.critical(f"Invalid raster file: {file}")
            raise QgsProcessingException(f"Invalid raster file: {file}")
        return raster_layer

    def __validate_indices(self):
        for index in self.selected_indices:
            if index not in RasterIndexCalculator.indices_formulas:
                logging.warning(f"Unsupported index: {index}")
                raise ValueError(f"Unsupported index: {index}")

    @staticmethod
    def create_tasks(input_files:list[str], output_dir:str, band_mapping:dict[str,int], raster_layers: list[QgsRasterLayer], selected_indices: list[str]):
        for input_file in input_files:
            input_file_name = os.path.split(os.path.splitext(input_file)[0])[-1]
            raster_layer = RasterIndexCalculator.load_raster_layer(input_file)
            raster_memory_usage = RasterIndexCalculator.__calculate_raster_memory_usage(raster_layer)
            total_memory_usage = raster_memory_usage + raster_memory_usage / raster_layer.dataProvider().bandCount() # Input raster + calculated output raster

            for index in selected_indices:
                formula = RasterIndexCalculator.calculate_special_functions(raster_layer, RasterIndexCalculator.indices_formulas[index], band_mapping)

                output_in_memory_file = f"/vsimem/{input_file_name}_{index}.tiff"
                output_file = os.path.join(output_dir, f"{input_file_name}_{index}.tiff")

                logging.debug(f"Creating task for index: {index}, output file: {output_file}")
                task = RasterIndexCalculatorTask(
                    f"Calculate {index}",
                    raster_layer.clone(),
                    total_memory_usage,
                    index,
                    formula,
                    band_mapping,
                    output_in_memory_file,
                    output_file,
                )
                yield task
    
    @staticmethod
    def calculate_special_functions(raster:QgsRasterLayer, index:str, band_mapping:dict[str,int]):
        built_index = index

        special_functions = RasterIndexCalculator.extract_special_functions(index)

        while len(special_functions) > 0:

            for func in special_functions:
                full_match = func["whole_match"]
                func_name = func["function_name"]
                params = func["parameters"]

                if func_name == "band_max":
                    max_val = raster.dataProvider().bandStatistics(band_mapping[params[0]]).maximumValue
                    built_index = built_index.replace(full_match, str(max_val))
                elif func_name == "band_min":
                    min_val = raster.dataProvider().bandStatistics(band_mapping[params[0]]).minimumValue
                    built_index = built_index.replace(full_match, str(min_val))
                elif func_name == "band_mean":
                    mean_val = raster.dataProvider().bandStatistics(band_mapping[params[0]]).mean
                    built_index = built_index.replace(full_match, str(mean_val))
                elif func_name == "band_stddev":
                    stddev_val = raster.dataProvider().bandStatistics(band_mapping[params[0]]).stdDev
                    built_index = built_index.replace(full_match, str(stddev_val))
                elif func_name == "index":
                    index_val = RasterIndexCalculator.indices_formulas[params[0]]
                    built_index = built_index.replace(full_match, f"({index_val})")
            
            special_functions = RasterIndexCalculator.extract_special_functions(built_index)
            
        logging.debug(f"Input index: {index}; Built index: {built_index}")
        return built_index

    @staticmethod
    def extract_special_functions(expression):
        """
        Extracts function names, their parameters, and the whole match from a string.

        Args:
            expression (str): The input string containing functions.

        Returns:
            list: A list of dictionaries, each containing the function name, parameters, and the whole match.
        """
        # Regular expression to match functions in the format func_<name>(<parameters>)
        pattern = r"(func_(\w+)\(([^)]*)\))"
        matches = re.findall(pattern, expression)
        
        functions = []
        for full_match, func_name, params in matches:
            param_list = [param.strip() for param in params.split(",") if param.strip()]  # Split and clean parameters
            functions.append({
                "whole_match": full_match,
                "function_name": func_name,
                "parameters": param_list
            })
        
        return functions
        

    def execute(self):
        self.__validate_indices()

        start_time = time.time()

        task_generator = RasterIndexCalculator.create_tasks(self.input_files, self.output_dir, self.band_mapping, self.raster_layers, self.selected_indices)

        QgsApplication.taskManager().addTask(self.raster_save_task)

        # Start tasks
        for task in task_generator:
            if task.total_memory_usage > self.max_memory_usage:
                logging.warning(f"Task {task.description()} with {task.total_memory_usage} MB exceeds the maximum memory usage, skipping.")
                self.results.append({"index": task.index, "calculation_status": "error", "message": f"Task {task.description()} exceeds the maximum memory usage", "output_file": None, "time_spent": 0, "saving_status": None})
                continue

            self.memory_usage += task.total_memory_usage
            self.active_tasks.append(task)
            QgsApplication.taskManager().addTask(task)
            logging.debug("Adding a task")
            
            logging.debug(f"Memory usage: {self.memory_usage}")

            
            self.__transfer_finished_tasks_to_saving_queue()
            self.__save_rasters_from_memory_to_disk()
            self.__get_saved_rasters()

            # Wait for some tasks to finish or until the maximum number of concurrent tasks is reached
            if (self.memory_usage >= self.max_memory_usage or len(self.active_tasks) >= self.max_active_tasks):
                while (self.memory_usage >= self.max_memory_usage and len(self.active_tasks) >= self.max_active_tasks) and all(t.progress() != 100 for t in self.active_tasks):
                    logging.debug(f"Waiting {0.50:.2f}s, active tasks: {len(self.active_tasks)}")
                    time.sleep(0.50)
            
            
            # if len(self.saving_tasks_queue) > 0:
            #     self.save_raster_from_memory_to_disk(self.saving_tasks_queue.pop())

        # Wait for all tasks to finish
        while len(self.active_tasks) > 0:
            logging.debug(f"Waiting {0.50:.2f}s, active tasks: {len(self.active_tasks)}, memory usage: {self.memory_usage}")
            time.sleep(0.50)
            self.__transfer_finished_tasks_to_saving_queue()
            self.__save_rasters_from_memory_to_disk()
            self.__get_saved_rasters()

            # for task in self.saving_tasks_queue:
            #     self.save_raster_from_memory_to_disk(task)
            #     self.saving_tasks_queue.remove(task)

        while len(self.results) < self.number_of_tasks:
            logging.debug(f"Waiting {0.50:.2f}s, active saving: {len(self.active_tasks) - len(self.results)}, memory usage: {self.memory_usage}")
            time.sleep(0.50)
            self.__get_saved_rasters()


        self.total_time = time.time() - start_time

        return {
            "results": self.results,
            "total_time": self.total_time
        }
    
    def __get_saved_rasters(self):
        # Get saved rasters from the RasterSaveTask
        for saved_raster in self.raster_save_task.get_and_reset_saved_rasters():
            self.results.append(saved_raster["result"])
            self.__increase_progress()
            self.memory_usage -= saved_raster["total_saved_size"]

    def __save_rasters_from_memory_to_disk(self):
        self.raster_save_task.add_tasks(self.saving_tasks_queue)
        # for saving_task in self.saving_tasks_queue:
        #     self.raster_save_task.add_task(saving_task.output_file, saving_task.output_in_memory_file, saving_task.total_memory_usage, saving_task.description(), saving_task.result)

        self.saving_tasks_queue.clear()
    
    def __calculate_progress_step(self):
        return 100 / self.number_of_tasks

    def __increase_progress(self):
        self.__set_progress(round(self.progress + self.progress_step, 1))
    
    def __set_progress(self, progress):
        self.progress = progress
        logging.info(f"Progress: {self.progress}%")

    def __transfer_finished_tasks_to_saving_queue(self):
        """
        Transfers finished tasks from the active tasks list to the saving queue or handles failed tasks.
        This method iterates through the list of active tasks and checks their progress. If a task's progress
        is 100%, it is considered finished. Depending on the task's calculation status, it is either moved
        to the saving queue or handled as a failed task.
        - If the task's calculation status is "success", it is added to the saving tasks queue.
        - If the task's calculation status is not "success", it is treated as a failed task:
            - A warning is logged.
            - The memory usage is reduced by the task's total memory usage.
            - The task's result is appended to the results list.
            - The progress is increased.
        The task is then removed from the active tasks list.
        Logging:
            - Logs a debug message when a task is moved to the saving queue.
            - Logs a warning message when a task fails.
        Side Effects:
            - Modifies `self.active_tasks`, `self.saving_tasks_queue`, `self.memory_usage`, and `self.results`.
            - Calls `self.__increase_progress()` for failed tasks.
        Note:
            Ensure thread safety if this method is called in a multi-threaded environment.
        """
        for task in self.active_tasks:
            if task.progress() == 100:
                if task.result["calculation_status"] == "success":
                    logging.debug(f"Moving task from active to saving queue: {task.description()}")
                    self.saving_tasks_queue.append(task)
                else:
                    logging.warning(f"Task {task.description()} failed, skipping saving to disk.")
                    self.memory_usage -= task.total_memory_usage
                    self.results.append(task.result)
                    self.__increase_progress()
                    
                self.active_tasks.remove(task)

    @staticmethod
    def __calculate_raster_memory_usage(raster: QgsRasterLayer):
        # Get raster dimensions
        width = raster.dataProvider().ySize()  # Number of columns
        height = raster.dataProvider().xSize()  # Number of rows
        bands = raster.dataProvider().bandCount()  # Number of bands
        
        # Get the data type of the raster (e.g., Byte, Float32)
        bytes_per_pixel = raster.dataProvider().dataTypeSize(1) # 8  # Convert bits to bytes
        
        # Calculate memory usage
        memory_usage = width * height * bands * bytes_per_pixel
        logging.debug(f"Raster dimensions for {raster.name()}: {width}x{height}x{bands}; Bytes per pixel: {bytes_per_pixel}; Memory usage: {memory_usage / 1024 / 1024} MB")
        return memory_usage / 1024 / 1024  # Convert bytes to megabytes