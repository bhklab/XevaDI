from path import get_output_files_dict, get_input_files_dict
from utils import get_project_root
from pathlib import Path
from primary_tables import dataset_table, drug_table, tissue_table, patient_table, gene_table, sequencing_table, batch_table, model_table
from annotation_tables import drug_annotation_table
from model_sheets import model_sheets_table

# get the path of the root directory.
project_path = f'{get_project_root()}'

# input files and output files directory.
input_data_path = f'{project_path}/input_data'
output_data_path = f'{project_path}/output_data'

# raise an error if the input files' directory is not present.
if not Path(input_data_path).is_dir():
    raise FileNotFoundError('Input data files are not present!')

# raise an error if the output data directory is present.
if Path(output_data_path).is_dir():
    raise Exception('Output directory is already present!')
else:
    Path(output_data_path).mkdir()

# get the input files' and output files' dictionary.
input_files_dict = get_input_files_dict(input_data_path)
output_files_dict = get_output_files_dict(output_data_path)

# <-----------------------------------------------------------Building Database
# Tables------------------------------------------------------->

# creating primary tables.
dataset_table(output_files_dict)
drug_table(input_files_dict, output_files_dict)
tissue_table(input_files_dict, output_files_dict)
patient_table(input_files_dict, output_files_dict)
gene_table(project_path, output_files_dict)
sequencing_table(project_path, output_files_dict)
batch_table(input_files_dict, output_files_dict)
model_table(input_files_dict, output_files_dict)

# creating annotation tables.
drug_annotation_table(input_files_dict, output_files_dict)

# creating secondary tables.

# creating model sheets table.
model_sheets_table(input_files_dict, output_files_dict)
