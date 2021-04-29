from path import get_output_files_path, get_input_files_path
from secondary_tables import batch_response_table, batch_information_table, model_response_table, \
    drug_screening_table, model_information_table, modelid_moleculardata_mapping, mutation, copy_number_variation, \
    rna_sequencing
from utils import get_project_root
from pathlib import Path
from primary_tables import dataset_table, drug_table, tissue_table, patient_table, gene_table, sequencing_table, \
    batch_table, model_table
from annotation_tables import drug_annotation_table
from model_sheets import model_sheets_table
from dataset_join_tables import datasets_tissues_table, datasets_drugs_table, \
    datasets_genes_table


# get the path of the root directory.
project_path = f'{get_project_root()}'

# input files and output files directory.
input_data_path = f'{project_path}/input_data'
output_data_path = f'{project_path}/output_datas'

# raise an error if the input files' directory is not present.
if not Path(input_data_path).is_dir():
    raise FileNotFoundError('Input data files are not present!')

# raise an error if the output data directory is present.
if Path(output_data_path).is_dir():
    raise Exception('Output directory is already present!')
else:
    Path(output_data_path).mkdir()

# get the input files' and output files' dictionary.
input_files_dict = get_input_files_path(input_data_path)
output_files_dict = get_output_files_path(output_data_path)

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

# creating dataset join tables.
datasets_tissues_table(input_files_dict, output_files_dict)
datasets_drugs_table(input_files_dict, output_files_dict)
# datasets_genes_table(input_files_dict, output_files_dict)

# creating secondary tables.
batch_response_table(input_files_dict, output_files_dict)
batch_information_table(input_files_dict, output_files_dict)
model_response_table(input_files_dict, output_files_dict)
drug_screening_table(input_files_dict, output_files_dict)
model_information_table(input_files_dict, output_files_dict)
modelid_moleculardata_mapping(input_files_dict, output_files_dict)
mutation(input_files_dict, output_files_dict)
copy_number_variation(input_files_dict, output_files_dict)
rna_sequencing(input_files_dict, output_files_dict)

# creating model sheets table.
model_sheets_table(input_files_dict, output_files_dict)
