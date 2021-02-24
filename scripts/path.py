import glob
from utils import get_project_root
from typing import Dict


def get_input_files_path(path: str) -> Dict[str, list]:
    """
    This function creates a dictionary of the path of the input files for the database tables.
    The dictionary has a key that is string and value that is a list of the input files for the
    corresponding key.

    Argument(s):
        path(str): absolute path to the parent's parent directory.

    Returns:
        Dict[str, list]: dictionary of the path of the output files.
    """
    return {
        'batch_information': glob.glob(f'{path}/input_data/*/batch_information.*'),
        'batch_response': glob.glob(f'{path}/input_data/*/batch_response.*'),
        'copy_number_variation': glob.glob(f'{path}/input_data/*/copy_number_variation.*'),
        'drug_screening': glob.glob(f'{path}/input_data/*/drug_screening.*'),
        'model_information': glob.glob(f'{path}/input_data/*/model_information.*'),
        'model_response': glob.glob(f'{path}/input_data/*/model_response.*'),
        'modelid_moleculardata_mapping': glob.glob(f'{path}/input_data/*/modelid_moleculardata_mapping.*'),
        'mutation': glob.glob(f'{path}/input_data/*/mutation.*'),
        'rna_sequencing': glob.glob(f'{path}/input_data/*/rna_sequencing.*'),
        'model_sheet': f'{path}/input_data/model_sheets.csv',
        'drug_annotation': f'{path}/input_data/drug_annotations.csv',
        'pubchem_annotation': f'{path}/input_data/drug_annotations_1.csv',
    }


def get_output_files_path(path: str) -> Dict[str, str]:
    """
    This function creates a dictionary of the path of the output files for the database tables.

    Argument(s):
        path(str): absolute path to the parent's parent directory.

    Returns:
        Dict[str, str]: dictionary of the path of the output files.
    """
    return {
        'drug': f'{path}/output_data/drugs.csv',
        'model': f'{path}/output_data/models.csv',
        'dataset': f'{path}/output_data/datasets.csv',
        'patient': f'{path}/output_data/patients.csv',
        'tissue': f'{path}/output_data/tissues.csv',
        'batch': f'{path}/output_data/batches.csv',
        'sequencing': f'{path}/output_data/sequencing.csv',
        'gene': f'{path}/output_data/genes.csv',
        'batch_response': f'{path}/output_data/batch_response.csv',
        'batch_information': f'{path}/output_data/batch_information.csv',
        'model_response': f'{path}/output_data/model_response.csv',
        'drug_screening': f'{path}/output_data/drug_screening.csv',
        'model_information': f'{path}/output_data/model_information.csv',
        'mutation': f'{path}/output_data/mutation.csv',
        'copy_number_variation': f'{path}/output_data/copy_number_variation.csv',
        'rna_sequencing': f'{path}/output_data/rna_sequencing.csv',
        'modelid_moleculardata_mapping': f'{path}/output_data/modelid_moleculardata_mapping.csv',
        'model_sheet': f'{path}/output_data/model_sheets.csv',
        'drug_annotation': f'{path}/output_data/drug_annotations.csv',
    }
