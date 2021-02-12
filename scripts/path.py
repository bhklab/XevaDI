from utils import get_project_root
from typing import Dict


def get_output_files_path(path: str) -> Dict[str, str]:
    """
    This function creates a dictionary of the path of the output files for the database tables.

    Argument(s):
        path(str): absolute path to the parent's parent directory.

    Returns:
        Dict: dictionary of the path of the output files.
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
    }
