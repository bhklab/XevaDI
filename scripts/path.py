import glob
from typing import Dict


def get_input_files_path(path: str) -> Dict[str, list]:
    """
    This function creates a dictionary of the path of the input files for the database tables.
    The dictionary has a key that is string and value that is a list of the input files for the
    corresponding key.

    Argument(s):
        path(str): path to the input data directory.

    Returns:
        Dict[str, list]: dictionary of the path of the output files.
    """
    return {
        'batch_information': glob.glob(f'{path}/*/batch_information.*'),
        'batch_response': glob.glob(f'{path}/*/batch_response.*'),
        'copy_number_variation': glob.glob(f'{path}/*/copy_number_variation.*'),
        'drug_screening': glob.glob(f'{path}/*/drug_screening.*'),
        'model_information': glob.glob(f'{path}/*/model_information.*'),
        'model_response': glob.glob(f'{path}/*/model_response.*'),
        'modelid_moleculardata_mapping': glob.glob(f'{path}/*/modelid_moleculardata_mapping.*'),
        'mutation': glob.glob(f'{path}/*/mutation.*'),
        'rna_sequencing': glob.glob(f'{path}/*/rna_sequencing.*'),
        'model_sheet': f'{path}/TNBC/model_information_FileLink.xlsx',
        'drug_annotation': f'{path}/drug_annotations.csv',
        'pubchem_annotation': f'{path}/drug_annotations_1.csv',
        'gene_drug_tissue': glob.glob(f'{path}/BiomarkerData/**', recursive=True)
    }


def get_output_files_path(path: str) -> Dict[str, str]:
    """
    This function creates a dictionary of the path of the output files for the database tables.

    Argument(s):
        path(str): path to the output data directory.

    Returns:
        Dict[str, str]: dictionary of the path of the output files.
    """
    return {
        'drug': f'{path}/drugs.csv',
        'model': f'{path}/models.csv',
        'dataset': f'{path}/datasets.csv',
        'patient': f'{path}/patients.csv',
        'tissue': f'{path}/tissues.csv',
        'batch': f'{path}/batches.csv',
        'sequencing': f'{path}/sequencing.csv',
        'gene': f'{path}/genes.csv',
        'batch_response': f'{path}/batch_response.csv',
        'batch_information': f'{path}/batch_information.csv',
        'model_response': f'{path}/model_response.csv',
        'drug_screening': f'{path}/drug_screening.csv',
        'model_information': f'{path}/model_information.csv',
        'mutation': f'{path}/mutation.csv',
        'copy_number_variation': f'{path}/copy_number_variation.csv',
        'rna_sequencing': f'{path}/rna_sequencing.csv',
        'modelid_moleculardata_mapping': f'{path}/modelid_moleculardata_mapping.csv',
        'model_sheet': f'{path}/model_sheets.csv',
        'drug_annotation': f'{path}/drug_annotations.csv',
        'dataset_patient': f'{path}/datasets_patients.csv',
        'dataset_tissue': f'{path}/datasets_tissues.csv',
        'dataset_drug': f'{path}/datasets_drugs.csv',
        'dataset_gene': f'{path}/datasets_genes.csv',
        'gene_drug_tissue': f'{path}/gene_drug_tissue.csv',
    }
