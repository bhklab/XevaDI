import glob
import re
import numpy as np
from typing import NoReturn, Dict
from utils import read_data_in_data_frame, concat_data_frame, write_data_to_csv, create_series, comment, create_unique_list


def dataset_table(output_files: Dict) -> NoReturn:
    """
    This function creates a pandas data series from the dataset list
    and writes it to the csv file.

    Arguments:
        output_files (dict): Contains the dictionary of the output files.
    """

    # comment that the dataset table is being built.
    comment('dataset')

    # dataset list.
    datasets = ['PDXE (Breast Cancer)', 'PDXE (Colorectal Cancer)', 'PDXE (Cutaneous Melanoma)',
                'PDXE (Gastric Cancer)', 'PDXE (Non-small Cell Lung Carcinoma)', 'PDXE (Pancreatic Ductal Carcinoma)',
                'UHN (Breast Cancer)', 'McGill (Breast Cancer)']

    # create dataset series.
    dataset_series = create_series(datasets, 'dataset_name')

    # write data to the csv file.
    write_data_to_csv(dataset_series, output_files['dataset'], 'dataset_id')


def drug_table(input_files: Dict, output_files: Dict) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        input_files (dict): Contains the dictionary of the input files.
        output_files (dict): Contains the dictionary of the output files.
    """

    # comment that the drug table is being built.
    comment('drug')

    # data type variable containing the datatype of drug.
    data_type = {'drug': str}

    # unique drug list.
    drugs = create_unique_list(
        input_files['drug_screening'], 'drug', True, data_type)

    # create the drug data frame.
    drug_series = create_series(np.unique(drugs), 'drug_name')

    # write data/dataframe to the csv file.
    write_data_to_csv(drug_series, output_files['drug'], 'drug_id')


def tissue_table(input_files: Dict, output_files: Dict) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        input_files (dict): Contains the dictionary of the input files.
        output_files (dict): Contains the dictionary of the output files.
    """

    # comment that the tissue table is being built.
    comment('tissue')

    # data type variable containing the datatype of tissue.
    data_type = {'tissue': str}

    # unique tissue list.
    tissues = create_unique_list(
        input_files['model_information'], 'tissue', False, data_type)

    # create the tissue panda series.
    tissue_series = create_series(np.unique(tissues), 'tissue_name')

    # write pandas series to the csv file.
    write_data_to_csv(tissue_series, output_files['tissue'], 'tissue_id')


def patient_table(input_files: Dict, output_files: Dict) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        input_files (dict): Contains the dictionary of the input files.
        output_files (dict): Contains the dictionary of the output files.
    """

    # comment that the patient table is being built.
    comment('patient')

    # data type variable containing the datatype of the patient id.
    data_type = {'patient.id': str}

    # unique patient list.
    patients = create_unique_list(
        input_files['model_information'], 'patient.id', False, data_type)

    # create the patient panda series.
    patient_series = create_series(np.unique(patients), 'patient')

    # write pandas series to the csv file.
    write_data_to_csv(patient_series, output_files['patient'], 'patient_id')


def gene_table(path: str, output_files: Dict) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        path(str): absolute path to the parent's parent directory.
        output_files (dict): Contains the dictionary of the output files.
    """

    # comment that the gene table is being built.
    comment('gene')

    # input files to read.
    input_files = [f for f in glob.glob(f'{path}/input_data/*/*')
                   if re.search(r'(copy_number_variation|mutation|rna_sequencing)', f)]

    # data type variable containing the datatype of gene.
    data_type = {'gene.id': str}

    # unique gene list.
    genes = create_unique_list(input_files, 'gene.id', False, data_type)

    # create the gene panda series.
    gene_series = create_series(np.unique(genes), 'gene_name')

    # write pandas series to the csv file.
    write_data_to_csv(gene_series, output_files['gene'], 'gene_id')


def sequencing_table(path: str, output_files: Dict) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        path(str): absolute path to the parent's parent directory.
        output_files (dict): Contains the dictionary of the output files.
    """

    # comment that the gene table is being built.
    comment('sequencing')

    # input files to read and output file path.
    input_files = [f for f in glob.glob(f'{path}/input_data/*/*')
                   if re.search(r'(copy_number_variation|mutation|rna_sequencing)', f)]

    # data type variable containing the datatype of sequencing.
    data_type = {'sequencing.uid': str}

    # sequencing list.
    sequencing_uids = create_unique_list(
        input_files, 'sequencing.uid', False, data_type)

    # create the gene panda series.
    sequencing_series = create_series(
        np.unique(sequencing_uids), 'sequencing_id')

    # write pandas series to the csv file.
    write_data_to_csv(sequencing_series,
                      output_files['sequencing'], 'sequencing_uid')


def batch_table(input_files: Dict, output_files: Dict) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        input_files (dict): Contains the dictionary of the input files.
        output_files (dict): Contains the dictionary of the output files.
    """

    # comment that the batch table is being built.
    comment('batch')

    # data type variable containing the datatype of batch.
    data_type = {'batch.id': str}

    # batches list.
    batches = create_unique_list(
        input_files['batch_information'], 'batch.id', False, data_type)

    # create the batch panda series.
    batch_series = create_series(np.unique(batches), 'batch')

    # write pandas series to the csv file.
    write_data_to_csv(batch_series, output_files['batch'], 'batch_id')


def model_table(input_files: Dict, output_files: Dict) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        input_files (dict): Contains the dictionary of the input files.
        output_files (dict): Contains the dictionary of the output files.
    """

    # comment that the model table is being built.
    comment('model')

    # concatenated data frame.
    model_input_df = concat_data_frame(
        input_files['model_information'], {'model.id': str, 'patient.id': str})

    # patient information data frame.
    patient_df = read_data_in_data_frame(
        output_files['patient'], {'patient_id': int, 'patient': str})

    # merge the data frames.
    merged_df = model_input_df.merge(
        patient_df, left_on='patient.id', right_on='patient')

    # renaming column in the dataframe.
    merged_df.rename(columns={'model.id': 'model'}, inplace=True)

    # write pandas series to the csv file.
    write_data_to_csv(
        merged_df[['model', 'patient_id']], output_files['model'], 'model_id')
