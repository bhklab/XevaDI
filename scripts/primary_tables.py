import glob
import pathlib
import re
import pandas as pd
import numpy as np
import os
from typing import List, NoReturn
from utils import get_project_root, read_data_in_data_frame, concat_data_frame, write_data_to_csv, create_series, comment, create_unique_list


def dataset_table(path: str) -> NoReturn:
    """
    This function creates a pandas data series from the dataset list
    and writes it to the csv file.

    Arguments:
        path(str): absolute path to the parent's parent directory.
    """

    # comment that the dataset table is being built.
    comment('dataset')

    # output file.
    dataset_output_file = f'{path}/output_data/datasets.csv'

    # dataset list.
    datasets = ['PDXE (Breast Cancer)', 'PDXE (Colorectal Cancer)', 'PDXE (Cutaneous Melanoma)',
                'PDXE (Gastric Cancer)', 'PDXE (Non-small Cell Lung Carcinoma)', 'PDXE (Pancreatic Ductal Carcinoma)',
                'UHN (Breast Cancer)', 'McGill (Breast Cancer)']

    # create dataset series.
    dataset_series = create_series(datasets, 'dataset_name')

    # write data to the csv file.
    write_data_to_csv(dataset_series, dataset_output_file, 'dataset_id')


def drug_table(path: str) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        path(str): absolute path to the parent's parent directory.
    """

    # comment that the drug table is being built.
    comment('drug')

    # input files to read and output file path.
    input_files = glob.glob(f'{path}/input_data/*/drug_screening.*')
    drug_output_file = f'{path}/output_data/drugs.csv'

    # data type variable containing the datatype of drug.
    data_type = {'drug': str}

    # unique drug list.
    drugs = create_unique_list(input_files, 'drug', True, data_type)

    # create the drug data frame.
    drug_series = create_series(np.unique(drugs), 'drug_name')

    # write data/dataframe to the csv file.
    write_data_to_csv(drug_series, drug_output_file, 'drug_id')


def tissue_table(path: str) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        path(str): absolute path to the parent's parent directory.
    """

    # comment that the tissue table is being built.
    comment('tissue')

    # input files to read and output file path.
    input_files = glob.glob(f'{path}/input_data/*/model_information.*')
    tissue_output_file = f'{path}/output_data/tissues.csv'

    # data type variable containing the datatype of tissue.
    data_type = {'tissue': str}

    # unique tissue list.
    tissues = create_unique_list(input_files, 'tissue', False, data_type)

    # create the tissue panda series.
    tissue_series = create_series(np.unique(tissues), 'tissue_name')

    # write pandas series to the csv file.
    write_data_to_csv(tissue_series, tissue_output_file, 'tissue_id')


def patient_table(path: str) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        path(str): absolute path to the parent's parent directory.
    """

    # comment that the patient table is being built.
    comment('patient')

    # input files to read and output file path.
    input_files = glob.glob(f'{path}/input_data/*/model_information.*')
    patient_output_file = f'{path}/output_data/patients.csv'

    # data type variable containing the datatype of the patient id.
    data_type = {'patient.id': str}

    # unique patient list.
    patients = create_unique_list(input_files, 'patient.id', False, data_type)

    # create the patient panda series.
    patient_series = create_series(np.unique(patients), 'patient')

    # write pandas series to the csv file.
    write_data_to_csv(patient_series, patient_output_file, 'patient_id')


def gene_table(path: str) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        path(str): absolute path to the parent's parent directory.
    """

    # comment that the gene table is being built.
    comment('gene')

    # input files to read and output file path.
    input_files = [f for f in glob.glob(f'{path}/input_data/*/*')
                   if re.search(r'(copy_number_variation|mutation|rna_sequencing)', f)]
    gene_output_file = f'{path}/output_data/genes.csv'

    # data type variable containing the datatype of gene.
    data_type = {'gene.id': str}

    # unique gene list.
    genes = create_unique_list(input_files, 'gene.id', False, data_type)

    # create the gene panda series.
    gene_series = create_series(np.unique(genes), 'gene_name')

    # write pandas series to the csv file.
    write_data_to_csv(gene_series, gene_output_file, 'gene_id')


def sequencing_table(path: str) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        path(str): absolute path to the parent's parent directory.
    """

    # comment that the gene table is being built.
    comment('sequencing')

    # input files to read and output file path.
    input_files = [f for f in glob.glob(f'{path}/input_data/*/*')
                   if re.search(r'(copy_number_variation|mutation|rna_sequencing)', f)]
    sequencing_output_file = f'{path}/output_data/sequencing.csv'

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
                      sequencing_output_file, 'sequencing_uid')


def batch_table(path: str) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        path(str): absolute path to the parent's parent directory.
    """

    # comment that the batch table is being built.
    comment('batch')

    # input files to read and output file path.
    input_files = glob.glob(f'{path}/input_data/*/batch_information.*')
    batch_output_file = f'{path}/output_data/batches.csv'

    # data type variable containing the datatype of batch.
    data_type = {'batch.id': str}

    # batches list.
    batches = create_unique_list(input_files, 'batch.id', False, data_type)

    # create the batch panda series.
    batch_series = create_series(np.unique(batches), 'batch')

    # write pandas series to the csv file.
    write_data_to_csv(batch_series, batch_output_file, 'batch_id')


def model_table(path: str) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        path(str): absolute path to the parent's parent directory.
    """

    # comment that the model table is being built.
    comment('model')

    # input files to read and output file path.
    input_files = glob.glob(f'{path}/input_data/*/model_information.*')
    patient_file = f'{path}/output_data/patients.csv'
    model_output_file = f'{path}/output_data/models.csv'

    # concatenated data frame.
    model_input_df = concat_data_frame(input_files)

    # patient information data frame.
    patient_df = read_data_in_data_frame(patient_file)

    # merge the data frames.
    merged_df = model_input_df.merge(
        patient_df, left_on='patient.id', right_on='patient')

    # set the index to begin with 1 instead of 0.
    merged_df.index = np.arange(1, len(merged_df) + 1)

    # renaming column in the dataframe.
    merged_df.rename(columns={'model.id': 'model'}, inplace=True)

    # write pandas series to the csv file.
    write_data_to_csv(
        merged_df[['model', 'patient_id']], model_output_file, 'model_id')


def build_primary_tables() -> NoReturn:
    """
    This function builds all the primary tables in the database.
    """

    # get the path of the root directory.
    project_path = f'{get_project_root()}'

    # calling functions to create the data for primary tables.
    dataset_table(project_path)
    drug_table(project_path)
    tissue_table(project_path)
    patient_table(project_path)
    gene_table(project_path)
    batch_table(project_path)
    model_table(project_path)
    sequencing_table(project_path)


# building the primary tables.
build_primary_tables()
