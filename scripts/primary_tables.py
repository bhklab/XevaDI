import glob
import pathlib
import re
import pandas as pd
import numpy as np
import os
from typing import List, NoReturn
from utils import get_project_root, read_data_in_data_frame, concat_data_frame, write_df_to_csv, create_series, comment


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
    datasets = ['PDXE(Breast Cancer)', 'PDXE(Colorectal Cancer)', 'PDXE(Cutaneous Melanoma)',
                'PDXE(Gastric Cancer)', 'PDXE(Non-small Cell Lung Carcinoma)', 'PDXE(Pancreatic Ductal Carcinoma)',
                'UHN(Breast Cancer)', 'McGill(Breast Cancer)']

    # create dataset series.
    dataset_series = create_series(datasets, 'dataset_name')

    # write data to the csv file.
    write_df_to_csv(dataset_series, dataset_output_file, 'dataset_id')


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

    # concatenated data frame.
    drug_input_df = concat_data_frame(input_files)

    # create the drug data frame.
    drug_df = create_series(
        drug_input_df['drug'].str.upper().unique(), 'drug_name')

    # write data/dataframe to the csv file.
    write_df_to_csv(drug_df, drug_output_file, 'drug_id')


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

    # concatenated data frame.
    tissue_input_df = concat_data_frame(input_files)

    # create the tissue panda series.
    tissue_series = create_series(
        tissue_input_df['tissue'].unique(), 'tissue_name')

    # write pandas series to the csv file.
    write_df_to_csv(tissue_series, tissue_output_file, 'tissue_id')


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

    # concatenated data frame.
    patient_input_df = concat_data_frame(input_files)

    # create the patient panda series.
    patient_series = create_series(
        patient_input_df['patient.id'].unique(), 'patient')

    # write pandas series to the csv file.
    write_df_to_csv(patient_series, patient_output_file, 'patient_id')


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

    # gene list.
    genes = []
    for file in input_files:
        genes.extend(read_data_in_data_frame(file)['gene.id'].unique())

    # create the gene panda series.
    gene_series = create_series(np.array(list(set(genes))), 'gene_name')

    # write pandas series to the csv file.
    write_df_to_csv(gene_series, gene_output_file, 'gene_id')


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

    # concatenated data frame.
    batch_input_df = concat_data_frame(input_files)

    # create the batch panda series.
    batch_series = create_series(batch_input_df['batch.id'].unique(), 'batch')

    # write pandas series to the csv file.
    write_df_to_csv(batch_series, batch_output_file, 'batch_id')


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
    model_output_file = f'{path}/output_data/models.csv'

    # concatenated data frame.
    model_input_df = concat_data_frame(input_files)

    # create the model panda series.
    model_series = create_series(model_input_df['model.id'].unique(), 'model')

    # write pandas series to the csv file.
    write_df_to_csv(model_series, model_output_file, 'model_id')


def build_primary_tables():
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


# building the primary tables.
build_primary_tables()
