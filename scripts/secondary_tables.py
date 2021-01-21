import glob
import pandas as pd
import numpy as np
from utils import get_project_root, read_data_in_data_frame, concat_data_frame, write_data_to_csv, comment
from typing import NoReturn


def batch_response_table(path: str) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        path(str): absolute path to the parent's parent directory.
    """

    # comment.
    comment('batch_response')

    # input files for batch_response and batch file path and output file path.
    input_files = glob.glob(f'{path}/input_data/*/batch_response.*')
    batch_file = f'{path}/output_data/batches.csv'
    batch_response_output_file = f'{path}/output_data/batch_response.csv'

    # concatenated data frame.
    batch_response_input_df = concat_data_frame(input_files)

    # batch data frame.
    batch_df = read_data_in_data_frame(batch_file)

    # merging the batch response df and batch df.
    merged_df = batch_response_input_df.merge(
        batch_df, left_on='batch.id', right_on='batch')

    merged_df.index = np.arange(1, len(merged_df) + 1)

    # writing the modified df to the csv file for batch_response table.
    write_data_to_csv(
        merged_df[['batch_id', 'response_type', 'value']],
        batch_response_output_file, 'id')


def batch_information_table(path: str) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:a
        path(str): absolute path to the parent's parent directory.
    """

    # comment
    comment('batch_information')

    # input files for batch_information and batch file path and output file path.
    input_files = glob.glob(f'{path}/input_data/*/batch_information.*')
    batch_file = f'{path}/output_data/batches.csv'
    model_file = f'{path}/output_data/models.csv'
    batch_information_output_file = f'{path}/output_data/batch_information.csv'

    # concatenated data frame.
    batch_information_input_df = concat_data_frame(input_files)

    # batch and model data frame.
    batch_df = read_data_in_data_frame(batch_file)
    model_df = read_data_in_data_frame(model_file)

    # merging the batch information df and batch df.
    merged_df = batch_information_input_df.merge(
        batch_df, left_on='batch.id', right_on='batch').merge(model_df, left_on='model.id', right_on='model')

    merged_df.index = np.arange(1, len(merged_df) + 1)

    # writing the modified df to the csv file for batch_information table.
    write_data_to_csv(
        merged_df[['batch_id', 'model_id', 'type']],
        batch_information_output_file, 'id')


def model_response_table(path: str) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:a
        path(str): absolute path to the parent's parent directory.
    """

    # comment
    comment('model_response')

    # input files for model_response and drug file path and output file path.
    input_files = glob.glob(f'{path}/input_data/*/model_response.*')
    drug_file = f'{path}/output_data/drugs.csv'
    model_file = f'{path}/output_data/models.csv'
    model_response_output_file = f'{path}/output_data/model_response.csv'

    # concatenated data frame.
    model_response_df = concat_data_frame(input_files)
    model_response_df['drug'] = model_response_df['drug'].str.upper()

    # drug and model data frame.
    drug_df = read_data_in_data_frame(drug_file)
    model_df = read_data_in_data_frame(model_file)

    # merging the drug df and model df.
    merged_df = model_response_df.merge(
        drug_df, left_on='drug', right_on='drug_name').merge(model_df, left_on='model.id', right_on='model')

    merged_df.index = np.arange(1, len(merged_df) + 1)

    # writing the modified df to the csv file for model_response table.
    write_data_to_csv(
        merged_df[['drug_id', 'model_id', 'response_type', 'value']],
        model_response_output_file, 'id')


def drug_screening_table(path: str) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:a
        path(str): absolute path to the parent's parent directory.
    """

    # comment
    comment('drug_screening')

    # input files for drug_screening and drug file path and output file path.
    input_files = glob.glob(f'{path}/input_data/*/drug_screening.*')
    drug_file = f'{path}/output_data/drugs.csv'
    model_file = f'{path}/output_data/models.csv'
    drug_screening_output_file = f'{path}/output_data/drug_screening.csv'

    # concatenated data frame.
    drug_screening_df = concat_data_frame(input_files)
    drug_screening_df['drug'] = drug_screening_df['drug'].str.upper()

    # drug and model data frame.
    drug_df = read_data_in_data_frame(drug_file)
    model_df = read_data_in_data_frame(model_file)

    # merging the drug df and model df.
    merged_df = drug_screening_df.merge(
        drug_df, left_on='drug', right_on='drug_name').merge(model_df, left_on='model.id', right_on='model')

    merged_df.index = np.arange(1, len(merged_df) + 1)

    # renaming column in the dataframe.
    merged_df.rename(columns={'volume.normal': 'volume_normal'}, inplace=True)

    # writing the modified df to the csv file for drug_screening table.
    write_data_to_csv(
        merged_df[['drug_id', 'model_id', 'time', 'volume', 'volume_normal']],
        drug_screening_output_file, 'id')


def model_information_table(path: str) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:a
        path(str): absolute path to the parent's parent directory.
    """

    # comment
    comment('model_information')

    # input files for model_information and drug file path and output file path.
    input_files = glob.glob(f'{path}/input_data/*/model_information.*')
    drug_file = f'{path}/output_data/drugs.csv'
    model_file = f'{path}/output_data/models.csv'
    dataset_file = f'{path}/output_data/datasets.csv'
    patient_file = f'{path}/output_data/patients.csv'
    tissue_file = f'{path}/output_data/tissues.csv'
    model_information_output_file = f'{path}/output_data/model_information.csv'

    # concatenated data frame.
    model_information_df = concat_data_frame(input_files)
    model_information_df['drug'] = model_information_df['drug'].str.upper()

    # drug and model data frame.
    drug_df = read_data_in_data_frame(drug_file)
    model_df = read_data_in_data_frame(model_file)[['model_id', 'model']]
    dataset_df = read_data_in_data_frame(dataset_file)
    tissue_df = read_data_in_data_frame(tissue_file)
    patient_df = read_data_in_data_frame(patient_file)

    # merged df.
    merged_df = model_information_df.merge(
        drug_df, left_on='drug', right_on='drug_name').merge(
            model_df, left_on='model.id', right_on='model').merge(
                tissue_df, left_on='tissue', right_on='tissue_name').merge(
                    patient_df, left_on='patient.id', right_on='patient').merge(
                        dataset_df, left_on='dataset', right_on='dataset_name')

    merged_df.index = np.arange(1, len(merged_df) + 1)

    # writing the modified df to the csv file for model_information table.
    write_data_to_csv(
        merged_df[['model_id', 'tissue_id',
                   'patient_id', 'drug_id', 'dataset_id']],
        model_information_output_file, 'id')


def build_secondary_tables() -> NoReturn:
    # get the path of the root directory.
    project_path = f'{get_project_root()}'

    batch_response_table(project_path)
    batch_information_table(project_path)
    model_response_table(project_path)
    drug_screening_table(project_path)
    model_information_table(project_path)


build_secondary_tables()
