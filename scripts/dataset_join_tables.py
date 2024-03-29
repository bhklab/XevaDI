import pandas as pd
from utils import get_project_root, read_data_in_data_frame, concat_data_frame, write_data_to_csv, comment
from typing import NoReturn, Dict


def model_information_df(data: Dict) -> pd.DataFrame:
    """
    This function creates the data frame for model information data.

    Arguments:
        data (dict): Contains the dictionary of the input files.

    Returns:
        DataFrame: returns the concatenated dataframe of the model_information data.
    """
    return concat_data_frame(data['model_information'], {'patient.id': str, 'dataset': str}).replace('TNBC', 'UHN (Breast Cancer)')


def datasets_tissues_table(input_files: Dict, output_files: Dict) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        output_files (dict): Contains the dictionary of the output files.
        input_files (dict): Contains the dictionary of the input files.
    """
    # comment.
    comment('datasets_tissues')

    # tissue data frame.
    tissue_df = read_data_in_data_frame(
        output_files['tissue'], {'tissue_id': int, 'tissue_name': str})

    # dataset data frame.
    dataset_df = read_data_in_data_frame(
        output_files['dataset'], {'dataset_id': int, 'dataset': str})

    # merge model information data frame with dataset and tissue data frame.
    merged_df = model_information_df(input_files).merge(
        tissue_df, left_on='tissue', right_on='tissue_name').merge(
            dataset_df, left_on='dataset', right_on='dataset_name')

    # drop the duplicates and grab dataset_id and tissue_id.
    final_df = merged_df.drop_duplicates(['dataset_id', 'tissue_id'])[[
        'dataset_id', 'tissue_id']].sort_values(by=['dataset_id', 'tissue_id'])

    # writing the modified df to the csv file for datasets_tissues table.
    write_data_to_csv(
        final_df, output_files['dataset_tissue'], data_type={'dataset_id': int, 'tissue_id': int})


def datasets_drugs_table(input_files: Dict, output_files: Dict) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        output_files (dict): Contains the dictionary of the output files.
        input_files (dict): Contains the dictionary of the input files.
    """
    # comment.
    comment('datasets_drugs')

    # drug data frame.
    drug_df = read_data_in_data_frame(
        output_files['drug'], {'drug_id': int, 'drug_name': str})

    # dataset data frame.
    dataset_df = read_data_in_data_frame(
        output_files['dataset'], {'dataset_id': int, 'dataset': str})

    # model information df.
    model_information = model_information_df(input_files)
    model_information['drug'] = model_information['drug'].str.upper()

    # merge model information data frame with dataset and drug data frame.
    merged_df = model_information.merge(
        drug_df, left_on='drug', right_on='drug_name').merge(
            dataset_df, left_on='dataset', right_on='dataset_name')

    # drop the duplicates and grab dataset_id and drug_id.
    final_df = merged_df.drop_duplicates(['dataset_id', 'drug_id'])[[
        'dataset_id', 'drug_id']].sort_values(by=['dataset_id', 'drug_id'])

    # writing the modified df to the csv file for datasets_drugs table.
    write_data_to_csv(
        final_df, output_files['dataset_drug'], data_type={'dataset_id': int, 'drug_id': int})


def datasets_genes_table(input_files: Dict, output_files: Dict) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        output_files (dict): Contains the dictionary of the output files.
        input_files (dict): Contains the dictionary of the input files.
    """
    # comment.
    comment('datasets_genes')
