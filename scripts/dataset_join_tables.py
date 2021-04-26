import pandas as pd
from utils import get_project_root, read_data_in_data_frame, concat_data_frame, write_data_to_csv, comment
from typing import NoReturn, Dict


def datasets_patients_table(input_files: Dict, output_files: Dict) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        output_files (dict): Contains the dictionary of the output files.
        input_files (dict): Contains the dictionary of the input files.
    """
    # comment.
    comment('datasets_patients')

    # concatenated data frame.
    model_information_input_df = concat_data_frame(
        input_files['model_information'], {'patient.id': str, 'dataset': str}).replace(
        'TNBC', 'UHN (Breast Cancer)')

    # patient data frame.
    patient_df = read_data_in_data_frame(
        output_files['patient'], {'patient_id': int, 'patient': str})

    # dataset data frame.
    dataset_df = read_data_in_data_frame(
        output_files['dataset'], {'dataset_id': int, 'dataset': str})

    # merge model information data frame with dataset and patient data frame.
    merged_df = model_information_input_df.merge(
        patient_df, left_on='patient.id', right_on='patient').merge(
            dataset_df, left_on='dataset', right_on='dataset_name')

    # drop the duplicates and grab dataset_id and patient_id.
    final_df = merged_df.drop_duplicates(['dataset_id', 'patient_id'])[[
        'dataset_id', 'patient_id']].sort_values(by=['dataset_id', 'patient_id'])

    # writing the modified df to the csv file for datasets_patients table.
    write_data_to_csv(
        final_df, output_files['dataset_patient'], data_type={'dataset_id': int, 'patient_id': int})
