import glob
import pandas as pd
import numpy as np
from utils import get_project_root, read_data_in_data_frame, concat_data_frame, write_data_to_csv, comment
from path import get_output_files_path, get_input_files_path
from typing import NoReturn, Dict


def batch_response_table(output_files: Dict, input_files: Dict) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        output_files (dict): Contains the dictionary of the output files.
        input_files (dict): Contains the dictionary of the input files.
    """
    # comment.
    comment('batch_response')

    # concatenated data frame.
    batch_response_input_df = concat_data_frame(input_files['batch_response'])

    # batch data frame.
    batch_df = read_data_in_data_frame(output_files['batch'])

    # merging the batch response df and batch df.
    merged_df = batch_response_input_df.merge(
        batch_df, left_on='batch.id', right_on='batch')

    merged_df.index = np.arange(1, len(merged_df) + 1)

    # writing the modified df to the csv file for batch_response table.
    write_data_to_csv(
        merged_df[['batch_id', 'response_type', 'value']],
        output_files['batch_response'], 'id')


def batch_information_table(output_files: Dict, input_files: Dict) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        output_files (dict): Contains the dictionary of the output files.
        input_files (dict): Contains the dictionary of the input files.
    """

    # comment
    comment('batch_information')

    # concatenated data frame.
    batch_information_input_df = concat_data_frame(
        input_files['batch_information'])

    # batch and model data frame.
    batch_df = read_data_in_data_frame(output_files['batch'])
    model_df = read_data_in_data_frame(output_files['model'])

    # merging the batch information df and batch df.
    merged_df = batch_information_input_df.merge(
        batch_df, left_on='batch.id', right_on='batch').merge(model_df, left_on='model.id', right_on='model')

    merged_df.index = np.arange(1, len(merged_df) + 1)

    # writing the modified df to the csv file for batch_information table.
    write_data_to_csv(
        merged_df[['batch_id', 'model_id', 'type']],
        output_files['batch_information'], 'id')


def model_response_table(output_files: Dict, input_files: Dict) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        output_files (dict): Contains the dictionary of the output files.
        input_files (dict): Contains the dictionary of the input files.
    """

    # comment
    comment('model_response')

    # concatenated data frame.
    model_response_df = concat_data_frame(input_files['model_response'])
    model_response_df['drug'] = model_response_df['drug'].str.upper()

    # drug and model data frame.
    drug_df = read_data_in_data_frame(output_files['drug'])
    model_df = read_data_in_data_frame(output_files['model'])

    # merging the drug df and model df.
    merged_df = model_response_df.merge(
        drug_df, left_on='drug', right_on='drug_name').merge(model_df, left_on='model.id', right_on='model')

    merged_df.index = np.arange(1, len(merged_df) + 1)

    # writing the modified df to the csv file for model_response table.
    write_data_to_csv(
        merged_df[['drug_id', 'model_id', 'response_type', 'value']],
        output_files['model_response'], 'id')


def drug_screening_table(output_files: Dict, input_files: Dict) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        output_files (dict): Contains the dictionary of the output files.
        input_files (dict): Contains the dictionary of the input files.
    """

    # comment
    comment('drug_screening')

    # concatenated data frame.
    drug_screening_df = concat_data_frame(input_files['drug_screening'])
    drug_screening_df['drug'] = drug_screening_df['drug'].str.upper()

    # drug and model data frame.
    drug_df = read_data_in_data_frame(output_files['drug'])
    model_df = read_data_in_data_frame(output_files['model'])

    # merging the drug df and model df.
    merged_df = drug_screening_df.merge(
        drug_df, left_on='drug', right_on='drug_name').merge(model_df, left_on='model.id', right_on='model')

    merged_df.index = np.arange(1, len(merged_df) + 1)

    # renaming column in the dataframe.
    merged_df.rename(columns={'volume.normal': 'volume_normal'}, inplace=True)

    # writing the modified df to the csv file for drug_screening table.
    write_data_to_csv(
        merged_df[['drug_id', 'model_id', 'time', 'volume', 'volume_normal']],
        output_files['drug_screening'], 'id')


def model_information_table(output_files: Dict, input_files: Dict) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        output_files (dict): Contains the dictionary of the output files.
        input_files (dict): Contains the dictionary of the input files.
    """

    # comment
    comment('model_information')

    # concatenated data frame.
    model_information_df = concat_data_frame(input_files['model_information'])
    model_information_df['drug'] = model_information_df['drug'].str.upper()

    # update if the dataset name is 'TNBC' to 'UHN (Breast Cancer)'
    model_information_df['dataset'] = model_information_df['dataset'].replace(
        'TNBC', 'UHN (Breast Cancer)')

    # drug and model data frame.
    drug_df = read_data_in_data_frame(output_files['drug'])
    model_df = read_data_in_data_frame(output_files['model'])[
        ['model_id', 'model']]
    dataset_df = read_data_in_data_frame(output_files['dataset'])
    tissue_df = read_data_in_data_frame(output_files['tissue'])
    patient_df = read_data_in_data_frame(output_files['patient'])

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
        output_files['model_information'], 'id')


def modelid_moleculardata_mapping(output_files: Dict, input_files: Dict) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        output_files (dict): Contains the dictionary of the output files.
        input_files (dict): Contains the dictionary of the input files.
    """

    # comment.
    comment('modelid_moleculardata_mapping')

    # concatenated data frame.
    modelid_moleculardata_mapping_df = concat_data_frame(
        input_files['modelid_moleculardata_mapping'])

    # sequencing and gene data frame.
    sequencing_df = read_data_in_data_frame(output_files['sequencing'])
    model_df = read_data_in_data_frame(output_files['model'])

    # merging dfs.
    merged_df = modelid_moleculardata_mapping_df.merge(
        model_df, left_on='model.id', right_on='model').merge(
            sequencing_df, left_on='biobase.id', right_on='sequencing_id')

    merged_df.index = np.arange(1, len(merged_df) + 1)

    # writing the modified df to the csv file for drug_screening table.
    write_data_to_csv(
        merged_df[['model_id', 'sequencing_uid', 'mDataType']],
        output_files['modelid_moleculardata_mapping'], 'id')


def mutation(output_files: Dict, input_files: Dict) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        output_files (dict): Contains the dictionary of the output files.
        input_files (dict): Contains the dictionary of the input files.
    """

    # comment.
    comment('mutation')

    # sequencing and gene data frame.
    sequencing_df = read_data_in_data_frame(
        output_files['sequencing'], {'sequencing_uid': int, 'sequencing_id': str})
    gene_df = read_data_in_data_frame(
        output_files['gene'], {'gene_id': int, 'gene_name': str})

    # looping through each file and creating the df and writing it to the csv file.
    for file in input_files['mutation']:
        mutation_df = read_data_in_data_frame(
            file, {'id': int, 'gene_id': int, 'sequencing_id': str, 'value': str})
        merged_df = mutation_df.merge(
            sequencing_df, left_on='sequencing.uid', right_on='sequencing_id').merge(
                gene_df, left_on='gene.id', right_on='gene_name')
        write_data_to_csv(
            merged_df[['gene_id', 'sequencing_uid', 'value']], output_files['mutation'], 'id', {'gene_id': int, 'sequencing_uid': str, 'value': str})


def copy_number_variation(output_files: Dict, input_files: Dict) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        output_files (dict): Contains the dictionary of the output files.
        input_files (dict): Contains the dictionary of the input files.
    """

    # comment.
    comment('copy_number_variation')

    # sequencing and gene data frame.
    sequencing_df = read_data_in_data_frame(
        output_files['sequencing'], {'sequencing_uid': int, 'sequencing_id': str})
    gene_df = read_data_in_data_frame(
        output_files['gene'], {'gene_id': int, 'gene_name': str})

    # looping through each file and creating the df and writing it to the csv file.
    for file in input_files['copy_number_variation']:
        copy_number_variation_df = read_data_in_data_frame(
            file, {'id': int, 'gene_id': int, 'sequencing_id': str, 'value': str})
        merged_df = copy_number_variation_df.merge(
            sequencing_df, left_on='sequencing.uid', right_on='sequencing_id').merge(
                gene_df, left_on='gene.id', right_on='gene_name')
        write_data_to_csv(
            merged_df[['gene_id', 'sequencing_uid', 'value']], output_files['copy_number_variation'], 'id', {'gene_id': int, 'sequencing_uid': str, 'value': str})


def rna_sequencing(output_files: Dict, input_files: Dict) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        output_files (dict): Contains the dictionary of the output files.
        input_files (dict): Contains the dictionary of the input files.
    """

    # comment.
    comment('rna_sequencing')

    # sequencing and gene data frame.
    sequencing_df = read_data_in_data_frame(
        output_files['sequencing'], {'sequencing_uid': int, 'sequencing_id': str})
    gene_df = read_data_in_data_frame(
        output_file['gene'], {'gene_id': int, 'gene_name': str})

    # looping through each file and creating the df and writing it to the csv file.
    for file in input_files['rna_sequencing']:
        rna_sequencing_df = read_data_in_data_frame(
            file, {'id': int, 'gene_id': int, 'sequencing_id': str, 'value': str})
        merged_df = rna_sequencing_df.merge(
            sequencing_df, left_on='sequencing.uid', right_on='sequencing_id').merge(
                gene_df, left_on='gene.id', right_on='gene_name')
        write_data_to_csv(
            merged_df[['gene_id', 'sequencing_uid', 'value']], output_files['rna_sequencing'], 'id', {'gene_id': int, 'sequencing_uid': str, 'value': str})


def build_secondary_tables() -> NoReturn:
    # get the path of the root directory.
    project_path = f'{get_project_root()}'

    # get the path of the output files and the input files.
    output_files_path = get_output_files_path(project_path)
    input_files_path = get_input_files_path(project_path)

    # calling the functions to make the secondary tables.s
    batch_response_table(output_files_path, input_files_path)
    batch_information_table(output_files_path, input_files_path)
    model_response_table(output_files_path, input_files_path)
    drug_screening_table(output_files_path, input_files_path)
    model_information_table(output_files_path, input_files_path)
    modelid_moleculardata_mapping(output_files_path, input_files_path)
    mutation(output_files_path, input_files_path)
    copy_number_variation(output_files_path, input_files_path)
    rna_sequencing(output_files_path, input_files_path)


build_secondary_tables()
