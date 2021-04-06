import os
import pandas as pd
from utils import read_data_in_data_frame, write_data_to_csv, comment
from typing import NoReturn, Dict, List


def get_pubchem_id(data: pd.Series) -> List['str']:
    """
    Functions creates an array of the pubchem ids from the input data.

    Arguments:
        data (pandas series): Pandas series of the input data.

    Returns:
        List['str']: returns a list of pubchem ids.
    """
    pubchem_ids = []
    for i in data.str.split('+'):
        if type(i) is list:
            pubchem_ids.append(','.join([j.split('/')[-1] for j in i]))
        else:
            pubchem_ids.append('')
    return pubchem_ids


def drug_annotation_table(input_files: Dict, output_files: Dict) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        input_files (dict): Contains the dictionary of the input files.
        output_files (dict): Contains the dictionary of the output files.
    """

    # comment
    comment('drug_annotation')

    # drug annotation and drug data frame.
    annotation_df = read_data_in_data_frame(input_files['drug_annotation'])
    annotation_df_with_pubchem = read_data_in_data_frame(
        input_files['pubchem_annotation'])
    drug_df = read_data_in_data_frame(output_files['drug'])

    # changing the drug names to uppercase.
    annotation_df['Drug-Name'] = annotation_df['Drug-Name'].str.upper()
    annotation_df_with_pubchem['drug_name'] = annotation_df_with_pubchem['drug_name'].str.upper(
    )
    annotation_df_with_pubchem['source'] = get_pubchem_id(
        annotation_df_with_pubchem['source'])

    # selecting the required columns.
    annotation_df = annotation_df[[
        'Drug-Name', 'Targets', 'Treatment.type', 'Class', 'Class Names']]
    annotation_df_with_pubchem = annotation_df_with_pubchem[[
        'drug_name', 'standard_name', 'source']]

    # merging drug df and annotation df.
    merged_df = drug_df.merge(
        annotation_df, left_on='drug_name', right_on='Drug-Name', how='left').merge(
        annotation_df_with_pubchem, left_on='drug_name', right_on='drug_name', how='left')

    # renaming the columns for the final output.
    merged_df.rename(columns={'Targets': 'targets', 'Treatment.type': 'treatment_type',
                              'Class': 'class', 'Class Names': 'class_name', 'source': 'pubchemid'}, inplace=True)

    # writing the modified df to the csv file for drug_annotations table.
    if not os.path.isfile(output_files['drug_annotation']):
        write_data_to_csv(
            merged_df[['drug_id', 'standard_name', 'targets',
                       'treatment_type', 'class', 'class_name', 'pubchemid']],
            output_files['drug_annotation'])
    else:
        raise ValueError('Drug annotation file is already present!')
