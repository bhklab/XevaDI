import glob
import pandas as pd
import numpy as np
from utils import get_project_root, read_data_in_data_frame, concat_data_frame, write_data_to_csv, comment
from typing import NoReturn


def drug_annotation_table(path: str) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        path(str): absolute path to the parent's parent directory.
    """

    # comment
    comment('drug_annotation')

    # input file for drug annotations and path to the output file.
    annotation_file = f'{path}/input_data/drug_annotations.csv'
    annotation_file_pubchem = f'{path}/input_data/drug_annotations_1.csv'
    drug_file = f'{path}/output_data/drugs.csv'
    drug_annotation_output_file = f'{path}/output_data/drug_annotations.csv'

    # drug annotation and drug data frame.
    annotation_df = read_data_in_data_frame(annotation_file)
    annotation_df_with_pubchem = read_data_in_data_frame(
        annotation_file_pubchem)
    drug_df = read_data_in_data_frame(drug_file)

    # changing the drug names to uppercase.
    annotation_df['Drug-Name'] = annotation_df['Drug-Name'].str.upper()
    annotation_df_with_pubchem['Drug-Name'] = annotation_df_with_pubchem['Drug-Name'].str.upper()

    # selecting the required columns.
    annotation_df = annotation_df[[
        'Drug-Name', 'Targets', 'Treatment.type', 'Class', 'Class Names', 'Source']]
    annotation_df_with_pubchem = annotation_df_with_pubchem[[
        'Drug-Name', 'Standard-Name (PubChem)', 'PubchemID']]

    # merging drug df and annotation df.
    merged_df = drug_df.merge(
        annotation_df, left_on='drug_name', right_on='Drug-Name', how='left').merge(
        annotation_df_with_pubchem, left_on='drug_name', right_on='Drug-Name', how='left')

    # renamining the columns for the final output.
    merged_df.rename(columns={'Standard-Name (PubChem)': 'standard_name', 'Targets': 'targets', 'Treatment.type': 'treatment_type',
                              'Class': 'class', 'Class Names': 'class_name', 'Source': 'source'}, inplace=True)

    # writing the modified df to the csv file for drug_annotations table.
    write_data_to_csv(
        merged_df[['drug_id', 'standard_name', 'targets',
                   'treatment_type', 'class', 'class_name', 'source']],
        drug_annotation_output_file)


def build_annotation_tables() -> NoReturn:
    # get the path of the root directory.
    project_path = f'{get_project_root()}'

    # creating annotation table(s).
    drug_annotation_table(project_path)


build_annotation_tables()
