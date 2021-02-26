import os
from utils import read_data_in_data_frame, write_data_to_csv, comment
from typing import NoReturn, Dict


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

    # renaming the columns for the final output.
    merged_df.rename(columns={'Standard-Name (PubChem)': 'standard_name', 'Targets': 'targets', 'Treatment.type': 'treatment_type',
                              'Class': 'class', 'Class Names': 'class_name', 'Source': 'source'}, inplace=True)

    # writing the modified df to the csv file for drug_annotations table.
    if not os.path.isfile(output_files['drug_annotation']):
        write_data_to_csv(
            merged_df[['drug_id', 'standard_name', 'targets',
                       'treatment_type', 'class', 'class_name', 'source']],
            output_files['drug_annotation'])
    else:
        raise ValueError('Drug annotation file is already present!')
