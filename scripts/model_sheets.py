import os
from utils import read_data_in_data_frame, write_data_to_csv, comment
from typing import NoReturn, Dict


def model_sheets_table(input_files: Dict, output_files: Dict) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        input_files (dict): Contains the dictionary of the input files.
        output_files (dict): Contains the dictionary of the output files.
    """

    # comment
    comment('model_sheets')

    # model sheet data frame.
    sheets_df = read_data_in_data_frame(input_files['model_sheet'])

    # renaming the columns for the final output.
    sheets_df.rename(columns={'model.id': 'model_id',
                              'file.url': 'link'}, inplace=True)

    # write data to the csv file.
    if not os.path.isfile(output_files['model_sheet']):
        write_data_to_csv(
            sheets_df[['model_id', 'link', 'row']], output_files['model_sheet'])
    else:
        raise ValueError('Model sheet file is already present!!')
