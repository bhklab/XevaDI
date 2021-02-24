import os
from utils import get_project_root, read_data_in_data_frame, concat_data_frame, write_data_to_csv, comment
from typing import NoReturn, Dict
from path import get_output_files_path, get_input_files_path


def model_sheets_table(output_files: Dict, input_files: Dict) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        output_files (dict): Contains the dictionary of the output files.
        input_files (dict): Contains the dictionary of the input files.
    """

    # comment
    comment('model_sheets')

    # model sheet data frame.
    sheets_df = read_data_in_data_frame(input_files['model_sheet'])

    # write data to the csv file.
    if not os.path.isfile(output_files['model_sheet']):
        write_data_to_csv(
            sheets_df[['model_id', 'link', 'row']], output_files['model_sheet'])
    else:
        raise ValueError('Model sheet file is already present!!')


# get the path of the root directory.
project_path = f'{get_project_root()}'

# get the path of the output files and the input files.
output_files_path = get_output_files_path(project_path)
input_files_path = get_input_files_path(project_path)

# model sheets table.
model_sheets_table(output_files_path, input_files_path)
