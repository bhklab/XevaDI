from utils import get_project_root, read_data_in_data_frame, concat_data_frame, write_data_to_csv, comment
from typing import NoReturn


def model_sheets_table(path: str) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
        path(str): absolute path to the parent's parent directory.
    """

    # comment
    comment('model_sheets')

    # input file for model sheets and path to the output file.
    model_sheet_input = f'{path}/input_data/model_sheets.csv'
    model_sheet_output = f'{path}/output_data/model_sheets.csv'

    # model sheet data frame.
    sheets_df = read_data_in_data_frame(model_sheet_input)
    print(sheets_df)

    # write data to the csv file.
    write_data_to_csv(
        sheets_df[['model_id', 'link', 'row']], model_sheet_output)


# get the path of the root directory.
project_path = f'{get_project_root()}'
# model sheets table.
model_sheets_table(project_path)
