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
    print(merged_df)


def build_join_tables() -> NoReturn:
    # get the path of the root directory.
    project_path = f'{get_project_root()}'

    batch_response_table(project_path)


build_join_tables()
