import glob
import pathlib
import pandas as pd
import os
from typing import List, NoReturn
from utils import get_project_root


def read_data_in_data_frame(file: str) -> pd.DataFrame:
    """
        This function takes the file path as an input and
        based on the type of the file either excel or csv
        return a dataframe.

        Arguments:
            file (str): The name of the file.

        Returns:
            DataFrame: returns a dataframe created from the input file.
    """
    if 'xlsx' in file:
        return pd.read_excel(file, engine='openpyxl')
    elif 'csv' in file:
        return pd.read_csv(file)
    else:
        raise ValueError(
            'Invalid Argument to the read_data_in_data_frame function!')


def concat_data_frame(files: List[str]) -> pd.DataFrame:
    """
        This function takes the list of the input files
        and returns the concatenated dataframe that is created
        using the input files.

        Arguments:
            files (list of str): List of the input files.

        Returns:
            DataFrame: returns a concatenated dataframe.
    """
    data_frames = []
    for file in files:
        data_frames.append(read_data_in_data_frame(file))
    return pd.concat(data_frames)


def write_df_to_csv(df: pd.DataFrame, path: str, label: str) -> NoReturn:
    df.to_csv(path, index_label=label)


print(" <----------------------------------------- Started Building Drug Table -----------------------------------------> ")

# input files to read and output file path.
input_files = glob.glob('../input_data/*/drug_screening.*')
output_file = '~/Desktop/drugs.csv'

# concatenated data frame.
drug_input_df = concat_data_frame(input_files)

# get the list of the unique drugs.
unique_drugs = drug_input_df['drug'].str.upper().unique()

# create the drug data frame.
drug_df = pd.DataFrame(unique_drugs, columns=['drug_name'])
drug_df = drug_df.rename_axis('drug_id', axis=1)
drug_df.index += 1

# write data/dataframe to the csv file.
write_df_to_csv(drug_df, output_file, 'drug_id')

print(" <----------------------------------------- Done Building Drug Table ------------------------------------------> ")
