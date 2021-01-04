import glob
import pathlib
import pandas as pd
import numpy as np
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


def write_df_to_csv(data_frame: pd.DataFrame, path: str, label: str) -> NoReturn:
    """
        This function write the dataframe to the give path
        and assigns the index label to the value passed in the parameter.

        Arguments:
            data_frame (pandas dataframe): Data Frame that will be written to the file.
            path (str): The path of the output CSV file.
            label (str): Index label for .to_csv function in pandas.

        Returns:
            The function doesn't return anything.
    """
    data_frame.to_csv(path, index_label=label)


print(" <----------------------------------------- (Start Building Drug Table) ----------------------------------------> \n")
# input files to read and output file path.
input_files = glob.glob('../input_data/*/drug_screening.*')
output_file = '../output_data/drugs.csv'

# concatenated data frame.
drug_input_df = concat_data_frame(input_files)

# get the list of the unique drugs.
unique_drugs = drug_input_df['drug'].str.upper().unique()

# create the drug data frame.
index = np.arange(1, len(unique_drugs) + 1)
drug_df = pd.DataFrame(unique_drugs, columns=['drug_name'], index=index)

# write data/dataframe to the csv file.
write_df_to_csv(drug_df, output_file, 'drug_id')
print(" <----------------------------------------- (Done Building Drug Table) ------------------------------------------> \n")


print(" <----------------------------------------- (Start Building Dataset Table) ----------------------------------------> \n")
# output file.
output_file = '../output_data/datasets.csv'

# dataset list.
datasets = ['PDXE(Breast Cancer)', 'PDXE(Colorectal Cancer)', 'PDXE(Cutaneous Melanoma)',
            'PDXE(Gastric Cancer)', 'PDXE(Non-small Cell Lung Carcinoma)', 'PDXE(Pancreatic Ductal Carcinoma)',
            'UHN(Breast Cancer)', 'McGill(Breast Cancer)']

# create dataset series.
index = np.arange(1, len(datasets) + 1)
dataset_series = pd.Series(datasets, name='dataset_name', index=index)

# write data to the csv file.
write_df_to_csv(dataset_series, output_file, 'dataset_id')
print(" <----------------------------------------- (Done Building Dataset Table) ----------------------------------------> \n")
