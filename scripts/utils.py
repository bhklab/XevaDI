from pathlib import Path
from typing import List, NoReturn
import numpy as np
import pandas as pd


def get_project_root() -> Path:
    """
    Returns:
       The root path of the project.
    """
    return Path(__file__).parent.parent.absolute()


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


def create_series(data: List[str], name: str) -> pd.Series:
    """
        This function takes the data and name as the input
        to create a new pandas series.

        Arguments:
        data (list of str): This is the input for the pandas series.
        name (str): The name argument for pandas.Series that gives a name to the series.

        Returns:
        Series: retuns a pandas series.
    """
    index = np.arange(1, len(data) + 1)
    return pd.Series(data, name=name, index=index)
