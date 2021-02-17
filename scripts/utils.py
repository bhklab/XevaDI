from pathlib import Path
from typing import List, NoReturn, Union
import os
import numpy as np
import pandas as pd


def get_project_root() -> Path:
    """
    Returns:
       The root path of the project.
    """
    return Path(__file__).parent.parent.absolute()


def comment(table):
    """
    This function prints a comment for the table that's being built.

    Arguments:
        table(str): Name of the table.

    Returns:
        `None`.
    """
    print(
        f'<{40 * "-"} (Start Building {table} Table) {40 * "-"}> \n')


def read_data_in_data_frame(file: str, data_type=None) -> pd.DataFrame:
    """
        This function takes the file path as an input and
        based on the type of the file either excel or csv
        return a dataframe.

        Arguments:
            file (str): The name of the file.
            data_type (dict): The data type of that data that will be read with the default value None.

        Returns:
            DataFrame: returns a dataframe created from the input file.
    """
    if 'xlsx' in file:
        return pd.read_excel(file, engine='openpyxl', dtype=data_type)
    elif 'csv' in file:
        return pd.read_csv(file, dtype=data_type)
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


def write_data_to_csv(data: Union[pd.Series, pd.DataFrame], path: str, label=None, data_type=None) -> NoReturn:
    """
        This function write the data to the give path
        and assigns the index label to the value passed in the parameter.

        Arguments:
            data (pandas series or pandas data frame): Data that will be written to the file.
            path (str): The path of the output CSV file.
            label (str): Index label for .to_csv function in pandas.
            data_type (dict): The data type of that data that will be read with the default value None.

        Returns:
            The function doesn't return anything.
    """
    # setting index to false if the label is None.
    index = False if label == None else True

    # if file does not exist write header.
    if not os.path.isfile(path):
        data.index = np.arange(1, len(data) + 1)
        data.to_csv(path, index=index, index_label=label)
    else:  # else it exists so append without writing the header.
        last_id = read_data_in_data_frame(
            path, data_type=data_type).tail(1)[label]
        data.index = np.arange(int(last_id) + 1, len(data) + int(last_id) + 1)
        data.to_csv(path, index_label=label, mode='a',
                    header=False, index=index)


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


def create_unique_list(files: List['str'], column: str, to_upper=False, data_type=None) -> List['str']:
    """
        This function creates a unique list from the list of files.

        Arguments:
        files (list of str): This is the list of the files to iterate through.
        column (str): the column that has to be selected from the data frame to be added to the list.
        to_upper (bool): to convert the data to upper case or not.
        data_type (dict): the data type of the data for the unqiue list.

        Returns:
        List: return a list of the unique elements.
    """

    # unique list of the data.
    unique_list = []
    # loops through each file and appends the unique data to the list.
    for file in files:
        if to_upper:
            unique_list.extend(read_data_in_data_frame(file, data_type=data_type)
                               [column].str.upper().unique())
        else:
            unique_list.extend(read_data_in_data_frame(
                file, data_type=data_type)[column].unique())

    return unique_list
