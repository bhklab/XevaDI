import glob
import pathlib
import pandas as pd
import numpy as np
import os
from typing import List, NoReturn
from utils import get_project_root, read_data_in_data_frame, concat_data_frame, write_df_to_csv


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
