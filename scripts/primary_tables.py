import glob
import pathlib
import pandas as pd
import numpy as np
import os
from typing import List, NoReturn
from utils import get_project_root, read_data_in_data_frame, concat_data_frame, write_df_to_csv, create_series


print(" <----------------------------------------- (Start Building Drug Table) ----------------------------------------> \n")
# input files to read and output file path.
input_files = glob.glob('../input_data/*/drug_screening.*')
drug_output_file = '../output_data/drugs.csv'

# concatenated data frame.
drug_input_df = concat_data_frame(input_files)

# get the list of the unique drugs.
unique_drugs = drug_input_df['drug'].str.upper().unique()

# create the drug data frame.
drug_df = create_series(unique_drugs, 'drug_name')

# write data/dataframe to the csv file.
write_df_to_csv(drug_df, drug_output_file, 'drug_id')
print(" <----------------------------------------- (Done Building Drug Table) ------------------------------------------> \n")


print(" <----------------------------------------- (Start Building Dataset Table) ----------------------------------------> \n")
# output file.
dataset_output_file = '../output_data/datasets.csv'

# dataset list.
datasets = ['PDXE(Breast Cancer)', 'PDXE(Colorectal Cancer)', 'PDXE(Cutaneous Melanoma)',
            'PDXE(Gastric Cancer)', 'PDXE(Non-small Cell Lung Carcinoma)', 'PDXE(Pancreatic Ductal Carcinoma)',
            'UHN(Breast Cancer)', 'McGill(Breast Cancer)']

# create dataset series.
dataset_series = create_series(datasets, 'dataset_name')

# write data to the csv file.
write_df_to_csv(dataset_series, dataset_output_file, 'dataset_id')
print(" <----------------------------------------- (Done Building Dataset Table) ----------------------------------------> \n")


print(" <----------------------------------------- (Start Building Tissue Table) ----------------------------------------> \n")
# input files to read and output file path.
input_files = glob.glob('../input_data/*/model_information.*')
tissue_output_file = '../output_data/tissues.csv'

# concatenated data frame.
tissue_input_df = concat_data_frame(input_files)

# get the list of the unique tissues.
unique_tissues = tissue_input_df['tissue'].unique()

# create the tissue panda series.
tissue_series = create_series(unique_tissues, 'tissue_name')

# write pandas series to the csv file.
write_df_to_csv(tissue_series, tissue_output_file, 'tissue_id')
print(" <----------------------------------------- (Done Building Tissue Table) ----------------------------------------> \n")


print(" <----------------------------------------- (Start Building Patient Table) ----------------------------------------> \n")
# input files to read and output file path.
input_files = glob.glob('../input_data/*/model_information.*')
patient_output_file = '../output_data/patients.csv'

# concatenated data frame.
patient_input_df = concat_data_frame(input_files)

# get the list of the unique patients.
unique_patients = patient_input_df['patient.id'].unique()

# create the tissue panda series.
patient_series = create_series(unique_patients, 'patient')

# write pandas series to the csv file.
write_df_to_csv(patient_series, patient_output_file, 'patient_id')
print(" <----------------------------------------- (Done Building Patient Table) ----------------------------------------> \n")


print(" <----------------------------------------- (Start Building Gene Table) ----------------------------------------> \n")
# input files to read and output file path.
input_files = glob.glob('../input_data/*/copy_number_variation.*')
gene_output_file = '../output_data/genes.csv'

# concatenated data frame.
gene_input_df = concat_data_frame(input_files)

# get the list of the unique genees.
unique_genes = gene_input_df['gene.id'].unique()

# create the tissue panda series.
gene_series = create_series(unique_genes, 'gene_name')

# write pandas series to the csv file.
write_df_to_csv(gene_series, gene_output_file, 'gene_id')
print(" <----------------------------------------- (Done Building Gene Table) ----------------------------------------> \n")


print(" <----------------------------------------- (Start Building Batch Table) ----------------------------------------> \n")
# input files to read and output file path.
input_files = glob.glob('../input_data/*/batch_information.*')
batch_output_file = '../output_data/batches.csv'

# concatenated data frame.
batch_input_df = concat_data_frame(input_files)

# get the list of the unique batches.
unique_batches = batch_input_df['batch.id'].unique()

# create the tissue panda series.
batch_series = create_series(unique_batches, 'batch')

# write pandas series to the csv file.
write_df_to_csv(batch_series, batch_output_file, 'batch_id')
print(" <----------------------------------------- (Done Building Batch Table) ----------------------------------------> \n")


print(" <----------------------------------------- (Start Building Model Table) ----------------------------------------> \n")
print(" <----------------------------------------- (Start Building Model Table) ----------------------------------------> \n")
