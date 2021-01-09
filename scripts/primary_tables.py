import glob
import pathlib
import re
import pandas as pd
import numpy as np
import os
from typing import List, NoReturn
from utils import get_project_root, read_data_in_data_frame, concat_data_frame, write_df_to_csv, create_series, comment


def dataset_table():
    # comment that the dataset table is being built.
    comment('dataset')

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


def drug_table():
    # comment that the drug table is being built.
    comment('drug')

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


def tissue_table():
    # comment that the tissue table is being built.
    comment('tissue')

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


def patient_table():
    # comment that the patient table is being built.
    comment('patient')

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


def gene_table():
    # comment that the gene table is being built.
    comment('gene')

    # input files to read and output file path.
    input_files = [f for f in glob.glob('../input_data/*/*')
                   if re.search(r'(copy_number_variation|mutation|rna_sequencing)', f)]

    gene_output_file = '../output_data/genes.csv'

    genes = []
    for file in input_files:
        genes.extend(read_data_in_data_frame(file)['gene.id'].unique())

    # create the tissue panda series.
    gene_series = create_series(np.array(list(set(genes))), 'gene_name')

    # write pandas series to the csv file.
    write_df_to_csv(gene_series, gene_output_file, 'gene_id')


def batch_table():
    # comment that the batch table is being built.
    comment('batch')

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


#" <----------------------------------------- (Start Building Model Table) ----------------------------------------> \n"
#" <----------------------------------------- (Start Building Model Table) ----------------------------------------> \n"


def build_primary_tables():
    """
    This function builds all the primary tables in the database.
    """
    dataset_table()
    drug_table()
    tissue_table()
    patient_table()
    gene_table()
    batch_table()


# building the primary tables.
build_primary_tables()
