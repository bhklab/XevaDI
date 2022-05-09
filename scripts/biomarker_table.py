from os import read
import pandas as pd
from utils import get_project_root, read_data_in_data_frame, concat_data_frame, write_data_to_csv, comment
from typing import NoReturn, Dict
from mappers import tissue_mapping, dataset_mapping


def gene_drug_tissue_table(input_files: Dict, output_files: Dict) -> NoReturn:
    """
    This function creates the data frame from the input files, concatenates them
    and write it to the csv file.

    Arguments:
            output_files (dict): Contains the dictionary of the output files.
            input_files (dict): Contains the dictionary of the input files.
    """
    # comment
    comment('gene_drug_tissue')

    # get data frames for gene, drug, tissue, dataset
    gene_df = read_data_in_data_frame(
        output_files['gene'], {'gene_id': int, 'gene_name': str})
    drug_df = read_data_in_data_frame(
        output_files['drug'], {'drug_id': int, 'drug_name': str})
    tissue_df = read_data_in_data_frame(
        output_files['tissue'], {'tissue_id': int, 'tissue_name': str})
    dataset_df = read_data_in_data_frame(
        output_files['dataset'], {'dataset_id': int, 'dataset_name': str})

    # looping through each file and creating df, writing it to the csv file
    for file in input_files['gene_drug_tissue']:
        if 'csv' in file or 'xlsx' in file:
            # create the data frame
            gene_drug_tissue_df = read_data_in_data_frame(file)

            # get the dataset and tissue name
            dataset = gene_drug_tissue_df['dataset'][0]
            tissue = gene_drug_tissue_df['tissue'][0]

            # update the data frame with the corresponding values
            gene_drug_tissue_df['dataset'] = dataset_mapping[dataset]
            gene_drug_tissue_df['tissue'] = tissue_mapping[tissue]

            # create merged df
            merged_df = gene_drug_tissue_df.merge(
                drug_df, left_on='compound', right_on='drug_name').merge(
                tissue_df, left_on='tissue', right_on='tissue_name').merge(
                gene_df, left_on='gene', right_on='gene_name').merge(
                dataset_df, left_on='dataset', right_on='dataset_name').round(6)

            # write data to the csv file
            print(f'Writing File -----> {file}\n')

            # write data to the csv file
            write_data_to_csv(
                merged_df[['gene_id', 'drug_id', 'tissue_id', 'dataset_id', 'estimate',
                           'n', 'pvalue', 'fdr', 'mDataType', 'metric']],
                output_files['gene_drug_tissue'],
                'id',
                {
                    'gene_id': int, 'drug_id': int, 'tissue_id': int, 'dataset_id': int, 'estimate': float,
                    'n': int, 'pvalue': float, 'fdr': float, 'mDataType': str, 'metric': str
                }
            )
