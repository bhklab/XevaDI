import pandas as pd
import numpy as np

# print(pd.read_excel('./input_data/uhn_tnbc/copy_number_variation.xlsx', engine='openpyxl'))
# print(pd.read_csv('./input_data/pdxe/copy_number_variation.csv'))
# print(pd.read_csv('./output_data/mutation.csv'))

# types = ['mutation', 'copy_number_variation']

# unique = []

# for i in types:
#     df = ''
#     if 'xlsx' in file:
#         df = pd.read_excel(
#             f'./input_data/uhn_tnbc/{i}.xlsx', engine='openpyxl', dtype=type)
#     elif 'csv' in file:
#         df = pd.read_csv(f'./input_data/uhn_tnbc/{i}.xlsx', dtype=type)

#     ids = df['sequencing.uid'].unique()
#     print(len(ids), ids)

# df = pd.read_excel(
#     './input_data/uhn_tnbc/mutation.xlsx', engine='openpyxl')

# reslt_df = df['gene.id']

# date = []

# for i in np.array(reslt_df):
#     if type(i) is not str:
#         print(i)
#         date.append(i)

# print(set(date))

# gene_dict = pd.read_csv(
#     './output_data/genes.csv')['gene_name'].to_dict().values()

# sequencing_dict = pd.read_csv(
#     './output_data/sequencing.csv')['sequencing_id'].to_dict().values()

# mutation_list = pd.read_excel(
#     './input_data/uhn_tnbc/mutation.xlsx', engine='openpyxl', converters={'sequencing.uid': str})['sequencing.uid'].tolist()

# mutation_list = pd.read_csv(
#     './input_data/uhn_tnbc/rna_sequencing.csv')['sequencing.uid'].tolist()


# print(set(mutation_list))

# u_l = []
# u_d = {}

# for i in mutation_list:
#     if i not in list(sequencing_dict):
#         u_l.append((type(i)))
#         u_d[i] = type(i)

# print(set(u_l), u_d)

# print(pd.read_csv('./input_data/pdxe/model_information.csv'))

# print(pd.read_excel('./input_data/uhn_tnbc/model_information.xlsx', engine='openpyxl'))


# print(pd.read_csv('./input_data/pdxe/batch_response.csv'))

# print(pd.read_excel('./input_data/uhn_tnbc/batch_response.xlsx', engine='openpyxl'))


# print(pd.read_csv('./input_data/pdxe/drug_screening.csv'))

# print(pd.read_excel('./input_data/uhn_tnbc/drug_screening.xlsx', engine='openpyxl'))


# print(pd.read_csv('./input_data/pdxe/modelid_moleculardata_mapping.csv'))

# print(pd.read_excel(
#     './input_data/uhn_tnbc/modelid_moleculardata_mapping.xlsx', engine='openpyxl'))

# print(pd.read_csv(
#     './input_data/pdxe/model_information.csv').drop_duplicates(['patient.id', 'dataset']))

# print(pd.read_excel('./input_data/uhn_tnbc/model_information.xlsx', engine='openpyxl'))


print(pd.read_csv('./input_data/pdxe/model_information.csv'))

print(pd.read_excel('./input_data/uhn_tnbc/model_information.xlsx', engine='openpyxl'))
