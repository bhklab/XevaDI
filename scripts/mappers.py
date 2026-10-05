# input dataset rename object
# to map dataset names in the input model_information files to actual dataset names in the database
input_dataset_renames = {
    "TNBC": "UHN (Breast Cancer)",
    "TNBC_v2": "UHN_v2 (Breast Cancer)",
}

# dataset mapper object
# to map dataset names in the biomarker data to actual dataset names in the database
dataset_mapping = {
    "PDXE_Breast_Cancer_XevaSet": "PDXE (Breast Cancer)",
    "PDXE_Colorectal_Cancer_XevaSet": "PDXE (Colorectal Cancer)",
    "PDXE_Cutaneous_Melanoma_XevaSet": "PDXE (Cutaneous Melanoma)",
    "PDXE_Non-small_Cell_Lung_Carcinoma_XevaSet": "PDXE (Non-small Cell Lung Carcinoma)",
    "PDXE_Pancreatic_Ductal_Carcinoma_XevaSet": "PDXE (Pancreatic Ductal Carcinoma)",
    "McGill_Breast_Cancer_XevaSet": "McGill (Breast Cancer)",
    "TNBC_XevaSet": "UHN (Breast Cancer)",
    "Tsao_Lung_Cancer_XevaSet": "Tsao (Lung Cancer)",
}

# tissue mapper object
# to map tissue names in the biomarker data to actual tissue names in the database
tissue_mapping = {
    "breast": "Breast Cancer",
    "colorectal cancer": "Colorectal Cancer",
    "melanoma": "Cutaneous Melanoma",
    "lung": "Non-small Cell Lung Carcinoma",
    "pancreatic cancer": "Pancreatic Ductal Carcinoma",
}
