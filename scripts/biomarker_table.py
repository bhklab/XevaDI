import pandas as pd
import numpy as np
from typing import NoReturn, Dict
from utils import read_data_in_data_frame, write_data_to_csv, comment
from mappers import tissue_mapping, dataset_mapping


def _norm(s: pd.Series) -> pd.Series:
    return s.astype("string").str.lower().str.strip()


def _make_lookup_dict(df: pd.DataFrame, key_col: str, val_col: str, name: str) -> dict:
    """Normalize keys, drop duplicate keys (keep first), return dict."""
    tmp = df[[key_col, val_col]].copy()
    tmp[key_col] = _norm(tmp[key_col].astype("string"))
    # log duplicates (optional)
    dup_mask = tmp.duplicated(subset=[key_col], keep=False)
    if dup_mask.any():
        dup_count = int(dup_mask.sum())
        unique_dup_keys = int(tmp.loc[dup_mask, key_col].nunique())
        print(
            f"[WARN] {name}: {dup_count} duplicate rows across {unique_dup_keys} duplicated keys. Keeping first occurrence per key."
        )
    tmp = tmp.drop_duplicates(subset=[key_col], keep="first")
    return dict(zip(tmp[key_col].tolist(), tmp[val_col].tolist()))


def gene_drug_tissue_table(input_files: Dict, output_files: Dict) -> NoReturn:
    comment("gene_drug_tissue")

    # Load dim tables
    gene_df = read_data_in_data_frame(
        output_files["gene"], {"gene_id": int, "gene_name": str}
    )
    drug_df = read_data_in_data_frame(
        output_files["drug"], {"drug_id": int, "drug_name": str}
    )
    tissue_df = read_data_in_data_frame(
        output_files["tissue"], {"tissue_id": int, "tissue_name": str}
    )
    dataset_df = read_data_in_data_frame(
        output_files["dataset"], {"dataset_id": int, "dataset_name": str}
    )

    # Build deduped lookup dicts
    gene_lookup = _make_lookup_dict(gene_df, "gene_name", "gene_id", "gene_lookup")
    drug_lookup = _make_lookup_dict(drug_df, "drug_name", "drug_id", "drug_lookup")
    tissue_lookup = _make_lookup_dict(
        tissue_df, "tissue_name", "tissue_id", "tissue_lookup"
    )
    dataset_lookup = _make_lookup_dict(
        dataset_df, "dataset_name", "dataset_id", "dataset_lookup"
    )

    out_rows = []

    usecols = [
        "gene",
        "compound",
        "dataset",
        "tissue",
        "estimate",
        "CI_lower",
        "CI_upper",
        "pvalue",
        "fdr",
        "n",
        "mDataType",
        "metric",
    ]

    for file in input_files["gene_drug_tissue"]:
        if not (file.endswith(".csv") or file.endswith(".xlsx")):
            continue

        print(f"Reading {file} ...")
        df = read_data_in_data_frame(file)

        # Trim to required columns if present
        missing_cols = [c for c in usecols if c not in df.columns]
        if missing_cols:
            print(f"[WARN] Skipping {file}: missing columns {missing_cols}")
            continue

        df = df[usecols].copy()

        # Normalize free-text keys
        df["gene"] = _norm(df["gene"])
        df["compound"] = _norm(df["compound"])

        # Map dataset/tissue names from file → canonical names via mappers → ids
        if df["dataset"].nunique(dropna=False) == 1:
            # Single value path
            ds_original = df["dataset"].iloc[0]
            ds_canonical = dataset_mapping.get(
                ds_original, ds_original
            )  # fall back gracefully
            ds_key = str(ds_canonical).lower().strip()
            df["dataset_id"] = dataset_lookup.get(ds_key, pd.NA)
        else:
            ds_canonical_series = df["dataset"].map(lambda x: dataset_mapping.get(x, x))
            df["dataset_id"] = _norm(ds_canonical_series.astype("string")).map(
                dataset_lookup
            )

        # TISSUE
        if df["tissue"].nunique(dropna=False) == 1:
            ts_original = df["tissue"].iloc[0]
            ts_canonical = tissue_mapping.get(ts_original, ts_original)
            ts_key = str(ts_canonical).lower().strip()
            df["tissue_id"] = tissue_lookup.get(ts_key, pd.NA)
        else:
            ts_canonical_series = df["tissue"].map(lambda x: tissue_mapping.get(x, x))
            df["tissue_id"] = _norm(ts_canonical_series.astype("string")).map(
                tissue_lookup
            )

        # Gene/drug id lookups (using dicts avoids the non-unique index issue)
        df["gene_id"] = df["gene"].map(gene_lookup)
        df["drug_id"] = df["compound"].map(drug_lookup)

        # Rename CI columns
        df = df.rename(columns={"CI_lower": "ci_lower", "CI_upper": "ci_upper"})

        # Dtype tightening
        for c in ["estimate", "ci_lower", "ci_upper", "pvalue", "fdr"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("float32")
        df["n"] = pd.to_numeric(df["n"], errors="coerce").astype("Int32")

        # Drop rows with missing foreign keys; report what got dropped
        before = len(df)
        df_out = df[
            [
                "gene_id",
                "dataset_id",
                "drug_id",
                "tissue_id",
                "estimate",
                "ci_lower",
                "ci_upper",
                "pvalue",
                "fdr",
                "n",
                "mDataType",
                "metric",
            ]
        ].dropna(subset=["gene_id", "dataset_id", "drug_id", "tissue_id"])
        dropped = before - len(df_out)
        if dropped:
            print(f"[INFO] {file}: dropped {dropped} rows due to missing FK(s).")

        out_rows.append(df_out)

    if not out_rows:
        print("No input files processed.")
        return

    result = pd.concat(out_rows, ignore_index=True)

    result[["estimate", "ci_lower", "ci_upper", "pvalue", "fdr"]] = result[
        ["estimate", "ci_lower", "ci_upper", "pvalue", "fdr"]
    ].round(6)

    write_data_to_csv(
        result,
        output_files["gene_drug_tissue"],
        "id",
        {
            "gene_id": int,
            "dataset_id": int,
            "drug_id": int,
            "tissue_id": int,
            "estimate": float,
            "ci_lower": float,
            "ci_upper": float,
            "pvalue": float,
            "fdr": float,
            "n": "Int32",
            "mDataType": str,
            "metric": str,
        },
    )
