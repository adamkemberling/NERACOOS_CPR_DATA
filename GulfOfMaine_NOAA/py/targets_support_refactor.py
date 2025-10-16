# Refactoring of targets_support.R in python for use in Dagster
import pandas as pd
import re
from pathlib import Path


def separate_measure_scales(raw_file: Path, sample_type: str) -> dict:
    """
    Reads either the 'phyto' or 'zoo' sheet, separates the metadata rows
    (notes and Marmap codes) from the main data block.
    """

    sheet_name = "phytoplankton" if sample_type == "phyto" else "zooplankton"

    df_raw = pd.read_excel(raw_file, sheet_name=sheet_name, header=None)

    # Extract note line
    note = str(df_raw.iloc[0, 0]).strip()

    # Locate header row (starts with 'Cruise')
    header_row_idx = df_raw[df_raw.iloc[:, 0].astype(str).str.contains("Cruise", na=False)].index[0]

    # Read again using that row as the column names
    df = pd.read_excel(
        raw_file,
        sheet_name=sheet_name,
        skiprows=header_row_idx,
        header=0
    )

    # Parse Marmap metadata rows depending on sample type
    if sample_type == "phyto":
        marmap_row = df_raw.iloc[header_row_idx + 1]
        marmap_codes = marmap_row.dropna().to_dict()

        meta = {
            "note": note,
            "marmap_codes": marmap_codes
        }

    elif sample_type == "zoo":
        # Rows below header
        stage_row = df_raw.iloc[header_row_idx + 1]
        taxon_code_row = df_raw.iloc[header_row_idx + 2]
        stage_code_row = df_raw.iloc[header_row_idx + 3]

        # Build a dataframe mapping column name → metadata
        meta = pd.DataFrame({
            "column_name": df.columns,
            "stage": stage_row.values,
            "marmap_taxon_code": taxon_code_row.values,
            "marmap_stage_code": stage_code_row.values
        }).dropna(subset=["column_name"])

        meta = {
            "note": note,
            "zoo_meta": meta
        }

    return {"data": df, "meta": meta}
  
  
def clean_phyto_names(name: str) -> str:
    """Apply string cleaning rules from the R script."""
    name = re.sub(r"'", "", name)
    name = re.sub(r" \d", "", name)
    name = re.sub(r"\d", "", name)
    name = re.sub(r"\.", "", name)
    name = re.sub(r" _", "_", name)
    return name



def pull_phyto_pieces(phyto_raw: dict, return_option: str) -> pd.DataFrame:
    """
    Splits the phytoplankton dataset into metadata ('key') or abundance values.
    """
    df = phyto_raw["data"].copy()

    # Identify key (non-species) columns
    non_species_cols = [
        "Cruise", "Station", "Year", "Month", "Day", "Hour", "Minute",
        "Latitude (degrees)", "Longitude (degrees)", "Phytoplankton Color Index"
    ]

    if return_option == "key":
        return df[non_species_cols].drop_duplicates()

    elif return_option == "abundances":
        abund_cols = [c for c in df.columns if c not in non_species_cols]
        return df[["Cruise", "Station"] + abund_cols]

    else:
        raise ValueError("return_option must be 'key' or 'abundances'.")
      




def pivot_phytoplankton(abund_df: pd.DataFrame, key_df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts wide-format abundance data into a long-format ERDDAP-style table.
    """
    id_vars = ["Cruise", "Station"]
    df_long = abund_df.melt(id_vars=id_vars, var_name="Species", value_name="Abundance")

    # Join back metadata (station info, coordinates, etc.)
    out = df_long.merge(key_df, on=["Cruise", "Station"], how="left")
    return out
      




def pull_zoo_pieces(noaa_zoo: dict, return_option: str) -> pd.DataFrame:
    """
    Splits the zooplankton dataset into key metadata or abundance data.
    """
    df = noaa_zoo["data"].copy()

    non_species_cols = [
        "Cruise", "Station", "Year", "Month", "Day", "Hour", "Minute",
        "Latitude (degrees)", "Longitude (degrees)", "Phytoplankton Color Index"
    ]

    if return_option == "key":
        return df[non_species_cols].drop_duplicates()

    elif return_option == "abundances":
        abund_cols = [c for c in df.columns if c not in non_species_cols]
        return df[["Cruise", "Station"] + abund_cols]

    else:
        raise ValueError("return_option must be 'key' or 'abundances'.")




def pivot_zooplankton(abund_df: pd.DataFrame, key_df: pd.DataFrame, meta_df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts wide zooplankton abundance data into long-format ERDDAP-ready form,
    adding Marmap taxonomic and stage codes.
    """
    id_vars = ["Cruise", "Station"]
    df_long = abund_df.melt(id_vars=id_vars, var_name="column_name", value_name="Abundance")

    # Merge abundance data with stage/taxon metadata
    df_long = df_long.merge(meta_df, on="column_name", how="left")

    # Merge with location/time metadata
    out = df_long.merge(key_df, on=["Cruise", "Station"], how="left")

    # Optionally rename fields for clarity
    out = out.rename(columns={
        "marmap_taxon_code": "Marmap_Taxon_Code",
        "marmap_stage_code": "Marmap_Stage_Code",
        "stage": "Development_Stage"
    })

    return out






# This all should be done as one step in dagster:
def process_plankton(raw_file: Path):
    
    # --- Phytoplankton ---
    # Raw data, has a sheet for zooplankton and phytoplankton
    
    # Pull the phytoplankton sheet
    phyto_raw         = separate_measure_scales(raw_file, sample_type="phyto") 
    # This is where dylan filters to only new observations
    
    # Split the header out as a key
    phyto_key         = pull_phyto_pieces(phyto_raw, return_option="key") 
    # Pull the phytoplankton abundance data beneath the key
    phyto_abund       = pull_phyto_pieces(phyto_raw, return_option="abundances")
    # Join them and pivot them into a longer dataset
    noaa_phyto_erddap = pivot_phytoplankton(phyto_abund, phyto_key)

    # --- Zooplankton ---
    zp_raw    = separate_measure_scales(raw_file, sample_type="zoo")
    zp_key    = pull_zoo_pieces(zp_raw, return_option="key")
    zp_abund  = pull_zoo_pieces(zp_raw, return_option="abundances")
    noaa_zp_erddap = pivot_zooplankton(zp_abund, zp_key)

    return noaa_zp_erddap, noaa_phyto_erddap
      
