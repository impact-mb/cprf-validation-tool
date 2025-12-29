import io
import os
import zipfile
from datetime import datetime

import base64  # not actually needed now, but if you reuse logic you can remove it
import pandas as pd

import tkinter as tk
from tkinter import filedialog, messagebox


# --------------- Core Processing Function --------------- #
def process_excel(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate CPRF Excel data and return flagged DataFrame.

    Rules implemented:
    - Drop rows containing "Applied filters:" in any column
    - Drop columns DOCUMENTTYPE and DOCUMENTNO (if present)
    - School UDISE length check (must be exactly 11 characters)
    - DATE OF BIRTH format/missing check
    - AGE range check (only for PROGRAMSUBTYPENAME == 'ADOLOSCENT')
    - Parent Consent check
    - P_Age check (if ANY 0 exists → error)
    - RELIGIONNAME missing check
    - Total_Errors = sum of all error flags
    """

    df = df.copy()

    # ---------- DROP ROWS containing "Applied filters:" anywhere ---------- #
    df = df[
        ~df.apply(
            lambda row: row.astype(str)
            .str.contains("Applied filters:", case=False, na=False)
            .any(),
            axis=1,
        )
    ].reset_index(drop=True)

    # ---------- DROP UNUSED COLUMNS IF PRESENT ---------- #
    df = df.drop(columns=["DOCUMENTTYPE", "DOCUMENTNO"], errors="ignore")

    # ---------- CHECK REQUIRED COLUMNS ---------- #
    required_cols = [
        "School UDISE",
        "DATE OF BIRTH",
        "AGE",
        "PROGRAMSUBTYPENAME",
        "Parent Consent",
        "P_Age",
        "RELIGIONNAME",
        "ProgramLaunchName",
    ]
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")

    # ---------- INITIALIZE FLAG COLUMNS ---------- #
    df["ERROR_SCHOOL_UDISE"] = 0
    df["ERROR_DOB_FORMAT"] = 0
    df["ERROR_AGE_RANGE"] = 0
    df["ERROR_PARENT_CONSENT"] = 0
    df["ERROR_P_AGE"] = 0
    df["ERROR_RELIGIONNAME"] = 0

    # ---------- 1. School UDISE Length Check (len != 11) ---------- #
    udise_str = df["School UDISE"].astype(str).str.strip()
    df.loc[udise_str.str.len() != 11, "ERROR_SCHOOL_UDISE"] = 1

    # ---------- 2. DATE OF BIRTH Format/Missing Check ---------- #
    dob_str = df["DATE OF BIRTH"].astype(str)

    is_missing_dob = (
        df["DATE OF BIRTH"].isna()
        | dob_str.str.strip().eq("")
        | dob_str.str.lower().eq("nan")
    )

    contains_1_1 = dob_str.str.contains("1-1", case=False, na=False)
    contains_1jan = dob_str.str.contains("1jan", case=False, na=False)

    df.loc[is_missing_dob | contains_1_1 | contains_1jan, "ERROR_DOB_FORMAT"] = 1

    # ---------- 3. AGE Range Check (only ADOLOSCENT) ---------- #
    age_numeric = pd.to_numeric(df["AGE"], errors="coerce")
    program_subtype = df["PROGRAMSUBTYPENAME"].astype(str).str.upper().str.strip()

    is_adolescent = program_subtype.eq("ADOLOSCENT")
    age_out_range = (age_numeric <= 9) | (age_numeric >= 18)

    df.loc[is_adolescent & age_out_range, "ERROR_AGE_RANGE"] = 1

    # ---------- 4. Parent Consent Check ---------- #
    pc_str = df["Parent Consent"].astype(str)

    is_missing_pc = (
        df["Parent Consent"].isna()
        | pc_str.str.strip().eq("")
        | pc_str.str.lower().eq("nan")
    )

    is_no_pc = pc_str.str.strip().str.lower().eq("no")

    df.loc[is_missing_pc | is_no_pc, "ERROR_PARENT_CONSENT"] = 1

    # ---------- 5. P_Age Check (if ANY 0 exists → error) ---------- #
    p_age_str = df["P_Age"].astype(str)
    # \b0\b ensures 0 as a separate value (handles "0", "0, 35", "35, 0", "0, 0, 35", etc.)
    contains_zero = p_age_str.str.contains(r"\b0\b", regex=True, na=False)
    df.loc[contains_zero, "ERROR_P_AGE"] = 1

    # ---------- 6. RELIGIONNAME Missing Check ---------- #
    rel_str = df["RELIGIONNAME"].astype(str)

    is_missing_rel = (
        df["RELIGIONNAME"].isna()
        | rel_str.str.strip().eq("")
        | rel_str.str.lower().eq("nan")
        | rel_str.str.lower().eq("missing")
    )

    df.loc[is_missing_rel, "ERROR_RELIGIONNAME"] = 1

    # ---------- Total_Errors (sum of all flags) ---------- #
    error_cols = [
        "ERROR_SCHOOL_UDISE",
        "ERROR_DOB_FORMAT",
        "ERROR_AGE_RANGE",
        "ERROR_PARENT_CONSENT",
        "ERROR_P_AGE",
        "ERROR_RELIGIONNAME",
    ]
    df["Total_Errors"] = df[error_cols].sum(axis=1)

    return df


# --------------- Rules Sheet (Sheet Shee2) --------------- #
def build_rules_sheet() -> pd.DataFrame:
    """Create a DataFrame describing all validation rules."""
    data = [
        [
            "Rows removed",
            'Any row where any cell contains the text "Applied filters:" is removed before validation.',
        ],
        [
            "Dropped columns",
            "Columns DOCUMENTTYPE and DOCUMENTNO are dropped if present.",
        ],
        [
            "School UDISE",
            "ERROR_SCHOOL_UDISE = 1 when School UDISE (as text) length is not exactly 11 characters.",
        ],
        [
            "DATE OF BIRTH",
            "ERROR_DOB_FORMAT = 1 when DATE OF BIRTH is blank / NaN / 'nan' OR contains '1-1' OR contains '1Jan'.",
        ],
        [
            "AGE (ADOLOSCENT only)",
            'ERROR_AGE_RANGE = 1 when PROGRAMSUBTYPENAME = "ADOLOSCENT" and AGE <= 9 or AGE >= 18.',
        ],
        [
            "Parent Consent",
            "ERROR_PARENT_CONSENT = 1 when Parent Consent is blank / NaN / 'nan' OR equals 'No' (case-insensitive).",
        ],
        [
            "P_Age",
            "ERROR_P_AGE = 1 when P_Age contains any 0 value (e.g. '0', '0, 35', '35, 0', '0, 0, 40').",
        ],
        [
            "RELIGIONNAME",
            "ERROR_RELIGIONNAME = 1 when RELIGIONNAME is blank / NaN / 'nan' OR equals 'missing' (case-insensitive).",
        ],
        [
            "Total_Errors",
            "Total_Errors is the sum of all error flags: ERROR_SCHOOL_UDISE, ERROR_DOB_FORMAT, "
            "ERROR_AGE_RANGE, ERROR_PARENT_CONSENT, ERROR_P_AGE, ERROR_RELIGIONNAME.",
        ],
        [
            "Error_Tier (Gold/Silver/Bronze/Iron)",
            "Each row is assigned a quality tier based on its Total_Errors relative to the maximum Total_Errors "
            "in the file: if max Total_Errors = 0 → all Gold; else: 0 errors = Gold; "
            "0 < errors/max ≤ 0.33 = Silver; 0.33 < errors/max ≤ 0.66 = Bronze; > 0.66 = Iron.",
        ],
        [
            "ProgramLaunchName split (ZIP)",
            "The ZIP download output creates one Excel file per unique ProgramLaunchName "
            "from the validated data.",
        ],
    ]
    return pd.DataFrame(data, columns=["Check_Name", "Logic_Description"])


# --------------- Error Tier Classification --------------- #
def classify_error_tier(total_errors: int, max_errors: int) -> str:
    """Classify a row into Gold/Silver/Bronze/Iron based on Total_Errors."""
    if max_errors == 0:
        return "Gold"
    if total_errors == 0:
        return "Gold"
    ratio = total_errors / max_errors
    if ratio <= 0.33:
        return "Silver"
    elif ratio <= 0.66:
        return "Bronze"
    else:
        return "Iron"


# --------------- Helper: safe filename from ProgramLaunchName --------------- #
def safe_filename_from_pln(pln_value: str) -> str:
    text = str(pln_value).strip()
    safe = "".join(c if c.isalnum() or c in (" ", "_", "-") else "_" for c in text)
    safe = "_".join(safe.split())  # spaces -> single underscore
    return safe[:80] or "ProgramLaunchName"


# --------------- Desktop Workflow --------------- #
def run_desktop_tool():
    # Hide root Tk window
    root = tk.Tk()
    root.withdraw()
    root.title("CPRF Validation Desktop Tool")

    messagebox.showinfo(
        "CPRF Validation Tool",
        "Step 1: Select the CPRF Excel file (.xlsx)\n\n"
        "Required columns:\n"
        "- School UDISE\n- DATE OF BIRTH\n- AGE\n- PROGRAMSUBTYPENAME\n"
        "- Parent Consent\n- P_Age\n- RELIGIONNAME\n- ProgramLaunchName",
    )

    # --- Select input file ---
    input_path = filedialog.askopenfilename(
        title="Select CPRF Excel file",
        filetypes=[("Excel files", "*.xlsx")],
    )

    if not input_path:
        messagebox.showwarning("CPRF Validation Tool", "No file selected. Exiting.")
        return

    # --- Select output folder ---
    messagebox.showinfo(
        "CPRF Validation Tool",
        "Step 2: Select the folder where output files should be saved.",
    )

    output_folder = filedialog.askdirectory(
        title="Select output folder",
    )

    if not output_folder:
        messagebox.showwarning(
            "CPRF Validation Tool", "No output folder selected. Exiting."
        )
        return

    try:
        # --- Read & process data ---
        df = pd.read_excel(input_path)
        processed_df = process_excel(df)

        # Add Error_Tier
        max_errors = processed_df["Total_Errors"].max()
        processed_df["Error_Tier"] = processed_df["Total_Errors"].apply(
            lambda x: classify_error_tier(x, max_errors)
        )

        # Sort by Total_Errors desc
        processed_df = processed_df.sort_values(
            by="Total_Errors", ascending=False
        ).reset_index(drop=True)

        # Build error-only subset
        error_df = processed_df[processed_df["Total_Errors"] > 0].copy()

        # Rules sheet
        rules_df = build_rules_sheet()

        # Date string for filenames
        today_str = datetime.today().strftime("%Y%m%d")

        # --- File names ---
        full_file = os.path.join(
            output_folder, f"CPRF_validated_full_{today_str}.xlsx"
        )
        error_file = os.path.join(
            output_folder, f"CPRF_validated_errors_only_{today_str}.xlsx"
        )
        zip_file = os.path.join(
            output_folder, f"CPRF_by_ProgramLaunchName_{today_str}.zip"
        )

        # --- Save FULL dataset ---
        with pd.ExcelWriter(full_file, engine="openpyxl") as writer:
            processed_df.to_excel(writer, index=False, sheet_name="Validated_Data")
            rules_df.to_excel(writer, index=False, sheet_name="Shee2")

        # --- Save ERROR-only dataset ---
        with pd.ExcelWriter(error_file, engine="openpyxl") as writer:
            error_df.to_excel(writer, index=False, sheet_name="Error_Rows")
            rules_df.to_excel(writer, index=False, sheet_name="Shee2")

        # --- ZIP by ProgramLaunchName ---
        with zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED) as zipf:
            for pln, grp in processed_df.groupby("ProgramLaunchName"):
                pln_full = grp.copy()
                pln_errors = grp[grp["Total_Errors"] > 0].copy()

                file_buffer = io.BytesIO()
                with pd.ExcelWriter(file_buffer, engine="openpyxl") as writer:
                    pln_full.to_excel(
                        writer,
                        index=False,
                        sheet_name="Validated_Data",
                    )
                    pln_errors.to_excel(
                        writer,
                        index=False,
                        sheet_name="Error_Rows",
                    )
                    rules_df.to_excel(writer, index=False, sheet_name="Shee2")
                file_buffer.seek(0)

                safe_name = safe_filename_from_pln(pln)
                zipf.writestr(f"{safe_name}.xlsx", file_buffer.getvalue())

        msg = (
            "Validation complete!\n\n"
            f"Full validated file:\n{full_file}\n\n"
            f"Error-only file:\n{error_file}\n\n"
            f"ZIP by ProgramLaunchName:\n{zip_file}"
        )
        messagebox.showinfo("CPRF Validation Tool", msg)

    except Exception as e:
        messagebox.showerror("Error", f"Something went wrong:\n{e}")


if __name__ == "__main__":
    run_desktop_tool()