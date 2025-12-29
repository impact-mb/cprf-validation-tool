import io
import zipfile
from datetime import datetime
import base64

import pandas as pd
import streamlit as st
import requests


GITHUB_API_URL = "https://api.github.com"


# --------------- GitHub Counter Helpers --------------- #
def update_counter_github():
    """
    Read a counter from a file in the GitHub repo, increment it by 1,
    write updated value back to GitHub, and return the new value.

    Requires the following in Streamlit secrets (or .streamlit/secrets.toml locally):
    - GITHUB_OWNER         (e.g. "impact-mb")
    - GITHUB_REPO          (e.g. "cprf-validation-tool")
    - GITHUB_TOKEN         (a GitHub PAT with 'repo' scope)
    - COUNTER_FILE_PATH    (optional, default: "usage_counter.txt")

    If anything fails, returns None and the app continues.
    """
    try:
        owner = st.secrets["GITHUB_OWNER"]
        repo = st.secrets["GITHUB_REPO"]
        token = st.secrets["GITHUB_TOKEN"]
        path = st.secrets.get("COUNTER_FILE_PATH", "usage_counter.txt")
    except Exception:
        st.warning("GitHub counter not configured in secrets; skipping global counter.")
        return None

    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
    }

    url = f"{GITHUB_API_URL}/repos/{owner}/{repo}/contents/{path}"

    try:
        # 1) Get existing file (if present)
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            sha = data["sha"]
            content_b64 = data["content"]
            content_bytes = base64.b64decode(content_b64)
            try:
                current = int(content_bytes.decode("utf-8").strip())
            except ValueError:
                current = 0
        elif resp.status_code == 404:
            # File doesn't exist yet
            sha = None
            current = 0
        else:
            st.warning(f"GitHub counter GET failed: {resp.status_code} {resp.text}")
            return None

        new_value = current + 1

        # 2) Prepare new content
        new_content_str = str(new_value)
        new_content_b64 = base64.b64encode(new_content_str.encode("utf-8")).decode(
            "utf-8"
        )

        payload = {
            "message": f"Update usage counter to {new_value}",
            "content": new_content_b64,
        }
        if sha is not None:
            payload["sha"] = sha

        put_resp = requests.put(url, headers=headers, json=payload)
        if put_resp.status_code not in (200, 201):
            st.warning(
                f"GitHub counter PUT failed: {put_resp.status_code} {put_resp.text}"
            )
            return None

        return new_value

    except Exception as e:
        st.warning(f"GitHub counter update error: {e}")
        return None


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
    udise_str = (
        df["School UDISE"]
        .str.strip()
    )
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
    # ---------- 4. Parent Consent Check (NON-DESTRUCTIVE) ---------- #
    pc_raw = df["Parent Consent"]          # NEVER TOUCH THIS
    pc_work = pc_raw.astype(str).str.strip().str.lower()

    is_missing_pc = (
        pc_raw.isna() |
        pc_work.eq("") |
        pc_work.eq("nan")
    )

    is_no_pc = pc_work.eq("no")

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
    # Replace bad characters
    safe = "".join(c if c.isalnum() or c in (" ", "_", "-") else "_" for c in text)
    safe = "_".join(safe.split())  # spaces -> single underscore
    return safe[:80] or "ProgramLaunchName"


# --------------- Streamlit App --------------- #
def main():
    st.set_page_config(page_title="CPRF Validation Tool", layout="wide")

    # Title
    st.markdown(
        "<h1 style='text-align:center;'>CPRF Validation Tool</h1>",
        unsafe_allow_html=True,
    )

    # Main instructions + Total_Errors line
    st.write(
        """
Upload a CPRF `.xlsx` file with these mandatory columns:

- **School UDISE** (must be exactly 11 characters)
- **DATE OF BIRTH**
- **AGE**
- **PROGRAMSUBTYPENAME**
- **Parent Consent**
- **P_Age**
- **RELIGIONNAME**
- **ProgramLaunchName**

*(If your file has `DOCUMENTTYPE` or `DOCUMENTNO`, they will be dropped automatically.)*

**Validations performed:**

1. Removes rows containing `"Applied filters:"` in any column  
2. `ERROR_SCHOOL_UDISE = 1` → `School UDISE` length ≠ 11  
3. `ERROR_DOB_FORMAT = 1` → `DATE OF BIRTH` is blank / NaN / "nan" / contains `"1-1"` / `"1Jan"`  
4. `ERROR_AGE_RANGE = 1` → `PROGRAMSUBTYPENAME = "ADOLOSCENT"` and `AGE ≤ 9` or `AGE ≥ 18`  
5. `ERROR_PARENT_CONSENT = 1` → `Parent Consent` is blank / NaN / "nan" / "No"  
6. `ERROR_P_AGE = 1` → `P_Age` contains any `0` (even in comma-separated values)  
7. `ERROR_RELIGIONNAME = 1` → `RELIGIONNAME` is blank / NaN / "nan" / "missing"`  

**Total_Errors** = sum of all error flags for each row.
"""
    )

    # Score band table BEFORE explanation text
    st.markdown(
        """
<table style="width:70%; border-collapse: collapse; margin-left:auto; margin-right:auto; font-size:14px;">
    <tr style="border:1px solid #ddd; text-align:center; font-weight:bold;">
        <th style="border:1px solid #ddd; padding:8px;">Gold</th>
        <th style="border:1px solid #ddd; padding:8px;">Silver</th>
    </tr>
    <tr style="border:1px solid #ddd; text-align:left;">
        <td style="border:1px solid #ddd; padding:8px;">
            0 errors  
            <br>(or all rows if max errors = 0)
        </td>
        <td style="border:1px solid #ddd; padding:8px;">
            Low error count  
            <br>(0 &lt; errors/max ≤ 0.33)
        </td>
    </tr>
    <tr style="border:1px solid #ddd; text-align:center; font-weight:bold;">
        <th style="border:1px solid #ddd; padding:8px;">Bronze</th>
        <th style="border:1px solid #ddd; padding:8px;">Iron</th>
    </tr>
    <tr style="border:1px solid #ddd; text-align:left;">
        <td style="border:1px solid #ddd; padding:8px;">
            Medium error count  
            <br>(0.33 &lt; errors/max ≤ 0.66)
        </td>
        <td style="border:1px solid #ddd; padding:8px;">
            Highest error count  
            <br>(errors/max &gt; 0.66)
        </td>
    </tr>
</table>
""",
        unsafe_allow_html=True,
    )

    uploaded_file = st.file_uploader(
        "Upload CPRF Excel file (.xlsx)",
        type=["xlsx"],
        accept_multiple_files=False,
    )

    if uploaded_file is None:
        st.info("Please upload an Excel file to begin.")
        return

    st.success(f"File `{uploaded_file.name}` uploaded successfully.")

    try:
        with st.spinner("Reading and validating data..."):
            df = pd.read_excel(
                uploaded_file,
                dtype={"School UDISE": str}
            )
            processed_df = process_excel(df)

        # ---------- Add Error_Tier based on Total_Errors ----------
        max_errors = processed_df["Total_Errors"].max()
        processed_df["Error_Tier"] = processed_df["Total_Errors"].apply(
            lambda x: classify_error_tier(x, max_errors)
        )

        # ---------- Sort by Total_Errors (highest → lowest) ----------
        processed_df = processed_df.sort_values(
            by="Total_Errors", ascending=False
        ).reset_index(drop=True)

        # ---------- Global Counter (GitHub-backed) ----------
        counter_value = update_counter_github()

        st.success("Validation complete!")

        # ---------- SUMMARY METRICS ----------
        st.subheader("Summary")

        col_counter, col_blank = st.columns(2)
        with col_counter:
            if counter_value is not None:
                st.markdown(f"**Files processed (all-time):** {counter_value}")

        total_rows = len(processed_df)
        rows_with_errors = int((processed_df["Total_Errors"] > 0).sum())

        error_flag_cols = [
            "ERROR_SCHOOL_UDISE",
            "ERROR_DOB_FORMAT",
            "ERROR_AGE_RANGE",
            "ERROR_PARENT_CONSENT",
            "ERROR_P_AGE",
            "ERROR_RELIGIONNAME",
        ]
        flag_counts = {col: int(processed_df[col].sum()) for col in error_flag_cols}

        tier_counts = (
            processed_df["Error_Tier"]
            .value_counts()
            .reindex(["Gold", "Silver", "Bronze", "Iron"], fill_value=0)
        )

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Rows Summary**")
            st.write(f"- Total rows (after cleaning): **{total_rows}**")
            st.write(f"- Rows with `Total_Errors > 0`: **{rows_with_errors}**")

            st.markdown("**Rows by Quality Band (Error_Tier)**")
            for tier in ["Gold", "Silver", "Bronze", "Iron"]:
                st.write(f"- {tier}: **{tier_counts[tier]}** rows")

        with col2:
            st.markdown("**Error Counts by Category**")
            st.write(f"- ERROR_SCHOOL_UDISE: **{flag_counts['ERROR_SCHOOL_UDISE']}**")
            st.write(f"- ERROR_DOB_FORMAT: **{flag_counts['ERROR_DOB_FORMAT']}**")
            st.write(f"- ERROR_AGE_RANGE: **{flag_counts['ERROR_AGE_RANGE']}**")
            st.write(
                f"- ERROR_PARENT_CONSENT: **{flag_counts['ERROR_PARENT_CONSENT']}**"
            )
            st.write(f"- ERROR_P_AGE: **{flag_counts['ERROR_P_AGE']}**")
            st.write(
                f"- ERROR_RELIGIONNAME: **{flag_counts['ERROR_RELIGIONNAME']}**"
            )

        # --- PREVIEW (TOP 10 ERROR ROWS, already sorted by Total_Errors desc) ---
        st.subheader("Preview of Error Rows (Top 10 Only)")
        error_df = processed_df[processed_df["Total_Errors"] > 0].copy()

        if error_df.empty:
            st.info("No errors found! (Total_Errors = 0 for all rows)")
        else:
            st.dataframe(error_df.head(10))

        # --- BUILD RULES SHEET DATAFRAME ---
        rules_df = build_rules_sheet()

        # --- DATE STRING FOR FILENAMES ---
        today_str = datetime.today().strftime("%Y%m%d")

        # --- DOWNLOAD: FULL DATASET ---
        full_output = io.BytesIO()
        with pd.ExcelWriter(full_output, engine="openpyxl") as writer:
            processed_df.to_excel(writer, index=False, sheet_name="Validated_Data")
            rules_df.to_excel(writer, index=False, sheet_name="Shee2")
        full_output.seek(0)

        # --- DOWNLOAD: ERROR-ONLY DATASET ---
        error_output = io.BytesIO()
        with pd.ExcelWriter(error_output, engine="openpyxl") as writer:
            error_df.to_excel(writer, index=False, sheet_name="Error_Rows")
            rules_df.to_excel(writer, index=False, sheet_name="Shee2")
        error_output.seek(0)

        # --- DOWNLOAD: ZIP BY ProgramLaunchName (full + error subset, per PLN) ---
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
            for pln, grp in processed_df.groupby("ProgramLaunchName"):
                # Full data for this ProgramLaunchName
                pln_full = grp.copy()
                # Error-only subset for this ProgramLaunchName
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

        zip_buffer.seek(0)

        st.subheader("Download Outputs")
        col_a, col_b, col_c = st.columns(3)

        with col_a:
            st.download_button(
                label="Download Full Validated Excel",
                data=full_output.getvalue(),
                file_name=f"CPRF_validated_full_{today_str}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        with col_b:
            st.download_button(
                label="Download Error Rows Only",
                data=error_output.getvalue(),
                file_name=f"CPRF_validated_errors_only_{today_str}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        with col_c:
            st.download_button(
                label="Download ZIP by ProgramLaunchName",
                data=zip_buffer.getvalue(),
                file_name=f"CPRF_by_ProgramLaunchName_{today_str}.zip",
                mime="application/x-zip-compressed",
            )

    except ValueError as ve:
        st.error(f"Error: {ve}")
    except Exception as e:
        st.error(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()