# CPRF Validation Tool - User & Technical Manual

**Version:** v2026.01.02  
**Released by:** Magic Bus Impact Team  
**Release date:** 18 August 2026

## 1. Purpose

The CPRF Validation Tool is an internal data-quality utility used to review CPRF Excel files before further reporting, analysis, or programme follow-up. It applies a standard set of checks, highlights records requiring attention, and provides downloadable outputs for correction and review.

The tool is intended to reduce repeated manual checking and provide a consistent validation process across users.

## 2. Access and Login

The application is available only after successful login. User credentials are configured through Streamlit Secrets and passwords are stored as salted PBKDF2-SHA256 hashes rather than readable passwords.

Current user roles are:

- **Narendra** - Admin
- **MB_FPD** - User

A successful login is recorded in the designated Google Sheet login audit. The audit captures the username, role, login date and time in IST, event ID, and application version. Passwords and password hashes are never written to the audit log.

## 3. Application Navigation

After login, the application provides three permanent sections:

1. **Validation** - Upload and validate a CPRF Excel file.
2. **User Manual** - Read this manual in the application and download it as Markdown or Word.
3. **Release Notes** - Review changes introduced in each release.

## 4. Input File Requirements

The CPRF input must be an `.xlsx` file. The current validation engine expects the following core columns:

- School UDISE
- DATE OF BIRTH
- AGE
- PROGRAMSUBTYPENAME
- Parent Consent
- P_Age
- RELIGIONNAME
- ProgramLaunchName

If `DOCUMENTTYPE` or `DOCUMENTNO` are present, they are removed before validation.

## 5. Current Validation Rules

### School UDISE

`ERROR_SCHOOL_UDISE = 1` when School UDISE is not exactly 11 characters long.

### Date of Birth

`ERROR_DOB_FORMAT = 1` when DATE OF BIRTH is blank, missing, or contains patterns such as `1-1` or `1Jan`.

### Adolescent Age

For records where `PROGRAMSUBTYPENAME = ADOLOSCENT`, `ERROR_AGE_RANGE = 1` when AGE is 9 or below, or 18 or above.

### Parent Consent

`ERROR_PARENT_CONSENT = 1` when Parent Consent is blank, missing, or recorded as `No`.

### Parent Age

`ERROR_P_AGE = 1` when `P_Age` contains a zero value, including comma-separated values such as `0, 35` or `35, 0`.

### Religion

`ERROR_RELIGIONNAME = 1` when RELIGIONNAME is blank, missing, or recorded as `missing`.

### Total Errors

`Total_Errors` is the sum of the six validation error flags above.

## 6. Quality Band

Each record receives an `Error_Tier` based on its `Total_Errors` relative to the highest error count found in the uploaded file.

- **Gold:** 0 errors, or all rows when the maximum error count is 0
- **Silver:** error ratio greater than 0 and up to 0.33
- **Bronze:** error ratio greater than 0.33 and up to 0.66
- **Iron:** error ratio greater than 0.66

## 7. Missing Contact Number Follow-up

From v2026.01.01 onward, the tool separately identifies records where `CONTACTNUMBER` is blank or missing.

The follow-up output contains:

- REGIONNAME
- STATENAME
- DISTRICTNAME
- Community/School
- School Type
- School UDISE
- PROGRAMTYPENAME
- PROGRAMSUBTYPENAME
- ProgramLaunchName
- FUNDERNAME
- Child School Name
- CHILDID
- CONTACTNUMBER
- ContactNumberisMissing

For these records, `ContactNumberisMissing` is set to `Phone Number Missing`.

This is currently treated as a **follow-up list** and is not included in `Total_Errors`. It therefore does not change the Gold/Silver/Bronze/Iron classification.

## 8. Output Files

From v2026.01.02, output filenames retain the original uploaded filename and use only the processing date in `DDMMYYYY` format. Hours, minutes, and seconds are intentionally not included.

For an input file named `BACI_South_CPRF.xlsx` processed on 18 August 2026, outputs are:

- `BACI_South_CPRF_Validated_18082026.xlsx`
- `BACI_South_CPRF_Errors_18082026.xlsx`
- `BACI_South_CPRF_Missing_Contact_Numbers_18082026.xlsx`
- `BACI_South_CPRF_ProgramLaunch_Files_18082026.zip`

The main validated workbook also contains a `Contact_Number_Missing` worksheet.

## 9. Login Audit

Successful logins are appended to a Google Sheet tab named `Login_Log`.

The expected columns are:

| Event_ID | Username | Role | Login_Date | Login_Time | Login_Timestamp_IST | App_Version |
| --- | --- | --- | --- | --- | --- | --- |

Normal Streamlit page refreshes do not create login audit records. A row is created only when valid credentials are successfully submitted.

## 10. Data Flow

```text
User
  |
  v
Login Authentication
  |--------------------> Google Sheet: Login_Log
  |
  v
CPRF Excel Upload
  |
  v
Cleaning and Validation Engine
  |-- School UDISE
  |-- Date of Birth
  |-- Adolescent Age
  |-- Parent Consent
  |-- Parent Age
  |-- Religion
  |-- Missing Contact Number Follow-up
  |
  v
Validation Summary
  |
  |-- Full Validated Excel
  |-- Error-only Excel
  |-- Missing Contact Number Excel
  `-- ProgramLaunchName ZIP
```

## 11. Technical Structure

The application is maintained through GitHub and deployed on Streamlit Cloud.

```text
GitHub Repository
  |
  |-- app.py
  |-- requirements.txt
  |-- generate_password_hash.py
  |-- docs/
  |     |-- CPRF_VALIDATION_TOOL_MANUAL.md
  |     `-- RELEASE_NOTES.md
  |
  v
Streamlit Cloud
  |
  |-- User authentication
  |-- CPRF validation
  |-- Output generation
  `-- Google Sheet login audit
```

Sensitive information such as password hashes and Google service-account credentials must remain in Streamlit Secrets and must not be committed to GitHub.

## 12. Documentation Maintenance

`docs/CPRF_VALIDATION_TOOL_MANUAL.md` is the master manual. Update this file whenever a validation rule, output, login process, or user workflow changes.

The Streamlit application reads this file directly, which means the documentation shown in the application always reflects the version committed to GitHub.

Users can download the same content as:

- Markdown (`.md`)
- Microsoft Word (`.docx`)

## 13. Troubleshooting

### Login works but no Google Sheet record appears

Check:

- The Google Sheet is shared with the service-account email as Editor.
- The tab is named exactly `Login_Log`.
- `[login_sheet]` and `[gcp_service_account]` are configured in Streamlit Secrets.
- `gspread` is present in `requirements.txt`.
- Streamlit logs for `Google Sheet login logging failed:`.

### Input file is rejected

Check that all mandatory columns listed in Section 4 are present and that the uploaded file is an `.xlsx` file.

### Missing contact output is empty

This means no blank or recognised missing `CONTACTNUMBER` values were found in the processed data.

## 14. Version History

| Version | Release Date | Key Update |
| --- | --- | --- |
| v2026.01.02 | 18 Aug 2026 | In-app manual, downloadable Markdown/Word documentation, release notes, Google Sheet login audit, and simplified DDMMYYYY output filenames |
| v2026.01.01 | 18 Aug 2026 | Missing Contact Number follow-up dataset and related output |
| v2026.01 | 18 Aug 2026 | Initial controlled Streamlit release with multi-user hashed authentication and CPRF validation |

## 15. Ownership

**Application:** CPRF Validation Tool  
**Released by:** Magic Bus Impact Team  
**Current release:** v2026.01.02
