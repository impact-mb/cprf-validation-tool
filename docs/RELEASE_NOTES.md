# CPRF Validation Tool - Release Notes

## v2026.01.02 - Documentation, Audit and File Naming Update

**Release Date:** 18 August 2026  
**Released by:** Magic Bus Impact Team

### Added

- Added a permanent **User Manual** section after login.
- Added `docs/CPRF_VALIDATION_TOOL_MANUAL.md` as the master documentation file maintained in GitHub.
- Added in-app manual viewing.
- Added manual download as both `.md` and `.docx`.
- Added a permanent **Release Notes** section after login.
- Added Google Sheet audit logging for successful user logins.
- Login audit records include Event ID, Username, Role, Login Date, Login Time, IST Timestamp, and App Version.

### Changed

- Application version updated to **v2026.01.02**.
- Output filenames now use **DDMMYYYY only**.
- Hours, minutes and seconds have been removed from generated output filenames.
- Example output naming:
  - `BACI_South_CPRF_Validated_18082026.xlsx`
  - `BACI_South_CPRF_Errors_18082026.xlsx`
  - `BACI_South_CPRF_Missing_Contact_Numbers_18082026.xlsx`
  - `BACI_South_CPRF_ProgramLaunch_Files_18082026.zip`
- The older GitHub usage counter is no longer part of the application workflow. Usage visibility is now based on successful-login audit records in Google Sheets.

### Documentation Structure

After successful login, users can access:

1. **Validation**
2. **User Manual**
3. **Release Notes**

---

## Planned Major Release — v2026_02

**Target Release:** October 2026  
**Release Type:** Quarterly Major Release  
**Working Theme:** Data Quality Monitoring & Action Release

### Planned Enhancements

- Data Quality Dashboard with:
  - Total records
  - Clean records
  - Records with validation issues
  - Data Quality %
  - Issue counts by validation type

- Breakdown of data quality by:
  - Region
  - State
  - District
  - ProgramLaunchName
  - Funder

- Action-oriented Excel outputs with separate correction sheets for:
  - Missing Contact Number
  - Invalid School UDISE
  - DOB Issues
  - Age Issues
  - Parent Consent Issues
  - Parent Age Issues
  - Religion Issues

- Validation Run ID for every validation cycle.

- Validation audit history capturing:
  - Run ID
  - Username
  - Validation date
  - Source file name
  - Total records
  - Error records
  - Clean records
  - Data Quality %
  - App version

- Updated User Manual, DFD and Release Notes.

### Versioning Convention

- `2026_01` — Q1 Major Release
- `2026_01.01`, `2026_01.02`, etc. — Q1 Minor Releases
- `2026_02` — Q2 Major Release
- `2026_02.01`, `2026_02.02`, etc. — Q2 Minor Releases

## v2026.01.01 - Missing Contact Number Follow-up

**Release Date:** 18 August 2026  
**Released by:** Magic Bus Impact Team

### Added

- Added identification of records where `CONTACTNUMBER` is blank or missing.
- Added a dedicated Missing Contact Numbers follow-up dataset.
- Added `ContactNumberisMissing` with the value `Phone Number Missing` for identified records.
- Added `Contact_Number_Missing` worksheet to the main validated workbook.
- Added a separate Missing Contact Numbers Excel download.

### Follow-up Dataset Fields

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

### Scoring Note

Missing `CONTACTNUMBER` remains a follow-up requirement and is **not included in `Total_Errors`**. The existing Gold/Silver/Bronze/Iron classification is therefore unchanged by this check.

---

## v2026.01 - Initial Controlled Release

**Release Date:** 18 August 2026  
**Released by:** Magic Bus Impact Team

### Added

- Streamlit-based CPRF Validation Tool.
- Controlled login before validation access.
- Multi-user authentication.
- User roles for Admin and User access.
- Salted PBKDF2-SHA256 password verification.
- Streamlit Secrets-based credential configuration.
- Logout functionality.
- CPRF Excel upload and validation.
- Validation checks for School UDISE, Date of Birth, Adolescent Age, Parent Consent, Parent Age, and Religion.
- `Total_Errors` calculation.
- Gold/Silver/Bronze/Iron quality classification.
- Full validated Excel download.
- Error-only Excel download.
- ProgramLaunchName-wise ZIP download.

---

## Version Summary

| Version | Release Date | Key Update |
| --- | --- | --- |
| **v2026.01.02** | 18 Aug 2026 | Documentation, Google Sheet login audit and DDMMYYYY output naming |
| **v2026.01.01** | 18 Aug 2026 | Missing Contact Number follow-up output |
| **v2026.01** | 18 Aug 2026 | Initial controlled release |
