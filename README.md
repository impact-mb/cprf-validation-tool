# CPRF Validation Tool

**Release:** v2026.01  
**Released by:** Magic Bus Impact Team

A Streamlit-based CPRF data quality and validation utility.

## v2026.01 release changes

- Added a branded landing/sign-in page before the validation workspace.
- Added session-based authentication and Logout.
- Passwords are verified using a salted PBKDF2-SHA256 hash.
- The original login password is **not stored** in source code or Streamlit Secrets.
- Added `generate_password_hash.py` to generate the password hash locally.
- Added visible release identification: `v2026.01` and `Magic Bus Impact Team`.
- Existing CPRF validation rules and Excel/ZIP outputs remain unchanged in this release.

## 1. Generate the login password hash locally

From the project folder, run:

```bash
python generate_password_hash.py
```

Type your password twice. The password is hidden while typing. The script prints only a line similar to:

```toml
APP_PASSWORD_HASH = "pbkdf2_sha256$600000$...$..."
```

Copy that hash. Do **not** store the original password in GitHub.

## 2. Configure Streamlit Cloud

Open the Streamlit Cloud app and go to **Settings -> Secrets**. Add:

```toml
APP_USERNAME = "your_username"
APP_PASSWORD_HASH = "paste-the-generated-hash-here"
```

The repository contains `.streamlit/secrets.toml.example` as a reference only. The real `.streamlit/secrets.toml` is excluded through `.gitignore`.

## 3. Existing GitHub usage counter

The app can optionally maintain the existing GitHub-backed processing counter using:

```toml
GITHUB_OWNER = "your-owner"
GITHUB_REPO = "cprf-validation-tool"
GITHUB_TOKEN = "your-token"
COUNTER_FILE_PATH = "usage_counter.txt"
```

If these values are not configured, the validation tool still works; only the global counter is skipped.

## Run locally

```bash
pip install -r requirements.txt
python generate_password_hash.py
```

Create `.streamlit/secrets.toml` with your username and generated hash, then run:

```bash
streamlit run app.py
```
