# Team contribution workflow

Repository: `Maazwary786/Hospital-Readmission-Pipeline-Databricks`

## Team

- `Maazwary786` — repository owner and maintainer.
- `Hitesh0825` — invited collaborator; accept the GitHub invitation before pushing.

Each contributor should create and push their own feature branch. Commits must be made
from that contributor's own GitHub account so GitHub can accurately show who did the work.
Do not commit or push using another teammate's account.

## Branch and pull-request workflow

1. Start with the latest integration branch:

   ```powershell
   git switch develop
   git pull origin develop
   ```

2. Create a task-specific branch under your GitHub username:

   ```powershell
   git switch -c feature/<github-username>/<short-task-name>
   ```

3. Make a focused change, run its relevant check, and commit it using your own Git identity:

   ```powershell
   git add <changed-files>
   git commit -m "Describe the change"
   git push -u origin feature/<github-username>/<short-task-name>
   ```

4. Open a pull request from that feature branch into `develop`. Include the task, files
   changed, validation performed, and any screenshots or terminal output that help review.
5. Have the other teammate review it. Merge to `develop` after approval and checks pass.
6. The maintainer releases reviewed work from `develop` to `main`.

Use GitHub issues to agree on task ownership before work begins. Do not split work by
creating commits for a teammate; each person should author their own contribution.

## Demo and data safety

- The input records created by `data_generator.py` are synthetic demo data.
- Generated reports and source-document PDFs/DOCX files are not published in this repository.
- Do not commit real patient data, API keys, `.env` files, Databricks credentials, or
  local virtual environments.
- `data/raw/mysql_ehr/patients.csv` and the raw Bronze patients table are intentionally
  ignored because they contain identity-like fields. Regenerate local demo inputs with
  `python data_generator.py`.
- The demo launcher generates a temporary Airflow password. Do not commit passwords,
  tokens, or generated local credentials.
