# dbt-datavault-kimball
Comparison between datavault and kimball data model in practice using dbt and postgres

Practical Application: A Flights Data Case Study
Theory is great, but practical implementation truly brings concepts to life. To demonstrate the tangible differences between Kimball and Data Vault, we'll use a simplified flights demo dataset. This dataset, though small, allows us to explore how each model handles common data engineering tasks, from initial ingestion into a staging layer to building the final presentation layer, and critically, how they adapt to schema changes.

All the code and data for this practical example are available in my GitHub repository: [Your GitHub Repository Link Here]

Pre-requisites:
To follow along with the practical steps, ensure you have the following set up:

PostgreSQL database: Installed and running (local installation or via Docker). If you need installation steps, refer to this link.
7-Zip (or equivalent 7z extractor): This is required to decompress the provided SQL dump file. Download it from https://www.7-zip.org/download.html.
DBT (Data Build Tool): Installed and configured to connect to your PostgreSQL database. Refer to the official DBT documentation for installation: https://docs.getdbt.com/docs/get-started/install.
Demo Flights Data: The compressed SQL dump file dump-demo-202506181537.7z will be part of the GitHub repository.

## Step 1: Loading the Staging Layer into PostgreSQL
Our journey begins by ingesting the raw flights data into a staging schema within PostgreSQL. This initial step is crucial for both Kimball and Data Vault approaches, as it provides a clean, raw copy of the source data before any transformations or modeling.

Since our dataset is provided as a compressed SQL dump (.7z), we first need to extract it and then use PostgreSQL's psql command-line tool to load it.

Decompress the SQL Dump: Navigate to the data/ directory within the cloned GitHub repository. Use your 7z utility to extract dump-demo-202506181537.7z. This will produce a plain SQL file (e.g., dump-demo-202506181537.sql).

Command Example (Windows Command Prompt, assuming 7z is in PATH): 7z x dump-demo-202506181537.7z
Note: Ensure the SQL file is extracted to a location accessible by your terminal/shell.

Prepare your PostgreSQL Database:
Import the SQL Dump: Use the psql command-line tool to import the extracted .sql file into your PostgreSQL database. This will create the necessary tables and populate them with the raw flight data, forming our staging layer.

Command Example: psql -h localhost -U <your_user> -d postgres -f dump-demo-202506181537.sql

Replace localhost with your PostgreSQL host if it's not local.
Replace your_user with your PostgreSQL username.
Replace flights_dw with the name of your database.
You might be prompted for your password.

After these steps, you will have your raw flights data loaded into PostgreSQL, ready for transformation and modeling in the subsequent layers. This setup provides the foundation for comparing Kimball and Data Vault methodologies in the following sections.