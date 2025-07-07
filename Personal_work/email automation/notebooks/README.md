# notebooks/ - Jupyter Notebooks

This folder contains all Jupyter notebooks for interactive data analysis, pipeline orchestration, and documentation. Each notebook is designed to be self-explanatory, with detailed markdown cells explaining every step.

## Purpose
- Demonstrate and document each step of the data pipeline
- Allow for interactive exploration, debugging, and visualization
- Serve as educational resources for new users

## Main Notebooks (Execution Order)

### 1. `1. automated_zoho_email_retrieval_pipeline.ipynb`
- **Purpose**: Download and parse raw email data from Zoho
- **Features**: API integration, email extraction, basic data cleaning
- **Output**: Raw email dataframe

### 2. `2. comprehensive_data_analysis_cleaning_pipeline.ipynb`
- **Purpose**: Clean and analyze raw data, explore data quality
- **Features**: Data cleaning, duplicate removal, format standardization
- **Output**: Cleaned email and customer data

### 3. `3. complete_business_id_and_optical_name_pipeline_fixed.ipynb`
- **Purpose**: Merge customer and email data, prepare for matching
- **Features**: business_id mapping, optical_name cleaning, customer-email linking
- **Output**: Matched customer-email data

### 4. `4. data_anonymization_cleaned.ipynb`
- **Purpose**: Orchestrate the complete anonymization and translation pipeline
- **Features**: PII detection and anonymization, Korean-English translation, data protection
- **Output**: Anonymized datasets for analysis

## Workflow
1. **Data Collection and Cleaning** (Notebooks 1, 2)
2. **Customer-Email Matching** (Notebook 3)
3. **Anonymization and Translation** (Notebook 4)
4. **Result Validation and Analysis**

## How to Use
- Open notebooks in order for step-by-step pipeline demonstration
- Each notebook contains detailed markdown for easy understanding
- Modify parameters and rerun cells as needed for your data

## Best Practices
- Do not include sensitive data in markdown or outputs
- Use notebooks for prototyping and documentation; use `src/` scripts for production
- Use `../data/` path for all data files

## Data Paths
- All notebooks use `../data/` path for data files
- Anonymized data is saved in `../data/` folder
- Never commit original data 