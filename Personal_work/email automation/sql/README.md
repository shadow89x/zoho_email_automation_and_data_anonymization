# sql/ - Database Schema & Import Scripts

This folder contains SQL schema files and Python scripts for importing data into a relational database.

## Purpose
- Define database schema for structured storage of processed data
- Provide scripts to import CSV data into SQL databases

## Main Files
- `schema.sql`: SQL statements to create tables and schema
- `create_schema.py`: Python script to create schema in a database
- `insert_csv_to_sql.py`: Python script to import CSV files into the database

## Usage
1. Edit `schema.sql` to match your database requirements
2. Run `create_schema.py` to create the schema
3. Use `insert_csv_to_sql.py` to import processed CSV data

## Extensibility
- Add new tables or modify schema as needed
- Adapt scripts for different database engines (PostgreSQL, MySQL, SQLite, etc.)
- Integrate with the main pipeline for automated data loading 