# Multinational Retail Data Centralisation

This project centralizes and streamlines data management in the retail sector, featuring Python scripts for data extraction, cleaning, and database utilities, focusing on handling diverse data formats and sources.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Repository Structure](#repository-structure)
- [Getting Started](#getting-started)
- [Contributing](#contributing)
- [License](#license)

## Overview

Aiming to centralize retail data management across multiple nations, this project includes modules for data extraction, cleaning, and database interaction, with a particular focus on efficient data handling and management.

## Technologies Used

This project employs a diverse range of technologies, ensuring robustness and efficiency in data management:

- **Python:** Primary programming language for script development.
- **Pandas:** For data manipulation and analysis.
- **Tabula:** Extracts tables from PDFs into DataFrame objects.
- **Requests:** HTTP library for API interactions.
- **Boto3:** AWS SDK for Python, used with Amazon S3.
- **SQLAlchemy:** SQL toolkit and ORM library.
- **YAML:** For configuration and data serialization.
- **PostgreSQL:** Database system for structured data storage.
- **Docker:** Utilized for containerizing the PostgreSQL database, enhancing portability and consistency across different environments.

## Features

### Data Extraction:

- **Databases:** Utilizes SQLAlchemy and custom connectors to interface with RDS databases. Capable of executing SQL queries and retrieving structured data efficiently.
- **PDFs:** Leverages the Tabula library to extract tables from PDF documents directly into DataFrame objects.
- **APIs:** Uses the Requests library for interacting with web APIs, fetching data in JSON format.
- **Amazon S3 Buckets:** Uses Boto3, the AWS SDK for Python, for interacting with Amazon S3, handling data types like CSV and JSON.

### Data Cleaning:

- Utilities to clean and transform the extracted data for consistency. This includes handling missing values, standardizing formats, and removing duplicates.

### Database Integration:

- Tools for uploading cleaned data into a centralized database system, ensuring data integrity and efficient storage.

## Repository Structure

```
├── data_extraction.py    # Script for extracting data from various sources
├── data_cleaning.py      # Script for cleaning and standardizing data
├── database_utils.py     # Utilities for database operations
├── main.py               # Central executable for running ETL workflows
└── config/               # Configuration files and templates
    ├── db_creds_local.yaml
    ├── db_creds.yaml
    ├── .env.template
    └── compose.yml
```

## Getting Started

To get started with this project, clone the repository, install required dependencies, and follow the setup instructions.

### Prerequisites

- Python 3.x
- Libraries: pandas, tabula, requests, boto3

### Installation

```bash
git clone multinational-retail-data-centralisation319
cd multinational-retail-data-centralisation319

pip install -r requirements.txt
```

## Running the Project

Execute `main.py` to initiate the ETL workflows. This script orchestrates the entire process of data extraction, transformation (cleaning and standardizing), and loading into the database.

```bash
python main.py
```


## Contributing

Contributions to improve the project are welcome. Please read the [contributing guide](https://github.com/freestyleabg/multinational-retail-data-centralisation319/wiki/Contribution-Guide) for details on our code of conduct, and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE.md) file for details.
