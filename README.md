# Centralized Data Management for Retail

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

Comprehensive scripts for extracting data from multiple sources, including:

  <!-- - **Databases:** Using SQL queries and database connections to retrieve structured data.
  - **PDFs:** Utilizing libraries like `tabula` to parse and extract data from PDF documents.
  - **APIs:** Fetching data from web APIs in JSON format.
  - **Amazon S3 Buckets:** Integrating with AWS to retrieve data stored in S3 buckets. -->

#### Databases

- **Relational Database Service (RDS):** Utilizes SQLAlchemy and custom connectors to interface with RDS databases. Capable of executing SQL queries and retrieving structured data efficiently.

#### PDFs

- **PDF Table Extraction:** Leverages the Tabula library to extract tables from PDF documents directly into DataFrame objects.

#### APIs

- **RESTful API Communication:** Uses the Requests library for interacting with web APIs, fetching data in JSON format.
- **Dynamic Endpoint Handling:** Capable of constructing API endpoints and headers for customized requests.

#### Amazon S3 Buckets

- **AWS S3 Integration:** Employs Boto3, the AWS SDK for Python, for interacting with Amazon S3, handling data types like CSV and JSON.
- **Complex URL Parsing:** Advanced parsing capabilities for extracting bucket and file path information from S3 URLs.

### Data Cleaning:

- Utilities to clean and transform the extracted data for consistency. This includes handling missing values, standardizing formats, and removing duplicates.

### Database Integration:

- Tools for uploading cleaned data into a centralized database system, ensuring data integrity and efficient storage.

## Repository Structure

- data_extraction.py
- data_cleaning.py
- database_utils.py

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

### Usage

Run the scripts individually to perform data extraction, cleaning, and database operations.

## Contributing

Contributions to improve the project are welcome. Please read the contributing guide for details on our code of conduct, and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.
