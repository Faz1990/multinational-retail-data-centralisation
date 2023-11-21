# Multinational-Retail-Data-Centralisation

## Table of Contents
1. [Project Description](#project-description)
2. [Installation](#installation)
3. [Usage](#usage)
4. [File Structure](#file-structure)
5. [License](#license)

## Project Description
> This project involves the development of a data centralization system for a multinational retail chain. The primary goal is to automate the extraction, cleaning, and storage of sales data from various sources, including RDS databases, S3 buckets, and JSON files. This system enhances data accessibility and reliability, significantly improving decision-making processes. Through this project, I've deepened my understanding of Python, SQL, AWS services, and data engineering principles.

## Installation
Instructions for installing your project.

Example:
```bash
# Clone the repository
git clone https://github.com/Faz1990/multinational-retail-data-centralisation.git

# Navigate to the project directory
cd multinational-retail-data-centralisation

# Install required dependencies
pip install -r requirements.txt

```
## Usage

Run the main script

```bash
python main.py

```

Running this script will execute the data processing pipeline, which involves extracting data from various sources, cleaning it, and uploading it to the database.

## File Structure

multinational-retail-data-centralisation/

├── database_utils.py    # Handles database connections

├── data_cleaning.py     # Cleans and prepares data

├── data_extraction.py   # Extracts data from various sources

├── main.py              # Main script orchestrating the process

└── README.md


## License

This project is licensed under the MIT License - see the LICENSE file for details.

