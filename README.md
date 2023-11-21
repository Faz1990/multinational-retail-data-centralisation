# Multinational-Retail-Data-Centralisation

## Table of Contents
1. [Personal Initiative and Learning](#personal-initiative-and-learning)
2. [Project Description](#project-description)
3. [Installation](#installation)
4. [Usage](#usage)
5. [File Structure](#file-structure)
6. [License](#license)

## Personal Initiative and Learning
⚙️ Important Note: This repository is the culmination of my self-driven efforts and learning trajectory. Distinct from numerous tutorial-led projects, the approaches and code within this repository originated from my independent ideation and execution. Every aspect of this project, from the initial code line to the overarching strategies, has been personally developed, guided by foundational concepts and insights provided by instructors at AiCore. This endeavor exemplifies my commitment to applying academic concepts to solve practical, real-life data engineering challenges, underscoring my skills in analytical thinking, problem resolution, and technical proficiency in the realm of data engineering.

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

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

