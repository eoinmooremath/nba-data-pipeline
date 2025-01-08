# NBA Historical Database & Public Access Pipeline

This project maintains one of the most comprehensive basketball statistical publicly available, encompassing:

- Every NBA game since 1946 (66,000+ games)
- Complete player statistics for each game where available (1.5 million+ records)
- Comprehensive team performances for each game where available (130,000+ records)
- Daily updates throughout the active season

The system combines an optimized SQL Server database with an automated pipeline that makes this wealth of historical data freely accessible through Kaggle, updating nightly to ensure currency.

**[Access the Complete NBA Dataset on Kaggle →](https://www.kaggle.com/datasets/eoinamoore/historical-nba-data-and-player-box-scores)**

## Project Overview

The project consists of two main components: a carefully optimized database schema and an automated data pipeline. The database preserves historical accuracy while maintaining high performance on resource-constrained infrastructure. The pipeline ensures this data remains current and publicly accessible.

## Database Architecture

The SQL Server implementation features sophisticated optimization strategies documented in two key files:

- [`schema.md`](docs/schema.md): Comprehensive documentation of database design decisions
- [`create_database.sql`](sql/create_database.sql): Complete SQL implementation

Key architectural features:
- Reverse chronological indexing for optimal data access
- Temporal data handling for team histories
- Strategic index design patterns
- Resource-efficient schema optimizations
- Performance tuning for t3.micro instances

## Automated Pipeline

The pipeline automates the entire process of collecting NBA statistics and making them publicly available. Each night, it executes a carefully orchestrated sequence of operations across multiple AWS services.

### Data Collection and Processing

The process begins with a Lambda function that monitors NBA.com for new game data. This function, triggered nightly by CloudWatch Events, uses Python and Pandas to process the latest NBA statistics. The function handles several critical tasks:

1. Scraping game data from NBA.com using sophisticated web scraping techniques
2. Processing and validating the statistical information
3. Updating the SQL Server database on Amazon RDS using optimized batch operations
4. Triggering the next phase through CloudWatch events upon successful completion

### Public Distribution Process

When the Lambda function completes successfully, it triggers a CloudWatch event that activates an EC2 instance. This instance executes the `nba_update.sh` shell script, which orchestrates the export and distribution process through several steps:

1. Generates a complete SQL dump of the database using `create_sql_dump.py`
2. Creates individual CSV files for each table using `export_tables.py`
3. Uploads both the SQL dump and CSV files to Kaggle

### Pipeline Architecture

```
[NBA.com] 
    │
    ▼
[Lambda Function] ─────────► [Amazon RDS]
    │                           │
    │                           │
    ▼                           ▼
[CloudWatch Event] ────────► [EC2 Instance]
                               │
                               ├── create_sql_dump.py
                               ├── export_tables.py
                               │
                               ▼
                          [Kaggle Dataset]
                               │
                               ├── NBA_Database.sql
                               └── Table_Name.csv (multiple files)
```

## Implementation Details

### Lambda Function (`lambda_function.py`)
- Collects nightly game data from NBA.com
- Processes statistics using Pandas
- Updates RDS database through optimized batch operations
- Triggers CloudWatch event upon completion

### Environment Setup (`setup_environment.sh`)
- One-time configuration script for EC2 instance
- Installs Python 3.8+, SQL Server ODBC driver
- Sets up AWS CLI and required Python packages
- Configures logging directories
- Establishes database and Kaggle credentials

### Database Export (`create_sql_dump.py`)
- Generates complete SQL backup with schema
- Implements memory-efficient batch processing
- Preserves all optimization features and indexes
- Handles large datasets within t3.micro constraints

### Table Export (`export_tables.py`)
- Creates individual CSV files for all tables
- Uses chunking for memory-efficient processing
- Maintains reverse chronological ordering
- Optimized for large-scale data export

### Export Orchestration (`nba_update.sh`)
- Coordinates execution of export scripts
- Manages EC2 resource allocation
- Handles Kaggle API uploads with retry logic
- Maintains comprehensive logging


### Setup Requirements

Setting up the pipeline requires configuring several AWS services:

**EC2 Configuration:**
- t3.micro instance with Amazon Linux 2
- IAM role with necessary RDS and CloudWatch permissions
- Installed dependencies: Python 3.8+, SQL Server ODBC driver
- Configured Kaggle API credentials

**Lambda Configuration:**
- Python 3.8 runtime environment
- IAM role with RDS and CloudWatch permissions
- Environment variables for database connection
- CloudWatch Event trigger for nightly execution

## 🤝 Contributing

I welcome contributions that enhance the pipeline's capabilities:

1. Open an issue to discuss proposed changes
2. Fork the repository
3. Create a feature branch
4. Submit a detailed pull request

## 📜 License

MIT License - See LICENSE file for details.

---

*Find the complete NBA dataset on [Kaggle](https://www.kaggle.com/datasets/eoinamoore/historical-nba-data-and-player-box-scores)*

## 🙏 Acknowledgments

This project is:
- Powered by NBA.com's comprehensive statistics
- Built on AWS's robust cloud infrastructure
- Inspired by Wyatt Walsh's NBA Database (https://www.kaggle.com/datasets/wyattowalsh/basketball)