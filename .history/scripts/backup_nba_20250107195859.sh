# NBA Data Pipeline

Unlock the complete history of NBA basketball through this enterprise-grade data pipeline. Housing every box score since 1946 and updating daily, this system maintains one of the most comprehensive NBA statistical databases ever assembled. Built for AWS cloud infrastructure, this pipeline automatically collects, processes, and archives detailed game statistics, creating a living record of basketball history that grows with each passing game.

## 🌟 Key Features

### Historical Database with Daily Updates
- Complete collection of NBA box scores from 1946 to present
- Automated daily game data collection from NBA.com
- Sophisticated player and team statistics processing
- Intelligent EC2 instance lifecycle management for efficient backups
- Daily updates to Kaggle dataset for public access

### High-Performance Export Engine
- Advanced temporal chunking for optimal performance
- Memory-optimized processing tailored for t3.micro instances
- Complete database schema preservation
- Streamlined CSV exports of analytical views and tables

### Cloud-Native Architecture
- Seamless integration with AWS RDS SQL Server
- Serverless processing via AWS Lambda
- Event-driven automation
- Robust error handling and recovery

## 🚀 Getting Started

### System Requirements

#### Development Environment
- Python 3.8 or higher
- SQL Server ODBC Driver 17
- Configured AWS CLI with appropriate permissions

#### AWS Infrastructure
- RDS SQL Server instance
- Lambda execution environment
- EC2 instance for backup operations
- EventBridge scheduling rules

### Initial Setup

1. Clone the repository and create your environment file:
```bash
cp .env.example .env
```

2. Configure your environment variables:
```bash
# Core Infrastructure
BASE_DIR=/home/ec2-user/nba_backup
SCRIPTS_DIR=/home/ec2-user/nba_backup/scripts

# Database Configuration
DB_SERVER=your-server.region.rds.amazonaws.com
DB_NAME=NBA_Database
DB_USERNAME=admin
DB_PASSWORD=your_password
DB_CONNECTION_TIMEOUT=30
DB_LOGIN_TIMEOUT=30
DB_COMMAND_TIMEOUT=300

# Integration Points
KAGGLE_USERNAME=your_username
KAGGLE_KEY=your_key
AWS_REGION=us-east-2
EC2_INSTANCE_ID=i-xxxxxxxxxxxxxxxxx

# Performance Tuning
EXPORT_CHUNK_SIZE=50000
PLAYER_STATS_CHUNK_SIZE=25000
LAMBDA_TIMEOUT=900
LAMBDA_MEMORY_SIZE=256
```

### Installation Guide

Set up your local development environment:

```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
.\venv\Scripts\activate   # Windows

# Install dependencies
pip install -r requirements.txt

# Install ODBC driver (Amazon Linux 2)
curl https://packages.microsoft.com/config/rhel/7/prod.repo | sudo tee /etc/yum.repos.d/mssql-release.repo
sudo ACCEPT_EULA=Y yum install -y msodbcsql17
```

## 💻 Usage

### Data Export Operations

Execute the full backup pipeline:
```bash
./scripts/backup_nba.sh
```

Run individual components:
```bash
python src/export_tables.py
python src/create_sql_dump.py
```

### Lambda Operations
- Automatic execution via EventBridge schedule
- Manual triggering through AWS Console
- CLI-based invocation available
- Real-time game data updates

## 🔧 Performance Optimization

The pipeline incorporates sophisticated performance optimizations:

- Dynamic resource allocation for t3.micro instances
- Intelligent CPU credit management
- Memory-efficient batch processing algorithms
- Configurable chunk sizes for different data types
- Optimized database query patterns

## 🤝 Contributing

We welcome contributions that enhance the pipeline's capabilities:

1. Open an issue to discuss proposed changes
2. Fork the repository
3. Create a feature branch
4. Submit a detailed pull request

## 📜 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

This project stands on the shoulders of giants:
- Powered by NBA.com's comprehensive statistics
- Built on AWS's robust cloud infrastructure
- Optimized for cost-effective, enterprise-grade deployment

## 📊 Data Structure

The database maintains several key components:

1. **Player Statistics**: Game-by-game box scores for every player in NBA history
2. **Team Statistics**: Comprehensive team-level statistics for every NBA game
3. **Games**: Detailed game records including outcomes, attendance, and arena information
4. **Players**: Biographical and physical data for all NBA players
5. **Team Histories**: Historical tracking of franchise changes and relocations
6. **Current Season Schedule**: Complete NBA schedule for the current season

## 📊 Architecture Diagram

```
[NBA.com API] → [Lambda Function] → [RDS SQL Server]
       ↓                 ↓                ↓
[Daily Updates] → [Data Processing] → [Export Pipeline]
       ↓                 ↓                ↓
[Historical Data] → [Database Updates] → [Kaggle Dataset]
```

## 🔍 Monitoring and Maintenance

The pipeline includes comprehensive monitoring capabilities:
- Detailed logging of all operations
- Error tracking and reporting
- Performance metrics collection
- Resource utilization monitoring

Regular maintenance tasks are automated through scheduled jobs, ensuring data integrity and system reliability.