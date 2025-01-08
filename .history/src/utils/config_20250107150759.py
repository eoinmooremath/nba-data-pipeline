import os
from pathlib import Path
import yaml
import logging

logger = logging.getLogger(__name__)

def load_config():
    """Load both environment variables and yaml config"""
    config = {
        'env': load_env_vars(),
        'app': load_yaml_config()
    }
    return config

def load_env_vars():
    """Load and validate environment variables"""
    required_vars = [
        'DB_SERVER',
        'DB_NAME',
        'DB_USERNAME',
        'DB_PASSWORD',
        'BASE_DIR',
        'SCRIPTS_DIR',
        'AWS_REGION',
        'EC2_INSTANCE_ID',
        'KAGGLE_USERNAME',
        'KAGGLE_KEY'
    ]
    
    env_vars = {}
    missing_vars = []
    
    for var in required_vars:
        value = os.getenv(var)
        if value is None:
            missing_vars.append(var)
        env_vars[var] = value
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
    return env_vars

def load_yaml_config():
    """Load yaml configuration file"""
    config_path = Path(__file__).parent.parent.parent / 'config' / 'config.yaml'
    
    try:
        with open(config_path) as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Error loading config.yaml: {str(e)}")
        raise