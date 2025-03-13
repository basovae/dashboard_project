# generate_prod_config.py
import os
import secrets
import base64
from cryptography.fernet import Fernet

def generate_fernet_key():
    """Generate a secure Fernet key for Airflow."""
    return Fernet.generate_key().decode()

def create_prod_env():
    """Create a production .env file with secure values."""
    with open('.env.template', 'r') as template_file:
        template = template_file.read()
    
    # Generate secure password
    db_password = secrets.token_urlsafe(16)
    
    # Generate Airflow Fernet key
    fernet_key = generate_fernet_key()
    
    # Replace values
    prod_env = template.replace('your_secure_password', db_password)
    prod_env = prod_env.replace('your_generated_fernet_key', fernet_key)
    
    with open('.env.production', 'w') as prod_file:
        prod_file.write(prod_env)
    
    print("Production environment file created at .env.production")
    print("Make sure to copy this file to .env before deploying")

if __name__ == "__main__":
    create_prod_env()
    