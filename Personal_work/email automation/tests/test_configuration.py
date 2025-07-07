#!/usr/bin/env python3
"""
Configuration and Setup Test Suite
Email-Customer Analytics Pipeline

This script validates all system configurations and dependencies
before running the main pipeline.

What: Comprehensive system validation
Why: Prevent runtime errors and ensure proper setup
How: Test each component independently with detailed feedback
Alternative: Manual testing (time-consuming and error-prone)
"""

import os
import sys
import json
import subprocess
import importlib
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tests/test_results.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ConfigurationTester:
    """
    Comprehensive configuration testing suite
    
    What: Validates all system components and configurations
    Why: Ensures reliable pipeline execution
    How: Sequential testing with detailed reporting
    Alternative: pytest framework (overkill for this use case)
    """
    
    def __init__(self):
        self.test_results = []
        self.root_path = Path(__file__).parent.parent
        self.start_time = datetime.now()
        
    def run_test(self, test_name: str, test_func) -> bool:
        """
        Execute individual test with error handling
        
        What: Standardized test execution wrapper
        Why: Consistent error handling and reporting
        How: Try-catch with detailed logging
        Alternative: unittest framework (more complex setup)
        """
        try:
            logger.info(f"🧪 Running test: {test_name}")
            result = test_func()
            if result:
                logger.info(f"✅ {test_name}: PASSED")
                self.test_results.append((test_name, "PASSED", ""))
                return True
            else:
                logger.error(f"❌ {test_name}: FAILED")
                self.test_results.append((test_name, "FAILED", "Test returned False"))
                return False
        except Exception as e:
            logger.error(f"❌ {test_name}: ERROR - {str(e)}")
            self.test_results.append((test_name, "ERROR", str(e)))
            return False
    
    def test_python_version(self) -> bool:
        """
        Validate Python version compatibility
        
        What: Check Python version meets minimum requirements
        Why: Prevent compatibility issues with newer packages
        How: Compare sys.version_info with minimum requirements
        Alternative: requirements.txt python_requires (less flexible)
        """
        min_version = (3, 11)
        current_version = sys.version_info[:2]
        
        if current_version >= min_version:
            logger.info(f"Python version: {sys.version}")
            return True
        else:
            logger.error(f"Python {min_version[0]}.{min_version[1]}+ required, got {current_version[0]}.{current_version[1]}")
            return False
    
    def test_required_packages(self) -> bool:
        """
        Validate all required packages are installed
        
        What: Check package availability and versions
        Why: Prevent ImportError during pipeline execution
        How: Import each package and check version where critical
        Alternative: pip freeze comparison (less informative)
        """
        required_packages = [
            ('pandas', '2.0.0'),
            ('numpy', '1.24.0'),
            ('psycopg2', None),
            ('requests', None),
            ('python-dotenv', None),
            ('tqdm', None),
            ('seaborn', None),
            ('matplotlib', None),
            ('openpyxl', None),
            ('sqlalchemy', None)
        ]
        
        missing_packages = []
        version_issues = []
        
        for package_name, min_version in required_packages:
            try:
                module = importlib.import_module(package_name.replace('-', '_'))
                if min_version and hasattr(module, '__version__'):
                    current_version = module.__version__
                    if current_version < min_version:
                        version_issues.append(f"{package_name}: {current_version} < {min_version}")
                logger.info(f"✓ {package_name}: Available")
            except ImportError:
                missing_packages.append(package_name)
                logger.error(f"✗ {package_name}: Missing")
        
        if missing_packages:
            logger.error(f"Missing packages: {', '.join(missing_packages)}")
            return False
        
        if version_issues:
            logger.warning(f"Version issues: {', '.join(version_issues)}")
            return False
            
        return True
    
    def test_credentials_file(self) -> bool:
        """
        Validate credentials configuration
        
        What: Check credentials.json exists and has required fields
        Why: Prevent authentication failures during API calls
        How: JSON parsing and field validation
        Alternative: Environment variables (less organized)
        """
        credentials_path = self.root_path / 'credentials.json'
        
        if not credentials_path.exists():
            logger.error("credentials.json not found. Please create from template.")
            return False
        
        try:
            with open(credentials_path, 'r') as f:
                credentials = json.load(f)
            
            # Check required sections
            required_sections = ['zoho', 'database']
            for section in required_sections:
                if section not in credentials:
                    logger.error(f"Missing section in credentials.json: {section}")
                    return False
            
            # Check Zoho credentials
            zoho_fields = ['client_id', 'client_secret', 'refresh_token']
            for field in zoho_fields:
                if field not in credentials['zoho'] or not credentials['zoho'][field]:
                    logger.error(f"Missing or empty Zoho credential: {field}")
                    return False
                if 'YOUR_' in credentials['zoho'][field]:
                    logger.error(f"Zoho credential not configured: {field}")
                    return False
            
            # Check database credentials
            db_fields = ['host', 'port', 'database', 'username', 'password']
            for field in db_fields:
                if field not in credentials['database'] or not credentials['database'][field]:
                    logger.error(f"Missing or empty database credential: {field}")
                    return False
            
            logger.info("✓ credentials.json: Valid structure")
            return True
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in credentials.json: {e}")
            return False
    
    def test_directory_structure(self) -> bool:
        """
        Validate required directory structure
        
        What: Check all necessary directories exist
        Why: Prevent file I/O errors during pipeline execution
        How: Path existence checks with creation suggestions
        Alternative: Auto-creation (may hide configuration issues)
        """
        required_dirs = [
            'data',
            'chunk', 
            'notebooks',
            'src',
            'sql',
            'logs'
        ]
        
        missing_dirs = []
        for dir_name in required_dirs:
            dir_path = self.root_path / dir_name
            if not dir_path.exists():
                missing_dirs.append(dir_name)
                logger.error(f"✗ Directory missing: {dir_name}")
            else:
                logger.info(f"✓ Directory exists: {dir_name}")
        
        if missing_dirs:
            logger.error(f"Create missing directories: {', '.join(missing_dirs)}")
            return False
        
        return True
    
    def test_docker_availability(self) -> bool:
        """
        Validate Docker and Docker Compose availability
        
        What: Check Docker services are available
        Why: Database services depend on Docker
        How: Command execution and version checks
        Alternative: Native PostgreSQL installation (more complex)
        """
        try:
            # Test Docker
            result = subprocess.run(['docker', '--version'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                logger.error("Docker not available")
                return False
            logger.info(f"✓ Docker: {result.stdout.strip()}")
            
            # Test Docker Compose
            result = subprocess.run(['docker-compose', '--version'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                # Try docker compose (newer syntax)
                result = subprocess.run(['docker', 'compose', 'version'], 
                                      capture_output=True, text=True, timeout=10)
                if result.returncode != 0:
                    logger.error("Docker Compose not available")
                    return False
            logger.info(f"✓ Docker Compose: Available")
            
            return True
            
        except (subprocess.TimeoutExpired, FileNotFoundError):
            logger.error("Docker/Docker Compose not found in PATH")
            return False
    
    def test_database_connection(self) -> bool:
        """
        Validate database connectivity
        
        What: Test PostgreSQL connection using credentials
        Why: Ensure database is accessible before pipeline runs
        How: psycopg2 connection test
        Alternative: SQLAlchemy connection (more overhead)
        """
        try:
            import psycopg2
            
            # Load credentials
            credentials_path = self.root_path / 'credentials.json'
            if not credentials_path.exists():
                logger.error("credentials.json required for database test")
                return False
            
            with open(credentials_path, 'r') as f:
                credentials = json.load(f)
            
            db_config = credentials['database']
            
            # Test connection
            conn = psycopg2.connect(
                host=db_config['host'],
                port=db_config['port'],
                database=db_config['database'],
                user=db_config['username'],
                password=db_config['password']
            )
            
            # Test basic query
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            logger.info(f"✓ Database connection: {version[0]}")
            return True
            
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            logger.info("Tip: Ensure Docker containers are running: docker-compose up -d")
            return False
    
    def test_data_files(self) -> bool:
        """
        Validate required data files exist
        
        What: Check for essential data files
        Why: Pipeline needs input data to process
        How: File existence checks with size validation
        Alternative: Auto-download (not applicable for private data)
        """
        data_dir = self.root_path / 'data'
        
        # Check if data directory has any files
        if not data_dir.exists():
            logger.error("data/ directory not found")
            return False
        
        data_files = list(data_dir.glob('*.csv')) + list(data_dir.glob('*.CSV'))
        
        if not data_files:
            logger.warning("No CSV files found in data/ directory")
            logger.info("This is normal for initial setup - data files will be generated")
            return True
        
        logger.info(f"✓ Found {len(data_files)} data files")
        
        # Check file sizes
        for file_path in data_files:
            size_mb = file_path.stat().st_size / (1024 * 1024)
            logger.info(f"  - {file_path.name}: {size_mb:.2f} MB")
        
        return True
    
    def test_jupyter_environment(self) -> bool:
        """
        Validate Jupyter environment
        
        What: Check Jupyter installation and kernel availability
        Why: Notebooks are primary interface for data analysis
        How: Command execution and kernel list check
        Alternative: JupyterLab (more features, similar testing)
        """
        try:
            # Test Jupyter installation
            result = subprocess.run(['jupyter', '--version'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                logger.error("Jupyter not available")
                return False
            
            logger.info("✓ Jupyter: Available")
            
            # Test kernel list
            result = subprocess.run(['jupyter', 'kernelspec', 'list'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                logger.info("✓ Jupyter kernels: Available")
            else:
                logger.warning("Jupyter kernels list failed")
            
            return True
            
        except (subprocess.TimeoutExpired, FileNotFoundError):
            logger.error("Jupyter not found in PATH")
            return False
    
    def generate_report(self) -> None:
        """
        Generate comprehensive test report
        
        What: Create detailed test results summary
        Why: Provide clear status overview and next steps
        How: Formatted report with recommendations
        Alternative: JSON report (less human-readable)
        """
        duration = datetime.now() - self.start_time
        
        print("\n" + "="*60)
        print("🧪 CONFIGURATION TEST REPORT")
        print("="*60)
        print(f"Test Duration: {duration.total_seconds():.2f} seconds")
        print(f"Total Tests: {len(self.test_results)}")
        
        passed = sum(1 for _, status, _ in self.test_results if status == "PASSED")
        failed = sum(1 for _, status, _ in self.test_results if status == "FAILED")
        errors = sum(1 for _, status, _ in self.test_results if status == "ERROR")
        
        print(f"✅ Passed: {passed}")
        print(f"❌ Failed: {failed}")
        print(f"🔥 Errors: {errors}")
        
        if failed > 0 or errors > 0:
            print("\n🔍 ISSUES FOUND:")
            for test_name, status, error in self.test_results:
                if status in ["FAILED", "ERROR"]:
                    print(f"  - {test_name}: {status}")
                    if error:
                        print(f"    {error}")
        
        print("\n📋 DETAILED RESULTS:")
        for test_name, status, error in self.test_results:
            status_icon = "✅" if status == "PASSED" else "❌" if status == "FAILED" else "🔥"
            print(f"  {status_icon} {test_name}: {status}")
        
        if passed == len(self.test_results):
            print("\n🎉 ALL TESTS PASSED!")
            print("Your environment is ready for the email analytics pipeline!")
            print("\nNext steps:")
            print("1. Run: jupyter lab")
            print("2. Open: automated_zoho_email_retrieval_pipeline.ipynb")
            print("3. Follow the notebook sequence")
        else:
            print("\n⚠️  SETUP INCOMPLETE")
            print("Please resolve the issues above before running the pipeline.")
            print("Refer to SETUP_GUIDE.md for detailed instructions.")
        
        print("="*60)

def main():
    """
    Main test execution function
    
    What: Run all configuration tests
    Why: Validate system readiness
    How: Sequential test execution with reporting
    Alternative: Parallel testing (may cause resource conflicts)
    """
    print("🚀 Starting Configuration Tests...")
    print("This will validate your environment setup.\n")
    
    tester = ConfigurationTester()
    
    # Define test suite
    tests = [
        ("Python Version", tester.test_python_version),
        ("Required Packages", tester.test_required_packages),
        ("Credentials Configuration", tester.test_credentials_file),
        ("Directory Structure", tester.test_directory_structure),
        ("Docker Availability", tester.test_docker_availability),
        ("Database Connection", tester.test_database_connection),
        ("Data Files", tester.test_data_files),
        ("Jupyter Environment", tester.test_jupyter_environment),
    ]
    
    # Run all tests
    for test_name, test_func in tests:
        tester.run_test(test_name, test_func)
    
    # Generate final report
    tester.generate_report()

if __name__ == "__main__":
    main() 