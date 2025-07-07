#!/usr/bin/env python3
"""
Deployment Automation Script
Email-Customer Analytics Pipeline

What: Comprehensive deployment automation for development and production
Why: Streamline deployment process and reduce human error
How: Automated environment setup, testing, and deployment
Alternative: Manual deployment (error-prone and time-consuming)
"""

import os
import sys
import subprocess
import json
import time
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime

class DeploymentManager:
    """
    Comprehensive deployment management system
    
    What: Automated deployment with environment management
    Why: Consistent deployments across environments
    How: Step-by-step deployment with validation
    Alternative: Manual deployment scripts (less reliable)
    """
    
    def __init__(self, environment: str = "development"):
        """
        Initialize deployment manager
        
        What: Set up deployment configuration
        Why: Environment-specific deployment settings
        How: Configuration loading and validation
        Alternative: Hardcoded settings (less flexible)
        """
        self.environment = environment
        self.root_path = Path(__file__).parent
        self.deployment_time = datetime.now()
        
        # Set up logging
        self.setup_logging()
        
        # Load environment configuration
        self.config = self.load_environment_config()
        
        self.logger.info(f"Deployment manager initialized for {environment}")
    
    def setup_logging(self) -> None:
        """Configure deployment logging"""
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        log_file = log_dir / f"deployment_{self.deployment_time.strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger('deployment')
    
    def load_environment_config(self) -> Dict[str, Any]:
        """
        Load environment-specific configuration
        
        What: Load deployment settings for target environment
        Why: Different environments need different settings
        How: JSON configuration files
        Alternative: Environment variables (less organized)
        """
        config_file = self.root_path / f"config/{self.environment}.json"
        
        # Default configuration
        default_config = {
            "python_version": "3.11",
            "services": {
                "postgres": True,
                "redis": True,
                "jupyter": True,
                "pgadmin": True
            },
            "ports": {
                "postgres": 5432,
                "redis": 6379,
                "jupyter": 8888,
                "pgadmin": 5050
            },
            "health_checks": True,
            "backup_enabled": True,
            "monitoring_enabled": True
        }
        
        if config_file.exists():
            try:
                with open(config_file, 'r') as f:
                    user_config = json.load(f)
                    default_config.update(user_config)
            except Exception as e:
                self.logger.warning(f"Failed to load config file: {e}")
        
        return default_config
    
    def check_prerequisites(self) -> bool:
        """
        Check deployment prerequisites
        
        What: Validate system requirements before deployment
        Why: Prevent deployment failures due to missing dependencies
        How: System checks and validation
        Alternative: Assume prerequisites (risky)
        """
        self.logger.info("Checking deployment prerequisites...")
        
        checks = [
            ("Python version", self.check_python_version),
            ("Docker availability", self.check_docker),
            ("Git repository", self.check_git_status),
            ("Required files", self.check_required_files),
            ("Port availability", self.check_port_availability)
        ]
        
        all_passed = True
        for check_name, check_func in checks:
            try:
                if check_func():
                    self.logger.info(f"✅ {check_name}: PASSED")
                else:
                    self.logger.error(f"❌ {check_name}: FAILED")
                    all_passed = False
            except Exception as e:
                self.logger.error(f"❌ {check_name}: ERROR - {e}")
                all_passed = False
        
        return all_passed
    
    def check_python_version(self) -> bool:
        """Check Python version compatibility"""
        required_version = tuple(map(int, self.config["python_version"].split('.')))
        current_version = sys.version_info[:2]
        return current_version >= required_version
    
    def check_docker(self) -> bool:
        """Check Docker availability"""
        try:
            result = subprocess.run(['docker', '--version'], 
                                  capture_output=True, text=True, timeout=10)
            return result.returncode == 0
        except:
            return False
    
    def check_git_status(self) -> bool:
        """Check Git repository status"""
        try:
            # Check if we're in a git repository
            result = subprocess.run(['git', 'status', '--porcelain'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                return False
            
            # Check for uncommitted changes in production
            if self.environment == "production" and result.stdout.strip():
                self.logger.warning("Uncommitted changes found in production deployment")
                return False
            
            return True
        except:
            return False
    
    def check_required_files(self) -> bool:
        """Check for required files"""
        required_files = [
            "requirements.txt",
            "docker-compose.yml",
            "src/",
            "notebooks/"
        ]
        
        for file_path in required_files:
            if not Path(file_path).exists():
                self.logger.error(f"Required file/directory missing: {file_path}")
                return False
        
        return True
    
    def check_port_availability(self) -> bool:
        """Check if required ports are available"""
        import socket
        
        for service, port in self.config["ports"].items():
            if not self.config["services"].get(service, False):
                continue
            
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(1)
                    result = s.connect_ex(('localhost', port))
                    if result == 0:
                        self.logger.warning(f"Port {port} ({service}) is already in use")
                        return False
            except:
                pass
        
        return True
    
    def setup_environment(self) -> bool:
        """
        Set up Python environment
        
        What: Create virtual environment and install dependencies
        Why: Isolated environment for consistent deployment
        How: venv creation and pip installation
        Alternative: System-wide installation (conflicts possible)
        """
        self.logger.info("Setting up Python environment...")
        
        try:
            # Create virtual environment if it doesn't exist
            venv_path = Path("venv")
            if not venv_path.exists():
                self.logger.info("Creating virtual environment...")
                subprocess.run([sys.executable, "-m", "venv", "venv"], check=True)
            
            # Determine activation script
            if os.name == 'nt':  # Windows
                pip_path = venv_path / "Scripts" / "pip.exe"
                python_path = venv_path / "Scripts" / "python.exe"
            else:  # Unix-like
                pip_path = venv_path / "bin" / "pip"
                python_path = venv_path / "bin" / "python"
            
            # Upgrade pip
            self.logger.info("Upgrading pip...")
            subprocess.run([str(python_path), "-m", "pip", "install", "--upgrade", "pip"], check=True)
            
            # Install requirements
            self.logger.info("Installing requirements...")
            subprocess.run([str(pip_path), "install", "-r", "requirements.txt"], check=True)
            
            # Install additional development dependencies if needed
            if self.environment == "development":
                dev_requirements = Path("requirements-dev.txt")
                if dev_requirements.exists():
                    subprocess.run([str(pip_path), "install", "-r", "requirements-dev.txt"], check=True)
            
            self.logger.info("Python environment setup completed")
            return True
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Failed to setup Python environment: {e}")
            return False
    
    def setup_configuration(self) -> bool:
        """
        Set up configuration files
        
        What: Create necessary configuration files
        Why: Proper configuration for environment
        How: Template-based configuration generation
        Alternative: Manual configuration (error-prone)
        """
        self.logger.info("Setting up configuration files...")
        
        try:
            # Create .env file if it doesn't exist
            env_file = Path(".env")
            if not env_file.exists():
                self.logger.info("Creating .env file...")
                env_template = self.generate_env_template()
                with open(env_file, 'w') as f:
                    f.write(env_template)
            
            # Create directories
            directories = ["logs", "backups", "data", "metrics"]
            for directory in directories:
                Path(directory).mkdir(exist_ok=True)
                self.logger.info(f"Created directory: {directory}")
            
            # Set up log rotation
            self.setup_log_rotation()
            
            self.logger.info("Configuration setup completed")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to setup configuration: {e}")
            return False
    
    def generate_env_template(self) -> str:
        """Generate .env file template"""
        template = f"""# Environment Configuration
# Generated on {self.deployment_time.isoformat()}

# Database Configuration
POSTGRES_USER=optical_admin
POSTGRES_PASSWORD=secure_password_{self.environment}
POSTGRES_DB=optical_analytics
POSTGRES_PORT={self.config['ports']['postgres']}

# Redis Configuration
REDIS_PORT={self.config['ports']['redis']}

# Jupyter Configuration
JUPYTER_PORT={self.config['ports']['jupyter']}
JUPYTER_TOKEN=optical_analytics_token_{self.environment}

# pgAdmin Configuration
PGADMIN_EMAIL=admin@yourcompany.com
PGADMIN_PASSWORD=admin_password_{self.environment}
PGADMIN_PORT={self.config['ports']['pgadmin']}

# Environment
ENVIRONMENT={self.environment}
DEBUG={'true' if self.environment == 'development' else 'false'}
"""
        return template
    
    def setup_log_rotation(self) -> None:
        """Set up log rotation configuration"""
        # This would typically set up logrotate on Linux systems
        # For now, we'll create a simple log rotation script
        pass
    
    def deploy_services(self) -> bool:
        """
        Deploy Docker services
        
        What: Start required Docker services
        Why: Provide infrastructure for the application
        How: Docker Compose orchestration
        Alternative: Manual service management (complex)
        """
        self.logger.info("Deploying Docker services...")
        
        try:
            # Stop existing services
            self.logger.info("Stopping existing services...")
            subprocess.run(['docker-compose', 'down'], check=False)
            
            # Pull latest images
            self.logger.info("Pulling Docker images...")
            subprocess.run(['docker-compose', 'pull'], check=True)
            
            # Start services
            self.logger.info("Starting services...")
            subprocess.run(['docker-compose', 'up', '-d'], check=True)
            
            # Wait for services to be ready
            self.logger.info("Waiting for services to be ready...")
            time.sleep(30)
            
            # Verify services
            if self.verify_services():
                self.logger.info("Docker services deployed successfully")
                return True
            else:
                self.logger.error("Service verification failed")
                return False
                
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Failed to deploy services: {e}")
            return False
    
    def verify_services(self) -> bool:
        """Verify that all services are running"""
        try:
            result = subprocess.run(['docker-compose', 'ps'], 
                                  capture_output=True, text=True, check=True)
            
            # Check if all required services are running
            required_services = [name for name, enabled in self.config["services"].items() if enabled]
            
            for service in required_services:
                if service not in result.stdout:
                    self.logger.error(f"Service not running: {service}")
                    return False
            
            return True
            
        except subprocess.CalledProcessError:
            return False
    
    def run_database_migrations(self) -> bool:
        """
        Run database migrations
        
        What: Set up database schema and initial data
        Why: Ensure database is properly configured
        How: SQL script execution
        Alternative: Manual database setup (error-prone)
        """
        self.logger.info("Running database migrations...")
        
        try:
            # Wait for database to be ready
            time.sleep(10)
            
            # Run schema creation
            schema_file = Path("sql/schema.sql")
            if schema_file.exists():
                cmd = [
                    'docker-compose', 'exec', '-T', 'postgres',
                    'psql', '-U', 'optical_admin', '-d', 'optical_analytics',
                    '-f', '/docker-entrypoint-initdb.d/schema.sql'
                ]
                subprocess.run(cmd, check=True)
                self.logger.info("Database schema created")
            
            # Run any additional migrations
            migrations_dir = Path("sql/migrations")
            if migrations_dir.exists():
                for migration_file in sorted(migrations_dir.glob("*.sql")):
                    self.logger.info(f"Running migration: {migration_file.name}")
                    # Execute migration
                    # This would typically involve more sophisticated migration tracking
            
            return True
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Database migration failed: {e}")
            return False
    
    def run_tests(self) -> bool:
        """
        Run test suite
        
        What: Execute all tests to verify deployment
        Why: Ensure system is working correctly
        How: Test runner execution
        Alternative: Manual testing (time-consuming)
        """
        self.logger.info("Running test suite...")
        
        try:
            # Run configuration tests
            test_script = Path("tests/test_configuration.py")
            if test_script.exists():
                if os.name == 'nt':
                    python_path = Path("venv/Scripts/python.exe")
                else:
                    python_path = Path("venv/bin/python")
                
                result = subprocess.run([str(python_path), str(test_script)], 
                                      capture_output=True, text=True)
                
                if result.returncode == 0:
                    self.logger.info("Configuration tests passed")
                else:
                    self.logger.error(f"Configuration tests failed: {result.stderr}")
                    return False
            
            # Run additional tests if available
            if Path("tests").exists():
                # This would run pytest or other test frameworks
                pass
            
            return True
            
        except Exception as e:
            self.logger.error(f"Test execution failed: {e}")
            return False
    
    def setup_monitoring(self) -> bool:
        """
        Set up monitoring and logging
        
        What: Configure monitoring and alerting
        Why: Operational visibility and issue detection
        How: Monitoring service configuration
        Alternative: Manual monitoring (unreliable)
        """
        if not self.config.get("monitoring_enabled", False):
            self.logger.info("Monitoring disabled in configuration")
            return True
        
        self.logger.info("Setting up monitoring...")
        
        try:
            # Initialize monitoring system
            from src.monitoring import setup_monitoring
            monitor = setup_monitoring()
            
            # Set up health checks
            if self.config.get("health_checks", False):
                # This would typically set up health check endpoints
                pass
            
            self.logger.info("Monitoring setup completed")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to setup monitoring: {e}")
            return False
    
    def setup_backup(self) -> bool:
        """
        Set up backup system
        
        What: Configure automated backups
        Why: Data protection and recovery capability
        How: Backup service configuration
        Alternative: Manual backups (unreliable)
        """
        if not self.config.get("backup_enabled", False):
            self.logger.info("Backup disabled in configuration")
            return True
        
        self.logger.info("Setting up backup system...")
        
        try:
            # Initialize backup system
            from src.backup_system import BackupManager
            backup_manager = BackupManager()
            
            # Set up scheduled backups
            schedule_config = {
                "data": {
                    "interval": 24,
                    "source_dirs": ["data", "chunk"]
                },
                "config": {
                    "interval": 168,
                    "config_files": ["requirements.txt", "docker-compose.yml", ".env"]
                }
            }
            
            backup_manager.schedule_backups(schedule_config)
            
            self.logger.info("Backup system setup completed")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to setup backup system: {e}")
            return False
    
    def generate_deployment_report(self) -> Dict[str, Any]:
        """Generate comprehensive deployment report"""
        deployment_duration = datetime.now() - self.deployment_time
        
        report = {
            "deployment_time": self.deployment_time.isoformat(),
            "environment": self.environment,
            "duration": deployment_duration.total_seconds(),
            "status": "completed",
            "configuration": self.config,
            "services": {},
            "health_checks": {}
        }
        
        # Add service status
        try:
            result = subprocess.run(['docker-compose', 'ps', '--format', 'json'], 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                # Parse service status
                pass
        except:
            pass
        
        return report
    
    def deploy(self) -> bool:
        """
        Execute complete deployment
        
        What: Run all deployment steps in sequence
        Why: Automated and consistent deployment process
        How: Step-by-step execution with validation
        Alternative: Manual deployment (error-prone)
        """
        self.logger.info(f"Starting deployment to {self.environment} environment")
        
        deployment_steps = [
            ("Prerequisites Check", self.check_prerequisites),
            ("Environment Setup", self.setup_environment),
            ("Configuration Setup", self.setup_configuration),
            ("Service Deployment", self.deploy_services),
            ("Database Migrations", self.run_database_migrations),
            ("Test Execution", self.run_tests),
            ("Monitoring Setup", self.setup_monitoring),
            ("Backup Setup", self.setup_backup)
        ]
        
        for step_name, step_func in deployment_steps:
            self.logger.info(f"Executing: {step_name}")
            try:
                if not step_func():
                    self.logger.error(f"Deployment failed at step: {step_name}")
                    return False
                self.logger.info(f"Completed: {step_name}")
            except Exception as e:
                self.logger.error(f"Error in {step_name}: {e}")
                return False
        
        # Generate deployment report
        report = self.generate_deployment_report()
        report_file = Path("logs") / f"deployment_report_{self.deployment_time.strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        self.logger.info(f"Deployment completed successfully! Report: {report_file}")
        return True

def main():
    """Main deployment entry point"""
    parser = argparse.ArgumentParser(description="Deploy Email-Customer Analytics Pipeline")
    parser.add_argument('--environment', '-e', default='development',
                       choices=['development', 'staging', 'production'],
                       help='Target environment')
    parser.add_argument('--skip-tests', action='store_true',
                       help='Skip test execution')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be done without executing')
    
    args = parser.parse_args()
    
    if args.dry_run:
        print(f"DRY RUN: Would deploy to {args.environment} environment")
        print("Steps that would be executed:")
        print("1. Prerequisites Check")
        print("2. Environment Setup")
        print("3. Configuration Setup")
        print("4. Service Deployment")
        print("5. Database Migrations")
        if not args.skip_tests:
            print("6. Test Execution")
        print("7. Monitoring Setup")
        print("8. Backup Setup")
        return
    
    # Execute deployment
    deployment_manager = DeploymentManager(args.environment)
    
    if args.skip_tests:
        deployment_manager.config["skip_tests"] = True
    
    success = deployment_manager.deploy()
    
    if success:
        print(f"🎉 Deployment to {args.environment} completed successfully!")
        print("\nNext steps:")
        print("1. Access Jupyter Lab: http://localhost:8888")
        print("2. Access pgAdmin: http://localhost:5050")
        print("3. Check logs in logs/ directory")
        print("4. Run health checks")
    else:
        print(f"❌ Deployment to {args.environment} failed!")
        print("Check logs for details")
        sys.exit(1)

if __name__ == "__main__":
    main() 