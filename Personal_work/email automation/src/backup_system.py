#!/usr/bin/env python3
"""
Automated Backup System
Email-Customer Analytics Pipeline

What: Comprehensive backup and recovery system for data and configurations
Why: Prevent data loss and ensure business continuity
How: Automated backups with versioning and compression
Alternative: Manual backups (unreliable and time-consuming)
"""

import os
import shutil
import gzip
import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
import logging
import subprocess
import tarfile
import threading
import schedule
import time

class BackupManager:
    """
    Comprehensive backup management system
    
    What: Automated backup creation, scheduling, and management
    Why: Protect against data loss and enable quick recovery
    How: File-based backups with compression and verification
    Alternative: Cloud backup services (more expensive, vendor lock-in)
    """
    
    def __init__(self, backup_dir: str = "backups", retention_days: int = 30):
        """
        Initialize backup manager
        
        What: Set up backup configuration and directories
        Why: Centralized backup management
        How: Directory structure and configuration setup
        Alternative: Configuration file (more complex)
        """
        self.backup_dir = Path(backup_dir)
        self.retention_days = retention_days
        self.backup_dir.mkdir(exist_ok=True)
        
        # Create subdirectories
        (self.backup_dir / "data").mkdir(exist_ok=True)
        (self.backup_dir / "config").mkdir(exist_ok=True)
        (self.backup_dir / "database").mkdir(exist_ok=True)
        (self.backup_dir / "logs").mkdir(exist_ok=True)
        
        # Set up logging
        self.logger = logging.getLogger('backup_manager')
        self.logger.setLevel(logging.INFO)
        
        # Create backup log
        backup_log = self.backup_dir / "backup.log"
        handler = logging.FileHandler(backup_log)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        
        self.logger.info("Backup manager initialized")
    
    def create_data_backup(self, source_dirs: List[str], backup_name: str = None) -> str:
        """
        Create compressed backup of data directories
        
        What: Archive and compress data directories
        Why: Preserve data integrity and save storage space
        How: Tar compression with verification
        Alternative: Simple copy (larger storage, no compression)
        """
        if backup_name is None:
            backup_name = f"data_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        backup_path = self.backup_dir / "data" / f"{backup_name}.tar.gz"
        
        try:
            self.logger.info(f"Creating data backup: {backup_name}")
            
            with tarfile.open(backup_path, "w:gz") as tar:
                for source_dir in source_dirs:
                    source_path = Path(source_dir)
                    if source_path.exists():
                        tar.add(source_path, arcname=source_path.name)
                        self.logger.info(f"Added to backup: {source_dir}")
                    else:
                        self.logger.warning(f"Source directory not found: {source_dir}")
            
            # Verify backup
            if self._verify_backup(backup_path):
                self.logger.info(f"Data backup created successfully: {backup_path}")
                return str(backup_path)
            else:
                self.logger.error(f"Backup verification failed: {backup_path}")
                return None
                
        except Exception as e:
            self.logger.error(f"Failed to create data backup: {e}")
            return None
    
    def create_config_backup(self, config_files: List[str], backup_name: str = None) -> str:
        """
        Create backup of configuration files
        
        What: Archive configuration files and settings
        Why: Preserve system configuration for recovery
        How: Selective file backup with metadata
        Alternative: Git versioning (requires repository setup)
        """
        if backup_name is None:
            backup_name = f"config_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        backup_path = self.backup_dir / "config" / f"{backup_name}.tar.gz"
        
        try:
            self.logger.info(f"Creating config backup: {backup_name}")
            
            with tarfile.open(backup_path, "w:gz") as tar:
                for config_file in config_files:
                    config_path = Path(config_file)
                    if config_path.exists():
                        tar.add(config_path, arcname=config_path.name)
                        self.logger.info(f"Added to backup: {config_file}")
                    else:
                        self.logger.warning(f"Config file not found: {config_file}")
            
            # Create metadata
            metadata = {
                "backup_name": backup_name,
                "created_at": datetime.now().isoformat(),
                "files": config_files,
                "backup_size": backup_path.stat().st_size
            }
            
            metadata_path = backup_path.with_suffix('.json')
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            self.logger.info(f"Config backup created successfully: {backup_path}")
            return str(backup_path)
            
        except Exception as e:
            self.logger.error(f"Failed to create config backup: {e}")
            return None
    
    def create_database_backup(self, db_config: Dict[str, str], backup_name: str = None) -> str:
        """
        Create database backup using pg_dump
        
        What: Export database to SQL dump file
        Why: Preserve database structure and data
        How: pg_dump with compression
        Alternative: Database replication (more complex)
        """
        if backup_name is None:
            backup_name = f"db_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        backup_path = self.backup_dir / "database" / f"{backup_name}.sql.gz"
        
        try:
            self.logger.info(f"Creating database backup: {backup_name}")
            
            # Build pg_dump command
            cmd = [
                'pg_dump',
                '-h', db_config['host'],
                '-p', str(db_config['port']),
                '-U', db_config['username'],
                '-d', db_config['database'],
                '--no-password',
                '--verbose'
            ]
            
            # Set password environment variable
            env = os.environ.copy()
            env['PGPASSWORD'] = db_config['password']
            
            # Execute pg_dump and compress
            with gzip.open(backup_path, 'wt') as f:
                result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, 
                                      env=env, text=True)
            
            if result.returncode == 0:
                self.logger.info(f"Database backup created successfully: {backup_path}")
                return str(backup_path)
            else:
                self.logger.error(f"Database backup failed: {result.stderr}")
                return None
                
        except Exception as e:
            self.logger.error(f"Failed to create database backup: {e}")
            return None
    
    def _verify_backup(self, backup_path: Path) -> bool:
        """
        Verify backup integrity
        
        What: Check if backup file is valid and complete
        Why: Ensure backup can be restored successfully
        How: File integrity checks and archive validation
        Alternative: Checksum verification (less comprehensive)
        """
        try:
            if not backup_path.exists():
                return False
            
            # Check file size
            if backup_path.stat().st_size == 0:
                return False
            
            # Verify archive integrity
            if backup_path.suffix == '.gz':
                if backup_path.name.endswith('.tar.gz'):
                    with tarfile.open(backup_path, "r:gz") as tar:
                        tar.getmembers()  # This will raise exception if corrupted
                else:
                    with gzip.open(backup_path, 'rt') as f:
                        f.read(1)  # Try to read first byte
            
            return True
            
        except Exception as e:
            self.logger.error(f"Backup verification failed: {e}")
            return False
    
    def list_backups(self, backup_type: str = None) -> List[Dict[str, Any]]:
        """
        List available backups
        
        What: Enumerate all backup files with metadata
        Why: Provide visibility into available backups
        How: Directory scanning with file analysis
        Alternative: Database catalog (more complex)
        """
        backups = []
        
        backup_types = ['data', 'config', 'database'] if backup_type is None else [backup_type]
        
        for btype in backup_types:
            backup_subdir = self.backup_dir / btype
            if not backup_subdir.exists():
                continue
            
            for backup_file in backup_subdir.glob("*.tar.gz"):
                try:
                    stat = backup_file.stat()
                    backup_info = {
                        "name": backup_file.stem,
                        "type": btype,
                        "path": str(backup_file),
                        "size": stat.st_size,
                        "created": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                        "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
                    }
                    
                    # Add metadata if available
                    metadata_file = backup_file.with_suffix('.json')
                    if metadata_file.exists():
                        with open(metadata_file, 'r') as f:
                            backup_info["metadata"] = json.load(f)
                    
                    backups.append(backup_info)
                    
                except Exception as e:
                    self.logger.error(f"Failed to read backup info: {backup_file} - {e}")
        
        return sorted(backups, key=lambda x: x['created'], reverse=True)
    
    def restore_backup(self, backup_path: str, restore_dir: str) -> bool:
        """
        Restore backup to specified directory
        
        What: Extract backup archive to target location
        Why: Enable recovery from backup
        How: Archive extraction with verification
        Alternative: Manual extraction (error-prone)
        """
        try:
            backup_file = Path(backup_path)
            restore_path = Path(restore_dir)
            
            if not backup_file.exists():
                self.logger.error(f"Backup file not found: {backup_path}")
                return False
            
            # Verify backup before restore
            if not self._verify_backup(backup_file):
                self.logger.error(f"Backup verification failed: {backup_path}")
                return False
            
            # Create restore directory
            restore_path.mkdir(parents=True, exist_ok=True)
            
            self.logger.info(f"Restoring backup: {backup_path} to {restore_dir}")
            
            # Extract archive
            with tarfile.open(backup_file, "r:gz") as tar:
                tar.extractall(restore_path)
            
            self.logger.info(f"Backup restored successfully to: {restore_dir}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to restore backup: {e}")
            return False
    
    def cleanup_old_backups(self) -> None:
        """
        Remove old backups based on retention policy
        
        What: Delete backups older than retention period
        Why: Manage storage space and maintain clean backup directory
        How: Age-based file deletion
        Alternative: Size-based cleanup (less predictable)
        """
        cutoff_date = datetime.now() - timedelta(days=self.retention_days)
        
        for backup_type in ['data', 'config', 'database']:
            backup_subdir = self.backup_dir / backup_type
            if not backup_subdir.exists():
                continue
            
            for backup_file in backup_subdir.glob("*"):
                try:
                    file_date = datetime.fromtimestamp(backup_file.stat().st_mtime)
                    if file_date < cutoff_date:
                        backup_file.unlink()
                        self.logger.info(f"Deleted old backup: {backup_file}")
                        
                        # Delete associated metadata
                        metadata_file = backup_file.with_suffix('.json')
                        if metadata_file.exists():
                            metadata_file.unlink()
                            
                except Exception as e:
                    self.logger.error(f"Failed to delete old backup: {backup_file} - {e}")
    
    def schedule_backups(self, schedule_config: Dict[str, Any]) -> None:
        """
        Schedule automated backups
        
        What: Set up recurring backup jobs
        Why: Ensure regular backups without manual intervention
        How: Schedule library with threading
        Alternative: Cron jobs (OS-specific)
        """
        self.logger.info("Setting up backup schedule")
        
        # Data backup schedule
        if 'data' in schedule_config:
            data_config = schedule_config['data']
            schedule.every(data_config.get('interval', 24)).hours.do(
                self._scheduled_data_backup, data_config.get('source_dirs', [])
            )
        
        # Config backup schedule
        if 'config' in schedule_config:
            config_config = schedule_config['config']
            schedule.every(config_config.get('interval', 168)).hours.do(  # Weekly
                self._scheduled_config_backup, config_config.get('config_files', [])
            )
        
        # Database backup schedule
        if 'database' in schedule_config:
            db_config = schedule_config['database']
            schedule.every(db_config.get('interval', 12)).hours.do(
                self._scheduled_database_backup, db_config.get('db_config', {})
            )
        
        # Cleanup schedule
        schedule.every(7).days.do(self.cleanup_old_backups)
        
        # Start scheduler thread
        self.scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.scheduler_thread.start()
        
        self.logger.info("Backup scheduler started")
    
    def _scheduled_data_backup(self, source_dirs: List[str]) -> None:
        """Scheduled data backup execution"""
        self.create_data_backup(source_dirs)
    
    def _scheduled_config_backup(self, config_files: List[str]) -> None:
        """Scheduled config backup execution"""
        self.create_config_backup(config_files)
    
    def _scheduled_database_backup(self, db_config: Dict[str, str]) -> None:
        """Scheduled database backup execution"""
        self.create_database_backup(db_config)
    
    def _run_scheduler(self) -> None:
        """Run backup scheduler in background thread"""
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    
    def generate_backup_report(self) -> Dict[str, Any]:
        """
        Generate comprehensive backup report
        
        What: Analyze backup status and provide recommendations
        Why: Visibility into backup health and compliance
        How: Backup analysis with metrics
        Alternative: Manual backup auditing (time-consuming)
        """
        report = {
            "generated_at": datetime.now().isoformat(),
            "retention_days": self.retention_days,
            "backup_directory": str(self.backup_dir),
            "summary": {},
            "backups": {},
            "recommendations": []
        }
        
        # Analyze backups by type
        for backup_type in ['data', 'config', 'database']:
            backups = self.list_backups(backup_type)
            
            if backups:
                total_size = sum(b['size'] for b in backups)
                latest_backup = max(backups, key=lambda x: x['created'])
                oldest_backup = min(backups, key=lambda x: x['created'])
                
                report["backups"][backup_type] = {
                    "count": len(backups),
                    "total_size": total_size,
                    "latest_backup": latest_backup['created'],
                    "oldest_backup": oldest_backup['created'],
                    "backups": backups
                }
                
                # Generate recommendations
                latest_date = datetime.fromisoformat(latest_backup['created'])
                days_since_last = (datetime.now() - latest_date).days
                
                if days_since_last > 7:
                    report["recommendations"].append(
                        f"No recent {backup_type} backup - last backup was {days_since_last} days ago"
                    )
                
                if len(backups) < 3:
                    report["recommendations"].append(
                        f"Few {backup_type} backups available - consider more frequent backups"
                    )
            else:
                report["backups"][backup_type] = {
                    "count": 0,
                    "total_size": 0,
                    "backups": []
                }
                report["recommendations"].append(f"No {backup_type} backups found")
        
        # Calculate summary
        total_backups = sum(b["count"] for b in report["backups"].values())
        total_size = sum(b["total_size"] for b in report["backups"].values())
        
        report["summary"] = {
            "total_backups": total_backups,
            "total_size": total_size,
            "total_size_mb": total_size / (1024 * 1024)
        }
        
        return report

def main():
    """
    Demo usage of backup system
    
    What: Demonstrate backup system capabilities
    Why: Show how to use the backup system
    How: Example operations with different backup types
    Alternative: Unit tests (more formal testing)
    """
    # Initialize backup manager
    backup_manager = BackupManager()
    
    # Example: Create data backup
    data_dirs = ["data", "chunk"]
    backup_path = backup_manager.create_data_backup(data_dirs)
    if backup_path:
        print(f"Data backup created: {backup_path}")
    
    # Example: Create config backup
    config_files = ["requirements.txt", "docker-compose.yml"]
    config_backup = backup_manager.create_config_backup(config_files)
    if config_backup:
        print(f"Config backup created: {config_backup}")
    
    # List all backups
    backups = backup_manager.list_backups()
    print(f"Available backups: {len(backups)}")
    
    # Generate backup report
    report = backup_manager.generate_backup_report()
    print(f"Backup report: {json.dumps(report, indent=2)}")
    
    # Schedule automated backups
    schedule_config = {
        "data": {
            "interval": 24,  # Every 24 hours
            "source_dirs": ["data", "chunk"]
        },
        "config": {
            "interval": 168,  # Weekly
            "config_files": ["requirements.txt", "docker-compose.yml"]
        }
    }
    
    backup_manager.schedule_backups(schedule_config)
    print("Automated backup scheduling configured")

if __name__ == "__main__":
    main() 