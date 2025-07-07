#!/usr/bin/env python3
"""
Monitoring and Logging System
Email-Customer Analytics Pipeline

What: Comprehensive monitoring, logging, and performance tracking system
Why: Essential for production deployment, debugging, and performance optimization
How: Structured logging, metrics collection, and health checks
Alternative: External monitoring tools (Prometheus, Grafana) - more complex setup
"""

import os
import sys
import json
import time
import logging
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from contextlib import contextmanager
import psutil
import pandas as pd

@dataclass
class MetricData:
    """
    Structured metric data container
    
    What: Standardized metric storage format
    Why: Consistent metric tracking across pipeline components
    How: Dataclass with serialization support
    Alternative: Dictionary (less type safety)
    """
    timestamp: datetime
    component: str
    metric_type: str
    value: float
    unit: str
    metadata: Dict[str, Any] = None

class PipelineMonitor:
    """
    Comprehensive pipeline monitoring system
    
    What: Central monitoring hub for all pipeline components
    Why: Provides visibility into system performance and health
    How: Metrics collection, logging, and alerting
    Alternative: Manual monitoring (unreliable and time-consuming)
    """
    
    def __init__(self, log_dir: str = "logs", metrics_dir: str = "metrics"):
        """
        Initialize monitoring system
        
        What: Set up logging and metrics collection
        Why: Centralized configuration for all monitoring
        How: Directory creation and logger configuration
        Alternative: Environment-based configuration (less flexible)
        """
        self.log_dir = Path(log_dir)
        self.metrics_dir = Path(metrics_dir)
        self.start_time = datetime.now()
        self.metrics_buffer = []
        self.performance_data = {}
        
        # Create directories
        self.log_dir.mkdir(exist_ok=True)
        self.metrics_dir.mkdir(exist_ok=True)
        
        # Configure logging
        self._setup_logging()
        
        # Initialize system monitoring
        self._initialize_system_monitoring()
        
        self.logger.info("Pipeline monitoring system initialized")
    
    def _setup_logging(self) -> None:
        """
        Configure comprehensive logging system
        
        What: Multi-level logging with file and console output
        Why: Detailed debugging and audit trail
        How: Python logging with custom formatters
        Alternative: External logging services (more complex)
        """
        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        simple_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # Configure main logger
        self.logger = logging.getLogger('pipeline_monitor')
        self.logger.setLevel(logging.DEBUG)
        
        # File handler for detailed logs
        file_handler = logging.FileHandler(
            self.log_dir / f'pipeline_{datetime.now().strftime("%Y%m%d")}.log'
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(detailed_formatter)
        
        # Console handler for important messages
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(simple_formatter)
        
        # Error file handler
        error_handler = logging.FileHandler(
            self.log_dir / f'errors_{datetime.now().strftime("%Y%m%d")}.log'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(detailed_formatter)
        
        # Add handlers
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        self.logger.addHandler(error_handler)
    
    def _initialize_system_monitoring(self) -> None:
        """
        Initialize system resource monitoring
        
        What: Set up CPU, memory, and disk monitoring
        Why: Track resource usage for performance optimization
        How: psutil library for system metrics
        Alternative: OS-specific tools (less portable)
        """
        try:
            self.system_info = {
                'cpu_count': psutil.cpu_count(),
                'memory_total': psutil.virtual_memory().total,
                'disk_usage': psutil.disk_usage('/').total if os.name != 'nt' else psutil.disk_usage('C:').total,
                'python_version': sys.version,
                'platform': sys.platform
            }
            self.logger.info(f"System monitoring initialized: {self.system_info}")
        except Exception as e:
            self.logger.error(f"Failed to initialize system monitoring: {e}")
            self.system_info = {}
    
    def record_metric(self, component: str, metric_type: str, value: float, 
                     unit: str = "", metadata: Dict[str, Any] = None) -> None:
        """
        Record performance metric
        
        What: Store metric data with timestamp and context
        Why: Track performance trends and identify bottlenecks
        How: Structured metric storage with buffering
        Alternative: Real-time database (more complex, higher overhead)
        """
        metric = MetricData(
            timestamp=datetime.now(),
            component=component,
            metric_type=metric_type,
            value=value,
            unit=unit,
            metadata=metadata or {}
        )
        
        self.metrics_buffer.append(metric)
        self.logger.debug(f"Metric recorded: {component}.{metric_type} = {value} {unit}")
        
        # Flush buffer if it gets too large
        if len(self.metrics_buffer) > 1000:
            self._flush_metrics()
    
    def _flush_metrics(self) -> None:
        """
        Flush metrics buffer to disk
        
        What: Save buffered metrics to persistent storage
        Why: Prevent memory overflow and ensure data persistence
        How: JSON serialization to dated files
        Alternative: Database storage (more complex setup)
        """
        if not self.metrics_buffer:
            return
        
        try:
            metrics_file = self.metrics_dir / f'metrics_{datetime.now().strftime("%Y%m%d")}.json'
            
            # Convert metrics to serializable format
            metrics_data = []
            for metric in self.metrics_buffer:
                metric_dict = asdict(metric)
                metric_dict['timestamp'] = metric.timestamp.isoformat()
                metrics_data.append(metric_dict)
            
            # Append to existing file or create new
            existing_data = []
            if metrics_file.exists():
                with open(metrics_file, 'r') as f:
                    existing_data = json.load(f)
            
            existing_data.extend(metrics_data)
            
            with open(metrics_file, 'w') as f:
                json.dump(existing_data, f, indent=2)
            
            self.logger.info(f"Flushed {len(self.metrics_buffer)} metrics to {metrics_file}")
            self.metrics_buffer.clear()
            
        except Exception as e:
            self.logger.error(f"Failed to flush metrics: {e}")
    
    @contextmanager
    def monitor_operation(self, operation_name: str, component: str = "pipeline"):
        """
        Context manager for operation monitoring
        
        What: Automatic timing and resource monitoring for operations
        Why: Consistent performance measurement across pipeline
        How: Context manager with exception handling
        Alternative: Manual timing (error-prone and inconsistent)
        
        Usage:
            with monitor.monitor_operation("data_processing", "etl"):
                # Your operation code here
                process_data()
        """
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss if 'psutil' in sys.modules else 0
        
        self.logger.info(f"Starting operation: {operation_name}")
        
        try:
            yield
            
            # Record successful completion
            duration = time.time() - start_time
            end_memory = psutil.Process().memory_info().rss if 'psutil' in sys.modules else 0
            memory_delta = end_memory - start_memory
            
            self.record_metric(component, "operation_duration", duration, "seconds", {
                "operation": operation_name,
                "status": "success"
            })
            
            self.record_metric(component, "memory_usage", memory_delta, "bytes", {
                "operation": operation_name,
                "type": "delta"
            })
            
            self.logger.info(f"Operation completed: {operation_name} ({duration:.2f}s)")
            
        except Exception as e:
            # Record failure
            duration = time.time() - start_time
            self.record_metric(component, "operation_duration", duration, "seconds", {
                "operation": operation_name,
                "status": "failed",
                "error": str(e)
            })
            
            self.logger.error(f"Operation failed: {operation_name} ({duration:.2f}s) - {e}")
            self.logger.error(traceback.format_exc())
            
            raise
    
    def log_data_quality(self, dataset_name: str, quality_metrics: Dict[str, Any]) -> None:
        """
        Log data quality metrics
        
        What: Record data quality indicators and anomalies
        Why: Track data integrity and identify quality issues
        How: Structured logging with quality thresholds
        Alternative: Manual quality checks (inconsistent and unreliable)
        """
        self.logger.info(f"Data quality assessment: {dataset_name}")
        
        for metric_name, value in quality_metrics.items():
            self.record_metric("data_quality", metric_name, value, "", {
                "dataset": dataset_name
            })
            
            # Log quality issues
            if metric_name == "null_percentage" and value > 10:
                self.logger.warning(f"High null percentage in {dataset_name}: {value}%")
            elif metric_name == "duplicate_percentage" and value > 5:
                self.logger.warning(f"High duplicate percentage in {dataset_name}: {value}%")
            elif metric_name == "outlier_percentage" and value > 15:
                self.logger.warning(f"High outlier percentage in {dataset_name}: {value}%")
    
    def log_processing_stats(self, component: str, stats: Dict[str, Any]) -> None:
        """
        Log processing statistics
        
        What: Record processing volumes and performance stats
        Why: Track throughput and identify performance bottlenecks
        How: Structured stat logging with trend analysis
        Alternative: Manual stat tracking (inconsistent)
        """
        self.logger.info(f"Processing stats for {component}: {stats}")
        
        for stat_name, value in stats.items():
            if isinstance(value, (int, float)):
                self.record_metric(component, stat_name, value)
    
    def check_system_health(self) -> Dict[str, Any]:
        """
        Comprehensive system health check
        
        What: Assess system resource usage and availability
        Why: Proactive monitoring and early warning system
        How: Resource thresholds and availability checks
        Alternative: External monitoring tools (more complex)
        """
        health_status = {
            "timestamp": datetime.now().isoformat(),
            "status": "healthy",
            "issues": []
        }
        
        try:
            # Check CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            if cpu_percent > 80:
                health_status["issues"].append(f"High CPU usage: {cpu_percent}%")
                health_status["status"] = "warning"
            
            # Check memory usage
            memory = psutil.virtual_memory()
            if memory.percent > 85:
                health_status["issues"].append(f"High memory usage: {memory.percent}%")
                health_status["status"] = "warning"
            
            # Check disk usage
            disk = psutil.disk_usage('/' if os.name != 'nt' else 'C:')
            disk_percent = (disk.used / disk.total) * 100
            if disk_percent > 90:
                health_status["issues"].append(f"High disk usage: {disk_percent:.1f}%")
                health_status["status"] = "critical"
            
            # Check log file sizes
            log_files = list(self.log_dir.glob("*.log"))
            large_logs = [f for f in log_files if f.stat().st_size > 100 * 1024 * 1024]  # 100MB
            if large_logs:
                health_status["issues"].append(f"Large log files: {[f.name for f in large_logs]}")
            
            # Record health metrics
            self.record_metric("system", "cpu_percent", cpu_percent, "%")
            self.record_metric("system", "memory_percent", memory.percent, "%")
            self.record_metric("system", "disk_percent", disk_percent, "%")
            
        except Exception as e:
            health_status["status"] = "error"
            health_status["issues"].append(f"Health check failed: {e}")
            self.logger.error(f"Health check error: {e}")
        
        return health_status
    
    def generate_performance_report(self, hours: int = 24) -> Dict[str, Any]:
        """
        Generate performance report
        
        What: Comprehensive performance analysis and recommendations
        Why: Identify optimization opportunities and track trends
        How: Metric aggregation and statistical analysis
        Alternative: Manual report generation (time-consuming)
        """
        try:
            # Load recent metrics
            cutoff_time = datetime.now() - timedelta(hours=hours)
            recent_metrics = self._load_recent_metrics(cutoff_time)
            
            if not recent_metrics:
                return {"error": "No metrics available for the specified time period"}
            
            # Analyze metrics
            report = {
                "period": f"Last {hours} hours",
                "generated_at": datetime.now().isoformat(),
                "summary": {},
                "components": {},
                "recommendations": []
            }
            
            # Component-wise analysis
            df = pd.DataFrame([asdict(m) for m in recent_metrics])
            
            for component in df['component'].unique():
                component_data = df[df['component'] == component]
                
                report["components"][component] = {
                    "total_operations": len(component_data),
                    "avg_duration": component_data[component_data['metric_type'] == 'operation_duration']['value'].mean(),
                    "total_memory_used": component_data[component_data['metric_type'] == 'memory_usage']['value'].sum(),
                    "error_rate": len(component_data[component_data['metadata'].str.contains('failed', na=False)]) / len(component_data)
                }
            
            # Generate recommendations
            self._generate_recommendations(report)
            
            return report
            
        except Exception as e:
            self.logger.error(f"Failed to generate performance report: {e}")
            return {"error": str(e)}
    
    def _load_recent_metrics(self, cutoff_time: datetime) -> List[MetricData]:
        """Load metrics from recent time period"""
        metrics = []
        
        try:
            # Check recent metric files
            for metrics_file in self.metrics_dir.glob("metrics_*.json"):
                with open(metrics_file, 'r') as f:
                    file_metrics = json.load(f)
                
                for metric_data in file_metrics:
                    metric_time = datetime.fromisoformat(metric_data['timestamp'])
                    if metric_time >= cutoff_time:
                        metric = MetricData(
                            timestamp=metric_time,
                            component=metric_data['component'],
                            metric_type=metric_data['metric_type'],
                            value=metric_data['value'],
                            unit=metric_data['unit'],
                            metadata=metric_data.get('metadata', {})
                        )
                        metrics.append(metric)
        
        except Exception as e:
            self.logger.error(f"Failed to load recent metrics: {e}")
        
        return metrics
    
    def _generate_recommendations(self, report: Dict[str, Any]) -> None:
        """Generate performance recommendations"""
        recommendations = []
        
        for component, stats in report["components"].items():
            if stats["avg_duration"] > 60:  # More than 1 minute
                recommendations.append(f"Consider optimizing {component} - average duration is {stats['avg_duration']:.1f}s")
            
            if stats["error_rate"] > 0.1:  # More than 10% errors
                recommendations.append(f"High error rate in {component}: {stats['error_rate']:.1%}")
            
            if stats["total_memory_used"] > 1024**3:  # More than 1GB
                recommendations.append(f"High memory usage in {component}: {stats['total_memory_used']/1024**3:.1f}GB")
        
        report["recommendations"] = recommendations
    
    def cleanup_old_logs(self, days: int = 30) -> None:
        """
        Clean up old log and metric files
        
        What: Remove old log files to prevent disk space issues
        Why: Maintain system health and prevent storage overflow
        How: File age checking and deletion
        Alternative: Log rotation tools (more complex setup)
        """
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # Clean log files
        for log_file in self.log_dir.glob("*.log"):
            if datetime.fromtimestamp(log_file.stat().st_mtime) < cutoff_date:
                log_file.unlink()
                self.logger.info(f"Deleted old log file: {log_file}")
        
        # Clean metric files
        for metric_file in self.metrics_dir.glob("metrics_*.json"):
            if datetime.fromtimestamp(metric_file.stat().st_mtime) < cutoff_date:
                metric_file.unlink()
                self.logger.info(f"Deleted old metric file: {metric_file}")
    
    def shutdown(self) -> None:
        """
        Graceful shutdown of monitoring system
        
        What: Flush remaining metrics and close resources
        Why: Ensure data integrity and clean shutdown
        How: Buffer flushing and resource cleanup
        Alternative: Abrupt shutdown (may lose metrics)
        """
        self.logger.info("Shutting down monitoring system...")
        
        # Flush remaining metrics
        self._flush_metrics()
        
        # Log session summary
        session_duration = datetime.now() - self.start_time
        self.logger.info(f"Monitoring session completed. Duration: {session_duration}")
        
        # Close logging handlers
        for handler in self.logger.handlers:
            handler.close()

# Global monitor instance
monitor = None

def get_monitor() -> PipelineMonitor:
    """
    Get global monitor instance
    
    What: Singleton pattern for monitor access
    Why: Ensure consistent monitoring across all components
    How: Global instance with lazy initialization
    Alternative: Dependency injection (more complex)
    """
    global monitor
    if monitor is None:
        monitor = PipelineMonitor()
    return monitor

def setup_monitoring(log_dir: str = "logs", metrics_dir: str = "metrics") -> PipelineMonitor:
    """
    Set up monitoring system
    
    What: Initialize monitoring with custom configuration
    Why: Allow configuration for different environments
    How: Factory function with parameters
    Alternative: Configuration file (more complex)
    """
    global monitor
    monitor = PipelineMonitor(log_dir, metrics_dir)
    return monitor

if __name__ == "__main__":
    # Demo usage
    monitor = setup_monitoring()
    
    # Example operations
    with monitor.monitor_operation("demo_operation", "demo"):
        time.sleep(2)
        monitor.record_metric("demo", "test_metric", 42.5, "units")
    
    # Health check
    health = monitor.check_system_health()
    print(f"System health: {health}")
    
    # Performance report
    report = monitor.generate_performance_report(1)
    print(f"Performance report: {json.dumps(report, indent=2)}")
    
    monitor.shutdown() 