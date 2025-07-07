#!/usr/bin/env python3
"""
Project Dashboard
Email-Customer Analytics Pipeline

What: Comprehensive project status dashboard with real-time monitoring
Why: Centralized visibility into system health, performance, and operations
How: Web-based dashboard with real-time updates
Alternative: Command-line monitoring (less visual and interactive)
"""

import os
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging
import threading
import webbrowser
from http.server import HTTPServer, SimpleHTTPRequestHandler
import socketserver
import subprocess

class ProjectDashboard:
    """
    Comprehensive project dashboard system
    
    What: Real-time monitoring and status dashboard
    Why: Centralized operations visibility and control
    How: Web-based interface with live data
    Alternative: Separate monitoring tools (more complex setup)
    """
    
    def __init__(self, port: int = 8080):
        """
        Initialize dashboard system
        
        What: Set up dashboard server and data collection
        Why: Centralized dashboard configuration
        How: Server setup and monitoring initialization
        Alternative: External dashboard tools (more complex)
        """
        self.port = port
        self.root_path = Path(__file__).parent
        self.dashboard_dir = self.root_path / "dashboard"
        self.static_dir = self.dashboard_dir / "static"
        self.data_dir = self.dashboard_dir / "data"
        
        # Create directories
        self.dashboard_dir.mkdir(exist_ok=True)
        self.static_dir.mkdir(exist_ok=True)
        self.data_dir.mkdir(exist_ok=True)
        
        # Set up logging
        self.setup_logging()
        
        # Initialize data collectors
        self.data_collectors = {}
        self.is_running = False
        
        self.logger.info("Project dashboard initialized")
    
    def setup_logging(self) -> None:
        """Configure dashboard logging"""
        self.logger = logging.getLogger('dashboard')
        self.logger.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
    
    def generate_dashboard_html(self) -> str:
        """
        Generate main dashboard HTML
        
        What: Create interactive dashboard interface
        Why: Visual representation of system status
        How: HTML template with JavaScript for real-time updates
        Alternative: Static HTML (no real-time updates)
        """
        html_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Email-Customer Analytics Pipeline Dashboard</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        .dashboard {
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 40px rgba(0,0,0,0.1);
            overflow: hidden;
        }
        
        .header {
            background: linear-gradient(135deg, #2c3e50 0%, #3498db 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }
        
        .header h1 {
            font-size: 2.5rem;
            margin-bottom: 10px;
        }
        
        .header p {
            font-size: 1.2rem;
            opacity: 0.9;
        }
        
        .status-bar {
            background: #f8f9fa;
            padding: 15px 30px;
            border-bottom: 1px solid #dee2e6;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .status-item {
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        .status-indicator {
            width: 12px;
            height: 12px;
            border-radius: 50%;
            background: #28a745;
            animation: pulse 2s infinite;
        }
        
        .status-indicator.warning {
            background: #ffc107;
        }
        
        .status-indicator.error {
            background: #dc3545;
        }
        
        @keyframes pulse {
            0% { opacity: 1; }
            50% { opacity: 0.5; }
            100% { opacity: 1; }
        }
        
        .dashboard-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            padding: 30px;
        }
        
        .card {
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.08);
            border: 1px solid #e9ecef;
        }
        
        .card h3 {
            color: #2c3e50;
            margin-bottom: 15px;
            font-size: 1.3rem;
        }
        
        .metric {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 10px 0;
            border-bottom: 1px solid #f1f3f4;
        }
        
        .metric:last-child {
            border-bottom: none;
        }
        
        .metric-value {
            font-weight: 600;
            color: #495057;
        }
        
        .service-status {
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 10px 0;
        }
        
        .service-indicator {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: #28a745;
        }
        
        .service-indicator.offline {
            background: #dc3545;
        }
        
        .progress-bar {
            width: 100%;
            height: 8px;
            background: #e9ecef;
            border-radius: 4px;
            overflow: hidden;
            margin: 10px 0;
        }
        
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, #28a745, #20c997);
            transition: width 0.3s ease;
        }
        
        .log-container {
            background: #2c3e50;
            color: #ecf0f1;
            font-family: 'Courier New', monospace;
            padding: 15px;
            border-radius: 10px;
            max-height: 300px;
            overflow-y: auto;
            font-size: 0.9rem;
        }
        
        .log-entry {
            margin-bottom: 5px;
            padding: 2px 0;
        }
        
        .log-timestamp {
            color: #95a5a6;
            margin-right: 10px;
        }
        
        .log-level-INFO {
            color: #3498db;
        }
        
        .log-level-WARNING {
            color: #f39c12;
        }
        
        .log-level-ERROR {
            color: #e74c3c;
        }
        
        .refresh-button {
            background: linear-gradient(135deg, #3498db, #2980b9);
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 25px;
            cursor: pointer;
            font-size: 0.9rem;
            transition: transform 0.2s;
        }
        
        .refresh-button:hover {
            transform: translateY(-2px);
        }
        
        .timestamp {
            font-size: 0.9rem;
            color: #6c757d;
        }
    </style>
</head>
<body>
    <div class="dashboard">
        <div class="header">
            <h1>📊 Email-Customer Analytics Pipeline</h1>
            <p>Real-time System Dashboard</p>
        </div>
        
        <div class="status-bar">
            <div class="status-item">
                <div class="status-indicator" id="system-status"></div>
                <span>System Status: <span id="system-status-text">Loading...</span></span>
            </div>
            <div class="status-item">
                <span class="timestamp">Last Updated: <span id="last-updated">--</span></span>
            </div>
            <div class="status-item">
                <button class="refresh-button" onclick="refreshDashboard()">🔄 Refresh</button>
            </div>
        </div>
        
        <div class="dashboard-grid">
            <!-- System Health Card -->
            <div class="card">
                <h3>🖥️ System Health</h3>
                <div class="metric">
                    <span>CPU Usage</span>
                    <span class="metric-value" id="cpu-usage">--</span>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill" id="cpu-progress" style="width: 0%"></div>
                </div>
                <div class="metric">
                    <span>Memory Usage</span>
                    <span class="metric-value" id="memory-usage">--</span>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill" id="memory-progress" style="width: 0%"></div>
                </div>
                <div class="metric">
                    <span>Disk Usage</span>
                    <span class="metric-value" id="disk-usage">--</span>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill" id="disk-progress" style="width: 0%"></div>
                </div>
            </div>
            
            <!-- Services Status Card -->
            <div class="card">
                <h3>🐳 Services Status</h3>
                <div class="service-status">
                    <div class="service-indicator" id="postgres-indicator"></div>
                    <span>PostgreSQL Database</span>
                </div>
                <div class="service-status">
                    <div class="service-indicator" id="redis-indicator"></div>
                    <span>Redis Cache</span>
                </div>
                <div class="service-status">
                    <div class="service-indicator" id="jupyter-indicator"></div>
                    <span>Jupyter Lab</span>
                </div>
                <div class="service-status">
                    <div class="service-indicator" id="pgadmin-indicator"></div>
                    <span>pgAdmin</span>
                </div>
            </div>
            
            <!-- Pipeline Status Card -->
            <div class="card">
                <h3>🔄 Pipeline Status</h3>
                <div class="metric">
                    <span>Total Emails Processed</span>
                    <span class="metric-value" id="emails-processed">--</span>
                </div>
                <div class="metric">
                    <span>Customer Matches</span>
                    <span class="metric-value" id="customer-matches">--</span>
                </div>
                <div class="metric">
                    <span>Data Quality Score</span>
                    <span class="metric-value" id="data-quality">--</span>
                </div>
                <div class="metric">
                    <span>Last Processing Run</span>
                    <span class="metric-value" id="last-run">--</span>
                </div>
            </div>
            
            <!-- Performance Metrics Card -->
            <div class="card">
                <h3>📈 Performance Metrics</h3>
                <div class="metric">
                    <span>Average Processing Time</span>
                    <span class="metric-value" id="avg-processing-time">--</span>
                </div>
                <div class="metric">
                    <span>Throughput (emails/min)</span>
                    <span class="metric-value" id="throughput">--</span>
                </div>
                <div class="metric">
                    <span>Error Rate</span>
                    <span class="metric-value" id="error-rate">--</span>
                </div>
                <div class="metric">
                    <span>Success Rate</span>
                    <span class="metric-value" id="success-rate">--</span>
                </div>
            </div>
            
            <!-- Storage & Backup Card -->
            <div class="card">
                <h3>💾 Storage & Backup</h3>
                <div class="metric">
                    <span>Data Directory Size</span>
                    <span class="metric-value" id="data-size">--</span>
                </div>
                <div class="metric">
                    <span>Backup Count</span>
                    <span class="metric-value" id="backup-count">--</span>
                </div>
                <div class="metric">
                    <span>Last Backup</span>
                    <span class="metric-value" id="last-backup">--</span>
                </div>
                <div class="metric">
                    <span>Backup Size</span>
                    <span class="metric-value" id="backup-size">--</span>
                </div>
            </div>
            
            <!-- Recent Activity Card -->
            <div class="card">
                <h3>📋 Recent Activity</h3>
                <div class="log-container" id="recent-logs">
                    <div class="log-entry">
                        <span class="log-timestamp">--:--:--</span>
                        <span class="log-level-INFO">Loading activity logs...</span>
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <script>
        // Dashboard JavaScript
        let refreshInterval;
        
        function refreshDashboard() {
            fetch('/api/status')
                .then(response => response.json())
                .then(data => updateDashboard(data))
                .catch(error => console.error('Error fetching data:', error));
        }
        
        function updateDashboard(data) {
            // Update timestamp
            document.getElementById('last-updated').textContent = new Date().toLocaleTimeString();
            
            // Update system status
            const systemStatus = data.system_health?.status || 'unknown';
            const statusElement = document.getElementById('system-status');
            const statusText = document.getElementById('system-status-text');
            
            statusElement.className = 'status-indicator';
            if (systemStatus === 'healthy') {
                statusElement.classList.add('healthy');
                statusText.textContent = 'Healthy';
            } else if (systemStatus === 'warning') {
                statusElement.classList.add('warning');
                statusText.textContent = 'Warning';
            } else {
                statusElement.classList.add('error');
                statusText.textContent = 'Error';
            }
            
            // Update system metrics
            if (data.system_metrics) {
                updateMetric('cpu-usage', data.system_metrics.cpu_percent, '%');
                updateProgress('cpu-progress', data.system_metrics.cpu_percent);
                
                updateMetric('memory-usage', data.system_metrics.memory_percent, '%');
                updateProgress('memory-progress', data.system_metrics.memory_percent);
                
                updateMetric('disk-usage', data.system_metrics.disk_percent, '%');
                updateProgress('disk-progress', data.system_metrics.disk_percent);
            }
            
            // Update services status
            if (data.services) {
                updateServiceStatus('postgres-indicator', data.services.postgres);
                updateServiceStatus('redis-indicator', data.services.redis);
                updateServiceStatus('jupyter-indicator', data.services.jupyter);
                updateServiceStatus('pgadmin-indicator', data.services.pgadmin);
            }
            
            // Update pipeline metrics
            if (data.pipeline_stats) {
                updateMetric('emails-processed', data.pipeline_stats.total_emails);
                updateMetric('customer-matches', data.pipeline_stats.customer_matches);
                updateMetric('data-quality', data.pipeline_stats.data_quality_score, '%');
                updateMetric('last-run', data.pipeline_stats.last_run);
            }
            
            // Update performance metrics
            if (data.performance) {
                updateMetric('avg-processing-time', data.performance.avg_processing_time, 's');
                updateMetric('throughput', data.performance.throughput);
                updateMetric('error-rate', data.performance.error_rate, '%');
                updateMetric('success-rate', data.performance.success_rate, '%');
            }
            
            // Update storage metrics
            if (data.storage) {
                updateMetric('data-size', data.storage.data_size);
                updateMetric('backup-count', data.storage.backup_count);
                updateMetric('last-backup', data.storage.last_backup);
                updateMetric('backup-size', data.storage.backup_size);
            }
            
            // Update recent logs
            if (data.recent_logs) {
                updateRecentLogs(data.recent_logs);
            }
        }
        
        function updateMetric(elementId, value, unit = '') {
            const element = document.getElementById(elementId);
            if (element && value !== undefined) {
                element.textContent = value + unit;
            }
        }
        
        function updateProgress(elementId, percentage) {
            const element = document.getElementById(elementId);
            if (element && percentage !== undefined) {
                element.style.width = percentage + '%';
            }
        }
        
        function updateServiceStatus(elementId, isOnline) {
            const element = document.getElementById(elementId);
            if (element) {
                element.className = 'service-indicator';
                if (!isOnline) {
                    element.classList.add('offline');
                }
            }
        }
        
        function updateRecentLogs(logs) {
            const container = document.getElementById('recent-logs');
            if (container && logs.length > 0) {
                container.innerHTML = logs.map(log => `
                    <div class="log-entry">
                        <span class="log-timestamp">${log.timestamp}</span>
                        <span class="log-level-${log.level}">${log.message}</span>
                    </div>
                `).join('');
            }
        }
        
        // Initialize dashboard
        document.addEventListener('DOMContentLoaded', function() {
            refreshDashboard();
            refreshInterval = setInterval(refreshDashboard, 30000); // Refresh every 30 seconds
        });
        
        // Cleanup on page unload
        window.addEventListener('beforeunload', function() {
            if (refreshInterval) {
                clearInterval(refreshInterval);
            }
        });
    </script>
</body>
</html>
        """
        return html_template
    
    def collect_system_data(self) -> Dict[str, Any]:
        """
        Collect comprehensive system data
        
        What: Gather real-time system metrics and status
        Why: Provide current system state for dashboard
        How: System API calls and file system analysis
        Alternative: External monitoring agents (more complex)
        """
        try:
            import psutil
            
            # System metrics
            system_metrics = {
                'cpu_percent': psutil.cpu_percent(interval=1),
                'memory_percent': psutil.virtual_memory().percent,
                'disk_percent': (psutil.disk_usage('/').used / psutil.disk_usage('/').total * 100) if os.name != 'nt' else (psutil.disk_usage('C:').used / psutil.disk_usage('C:').total * 100)
            }
            
            # System health
            system_health = {
                'status': 'healthy',
                'issues': []
            }
            
            if system_metrics['cpu_percent'] > 80:
                system_health['status'] = 'warning'
                system_health['issues'].append('High CPU usage')
            
            if system_metrics['memory_percent'] > 85:
                system_health['status'] = 'warning'
                system_health['issues'].append('High memory usage')
            
            if system_metrics['disk_percent'] > 90:
                system_health['status'] = 'error'
                system_health['issues'].append('High disk usage')
            
            # Service status
            services = self.check_services_status()
            
            # Pipeline statistics
            pipeline_stats = self.get_pipeline_stats()
            
            # Performance metrics
            performance = self.get_performance_metrics()
            
            # Storage information
            storage = self.get_storage_info()
            
            # Recent logs
            recent_logs = self.get_recent_logs()
            
            return {
                'timestamp': datetime.now().isoformat(),
                'system_metrics': system_metrics,
                'system_health': system_health,
                'services': services,
                'pipeline_stats': pipeline_stats,
                'performance': performance,
                'storage': storage,
                'recent_logs': recent_logs
            }
            
        except Exception as e:
            self.logger.error(f"Failed to collect system data: {e}")
            return {'error': str(e)}
    
    def check_services_status(self) -> Dict[str, bool]:
        """Check Docker services status"""
        services = {
            'postgres': False,
            'redis': False,
            'jupyter': False,
            'pgadmin': False
        }
        
        try:
            result = subprocess.run(['docker-compose', 'ps', '--format', 'json'], 
                                  capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                # Parse service status
                for service in services.keys():
                    if service in result.stdout:
                        services[service] = True
                        
        except Exception as e:
            self.logger.error(f"Failed to check services: {e}")
        
        return services
    
    def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get pipeline processing statistics"""
        stats = {
            'total_emails': 0,
            'customer_matches': 0,
            'data_quality_score': 0,
            'last_run': 'Never'
        }
        
        try:
            # Check for processing logs or stats files
            stats_file = Path("logs/pipeline_stats.json")
            if stats_file.exists():
                with open(stats_file, 'r') as f:
                    saved_stats = json.load(f)
                    stats.update(saved_stats)
        except Exception as e:
            self.logger.error(f"Failed to get pipeline stats: {e}")
        
        return stats
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics"""
        performance = {
            'avg_processing_time': 0,
            'throughput': 0,
            'error_rate': 0,
            'success_rate': 100
        }
        
        try:
            # Load performance data from metrics files
            metrics_dir = Path("metrics")
            if metrics_dir.exists():
                # Aggregate recent metrics
                pass
        except Exception as e:
            self.logger.error(f"Failed to get performance metrics: {e}")
        
        return performance
    
    def get_storage_info(self) -> Dict[str, Any]:
        """Get storage and backup information"""
        storage = {
            'data_size': '0 MB',
            'backup_count': 0,
            'last_backup': 'Never',
            'backup_size': '0 MB'
        }
        
        try:
            # Calculate data directory size
            data_dir = Path("data")
            if data_dir.exists():
                total_size = sum(f.stat().st_size for f in data_dir.rglob('*') if f.is_file())
                storage['data_size'] = f"{total_size / (1024*1024):.1f} MB"
            
            # Check backup information
            backup_dir = Path("backups")
            if backup_dir.exists():
                backup_files = list(backup_dir.rglob('*.tar.gz'))
                storage['backup_count'] = len(backup_files)
                
                if backup_files:
                    latest_backup = max(backup_files, key=lambda x: x.stat().st_mtime)
                    storage['last_backup'] = datetime.fromtimestamp(latest_backup.stat().st_mtime).strftime('%Y-%m-%d %H:%M')
                    
                    total_backup_size = sum(f.stat().st_size for f in backup_files)
                    storage['backup_size'] = f"{total_backup_size / (1024*1024):.1f} MB"
                    
        except Exception as e:
            self.logger.error(f"Failed to get storage info: {e}")
        
        return storage
    
    def get_recent_logs(self) -> List[Dict[str, str]]:
        """Get recent log entries"""
        logs = []
        
        try:
            log_dir = Path("logs")
            if log_dir.exists():
                # Find most recent log file
                log_files = list(log_dir.glob("*.log"))
                if log_files:
                    latest_log = max(log_files, key=lambda x: x.stat().st_mtime)
                    
                    # Read last 10 lines
                    with open(latest_log, 'r') as f:
                        lines = f.readlines()
                        recent_lines = lines[-10:] if len(lines) > 10 else lines
                        
                        for line in recent_lines:
                            if line.strip():
                                parts = line.split(' - ')
                                if len(parts) >= 3:
                                    logs.append({
                                        'timestamp': parts[0],
                                        'level': parts[1],
                                        'message': ' - '.join(parts[2:]).strip()
                                    })
        except Exception as e:
            self.logger.error(f"Failed to get recent logs: {e}")
        
        return logs
    
    def create_dashboard_files(self) -> None:
        """Create dashboard HTML and supporting files"""
        # Create main dashboard HTML
        dashboard_html = self.generate_dashboard_html()
        with open(self.dashboard_dir / "index.html", 'w') as f:
            f.write(dashboard_html)
        
        # Create API endpoint simulation
        api_handler = """
import json
import http.server
import socketserver
from pathlib import Path
import sys

# Add parent directory to path to import dashboard
sys.path.append(str(Path(__file__).parent.parent))

class DashboardAPIHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(Path(__file__).parent), **kwargs)
    
    def do_GET(self):
        if self.path == '/api/status':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            # Import and get dashboard data
            try:
                from dashboard import ProjectDashboard
                dashboard = ProjectDashboard()
                data = dashboard.collect_system_data()
                self.wfile.write(json.dumps(data).encode())
            except Exception as e:
                error_data = {'error': str(e)}
                self.wfile.write(json.dumps(error_data).encode())
        else:
            super().do_GET()

if __name__ == "__main__":
    PORT = 8080
    with socketserver.TCPServer(("", PORT), DashboardAPIHandler) as httpd:
        print(f"Dashboard server running at http://localhost:{PORT}")
        httpd.serve_forever()
"""
        
        with open(self.dashboard_dir / "server.py", 'w') as f:
            f.write(api_handler)
    
    def start_dashboard_server(self) -> None:
        """
        Start dashboard web server
        
        What: Launch web server for dashboard access
        Why: Provide web-based interface for monitoring
        How: HTTP server with API endpoints
        Alternative: Desktop application (more complex)
        """
        try:
            # Create dashboard files
            self.create_dashboard_files()
            
            # Start server in background thread
            def run_server():
                os.chdir(self.dashboard_dir)
                subprocess.run([sys.executable, "server.py"])
            
            server_thread = threading.Thread(target=run_server, daemon=True)
            server_thread.start()
            
            # Wait a moment for server to start
            time.sleep(2)
            
            # Open browser
            dashboard_url = f"http://localhost:{self.port}"
            self.logger.info(f"Dashboard available at: {dashboard_url}")
            
            try:
                webbrowser.open(dashboard_url)
            except Exception as e:
                self.logger.warning(f"Could not open browser automatically: {e}")
            
            self.is_running = True
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start dashboard server: {e}")
            return False
    
    def stop_dashboard_server(self) -> None:
        """Stop dashboard server"""
        self.is_running = False
        self.logger.info("Dashboard server stopped")
    
    def generate_status_report(self) -> str:
        """Generate comprehensive status report"""
        data = self.collect_system_data()
        
        report = f"""
📊 EMAIL-CUSTOMER ANALYTICS PIPELINE STATUS REPORT
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

🖥️  SYSTEM HEALTH
CPU Usage: {data.get('system_metrics', {}).get('cpu_percent', 'N/A')}%
Memory Usage: {data.get('system_metrics', {}).get('memory_percent', 'N/A')}%
Disk Usage: {data.get('system_metrics', {}).get('disk_percent', 'N/A')}%
Overall Status: {data.get('system_health', {}).get('status', 'Unknown')}

🐳 SERVICES STATUS
"""
        
        services = data.get('services', {})
        for service, status in services.items():
            status_icon = "✅" if status else "❌"
            report += f"{status_icon} {service.title()}: {'Online' if status else 'Offline'}\n"
        
        report += f"""
🔄 PIPELINE STATISTICS
Total Emails Processed: {data.get('pipeline_stats', {}).get('total_emails', 'N/A')}
Customer Matches: {data.get('pipeline_stats', {}).get('customer_matches', 'N/A')}
Data Quality Score: {data.get('pipeline_stats', {}).get('data_quality_score', 'N/A')}%
Last Run: {data.get('pipeline_stats', {}).get('last_run', 'Never')}

📈 PERFORMANCE METRICS
Average Processing Time: {data.get('performance', {}).get('avg_processing_time', 'N/A')}s
Throughput: {data.get('performance', {}).get('throughput', 'N/A')} emails/min
Error Rate: {data.get('performance', {}).get('error_rate', 'N/A')}%
Success Rate: {data.get('performance', {}).get('success_rate', 'N/A')}%

💾 STORAGE & BACKUP
Data Directory Size: {data.get('storage', {}).get('data_size', 'N/A')}
Backup Count: {data.get('storage', {}).get('backup_count', 'N/A')}
Last Backup: {data.get('storage', {}).get('last_backup', 'Never')}
Backup Size: {data.get('storage', {}).get('backup_size', 'N/A')}

🔗 QUICK LINKS
Dashboard: http://localhost:{self.port}
Jupyter Lab: http://localhost:8888
pgAdmin: http://localhost:5050
"""
        
        return report

def main():
    """Main dashboard entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Project Dashboard")
    parser.add_argument('--port', '-p', type=int, default=8080,
                       help='Dashboard port (default: 8080)')
    parser.add_argument('--no-browser', action='store_true',
                       help='Do not open browser automatically')
    parser.add_argument('--report', action='store_true',
                       help='Generate status report only')
    
    args = parser.parse_args()
    
    dashboard = ProjectDashboard(port=args.port)
    
    if args.report:
        # Generate and print status report
        report = dashboard.generate_status_report()
        print(report)
        
        # Save report to file
        report_file = Path("logs") / f"status_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        report_file.parent.mkdir(exist_ok=True)
        with open(report_file, 'w') as f:
            f.write(report)
        print(f"\nReport saved to: {report_file}")
        
    else:
        # Start dashboard server
        print("🚀 Starting Project Dashboard...")
        
        if dashboard.start_dashboard_server():
            print(f"✅ Dashboard running at http://localhost:{args.port}")
            print("Press Ctrl+C to stop the dashboard")
            
            try:
                # Keep server running
                while dashboard.is_running:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\n🛑 Stopping dashboard...")
                dashboard.stop_dashboard_server()
        else:
            print("❌ Failed to start dashboard server")
            sys.exit(1)

if __name__ == "__main__":
    main() 