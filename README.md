# ISO Downloader

Automated Linux ISO download tool with RabbitMQ queuing and configurable parallel downloads.

## Features

- **Parallel Downloads**: Configure how many ISOs to download simultaneously
- **Multiple Protocols**: Support for HTTP/HTTPS and rsync downloads
- **Message Queue**: RabbitMQ-based job queue for reliable download management
- **Periodic Scheduling**: Weekly, monthly, or daily automated downloads
- **Configuration Management**: YAML-based configuration with validation
- **Progress Tracking**: Real-time download progress and statistics
- **CLI Interface**: Easy-to-use command-line interface

## Prerequisites

- Python 3.8+
- RabbitMQ server
- rsync (for rsync downloads)

## Installation

1. Clone or download the project
2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Ensure RabbitMQ is running:
   ```bash
   # Ubuntu/Debian
   sudo systemctl start rabbitmq-server
   
   # Or using Docker
   docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
   ```

## Configuration

Edit `config.yaml` to customize:

- **RabbitMQ settings**: Host, port, credentials
- **Download settings**: Parallel downloads, directory, timeouts
- **Schedule**: Frequency and time for automatic downloads
- **ISO list**: Add/remove ISO URLs and configure types (http/rsync)

## Usage

### Start the scheduler (runs periodic downloads):
```bash
python main.py start-scheduler
```

### Start a worker (processes download jobs):
```bash
python main.py start-worker
```

### Trigger immediate download:
```bash
python main.py trigger-download
```

### Check status:
```bash
python main.py status
```

### List all ISOs:
```bash
python main.py list-isos
```

### Enable/disable ISOs:
```bash
python main.py toggle-iso --name "Ubuntu 24.04 LTS" --enable
python main.py toggle-iso --name "Ubuntu 24.04 LTS" --disable
```

## Architecture

1. **Scheduler** (`scheduler.py`): Periodically adds download jobs to RabbitMQ queue
2. **Queue Manager** (`queue_manager.py`): Handles RabbitMQ message publishing/consuming
3. **Download Manager** (`downloader.py`): Manages HTTP and rsync downloads with parallel execution
4. **Worker** (`worker.py`): Consumes jobs from queue and executes downloads
5. **Config Manager** (`config_manager.py`): Handles configuration validation and management

## Running in Production

For production deployments:

1. Run scheduler and worker as separate services
2. Configure RabbitMQ clustering for high availability
3. Use systemd service files for auto-restart
4. Monitor logs in `iso_downloader.log`
5. Set up log rotation

Example systemd service for the worker:
```ini
[Unit]
Description=ISO Downloader Worker
After=network.target rabbitmq-server.service

[Service]
Type=simple
User=iso-downloader
WorkingDirectory=/path/to/iso_downloader
ExecStart=/usr/bin/python3 main.py start-worker
Restart=always

[Install]
WantedBy=multi-user.target
```