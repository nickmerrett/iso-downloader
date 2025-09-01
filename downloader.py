import asyncio
import aiohttp
import aiofiles
import subprocess
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
import hashlib

logger = logging.getLogger(__name__)


class DownloadStats:
    def __init__(self):
        self.total_bytes = 0
        self.downloaded_bytes = 0
        self.start_time = datetime.now()
        self.end_time: Optional[datetime] = None
        
    @property
    def progress_percent(self) -> float:
        if self.total_bytes == 0:
            return 0.0
        return (self.downloaded_bytes / self.total_bytes) * 100
    
    @property
    def speed_mbps(self) -> float:
        if not self.end_time:
            elapsed = (datetime.now() - self.start_time).total_seconds()
        else:
            elapsed = (self.end_time - self.start_time).total_seconds()
        
        if elapsed == 0:
            return 0.0
        
        return (self.downloaded_bytes / (1024 * 1024)) / elapsed


class HTTPDownloader:
    def __init__(self, download_dir: str, chunk_size: int = 8192, timeout: int = 300):
        self.default_download_dir = Path(download_dir)
        self.chunk_size = chunk_size
        self.timeout = aiohttp.ClientTimeout(total=timeout)
    
    async def download(self, url: str, filename: Optional[str] = None, destination_dir: Optional[str] = None) -> Dict[str, Any]:
        if not filename:
            filename = url.split('/')[-1]
        
        # Use specific destination or default
        download_dir = Path(destination_dir) if destination_dir else self.default_download_dir
        download_dir.mkdir(parents=True, exist_ok=True)
        
        filepath = download_dir / filename
        stats = DownloadStats()
        
        try:
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                logger.info(f"Starting HTTP download: {filename}")
                
                async with session.get(url) as response:
                    response.raise_for_status()
                    
                    stats.total_bytes = int(response.headers.get('content-length', 0))
                    
                    async with aiofiles.open(filepath, 'wb') as file:
                        async for chunk in response.content.iter_chunked(self.chunk_size):
                            await file.write(chunk)
                            stats.downloaded_bytes += len(chunk)
                            
                            if stats.total_bytes > 0:
                                logger.debug(f"Progress: {stats.progress_percent:.1f}% - {filename}")
                
                stats.end_time = datetime.now()
                logger.info(f"HTTP download completed: {filename} ({stats.speed_mbps:.2f} MB/s)")
                
                return {
                    "success": True,
                    "filepath": str(filepath),
                    "size_bytes": stats.downloaded_bytes,
                    "speed_mbps": stats.speed_mbps,
                    "duration_seconds": (stats.end_time - stats.start_time).total_seconds()
                }
                
        except Exception as e:
            logger.error(f"HTTP download failed for {filename}: {e}")
            if filepath.exists():
                filepath.unlink()
            
            return {
                "success": False,
                "error": str(e),
                "filepath": str(filepath)
            }


class RsyncDownloader:
    def __init__(self, download_dir: str):
        self.default_download_dir = Path(download_dir)
    
    async def download(self, url: str, filename: Optional[str] = None, destination_dir: Optional[str] = None) -> Dict[str, Any]:
        if not filename:
            filename = url.split('/')[-1]
        
        # Use specific destination or default
        download_dir = Path(destination_dir) if destination_dir else self.default_download_dir
        download_dir.mkdir(parents=True, exist_ok=True)
        
        filepath = download_dir / filename
        start_time = datetime.now()
        
        try:
            logger.info(f"Starting rsync download: {filename}")
            
            cmd = [
                'rsync', 
                '-avP',  # archive, verbose, progress
                url,
                str(filepath)
            ]
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0:
                end_time = datetime.now()
                file_size = filepath.stat().st_size if filepath.exists() else 0
                duration = (end_time - start_time).total_seconds()
                speed_mbps = (file_size / (1024 * 1024)) / duration if duration > 0 else 0
                
                logger.info(f"Rsync download completed: {filename} ({speed_mbps:.2f} MB/s)")
                
                return {
                    "success": True,
                    "filepath": str(filepath),
                    "size_bytes": file_size,
                    "speed_mbps": speed_mbps,
                    "duration_seconds": duration
                }
            else:
                error_msg = stderr.decode().strip()
                logger.error(f"Rsync download failed for {filename}: {error_msg}")
                
                if filepath.exists():
                    filepath.unlink()
                
                return {
                    "success": False,
                    "error": error_msg,
                    "filepath": str(filepath)
                }
                
        except Exception as e:
            logger.error(f"Rsync download failed for {filename}: {e}")
            if filepath.exists():
                filepath.unlink()
            
            return {
                "success": False,
                "error": str(e),
                "filepath": str(filepath)
            }


class DownloadManager:
    def __init__(self, config):
        self.config = config
        self.http_downloader = HTTPDownloader(
            download_dir=config.download_directory,
            chunk_size=config.chunk_size,
            timeout=config.timeout
        )
        self.rsync_downloader = RsyncDownloader(
            download_dir=config.download_directory
        )
        self.active_downloads = 0
        self.semaphore = asyncio.Semaphore(config.max_parallel)
    
    async def download_iso(self, job: Dict[str, Any]) -> Dict[str, Any]:
        async with self.semaphore:
            self.active_downloads += 1
            
            try:
                destination_dir = job.get("destination_dir")
                
                if job["type"] == "http":
                    result = await self.http_downloader.download(job["url"], job["name"], destination_dir)
                elif job["type"] == "rsync":
                    result = await self.rsync_downloader.download(job["url"], job["name"], destination_dir)
                else:
                    result = {
                        "success": False,
                        "error": f"Unsupported download type: {job['type']}"
                    }
                
                result["job_name"] = job["name"]
                result["download_type"] = job["type"]
                result["destination_dir"] = destination_dir or self.config.download_directory
                
                return result
                
            finally:
                self.active_downloads -= 1