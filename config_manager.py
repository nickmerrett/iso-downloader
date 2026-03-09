from pathlib import Path
from typing import List, Dict, Any, Optional
import yaml
import logging
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class ISOConfig(BaseModel):
    name: str
    url: str
    type: str = Field(..., pattern="^(http|rsync|rsync_mirror|http_mirror|glob)$")
    enabled: bool = True
    discovered: bool = False
    destination_dir: Optional[str] = None
    exclude_patterns: Optional[List[str]] = None


class ISOGlobConfig(BaseModel):
    name: str
    base_url: str
    type: str = Field(..., pattern="^(http|rsync)$")
    enabled: bool = True
    mirror: bool = False  # rsync only: mirror entire subdirectories not yet present locally
    include_patterns: Optional[List[str]] = None
    exclude_patterns: Optional[List[str]] = None
    recursive: bool = False
    max_depth: int = 2
    destination_dir: Optional[str] = None


class RabbitMQConfig(BaseModel):
    host: str = "localhost"
    port: int = 5672
    username: str = "guest"
    password: str = "guest"
    queue_name: str = "iso_downloads"


class DownloadConfig(BaseModel):
    max_parallel: int = 3
    download_directory: str = "./downloads"
    chunk_size: int = 8192
    timeout: int = 300


class SchedulerConfig(BaseModel):
    frequency: str = Field(..., pattern="^(daily|weekly|monthly)$")
    time: str = Field(..., pattern="^([01]?[0-9]|2[0-3]):[0-5][0-9]$")


class AppConfig(BaseModel):
    rabbitmq: RabbitMQConfig
    download: DownloadConfig
    scheduler: SchedulerConfig
    isos: List[ISOConfig] = []
    iso_globs: List[ISOGlobConfig] = []


class ConfigManager:
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = Path(config_path)
        self.config: AppConfig = self._load_config()
    
    def _load_config(self) -> AppConfig:
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as file:
            config_data = yaml.safe_load(file)
        
        return AppConfig(**config_data)
    
    def get_enabled_isos(self) -> List[ISOConfig]:
        return [iso for iso in self.config.isos if iso.enabled]
    
    def get_enabled_globs(self) -> List[ISOGlobConfig]:
        return [glob for glob in self.config.iso_globs if glob.enabled]
    
    async def resolve_all_isos(self) -> List[ISOConfig]:
        """Resolve both direct ISOs and glob patterns into a unified list"""
        from iso_discovery import ISODiscoverer, ISOFilter
        
        all_isos = []
        
        # Add direct ISOs
        all_isos.extend(self.get_enabled_isos())
        
        # Resolve glob patterns
        enabled_globs = self.get_enabled_globs()
        logger.info(f"Processing {len(enabled_globs)} enabled sources")

        discoverer = ISODiscoverer()
        for glob_config in enabled_globs:
            logger.info(f"[{glob_config.name}] Checking {glob_config.type}://{glob_config.base_url.split('://')[-1]}"
                        f" (mirror={glob_config.mirror})")
            try:
                if glob_config.mirror and glob_config.type == "rsync":
                    local_base = Path(glob_config.destination_dir or self.config.download.download_directory)
                    discovered = await discoverer.discover_rsync_directories(
                        glob_config.base_url,
                        local_base,
                        include_patterns=glob_config.include_patterns,
                        exclude_patterns=glob_config.exclude_patterns
                    )
                elif glob_config.mirror and glob_config.type == "http":
                    local_base = Path(glob_config.destination_dir or self.config.download.download_directory)
                    discovered = await discoverer.discover_http_directories(
                        glob_config.base_url,
                        local_base,
                        include_patterns=glob_config.include_patterns,
                        exclude_patterns=glob_config.exclude_patterns
                    )
                elif glob_config.recursive:
                    discovered = await discoverer.discover_recursive(
                        glob_config.base_url,
                        glob_config.type,
                        glob_config.max_depth,
                        glob_config.include_patterns
                    )
                else:
                    discovered = await discoverer.discover_isos_from_url(
                        glob_config.base_url,
                        glob_config.type,
                        glob_config.include_patterns
                    )

                # Apply filters (not applied to mirror jobs — we want everything in the dir)
                if not glob_config.mirror and glob_config.exclude_patterns:
                    discovered = ISOFilter.filter_by_name_patterns(
                        discovered,
                        exclude_patterns=glob_config.exclude_patterns
                    )

                if discovered:
                    logger.info(f"[{glob_config.name}] Queuing {len(discovered)} jobs: "
                                + ", ".join(d['name'] for d in discovered[:5])
                                + (f" ... (+{len(discovered) - 5} more)" if len(discovered) > 5 else ""))
                else:
                    logger.info(f"[{glob_config.name}] Nothing new to queue")

                # Convert to ISOConfig objects
                for disc_iso in discovered:
                    iso_config = ISOConfig(
                        name=f"{glob_config.name} - {disc_iso['name']}",
                        url=disc_iso['url'],
                        type=disc_iso['type'],
                        enabled=True,
                        discovered=True,
                        destination_dir=disc_iso.get('destination_dir') or glob_config.destination_dir,
                        exclude_patterns=glob_config.exclude_patterns
                    )
                    all_isos.append(iso_config)

            except Exception as e:
                logger.error(f"[{glob_config.name}] Failed: {e}")
        
        # Deduplicate by URL
        seen_urls = set()
        unique_isos = []
        for iso in all_isos:
            if iso.url not in seen_urls:
                seen_urls.add(iso.url)
                unique_isos.append(iso)
        
        return unique_isos
    
    def reload_config(self) -> None:
        self.config = self._load_config()
    
    def save_config(self) -> None:
        config_dict = self.config.model_dump()
        with open(self.config_path, 'w') as file:
            yaml.dump(config_dict, file, default_flow_style=False, indent=2)