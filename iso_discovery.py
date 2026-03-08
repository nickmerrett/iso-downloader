import re
import asyncio
import aiohttp
import subprocess
import logging
from typing import List, Dict, Set
from urllib.parse import urljoin, urlparse
from pathlib import Path
import fnmatch

logger = logging.getLogger(__name__)


class ISODiscoverer:
    def __init__(self, timeout: int = 60):
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.iso_patterns = [
            "*.iso",
            "*.ISO",
            "*-dvd*.iso",
            "*-cd*.iso",
            "*-live*.iso",
            "*-install*.iso",
            "*-boot*.iso"
        ]
    
    async def discover_isos_from_url(self, base_url: str, url_type: str = "http", 
                                   patterns: List[str] = None) -> List[Dict[str, str]]:
        """Discover ISO files from a base URL"""
        if patterns is None:
            patterns = self.iso_patterns
        
        if url_type == "http":
            return await self._discover_http_isos(base_url, patterns)
        elif url_type == "rsync":
            return await self._discover_rsync_isos(base_url, patterns)
        else:
            raise ValueError(f"Unsupported URL type: {url_type}")
    
    async def _discover_http_isos(self, base_url: str, patterns: List[str]) -> List[Dict[str, str]]:
        """Discover ISOs from HTTP directory listing"""
        discovered_isos = []
        
        try:
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                logger.info(f"Discovering ISOs from HTTP directory: {base_url}")
                
                async with session.get(base_url) as response:
                    response.raise_for_status()
                    html_content = await response.text()
                    
                    # Extract links from HTML directory listing
                    iso_links = self._extract_iso_links_from_html(html_content, base_url, patterns)
                    
                    for link in iso_links:
                        iso_name = Path(link).name
                        discovered_isos.append({
                            "name": iso_name,
                            "url": link,
                            "type": "http",
                            "discovered": True
                        })
                        
                logger.info(f"Discovered {len(discovered_isos)} ISOs from {base_url}")
                
        except Exception as e:
            logger.error(f"Error discovering HTTP ISOs from {base_url}: {e}")
        
        return discovered_isos
    
    def _extract_iso_links_from_html(self, html_content: str, base_url: str, 
                                   patterns: List[str]) -> List[str]:
        """Extract ISO file links from HTML directory listing"""
        iso_links = []
        
        # Common patterns for directory listings
        link_patterns = [
            r'<a\s+[^>]*href=["\']([^"\']*\.iso)["\'][^>]*>',  # Standard href
            r'href=["\']([^"\']*\.iso)["\']',  # Simple href
            r'>([^<]*\.iso)<',  # Text content containing .iso
        ]
        
        for pattern in link_patterns:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            for match in matches:
                # Handle relative URLs
                if match.startswith(('http://', 'https://')):
                    full_url = match
                else:
                    full_url = urljoin(base_url, match)
                
                # Check if filename matches our ISO patterns
                filename = Path(full_url).name
                if any(fnmatch.fnmatch(filename.lower(), pattern.lower()) for pattern in patterns):
                    iso_links.append(full_url)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_links = []
        for link in iso_links:
            if link not in seen:
                seen.add(link)
                unique_links.append(link)
        
        return unique_links
    
    async def discover_http_directories(self, base_url: str, local_base: "Path",
                                        include_patterns: List[str] = None,
                                        exclude_patterns: List[str] = None) -> List[Dict[str, str]]:
        """List subdirectories from an HTTP directory listing and return mirror jobs for those not present locally."""
        from pathlib import Path as _Path
        discovered = []

        try:
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                logger.info(f"Discovering HTTP directories from: {base_url}")

                async with session.get(base_url) as response:
                    response.raise_for_status()
                    html = await response.text()

            # Match href links ending in / — these are directories in autoindex listings
            raw_links = re.findall(r'href=["\']([^"\'?#]+/)["\']', html, re.IGNORECASE)

            seen = set()
            for link in raw_links:
                # Skip parent dir, current dir, and absolute URLs
                if link in ('../', './') or link.startswith(('http://', 'https://', '/')):
                    continue
                name = link.rstrip('/')
                if not name or name in seen:
                    continue
                seen.add(name)

                if include_patterns and not any(fnmatch.fnmatch(name, p) for p in include_patterns):
                    continue
                if exclude_patterns and any(fnmatch.fnmatch(name, p) for p in exclude_patterns):
                    continue

                local_dir = _Path(local_base) / name
                if local_dir.exists():
                    logger.debug(f"Skipping {name} — already exists locally at {local_dir}")
                    continue

                dir_url = base_url.rstrip('/') + '/' + name + '/'
                discovered.append({
                    "name": name,
                    "url": dir_url,
                    "type": "http_mirror",
                    "discovered": True
                })

            if not discovered:
                # Flat directory — no subdirs found, mirror the base URL itself
                logger.info(f"No subdirectories found at {base_url}, creating single mirror job")
                name = base_url.rstrip('/').split('/')[-1]
                discovered.append({
                    "name": name,
                    "url": base_url,
                    "type": "http_mirror",
                    "discovered": True
                })
            else:
                logger.info(f"Discovered {len(discovered)} new remote directories from {base_url}")

        except Exception as e:
            logger.error(f"Error discovering HTTP directories from {base_url}: {e}")

        return discovered

    async def _rsync_list(self, url: str):
        """Run rsync --list-only and return (returncode, stdout, stderr)."""
        process = await asyncio.create_subprocess_exec(
            'rsync', '--list-only', url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        return process.returncode, stdout.decode(), stderr.decode()

    async def discover_rsync_directories(self, base_url: str, local_base: "Path",
                                         include_patterns: List[str] = None,
                                         exclude_patterns: List[str] = None) -> List[Dict[str, str]]:
        """List subdirectories on a remote rsync source and return mirror jobs for those not present locally."""
        from pathlib import Path as _Path
        discovered = []

        try:
            logger.info(f"Discovering rsync directories from: {base_url}")
            returncode, output, stderr = await self._rsync_list(base_url)

            if returncode != 0:
                logger.error(f"Rsync listing failed for {base_url}: {stderr.strip()}")
                return discovered

            for line in output.splitlines():
                line = line.strip()
                if not line:
                    continue
                parts = line.split()
                if len(parts) < 5:
                    continue
                permissions, name = parts[0], parts[-1]
                if not permissions.startswith('d') or name == '.':
                    continue

                if include_patterns and not any(fnmatch.fnmatch(name, p) for p in include_patterns):
                    continue
                if exclude_patterns and any(fnmatch.fnmatch(name, p) for p in exclude_patterns):
                    continue

                local_dir = _Path(local_base) / name
                if local_dir.exists():
                    logger.debug(f"Skipping {name} — already exists locally at {local_dir}")
                    continue

                dir_url = base_url.rstrip('/') + '/' + name + '/'
                discovered.append({
                    "name": name,
                    "url": dir_url,
                    "type": "rsync_mirror",
                    "discovered": True
                })

            if not discovered:
                # Flat directory — no subdirs found, mirror the base URL itself
                logger.info(f"No subdirectories found at {base_url}, creating single mirror job")
                name = base_url.rstrip('/').split('/')[-1]
                discovered.append({
                    "name": name,
                    "url": base_url,
                    "type": "rsync_mirror",
                    "discovered": True
                })
            else:
                logger.info(f"Discovered {len(discovered)} new remote directories from {base_url}")

        except Exception as e:
            logger.error(f"Error discovering rsync directories from {base_url}: {e}")

        return discovered

    async def _discover_rsync_isos(self, base_url: str, patterns: List[str]) -> List[Dict[str, str]]:
        """Discover ISOs from rsync directory listing"""
        discovered_isos = []
        
        try:
            logger.info(f"Discovering ISOs from rsync directory: {base_url}")
            
            # Use rsync to list directory contents
            cmd = ['rsync', '--list-only', base_url]
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0:
                output = stdout.decode()
                iso_files = self._extract_iso_files_from_rsync_output(output, base_url, patterns)
                
                for iso_file in iso_files:
                    iso_name = Path(iso_file).name
                    discovered_isos.append({
                        "name": iso_name,
                        "url": iso_file,
                        "type": "rsync",
                        "discovered": True
                    })
                
                logger.info(f"Discovered {len(discovered_isos)} ISOs from {base_url}")
            else:
                error_msg = stderr.decode().strip()
                logger.error(f"Rsync listing failed for {base_url}: {error_msg}")
                
        except Exception as e:
            logger.error(f"Error discovering rsync ISOs from {base_url}: {e}")
        
        return discovered_isos
    
    def _extract_iso_files_from_rsync_output(self, output: str, base_url: str, 
                                           patterns: List[str]) -> List[str]:
        """Extract ISO files from rsync --list-only output"""
        iso_files = []
        
        for line in output.split('\n'):
            line = line.strip()
            if not line:
                continue
            
            # Parse rsync output format: permissions size date time filename
            parts = line.split()
            if len(parts) >= 5:
                filename = parts[-1]  # Last part is filename
                
                # Check if it's a file (not directory) and matches ISO patterns
                if not parts[0].startswith('d') and filename.endswith(('.iso', '.ISO')):
                    if any(fnmatch.fnmatch(filename.lower(), pattern.lower()) for pattern in patterns):
                        # Construct full rsync URL
                        if base_url.endswith('/'):
                            full_url = base_url + filename
                        else:
                            full_url = base_url + '/' + filename
                        iso_files.append(full_url)
        
        return iso_files
    
    async def discover_recursive(self, base_url: str, url_type: str = "http", 
                               max_depth: int = 2, patterns: List[str] = None) -> List[Dict[str, str]]:
        """Recursively discover ISOs from subdirectories"""
        if patterns is None:
            patterns = self.iso_patterns
        
        all_isos = []
        visited = set()
        
        async def _recursive_discover(url: str, depth: int):
            if depth > max_depth or url in visited:
                return
            
            visited.add(url)
            
            # Discover ISOs in current directory
            isos = await self.discover_isos_from_url(url, url_type, patterns)
            all_isos.extend(isos)
            
            # If we want to go deeper, we'd need to discover subdirectories too
            # This is more complex and might not be needed for most use cases
        
        await _recursive_discover(base_url, 0)
        return all_isos


class ISOFilter:
    """Filter discovered ISOs based on various criteria"""
    
    @staticmethod
    def filter_by_name_patterns(isos: List[Dict[str, str]], 
                               include_patterns: List[str] = None,
                               exclude_patterns: List[str] = None) -> List[Dict[str, str]]:
        """Filter ISOs by filename patterns"""
        filtered = isos[:]
        
        if include_patterns:
            filtered = [
                iso for iso in filtered 
                if any(fnmatch.fnmatch(iso['name'].lower(), pattern.lower()) 
                      for pattern in include_patterns)
            ]
        
        if exclude_patterns:
            filtered = [
                iso for iso in filtered 
                if not any(fnmatch.fnmatch(iso['name'].lower(), pattern.lower()) 
                          for pattern in exclude_patterns)
            ]
        
        return filtered
    
    @staticmethod
    def filter_by_size(isos: List[Dict[str, str]], 
                      min_size_mb: int = None, 
                      max_size_mb: int = None) -> List[Dict[str, str]]:
        """Filter ISOs by file size (requires size information)"""
        # This would require additional HTTP HEAD requests or rsync --stats
        # For now, just return the input list
        return isos
    
    @staticmethod
    def deduplicate(isos: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """Remove duplicate ISOs based on name"""
        seen = set()
        unique_isos = []
        
        for iso in isos:
            key = iso['name'].lower()
            if key not in seen:
                seen.add(key)
                unique_isos.append(iso)
        
        return unique_isos