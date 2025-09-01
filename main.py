#!/usr/bin/env python3

import click
import logging
import asyncio
from pathlib import Path
from config_manager import ConfigManager
from queue_factory import create_queue_manager
from scheduler import DownloadScheduler
from worker import DownloadWorker


def setup_logging(debug: bool = False):
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


@click.group()
@click.option('--config', '-c', default='config.yaml', help='Configuration file path')
@click.option('--debug', '-d', is_flag=True, help='Enable debug logging')
@click.pass_context
def cli(ctx, config, debug):
    """ISO Downloader - Automated Linux ISO download tool with RabbitMQ queuing"""
    setup_logging(debug)
    ctx.ensure_object(dict)
    ctx.obj['config_path'] = config


@cli.command()
@click.pass_context
def start_scheduler(ctx):
    """Start the download scheduler"""
    config_path = ctx.obj['config_path']
    
    try:
        config_manager = ConfigManager(config_path)
        queue_manager = create_queue_manager(config_manager)
        scheduler = DownloadScheduler(config_manager, queue_manager)
        
        click.echo("Starting ISO download scheduler...")
        scheduler.start()
        
        try:
            while True:
                import time
                time.sleep(1)
        except KeyboardInterrupt:
            click.echo("\nShutting down scheduler...")
            scheduler.stop()
            queue_manager.disconnect()
            
    except Exception as e:
        click.echo(f"Error starting scheduler: {e}", err=True)


@cli.command()
@click.pass_context
def start_worker(ctx):
    """Start the download worker"""
    config_path = ctx.obj['config_path']
    
    try:
        worker = DownloadWorker(config_path)
        worker.start_worker()
    except Exception as e:
        click.echo(f"Error starting worker: {e}", err=True)


@cli.command()
@click.pass_context
def trigger_download(ctx):
    """Trigger immediate download of all enabled ISOs"""
    config_path = ctx.obj['config_path']
    
    async def _trigger():
        config_manager = ConfigManager(config_path)
        queue_manager = create_queue_manager(config_manager)
        
        queue_manager.connect()
        await queue_manager.publish_all_enabled_jobs(config_manager)
        
        # Get count including resolved ISOs
        all_isos = await config_manager.resolve_all_isos()
        click.echo(f"Triggered download for {len(all_isos)} ISOs (including discovered)")
        
        queue_manager.disconnect()
    
    try:
        asyncio.run(_trigger())
    except Exception as e:
        click.echo(f"Error triggering download: {e}", err=True)


@cli.command()
@click.pass_context
def status(ctx):
    """Show current status and queue information"""
    config_path = ctx.obj['config_path']
    
    try:
        config_manager = ConfigManager(config_path)
        queue_manager = QueueManager(config_manager)
        
        # Queue info
        queue_manager.connect()
        queue_info = queue_manager.get_queue_info()
        queue_manager.disconnect()
        
        # Configuration info
        enabled_isos = config_manager.get_enabled_isos()
        
        click.echo("=== ISO Downloader Status ===")
        click.echo(f"Config file: {config_path}")
        click.echo(f"Download directory: {config_manager.config.download.download_directory}")
        click.echo(f"Max parallel downloads: {config_manager.config.download.max_parallel}")
        click.echo(f"Scheduler frequency: {config_manager.config.scheduler.frequency}")
        click.echo(f"Scheduler time: {config_manager.config.scheduler.time}")
        click.echo()
        click.echo("=== Queue Status ===")
        click.echo(f"Pending jobs: {queue_info['message_count']}")
        click.echo(f"Active consumers: {queue_info['consumer_count']}")
        click.echo()
        click.echo("=== Enabled ISOs ===")
        for iso in enabled_isos:
            click.echo(f"  - {iso.name} ({iso.type})")
            
    except Exception as e:
        click.echo(f"Error getting status: {e}", err=True)


@cli.command()
@click.option('--name', help='ISO name to enable/disable')
@click.option('--enable/--disable', default=True, help='Enable or disable the ISO')
@click.pass_context
def toggle_iso(ctx, name, enable):
    """Enable or disable an ISO for downloading"""
    config_path = ctx.obj['config_path']
    
    try:
        config_manager = ConfigManager(config_path)
        
        # Find and update the ISO
        iso_found = False
        for iso in config_manager.config.isos:
            if iso.name == name:
                iso.enabled = enable
                iso_found = True
                break
        
        if not iso_found:
            click.echo(f"ISO '{name}' not found in configuration", err=True)
            return
        
        config_manager.save_config()
        status = "enabled" if enable else "disabled"
        click.echo(f"ISO '{name}' {status}")
        
    except Exception as e:
        click.echo(f"Error updating ISO: {e}", err=True)


@cli.command()
@click.pass_context
def list_isos(ctx):
    """List all configured ISOs"""
    config_path = ctx.obj['config_path']
    
    try:
        config_manager = ConfigManager(config_path)
        
        click.echo("=== Configured ISOs ===")
        for iso in config_manager.config.isos:
            status = "✓" if iso.enabled else "✗"
            click.echo(f"{status} {iso.name} ({iso.type})")
            click.echo(f"    {iso.url}")
            
    except Exception as e:
        click.echo(f"Error listing ISOs: {e}", err=True)


@cli.command()
@click.option('--url', required=True, help='Base URL to discover ISOs from')
@click.option('--type', 'url_type', default='http', type=click.Choice(['http', 'rsync']), help='URL type')
@click.option('--include', 'include_patterns', multiple=True, help='Include patterns (can specify multiple)')
@click.option('--exclude', 'exclude_patterns', multiple=True, help='Exclude patterns (can specify multiple)')
@click.option('--recursive', is_flag=True, help='Search recursively')
@click.option('--max-depth', default=2, help='Maximum recursion depth')
@click.pass_context
def discover(ctx, url, url_type, include_patterns, exclude_patterns, recursive, max_depth):
    """Discover ISOs from a directory URL"""
    from iso_discovery import ISODiscoverer, ISOFilter
    
    async def _discover():
        discoverer = ISODiscoverer()
        
        patterns = list(include_patterns) if include_patterns else None
        
        if recursive:
            discovered = await discoverer.discover_recursive(url, url_type, max_depth, patterns)
        else:
            discovered = await discoverer.discover_isos_from_url(url, url_type, patterns)
        
        if exclude_patterns:
            discovered = ISOFilter.filter_by_name_patterns(discovered, exclude_patterns=list(exclude_patterns))
        
        click.echo(f"Discovered {len(discovered)} ISOs from {url}:")
        for iso in discovered:
            click.echo(f"  - {iso['name']} ({iso['type']})")
            click.echo(f"    {iso['url']}")
    
    try:
        asyncio.run(_discover())
    except Exception as e:
        click.echo(f"Error discovering ISOs: {e}", err=True)


@cli.command()
@click.pass_context
def list_globs(ctx):
    """List all configured glob patterns"""
    config_path = ctx.obj['config_path']
    
    try:
        config_manager = ConfigManager(config_path)
        
        click.echo("=== Configured Glob Patterns ===")
        for glob in config_manager.config.iso_globs:
            status = "✓" if glob.enabled else "✗"
            click.echo(f"{status} {glob.name} ({glob.type})")
            click.echo(f"    Base URL: {glob.base_url}")
            if glob.include_patterns:
                click.echo(f"    Include: {', '.join(glob.include_patterns)}")
            if glob.exclude_patterns:
                click.echo(f"    Exclude: {', '.join(glob.exclude_patterns)}")
            click.echo(f"    Recursive: {glob.recursive}")
            click.echo()
            
    except Exception as e:
        click.echo(f"Error listing globs: {e}", err=True)


@cli.command()
@click.option('--name', help='Glob pattern name to enable/disable')
@click.option('--enable/--disable', default=True, help='Enable or disable the glob pattern')
@click.pass_context
def toggle_glob(ctx, name, enable):
    """Enable or disable a glob pattern"""
    config_path = ctx.obj['config_path']
    
    try:
        config_manager = ConfigManager(config_path)
        
        # Find and update the glob pattern
        glob_found = False
        for glob in config_manager.config.iso_globs:
            if glob.name == name:
                glob.enabled = enable
                glob_found = True
                break
        
        if not glob_found:
            click.echo(f"Glob pattern '{name}' not found in configuration", err=True)
            return
        
        config_manager.save_config()
        status = "enabled" if enable else "disabled"
        click.echo(f"Glob pattern '{name}' {status}")
        
    except Exception as e:
        click.echo(f"Error updating glob pattern: {e}", err=True)


@cli.command()
@click.pass_context
def preview_downloads(ctx):
    """Preview all ISOs that would be downloaded (including discovered ones)"""
    config_path = ctx.obj['config_path']
    
    async def _preview():
        config_manager = ConfigManager(config_path)
        all_isos = await config_manager.resolve_all_isos()
        
        click.echo(f"=== Preview: {len(all_isos)} ISOs would be downloaded ===")
        
        direct_isos = [iso for iso in all_isos if not iso.discovered]
        discovered_isos = [iso for iso in all_isos if iso.discovered]
        
        if direct_isos:
            click.echo(f"\nDirect ISOs ({len(direct_isos)}):")
            for iso in direct_isos:
                click.echo(f"  - {iso.name}")
                click.echo(f"    {iso.url}")
        
        if discovered_isos:
            click.echo(f"\nDiscovered ISOs ({len(discovered_isos)}):")
            for iso in discovered_isos:
                click.echo(f"  - {iso.name}")
                click.echo(f"    {iso.url}")
    
    try:
        asyncio.run(_preview())
    except Exception as e:
        click.echo(f"Error previewing downloads: {e}", err=True)


@cli.command()
@click.pass_context
def init_config(ctx):
    """Initialize a new configuration file"""
    config_path = ctx.obj['config_path']
    
    if Path(config_path).exists():
        if not click.confirm(f"Configuration file {config_path} already exists. Overwrite?"):
            return
    
    # Create default config (already exists in config.yaml)
    click.echo(f"Configuration initialized at {config_path}")
    click.echo("Edit the file to add your ISO URLs and adjust settings.")


if __name__ == '__main__':
    cli()