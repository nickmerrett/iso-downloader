#!/usr/bin/env python3
"""
Flatten nested directory structures left by wget mirroring.

Examples:
    # Use --subpath with * as wildcard for the variable part
    python flatten_dirs.py ./downloads/archlinux --subpath "archlinux.org/isos/*" --dry-run
    python flatten_dirs.py ./downloads/archlinux --subpath "archlinux.org/isos/*"

    # Or use --depth to walk N levels down automatically
    python flatten_dirs.py ./downloads/archlinux --depth 3 --dry-run
    python flatten_dirs.py ./downloads/archlinux --depth 3
"""

import argparse
import shutil
import stat
import subprocess
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def _rmtree(path: Path) -> bool:
    """Remove a directory tree. Returns True on success, False if it could not be removed."""
    result = subprocess.run(['rm', '-rf', str(path)], capture_output=True, text=True)
    if result.returncode == 0:
        return True

    logger.warning(f"  Could not remove {path} — skipping ({result.stderr.strip()})")
    return False


def find_nested_dir_by_subpath(top_dir: Path, subpath: str) -> Path | None:
    """Resolve a subpath pattern under top_dir, replacing * with the top-level dir name."""
    resolved = subpath.replace('*', top_dir.name)
    nested = top_dir / resolved
    if not nested.is_dir():
        logger.warning(f"Expected directory not found: {nested}")
        return None
    return nested


def find_nested_dir_by_depth(top_dir: Path, depth: int) -> Path | None:
    """Walk exactly `depth` levels down, following the single subdir at each level."""
    current = top_dir
    for _ in range(depth):
        subdirs = [p for p in current.iterdir() if p.is_dir()]
        if len(subdirs) != 1:
            logger.warning(f"Expected 1 subdir in {current}, found {len(subdirs)} — skipping")
            return None
        current = subdirs[0]
    return current


def flatten(base_dir: Path, subpath: str | None, depth: int, dry_run: bool) -> None:
    top_dirs = sorted(p for p in base_dir.iterdir() if p.is_dir())

    if not top_dirs:
        logger.error(f"No subdirectories found in {base_dir}")
        return

    logger.info(f"Found {len(top_dirs)} top-level directories in {base_dir}")

    for top_dir in top_dirs:
        if subpath:
            nested = find_nested_dir_by_subpath(top_dir, subpath)
        else:
            nested = find_nested_dir_by_depth(top_dir, depth)

        if nested is None:
            continue

        contents = list(nested.iterdir())
        if not contents:
            logger.warning(f"Nothing to move in {nested} — skipping")
            continue

        # Capture the intermediate root to delete BEFORE moving anything
        intermediate_root = top_dir / nested.relative_to(top_dir).parts[0]

        logger.info(f"{top_dir.name}: moving {len(contents)} items from "
                    f"{nested.relative_to(base_dir)} -> {top_dir.name}/")

        for item in contents:
            dest = top_dir / item.name
            if dest.exists():
                logger.warning(f"  Skipping {item.name} — already exists at destination")
                continue
            logger.info(f"  mv {item.relative_to(base_dir)} -> {dest.relative_to(base_dir)}")
            if not dry_run:
                try:
                    shutil.move(str(item), str(dest))
                except Exception as e:
                    logger.warning(f"  Could not move {item.name}: {e} — skipping")

        # Remove the intermediate structure captured before the move
        logger.info(f"  rm -rf {intermediate_root.relative_to(base_dir)}{' (dry-run)' if dry_run else ''}")
        if not dry_run:
            _rmtree(intermediate_root)


def main():
    parser = argparse.ArgumentParser(
        description="Flatten wget-style mirrored directory structures.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  # Explicit subpath — use * as placeholder for the top-level folder name
  python flatten_dirs.py ./downloads/archlinux --subpath "archlinux.org/isos/*"

  # Automatic depth — walk N levels down following single subdirs
  python flatten_dirs.py ./downloads/archlinux --depth 3

  # Always preview first
  python flatten_dirs.py ./downloads/archlinux --subpath "archlinux.org/isos/*" --dry-run
        """
    )
    parser.add_argument(
        "base_dir",
        type=Path,
        help="Base directory containing the top-level folders to flatten"
    )
    parser.add_argument(
        "--subpath",
        type=str,
        default=None,
        help="Intermediate path pattern to flatten, use * as placeholder for the folder name "
             "(e.g. 'archlinux.org/isos/*')"
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=3,
        help="Number of directory levels to collapse when --subpath is not set (default: 3)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would happen without making any changes"
    )

    args = parser.parse_args()

    if not args.base_dir.is_dir():
        logger.error(f"Directory not found: {args.base_dir}")
        return

    if args.dry_run:
        logger.info("DRY RUN — no changes will be made")

    flatten(args.base_dir, args.subpath, args.depth, args.dry_run)

    if not args.dry_run:
        logger.info("Done")


if __name__ == "__main__":
    main()
