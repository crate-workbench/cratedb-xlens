#!/usr/bin/env python3
"""
Standalone rules validation script for XMover shard size monitoring rules.

This script validates the YAML configuration file used by the shard size monitor
to ensure proper syntax, required fields, and rule structure.

Usage:
    python validate_rules.py [config_file]
    python validate_rules.py config/shard_size_rules.yaml
"""

import sys
import argparse
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from xmover.shard_size_monitor import validate_rules_file
except ImportError as e:
    print(f"Error importing validation module: {e}")
    print("Make sure you're running from the xmover project root directory")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Validate XMover shard size monitoring rules configuration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python validate_rules.py                              # Validate default rules
  python validate_rules.py config/shard_size_rules.yaml # Validate specific file
  python validate_rules.py my_custom_rules.yaml         # Validate custom rules
        """
    )
    
    parser.add_argument(
        'config_file', 
        nargs='?',
        default='config/shard_size_rules.yaml',
        help='Path to rules configuration file (default: config/shard_size_rules.yaml)'
    )
    
    args = parser.parse_args()
    
    # Resolve path relative to script location
    config_path = Path(args.config_file)
    if not config_path.is_absolute():
        config_path = Path(__file__).parent / config_path
    
    print(f"Validating rules configuration: {config_path}")
    print("-" * 60)
    
    if validate_rules_file(str(config_path)):
        print("\n✅ Validation completed successfully!")
        sys.exit(0)
    else:
        print("\n❌ Validation failed!")
        sys.exit(1)


if __name__ == '__main__':
    main()