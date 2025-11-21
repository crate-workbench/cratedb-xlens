"""
Problematic translogs package

This package contains the implementation for detecting and remediating tables
with problematic translog sizes in CrateDB clusters.

The package is organized into several modules:
- command.py: Main command implementation (ProblematicTranslogsCommand)
- display.py: Display utilities for rendering analysis results
- sql_generator.py: SQL generation utilities for shard management
- autoexec.py: Automatic execution logic for replica reset operations
"""

from .command import ProblematicTranslogsCommand

__all__ = ['ProblematicTranslogsCommand']
