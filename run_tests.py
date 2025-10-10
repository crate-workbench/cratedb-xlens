#!/usr/bin/env python3
"""
Test runner script for XMover CLI tests
Simple script to run all or specific test categories
"""

import subprocess
import sys
import os
from pathlib import Path


def run_command(cmd, description=""):
    """Run a command and handle output"""
    print(f"\n{'='*60}")
    if description:
        print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(cmd, capture_output=False, text=True, check=False)
        return result.returncode == 0
    except Exception as e:
        print(f"Error running command: {e}")
        return False


def main():
    """Main test runner"""
    # Change to project directory
    project_dir = Path(__file__).parent
    os.chdir(project_dir)
    
    print("XMover Test Runner")
    print("=" * 60)
    
    if len(sys.argv) > 1:
        test_type = sys.argv[1].lower()
    else:
        test_type = "all"
    
    success = True
    
    if test_type in ["all", "cli"]:
        print("\n🧪 Running CLI Command Tests...")
        success &= run_command([
            "uv", "run", "pytest", 
            "tests/test_cli_commands_simple.py", 
            "-v", "--tb=short"
        ], "CLI command tests (simplified)")
        
        # Also run original CLI tests if available
        success &= run_command([
            "uv", "run", "pytest", 
            "tests/test_cli_commands.py", 
            "-v", "--tb=short", "--maxfail=3"
        ], "CLI command tests (detailed)")
    
    if test_type in ["all", "enhanced"]:
        print("\n🚀 Running Enhanced Command Tests...")
        success &= run_command([
            "uv", "run", "pytest", 
            "tests/test_enhanced_commands.py", 
            "-v", "--tb=short"
        ], "Enhanced command tests")
    
    if test_type in ["all", "existing"]:
        print("\n📊 Running Existing Tests...")
        success &= run_command([
            "uv", "run", "pytest", 
            "tests/test_active_shard_monitor.py",
            "tests/test_distribution_analyzer.py", 
            "tests/test_problematic_translogs.py",
            "tests/test_recovery_monitor.py",
            "-v", "--tb=short"
        ], "Existing component tests")
    
    if test_type in ["all", "quick"]:
        print("\n⚡ Running Quick Test Suite...")
        success &= run_command([
            "uv", "run", "pytest", 
            "tests/", 
            "-x",  # Stop on first failure
            "--tb=line"
        ], "Quick test run")
    
    if test_type == "coverage":
        print("\n📈 Running Tests with Coverage...")
        try:
            subprocess.run(["uv", "add", "--dev", "pytest-cov"], 
                         check=True, capture_output=True)
            success &= run_command([
                "uv", "run", "pytest", 
                "tests/", 
                "--cov=src/xmover",
                "--cov-report=term-missing",
                "--cov-report=html"
            ], "Test coverage analysis")
        except subprocess.CalledProcessError:
            print("Could not install pytest-cov for coverage analysis")
    
    if test_type == "specific":
        if len(sys.argv) < 3:
            print("Usage: python run_tests.py specific <test_file_or_pattern>")
            sys.exit(1)
        
        test_pattern = sys.argv[2]
        print(f"\n🎯 Running Specific Tests: {test_pattern}")
        success &= run_command([
            "uv", "run", "pytest", 
            f"tests/{test_pattern}",
            "-v", "--tb=short"
        ], f"Specific tests: {test_pattern}")
    
    # Test summary
    print("\n" + "="*60)
    if success:
        print("✅ All tests completed successfully!")
        print("\n📋 Test Categories Available:")
        print("  uv run python run_tests.py all        # Run all tests")
        print("  uv run python run_tests.py cli        # Run CLI command tests")
        print("  uv run python run_tests.py enhanced   # Run enhanced feature tests")
        print("  uv run python run_tests.py existing   # Run existing component tests")
        print("  uv run python run_tests.py quick      # Quick test run (stop on failure)")
        print("  uv run python run_tests.py coverage   # Run with coverage analysis")
        print("  uv run python run_tests.py specific <pattern>  # Run specific test file/pattern")
    else:
        print("❌ Some tests failed!")
        print("\n🔧 Troubleshooting:")
        print("1. Make sure you're in the xmover project directory")
        print("2. Install test dependencies: uv sync")
        print("3. Check that pytest is installed: uv add --dev pytest")
        print("4. Run individual test files for more detailed error info")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())