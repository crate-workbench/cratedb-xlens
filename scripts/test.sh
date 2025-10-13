#!/bin/bash
# XMover Testing and Coverage Scripts
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Help function
show_help() {
    echo "XMover Test Runner"
    echo ""
    echo "Usage: ./scripts/test.sh [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  all                Run all tests with coverage"
    echo "  quick              Run all tests quickly (no coverage)"
    echo "  partition          Run only partition-related tests"
    echo "  safety             Run only safety-critical tests"
    echo "  coverage           Generate full coverage report (HTML + terminal)"
    echo "  coverage-html      Generate HTML coverage report only"
    echo "  coverage-partition Coverage for partition tests only"
    echo "  watch              Run tests in watch mode (re-run on changes)"
    echo "  clean              Clean coverage reports and cache"
    echo "  help               Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./scripts/test.sh all                # Run all tests with coverage"
    echo "  ./scripts/test.sh partition          # Test partition functionality"
    echo "  ./scripts/test.sh safety             # Test safety-critical features"
    echo "  ./scripts/test.sh coverage-html      # Generate HTML coverage report"
}

# Run all tests with coverage
test_all() {
    print_status "Running all tests with coverage..."
    uv run python -m pytest --cov --cov-report=html --cov-report=term
    print_success "All tests completed with coverage report"
    echo "HTML coverage report available at: htmlcov/index.html"
}

# Run tests quickly without coverage
test_quick() {
    print_status "Running all tests quickly (no coverage)..."
    uv run python -m pytest -q
    print_success "All tests completed"
}

# Run partition-specific tests
test_partition() {
    print_status "Running partition-related tests..."
    uv run python -m pytest -m partition -v --cov=src/xmover/database.py --cov=src/xmover/analyzer.py --cov-report=term-missing
    print_success "Partition tests completed"
}

# Run safety-critical tests
test_safety() {
    print_status "Running safety-critical tests..."
    uv run python -m pytest -m safety -v
    print_success "Safety tests completed"
}

# Generate full coverage report
coverage_full() {
    print_status "Generating comprehensive coverage report..."
    uv run python -m pytest --cov --cov-report=html --cov-report=term-missing --cov-branch
    print_success "Coverage report generated"
    echo "HTML report: htmlcov/index.html"
    echo "Open with: open htmlcov/index.html"
}

# Generate HTML coverage only
coverage_html() {
    print_status "Generating HTML coverage report..."
    uv run python -m pytest --cov --cov-report=html --quiet
    print_success "HTML coverage report generated"
    echo "Report available at: htmlcov/index.html"

    # Try to open the report automatically
    if command -v open >/dev/null 2>&1; then
        print_status "Opening coverage report in browser..."
        open htmlcov/index.html
    elif command -v xdg-open >/dev/null 2>&1; then
        print_status "Opening coverage report in browser..."
        xdg-open htmlcov/index.html
    else
        echo "Open manually: file://$(pwd)/htmlcov/index.html"
    fi
}

# Coverage for partition tests only
coverage_partition() {
    print_status "Generating coverage report for partition tests..."
    uv run python -m pytest -m partition --cov=src/xmover --cov-report=html --cov-report=term-missing
    print_success "Partition coverage report generated"
    echo "HTML report: htmlcov/index.html"
}

# Watch mode (requires pytest-watch or similar)
test_watch() {
    print_status "Starting test watch mode..."
    print_warning "This requires file watching. Tests will re-run when files change."
    print_status "Press Ctrl+C to stop"

    # Simple watch implementation using find and sleep
    last_modified=0
    while true; do
        # Check if any .py files have been modified
        current_modified=$(find src tests -name "*.py" -type f -exec stat -f "%m" {} \; 2>/dev/null | sort -rn | head -1)

        if [ "$current_modified" != "$last_modified" ]; then
            print_status "Changes detected, running tests..."
            uv run python -m pytest --tb=short -q
            last_modified=$current_modified
            print_status "Watching for changes... (Ctrl+C to stop)"
        fi

        sleep 2
    done
}

# Clean coverage reports and cache
clean() {
    print_status "Cleaning coverage reports and cache..."
    rm -rf htmlcov/
    rm -rf .coverage
    rm -rf .pytest_cache/
    rm -rf src/**/__pycache__/
    rm -rf tests/**/__pycache__/
    find . -name "*.pyc" -delete
    print_success "Cleaned coverage reports and cache"
}

# Validate partition fixes are working
validate_partition_fixes() {
    print_status "Validating partition fixes are working correctly..."

    # Run the original partition bug demonstration
    print_status "1. Running original partition bug demonstration..."
    if uv run python test_partition_bug.py > /dev/null 2>&1; then
        print_success "✓ Original bug demonstration runs successfully"
    else
        print_error "✗ Original bug demonstration failed"
        return 1
    fi

    # Run partition-specific tests
    print_status "2. Running partition-specific tests..."
    if uv run python -m pytest -m partition --quiet; then
        print_success "✓ All partition tests pass"
    else
        print_error "✗ Partition tests failed"
        return 1
    fi

    # Run safety-critical tests
    print_status "3. Running safety-critical tests..."
    if uv run python -m pytest -m safety --quiet; then
        print_success "✓ All safety tests pass"
    else
        print_error "✗ Safety tests failed"
        return 1
    fi

    # Check overall test suite
    print_status "4. Running full test suite..."
    if uv run python -m pytest --quiet; then
        print_success "✓ Full test suite passes"
    else
        print_error "✗ Full test suite has failures"
        return 1
    fi

    print_success "🎉 All partition fixes validated successfully!"
    echo ""
    echo "Summary:"
    echo "✓ Zone conflict detection is partition-aware"
    echo "✓ Dangerous moves are correctly prevented"
    echo "✓ Safe moves are still allowed"
    echo "✓ Non-partitioned tables work correctly"
    echo "✓ No regressions in existing functionality"
}

# Main script logic
case "$1" in
    "all")
        test_all
        ;;
    "quick")
        test_quick
        ;;
    "partition")
        test_partition
        ;;
    "safety")
        test_safety
        ;;
    "coverage")
        coverage_full
        ;;
    "coverage-html")
        coverage_html
        ;;
    "coverage-partition")
        coverage_partition
        ;;
    "watch")
        test_watch
        ;;
    "clean")
        clean
        ;;
    "validate")
        validate_partition_fixes
        ;;
    "help"|"")
        show_help
        ;;
    *)
        print_error "Unknown command: $1"
        echo ""
        show_help
        exit 1
        ;;
esac
