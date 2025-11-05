"""
CLI integration tests for autoexec functionality.

This module tests the command-line interface for the new autoexec flags
and validates parameter validation and help text.
"""

import pytest
import sys
from unittest.mock import Mock, patch, MagicMock
from click.testing import CliRunner

from src.xmover.cli import main
from src.xmover.commands.maintenance import MaintenanceCommands
from src.xmover.database import CrateDBClient


class TestAutoexecCLI:
    """Test the CLI interface for autoexec functionality"""
    
    @pytest.fixture
    def runner(self):
        """Click test runner"""
        return CliRunner()
    
    @pytest.fixture
    def mock_client(self):
        """Mock CrateDB client that returns success"""
        client = Mock(spec=CrateDBClient)
        client.test_connection.return_value = True
        return client
    
    def test_problematic_translogs_help_includes_autoexec(self, runner):
        """Test that help text includes new autoexec options"""
        result = runner.invoke(main, ['problematic-translogs', '--help'])
        
        assert result.exit_code == 0
        assert '--autoexec' in result.output
        assert '--dry-run' in result.output
        assert '--percentage' in result.output
        assert '--max-wait' in result.output
        assert '--log-format' in result.output
        
        # Check help descriptions
        assert 'Automatically execute replica reset operations' in result.output
        assert 'Simulate operations without actual database changes' in result.output
        assert 'Maximum seconds to wait for retention leases' in result.output
    
    def test_autoexec_and_execute_flags_mutually_exclusive(self, runner):
        """Test that --autoexec and --execute cannot be used together"""
        with patch('src.xmover.database.CrateDBClient') as mock_client_class:
            mock_client_class.return_value.test_connection.return_value = True
            
            result = runner.invoke(main, [
                'problematic-translogs', 
                '--autoexec', 
                '--execute'
            ])
            
            assert result.exit_code == 1
            assert 'mutually exclusive' in result.output
    
    def test_dry_run_requires_autoexec(self, runner):
        """Test that --dry-run requires --autoexec"""
        with patch('src.xmover.database.CrateDBClient') as mock_client_class:
            mock_client_class.return_value.test_connection.return_value = True
            
            result = runner.invoke(main, [
                'problematic-translogs',
                '--dry-run'
            ])
            
            assert result.exit_code == 1
            assert '--dry-run can only be used with --autoexec' in result.output
    
    def test_autoexec_basic_invocation(self, runner):
        """Test basic autoexec invocation"""
        with patch('src.xmover.database.CrateDBClient') as mock_client_class, \
             patch('src.xmover.commands.maintenance.MaintenanceCommands') as mock_maintenance_class:
            
            # Setup mocks
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            mock_maintenance = Mock()
            mock_maintenance_class.return_value = mock_maintenance
            
            result = runner.invoke(main, [
                'problematic-translogs',
                '--autoexec'
            ])
            
            assert result.exit_code == 0
            
            # Verify maintenance command was called with correct parameters
            mock_maintenance.problematic_translogs.assert_called_once()
            call_args = mock_maintenance.problematic_translogs.call_args
            args, kwargs = call_args
            
            # Check default values
            assert args[0] == 512  # sizemb
            assert args[1] is False  # execute
            assert args[2] is True   # autoexec
            assert args[3] is False  # dry_run
            assert args[4] == 200    # percentage
            assert args[5] == 720    # max_wait
            assert args[6] == 'console'  # log_format
    
    def test_autoexec_with_custom_parameters(self, runner):
        """Test autoexec with custom parameter values"""
        with patch('src.xmover.database.CrateDBClient') as mock_client_class, \
             patch('src.xmover.commands.maintenance.MaintenanceCommands') as mock_maintenance_class:
            
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            mock_maintenance = Mock()
            mock_maintenance_class.return_value = mock_maintenance
            
            result = runner.invoke(main, [
                'problematic-translogs',
                '--autoexec',
                '--sizeMB', '1024',
                '--percentage', '150',
                '--max-wait', '900',
                '--log-format', 'json'
            ])
            
            assert result.exit_code == 0
            
            call_args = mock_maintenance.problematic_translogs.call_args[0]
            assert call_args[0] == 1024   # sizemb
            assert call_args[2] is True   # autoexec
            assert call_args[4] == 150    # percentage
            assert call_args[5] == 900    # max_wait
            assert call_args[6] == 'json' # log_format
    
    def test_dry_run_with_autoexec(self, runner):
        """Test dry run mode with autoexec"""
        with patch('src.xmover.database.CrateDBClient') as mock_client_class, \
             patch('src.xmover.commands.maintenance.MaintenanceCommands') as mock_maintenance_class:
            
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            mock_maintenance = Mock()
            mock_maintenance_class.return_value = mock_maintenance
            
            result = runner.invoke(main, [
                'problematic-translogs',
                '--autoexec',
                '--dry-run'
            ])
            
            assert result.exit_code == 0
            
            call_args = mock_maintenance.problematic_translogs.call_args[0]
            assert call_args[2] is True   # autoexec
            assert call_args[3] is True   # dry_run
    
    def test_log_format_validation(self, runner):
        """Test log format parameter validation"""
        with patch('src.xmover.database.CrateDBClient') as mock_client_class:
            mock_client_class.return_value.test_connection.return_value = True
            
            # Test invalid log format
            result = runner.invoke(main, [
                'problematic-translogs',
                '--autoexec',
                '--log-format', 'invalid'
            ])
            
            assert result.exit_code != 0
            assert 'Invalid value for \'--log-format\'' in result.output
    
    def test_percentage_parameter_validation(self, runner):
        """Test percentage parameter accepts integers"""
        with patch('src.xmover.database.CrateDBClient') as mock_client_class, \
             patch('src.xmover.commands.maintenance.MaintenanceCommands') as mock_maintenance_class:
            
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            mock_maintenance = Mock()
            mock_maintenance_class.return_value = mock_maintenance
            
            result = runner.invoke(main, [
                'problematic-translogs',
                '--autoexec',
                '--percentage', '250'
            ])
            
            assert result.exit_code == 0
            
            call_args = mock_maintenance.problematic_translogs.call_args[0]
            assert call_args[4] == 250  # percentage
    
    def test_max_wait_parameter_validation(self, runner):
        """Test max-wait parameter accepts integers"""
        with patch('src.xmover.database.CrateDBClient') as mock_client_class, \
             patch('src.xmover.commands.maintenance.MaintenanceCommands') as mock_maintenance_class:
            
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            mock_maintenance = Mock()
            mock_maintenance_class.return_value = mock_maintenance
            
            result = runner.invoke(main, [
                'problematic-translogs',
                '--autoexec',
                '--max-wait', '1800'
            ])
            
            assert result.exit_code == 0
            
            call_args = mock_maintenance.problematic_translogs.call_args[0]
            assert call_args[5] == 1800  # max_wait
    
    def test_connection_failure_handling(self, runner):
        """Test handling of database connection failures"""
        with patch('src.xmover.database.CrateDBClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = False
            mock_client_class.return_value = mock_client
            
            result = runner.invoke(main, [
                'problematic-translogs',
                '--autoexec'
            ])
            
            assert result.exit_code == 1
            assert 'Could not connect to CrateDB' in result.output
    
    def test_autoexec_failure_exit_code(self, runner):
        """Test that autoexec failures result in proper exit codes"""
        with patch('src.xmover.database.CrateDBClient') as mock_client_class, \
             patch('src.xmover.commands.maintenance.MaintenanceCommands') as mock_maintenance_class, \
             patch('sys.exit') as mock_exit:
            
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            mock_maintenance = Mock()
            mock_maintenance._get_autoexec_exit_code.return_value = 3
            mock_maintenance.problematic_translogs.side_effect = SystemExit(3)
            mock_maintenance_class.return_value = mock_maintenance
            
            with pytest.raises(SystemExit):
                runner.invoke(main, [
                    'problematic-translogs',
                    '--autoexec'
                ])
    
    def test_backwards_compatibility_existing_flags(self, runner):
        """Test that existing flags still work as before"""
        with patch('src.xmover.database.CrateDBClient') as mock_client_class, \
             patch('src.xmover.commands.maintenance.MaintenanceCommands') as mock_maintenance_class:
            
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            mock_maintenance = Mock()
            mock_maintenance_class.return_value = mock_maintenance
            
            # Test existing --execute flag
            result = runner.invoke(main, [
                'problematic-translogs',
                '--execute',
                '--sizeMB', '256'
            ])
            
            assert result.exit_code == 0
            
            call_args = mock_maintenance.problematic_translogs.call_args[0]
            assert call_args[0] == 256    # sizemb
            assert call_args[1] is True   # execute
            assert call_args[2] is False  # autoexec
            assert call_args[3] is False  # dry_run
    
    def test_command_examples_in_help(self, runner):
        """Test that help includes usage examples"""
        result = runner.invoke(main, ['problematic-translogs', '--help'])
        
        assert result.exit_code == 0
        assert 'Examples:' in result.output
        assert 'xmover problematic-translogs --autoexec' in result.output
        assert 'xmover problematic-translogs --autoexec --dry-run' in result.output
        assert 'xmover problematic-translogs --autoexec --log-format json' in result.output
    
    def test_autoexec_mode_description_in_help(self, runner):
        """Test that help describes the three operation modes"""
        result = runner.invoke(main, ['problematic-translogs', '--help'])
        
        assert result.exit_code == 0
        assert 'ANALYSIS MODE' in result.output
        assert 'COMMAND GENERATION MODE' in result.output
        assert 'AUTOEXEC MODE' in result.output
        
        # Should describe what autoexec does
        assert 'Set number_of_replicas to 0' in result.output
        assert 'Monitor retention leases' in result.output
        assert 'Restore original replica count' in result.output


class TestAutoexecCLIErrorMessages:
    """Test error message clarity for autoexec CLI"""
    
    def test_clear_error_for_mutually_exclusive_flags(self):
        """Test clear error message for conflicting flags"""
        runner = CliRunner()
        
        with patch('src.xmover.database.CrateDBClient') as mock_client_class:
            mock_client_class.return_value.test_connection.return_value = True
            
            result = runner.invoke(main, [
                'problematic-translogs',
                '--autoexec',
                '--execute'
            ])
            
            assert result.exit_code == 1
            assert '--autoexec and --execute flags are mutually exclusive' in result.output
    
    def test_clear_error_for_dry_run_without_autoexec(self):
        """Test clear error message for dry-run without autoexec"""
        runner = CliRunner()
        
        with patch('src.xmover.database.CrateDBClient') as mock_client_class:
            mock_client_class.return_value.test_connection.return_value = True
            
            result = runner.invoke(main, [
                'problematic-translogs',
                '--dry-run'
            ])
            
            assert result.exit_code == 1
            assert '--dry-run can only be used with --autoexec' in result.output
    
    def test_parameter_type_error_messages(self):
        """Test error messages for invalid parameter types"""
        runner = CliRunner()
        
        # Test invalid integer for percentage
        result = runner.invoke(main, [
            'problematic-translogs',
            '--autoexec',
            '--percentage', 'invalid'
        ])
        
        assert result.exit_code != 0
        assert 'invalid' in result.output.lower()
    
    def test_help_shows_parameter_defaults(self):
        """Test that help shows default values for parameters"""
        runner = CliRunner()
        result = runner.invoke(main, ['problematic-translogs', '--help'])
        
        assert result.exit_code == 0
        assert 'default: 200' in result.output  # percentage default
        assert 'default: 720' in result.output  # max-wait default
        assert 'default: 512' in result.output  # sizeMB default


class TestAutoexecCLIEdgeCases:
    """Test edge cases and unusual parameter combinations"""
    
    def test_zero_max_wait(self):
        """Test behavior with zero max-wait"""
        runner = CliRunner()
        
        with patch('src.xmover.database.CrateDBClient') as mock_client_class, \
             patch('src.xmover.commands.maintenance.MaintenanceCommands') as mock_maintenance_class:
            
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            mock_maintenance = Mock()
            mock_maintenance_class.return_value = mock_maintenance
            
            result = runner.invoke(main, [
                'problematic-translogs',
                '--autoexec',
                '--max-wait', '0'
            ])
            
            assert result.exit_code == 0
            
            call_args = mock_maintenance.problematic_translogs.call_args[0]
            assert call_args[5] == 0  # max_wait
    
    def test_very_high_percentage(self):
        """Test behavior with very high percentage values"""
        runner = CliRunner()
        
        with patch('src.xmover.database.CrateDBClient') as mock_client_class, \
             patch('src.xmover.commands.maintenance.MaintenanceCommands') as mock_maintenance_class:
            
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            mock_maintenance = Mock()
            mock_maintenance_class.return_value = mock_maintenance
            
            result = runner.invoke(main, [
                'problematic-translogs',
                '--autoexec',
                '--percentage', '10000'
            ])
            
            assert result.exit_code == 0
            
            call_args = mock_maintenance.problematic_translogs.call_args[0]
            assert call_args[4] == 10000  # percentage
    
    def test_all_parameters_together(self):
        """Test using all autoexec parameters together"""
        runner = CliRunner()
        
        with patch('src.xmover.database.CrateDBClient') as mock_client_class, \
             patch('src.xmover.commands.maintenance.MaintenanceCommands') as mock_maintenance_class:
            
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            mock_maintenance = Mock()
            mock_maintenance_class.return_value = mock_maintenance
            
            result = runner.invoke(main, [
                'problematic-translogs',
                '--autoexec',
                '--dry-run',
                '--sizeMB', '2048',
                '--percentage', '300',
                '--max-wait', '1200',
                '--log-format', 'json'
            ])
            
            assert result.exit_code == 0
            
            call_args = mock_maintenance.problematic_translogs.call_args[0]
            assert call_args[0] == 2048     # sizemb
            assert call_args[1] is False    # execute
            assert call_args[2] is True     # autoexec
            assert call_args[3] is True     # dry_run
            assert call_args[4] == 300      # percentage
            assert call_args[5] == 1200     # max_wait
            assert call_args[6] == 'json'   # log_format


if __name__ == '__main__':
    pytest.main([__file__, '-v'])