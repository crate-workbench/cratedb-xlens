"""
Test watermark utility functions for dynamic disk usage thresholds
"""

import pytest
from unittest.mock import Mock
from cratedb_xlens.utils import (
    parse_watermark_percentage, 
    get_effective_disk_usage_threshold, 
    calculate_watermark_remaining_space
)


class TestWatermarkParsing:
    """Test parsing of watermark percentage values"""
    
    def test_parse_percentage_string_format(self):
        """Test parsing percentage string format like '85%'"""
        assert parse_watermark_percentage("85%") == 85.0
        assert parse_watermark_percentage("90%") == 90.0
        assert parse_watermark_percentage("95%") == 95.0
    
    def test_parse_decimal_string_format(self):
        """Test parsing decimal string format like '0.85'"""
        assert parse_watermark_percentage("0.85") == 85.0
        assert parse_watermark_percentage("0.90") == 90.0
        assert parse_watermark_percentage("0.95") == 95.0
    
    def test_parse_numeric_values(self):
        """Test parsing numeric values"""
        assert parse_watermark_percentage(0.85) == 85.0
        assert parse_watermark_percentage(85.0) == 85.0
        assert parse_watermark_percentage(0.90) == 90.0
    
    def test_parse_invalid_values(self):
        """Test parsing invalid values returns default"""
        assert parse_watermark_percentage("invalid") == 85.0
        assert parse_watermark_percentage(None) == 85.0
        assert parse_watermark_percentage("") == 85.0


class TestEffectiveDiskUsageThreshold:
    """Test calculation of effective disk usage thresholds"""
    
    def test_standard_watermarks_enabled(self):
        """Test standard watermarks with default safety buffer"""
        config = {
            'threshold_enabled': True,
            'watermarks': {
                'low': '85%',
                'high': '90%',
                'flood_stage': '95%'
            }
        }
        
        # Should return 85% - 2% safety buffer = 83%
        threshold = get_effective_disk_usage_threshold(config)
        assert threshold == 83.0
    
    def test_custom_safety_buffer(self):
        """Test custom safety buffer"""
        config = {
            'threshold_enabled': True,
            'watermarks': {
                'low': '85%',
                'high': '90%',
                'flood_stage': '95%'
            }
        }
        
        # With 5% safety buffer: 85% - 5% = 80%
        threshold = get_effective_disk_usage_threshold(config, safety_buffer_percent=5.0)
        assert threshold == 80.0
    
    def test_low_watermark_minimum_cap(self):
        """Test that very low watermarks are capped at 75%"""
        config = {
            'threshold_enabled': True,
            'watermarks': {
                'low': '70%',  # Very low
                'high': '75%',
                'flood_stage': '80%'
            }
        }
        
        # Should be capped at minimum 75%
        threshold = get_effective_disk_usage_threshold(config)
        assert threshold == 75.0
    
    def test_watermarks_disabled(self):
        """Test behavior when watermarks are disabled"""
        config = {
            'threshold_enabled': False,
            'watermarks': {
                'low': '85%',
                'high': '90%',
                'flood_stage': '95%'
            }
        }
        
        # Should return conservative default
        threshold = get_effective_disk_usage_threshold(config)
        assert threshold == 85.0
    
    def test_empty_config(self):
        """Test behavior with empty configuration"""
        config = {}
        
        # Should return conservative default
        threshold = get_effective_disk_usage_threshold(config)
        assert threshold == 85.0
    
    def test_missing_watermarks(self):
        """Test behavior when watermarks dict is missing"""
        config = {
            'threshold_enabled': True
            # No watermarks dict
        }
        
        # Should return conservative default
        threshold = get_effective_disk_usage_threshold(config)
        assert threshold == 85.0


class TestWatermarkRemainingSpace:
    """Test calculation of remaining space until watermarks"""
    
    def test_standard_usage_calculation(self):
        """Test calculation with standard usage scenario"""
        # 1TB node with 800GB used (80% usage)
        total_bytes = 1024 * 1024 * 1024 * 1024  # 1TB
        used_bytes = int(total_bytes * 0.80)      # 80% used
        
        config = {
            'threshold_enabled': True,
            'watermarks': {
                'low': '85%',
                'high': '90%', 
                'flood_stage': '95%'
            }
        }
        
        remaining = calculate_watermark_remaining_space(total_bytes, used_bytes, config)
        
        # At 80% usage:
        # - Low watermark (85%): 5% remaining = ~51GB
        # - High watermark (90%): 10% remaining = ~102GB  
        # - Flood stage (95%): 15% remaining = ~154GB
        
        expected_low = total_bytes * 0.05 / (1024**3)  # ~51GB
        expected_high = total_bytes * 0.10 / (1024**3)  # ~102GB
        expected_flood = total_bytes * 0.15 / (1024**3)  # ~154GB
        
        assert abs(remaining['remaining_to_low_gb'] - expected_low) < 1.0
        assert abs(remaining['remaining_to_high_gb'] - expected_high) < 1.0
        assert abs(remaining['remaining_to_flood_gb'] - expected_flood) < 1.0
    
    def test_watermarks_exceeded(self):
        """Test calculation when watermarks are exceeded"""
        total_bytes = 1024 * 1024 * 1024 * 1024  # 1TB
        used_bytes_high = int(total_bytes * 0.95)  # 95% used
        
        config = {
            'threshold_enabled': True,
            'watermarks': {
                'low': '85%',
                'high': '90%',
                'flood_stage': '95%'
            }
        }
        
        remaining = calculate_watermark_remaining_space(total_bytes, used_bytes_high, config)
        
        # All should be near 0 (capped at 0 even if negative)
        # Account for floating point precision
        assert remaining['remaining_to_low_gb'] < 0.01
        assert remaining['remaining_to_high_gb'] < 0.01
        assert remaining['remaining_to_flood_gb'] < 0.01
    
    def test_watermarks_disabled(self):
        """Test calculation when watermarks are disabled"""
        total_bytes = 1024 * 1024 * 1024 * 1024  # 1TB
        used_bytes = int(total_bytes * 0.80)      # 80% used
        
        config = {
            'threshold_enabled': False,
            'watermarks': {
                'low': '85%',
                'high': '90%',
                'flood_stage': '95%'
            }
        }
        
        remaining = calculate_watermark_remaining_space(total_bytes, used_bytes, config)
        
        # Should return very high values when disabled
        assert remaining['remaining_to_low_gb'] == 999999.0
        assert remaining['remaining_to_high_gb'] == 999999.0
        assert remaining['remaining_to_flood_gb'] == 999999.0


class TestRealWorldScenarios:
    """Test realistic production scenarios"""
    
    def test_cratedb_default_settings(self):
        """Test with CrateDB default watermark settings"""
        config = {
            'threshold_enabled': True,
            'watermarks': {
                'low': '85%',
                'high': '90%',
                'flood_stage': '95%',
                'enable_for_single_data_node': False
            }
        }
        
        threshold = get_effective_disk_usage_threshold(config)
        assert threshold == 83.0  # 85% - 2% safety buffer
    
    def test_conservative_settings(self):
        """Test with conservative watermark settings"""
        config = {
            'threshold_enabled': True,
            'watermarks': {
                'low': '80%',
                'high': '85%', 
                'flood_stage': '90%',
                'enable_for_single_data_node': False
            }
        }
        
        threshold = get_effective_disk_usage_threshold(config)
        assert threshold == 78.0  # 80% - 2% safety buffer
    
    def test_high_capacity_settings(self):
        """Test with high-capacity environment settings"""
        config = {
            'threshold_enabled': True,
            'watermarks': {
                'low': '90%',
                'high': '95%',
                'flood_stage': '98%',
                'enable_for_single_data_node': False
            }
        }
        
        threshold = get_effective_disk_usage_threshold(config)
        assert threshold == 88.0  # 90% - 2% safety buffer
    
    def test_command_override_scenarios(self):
        """Test command line override scenarios"""
        config = {
            'threshold_enabled': True,
            'watermarks': {
                'low': '85%',
                'high': '90%',
                'flood_stage': '95%'
            }
        }
        
        watermark_suggested = get_effective_disk_usage_threshold(config)
        assert watermark_suggested == 83.0
        
        # Test various user inputs
        test_cases = [
            {'user': 95.0, 'expected_used': 83.0, 'override': True},
            {'user': 90.0, 'expected_used': 83.0, 'override': True},
            {'user': 83.0, 'expected_used': 83.0, 'override': False},
            {'user': 80.0, 'expected_used': 80.0, 'override': False}
        ]
        
        for case in test_cases:
            effective = min(case['user'], watermark_suggested)
            assert effective == case['expected_used']
            
            if case['override']:
                assert case['user'] > watermark_suggested
            else:
                assert case['user'] <= watermark_suggested


class TestErrorHandling:
    """Test error handling and edge cases"""
    
    def test_malformed_watermark_values(self):
        """Test handling of malformed watermark values"""
        config = {
            'threshold_enabled': True,
            'watermarks': {
                'low': 'invalid_value',
                'high': None,
                'flood_stage': ''
            }
        }
        
        # Should not crash and return reasonable default
        threshold = get_effective_disk_usage_threshold(config)
        assert threshold == 83.0  # Based on default 85% low watermark
    
    def test_extreme_watermark_values(self):
        """Test handling of extreme watermark values"""
        config = {
            'threshold_enabled': True,
            'watermarks': {
                'low': '999%',  # Unrealistic high value
                'high': '1000%',
                'flood_stage': '1001%'
            }
        }
        
        # Should handle extreme values gracefully
        threshold = get_effective_disk_usage_threshold(config)
        # The function should apply the safety buffer to the parsed value
        assert threshold > 900  # Should be 999% - 2% = 997%
    
    def test_negative_values(self):
        """Test handling of negative values"""
        # This tests the robustness of the parsing function
        assert parse_watermark_percentage("-10%") == 85.0  # Should fall back to default
    
    def test_none_config(self):
        """Test handling of None configuration"""
        threshold = get_effective_disk_usage_threshold(None)
        assert threshold == 85.0  # Conservative default


@pytest.mark.integration
class TestWatermarkIntegration:
    """Integration tests simulating real command usage"""
    
    def test_recommend_command_simulation(self):
        """Simulate the recommend command watermark integration"""
        # Mock client with watermark configuration
        mock_client = Mock()
        mock_client.get_cluster_watermark_config.return_value = {
            'threshold_enabled': True,
            'watermarks': {
                'low': '85%',
                'high': '90%',
                'flood_stage': '95%',
                'enable_for_single_data_node': False
            }
        }
        
        # Simulate recommend command logic
        watermark_config = mock_client.get_cluster_watermark_config()
        effective_threshold = get_effective_disk_usage_threshold(watermark_config)
        
        assert effective_threshold == 83.0
        
        # Test user input scenarios
        user_inputs = [95.0, 90.0, 83.0, 80.0]
        expected_results = [
            (83.0, True),   # 95.0 -> 83.0 (override)
            (83.0, True),   # 90.0 -> 83.0 (override)  
            (83.0, False),  # 83.0 -> 83.0 (no override)
            (80.0, False)   # 80.0 -> 80.0 (no override)
        ]
        
        for user_input, (expected_effective, expected_override) in zip(user_inputs, expected_results):
            effective = min(user_input, effective_threshold)
            override = user_input > effective_threshold
            
            assert effective == expected_effective
            assert override == expected_override