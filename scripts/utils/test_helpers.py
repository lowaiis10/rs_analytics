"""
Connection Testing Utilities for rs_analytics

This module provides standardized output formatting and test result handling
for connection test scripts.

Usage:
    from scripts.utils.test_helpers import (
        print_header, print_success, print_error, print_info
    )
    
    print_header("Connection Test")
    print_success("Configuration loaded")
    print_info("Customer ID: 1234567890")
    print_error("Connection failed: Invalid token")
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


# ============================================
# Output Formatting Functions
# ============================================

def print_header(title: str) -> None:
    """
    Print a formatted section header.
    
    Args:
        title: Title text to display
        
    Example:
        >>> print_header("Connection Test")
        
        ============================================================
          Connection Test
        ============================================================
    """
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


def print_success(message: str) -> None:
    """
    Print a success message with checkmark indicator.
    
    Args:
        message: Success message to display
        
    Example:
        >>> print_success("Configuration loaded successfully")
          [OK] Configuration loaded successfully
    """
    print(f"  [OK] {message}")


def print_error(message: str) -> None:
    """
    Print an error message with X indicator.
    
    Args:
        message: Error message to display
        
    Example:
        >>> print_error("Connection failed: timeout")
          [X] Connection failed: timeout
    """
    print(f"  [X] {message}")


def print_info(message: str) -> None:
    """
    Print an informational message.
    
    Args:
        message: Info message to display
        
    Example:
        >>> print_info("Customer ID: 1234567890")
          [i] Customer ID: 1234567890
    """
    print(f"  [i] {message}")


def print_warning(message: str) -> None:
    """
    Print a warning message.
    
    Args:
        message: Warning message to display
        
    Example:
        >>> print_warning("Using test account with limited data")
          [!] Using test account with limited data
    """
    print(f"  [!] {message}")


def print_step(step_num: int, description: str) -> None:
    """
    Print a step indicator.
    
    Args:
        step_num: Step number
        description: Step description
        
    Example:
        >>> print_step(1, "Loading configuration")
        Step 1: Loading configuration...
    """
    print(f"Step {step_num}: {description}...")


def print_multiline_info(lines: List[str]) -> None:
    """
    Print multiple info lines.
    
    Args:
        lines: List of info messages
    """
    for line in lines:
        if line.strip():
            print_info(line.strip())


def print_fix_instructions(instructions: str) -> None:
    """
    Print fix instructions in a formatted block.
    
    Args:
        instructions: Multi-line instructions
        
    Example:
        >>> print_fix_instructions('''
        ... 1. Go to settings
        ... 2. Update the token
        ... ''')
        
          HOW TO FIX:
          1. Go to settings
          2. Update the token
    """
    print("\n  HOW TO FIX:")
    for line in instructions.strip().split('\n'):
        print(f"  {line}")
    print()


# ============================================
# Test Result Container
# ============================================

@dataclass
class TestStep:
    """
    Represents a single test step and its result.
    """
    name: str
    description: str
    passed: bool = False
    message: str = ""
    details: List[str] = field(default_factory=list)
    fix_instructions: Optional[str] = None


@dataclass
class TestResult:
    """
    Container for test results across multiple steps.
    
    Usage:
        result = TestResult("Google Ads Connection Test")
        
        # Add a passing step
        result.add_step("config", "Configuration", True, "Loaded successfully")
        
        # Add a failing step
        result.add_step("api", "API Connection", False, "Timeout", 
                       fix_instructions="Check your network connection")
        
        # Print summary
        result.print_summary()
    """
    test_name: str
    steps: List[TestStep] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.now)
    
    def add_step(
        self,
        name: str,
        description: str,
        passed: bool,
        message: str = "",
        details: Optional[List[str]] = None,
        fix_instructions: Optional[str] = None
    ) -> None:
        """
        Add a test step result.
        
        Args:
            name: Step identifier
            description: Human-readable step description
            passed: Whether the step passed
            message: Status message
            details: Optional list of detail lines
            fix_instructions: Instructions if step failed
        """
        step = TestStep(
            name=name,
            description=description,
            passed=passed,
            message=message,
            details=details or [],
            fix_instructions=fix_instructions
        )
        self.steps.append(step)
    
    @property
    def all_passed(self) -> bool:
        """Check if all steps passed."""
        return all(step.passed for step in self.steps)
    
    @property
    def passed_count(self) -> int:
        """Count of passed steps."""
        return sum(1 for step in self.steps if step.passed)
    
    @property
    def failed_count(self) -> int:
        """Count of failed steps."""
        return sum(1 for step in self.steps if not step.passed)
    
    def get_first_failure(self) -> Optional[TestStep]:
        """Get the first failed step, if any."""
        for step in self.steps:
            if not step.passed:
                return step
        return None
    
    def print_summary(self) -> None:
        """Print a summary of all test results."""
        print_header(f"{self.test_name} - Results")
        
        if self.all_passed:
            print_success("All tests passed!")
            print()
            print("  Summary:")
            for step in self.steps:
                print(f"  - {step.description}: Passed")
        else:
            print_error(f"Tests failed ({self.failed_count} of {len(self.steps)})")
            print()
            print("  Summary:")
            for step in self.steps:
                status = "Passed" if step.passed else "FAILED"
                print(f"  - {step.description}: {status}")
            
            # Show first failure details
            failure = self.get_first_failure()
            if failure and failure.fix_instructions:
                print()
                print_fix_instructions(failure.fix_instructions)
        
        print(f"\n  Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# ============================================
# Common Error Messages
# ============================================

def get_oauth_error_instructions(platform: str = "API") -> str:
    """
    Get standard OAuth error fix instructions.
    
    Args:
        platform: Platform name for customization
        
    Returns:
        Formatted instructions string
    """
    return f"""
1. Your OAuth tokens may have expired
2. Generate a new refresh token:
   - Go to OAuth Playground (https://developers.google.com/oauthplayground)
   - Configure with your client credentials
   - Authorize the {platform} scope
   - Exchange for refresh token
3. Update the refresh_token in your configuration
4. Run this test again
"""


def get_permission_error_instructions(platform: str = "API") -> str:
    """
    Get standard permission error fix instructions.
    
    Args:
        platform: Platform name for customization
        
    Returns:
        Formatted instructions string
    """
    return f"""
1. Verify your account ID is correct
2. Ensure your credentials have access to the {platform} account
3. Check that the correct permissions are granted:
   - For service accounts: Add to property/account access
   - For OAuth: Ensure the account owner authorized access
4. If using a manager account, configure the login_customer_id
"""


def get_api_not_enabled_instructions(api_name: str, console_url: str) -> str:
    """
    Get instructions for enabling an API.
    
    Args:
        api_name: Name of the API
        console_url: URL to enable the API
        
    Returns:
        Formatted instructions string
    """
    return f"""
The {api_name} is not enabled in your Google Cloud project.

1. Go to: {console_url}
2. Select your project
3. Click 'Enable'
4. Wait a few minutes for the change to take effect
5. Run this test again
"""
