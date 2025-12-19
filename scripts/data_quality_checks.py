"""
Data Quality Checks using Great Expectations
Validates incoming batch data (user profiles) before feature engineering.
Compatible with Great Expectations v0.18+
"""

import pandas as pd
import great_expectations as gx
from great_expectations.expectations import (
    ExpectColumnValuesToNotBeNull,
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToBeInSet,
    ExpectColumnValuesToBeUnique,
)
from typing import Tuple, Dict, Any, List


def validate_user_profiles(
    df: pd.DataFrame,
    fail_on_error: bool = True,
) -> Tuple[bool, Dict[str, Any]]:
    """
    Validate user profile DataFrame using Great Expectations.
    
    Args:
        df: Pandas DataFrame with user profile data
        fail_on_error: If True, raises exception on validation failure
    
    Returns:
        Tuple of (success: bool, results: dict)
    
    Raises:
        ValueError: If validation fails and fail_on_error is True
    """
    print("=" * 60)
    print("🔍 Data Quality Check: User Profiles")
    print("=" * 60)
    print(f"\n📊 Input: {len(df):,} rows, {len(df.columns)} columns")
    
    # Create context
    context = gx.get_context()
    
    # Create data source and add DataFrame
    data_source = context.data_sources.add_pandas("pandas_source")
    data_asset = data_source.add_dataframe_asset(name="user_profiles")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("full_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    
    # Define expectations
    expectations = [
        # 1. Age should not be null
        ExpectColumnValuesToNotBeNull(column="age"),
        
        # 2. Age should be between 18 and 120
        ExpectColumnValuesToBeBetween(column="age", min_value=18, max_value=120),
        
        # 3. Subscription tier must be in valid set
        ExpectColumnValuesToBeInSet(
            column="subscription_tier",
            value_set=["free", "basic", "premium", "enterprise"],
        ),
        
        # 4. User ID should not be null
        ExpectColumnValuesToNotBeNull(column="user_id"),
        
        # 5. User ID should be unique
        ExpectColumnValuesToBeUnique(column="user_id"),
        
        # 6. Country should not be null
        ExpectColumnValuesToNotBeNull(column="country"),
        
        # 7. Activity weight should be positive and bounded
        ExpectColumnValuesToBeBetween(
            column="activity_weight", min_value=0.0, max_value=10.0
        ),
    ]
    
    # Create expectation suite
    suite = context.suites.add(
        gx.ExpectationSuite(name="user_profiles_suite", expectations=expectations)
    )
    
    # Validate
    validation_result = batch.validate(suite)
    
    # Process results
    success = validation_result.success
    results_list = validation_result.results
    
    # Count successes and failures
    passed = sum(1 for r in results_list if r.success)
    failed = len(results_list) - passed
    
    print(f"\n📈 Validation Results:")
    print(f"   Total expectations: {len(results_list)}")
    print(f"   Passed: {passed}")
    print(f"   Failed: {failed}")
    print(f"   Success rate: {passed / len(results_list) * 100:.1f}%")
    
    # Print details
    print(f"\n📋 Expectation Details:")
    for result in results_list:
        exp_type = type(result.expectation_config).__name__
        column = getattr(result.expectation_config, 'column', 'N/A')
        status = "✅" if result.success else "❌"
        print(f"   {status} {exp_type} on '{column}'")
        
        if not result.success:
            unexpected_count = result.result.get("unexpected_count", 0)
            unexpected_percent = result.result.get("unexpected_percent", 0)
            print(f"      Unexpected: {unexpected_count} ({unexpected_percent:.2f}%)")
            
            # Show sample unexpected values
            unexpected_list = result.result.get("partial_unexpected_list", [])
            if unexpected_list:
                print(f"      Sample: {unexpected_list[:5]}")
    
    # Prepare results dictionary
    failed_expectations = [
        {
            "type": type(r.expectation_config).__name__,
            "column": getattr(r.expectation_config, 'column', None),
            "unexpected_count": r.result.get("unexpected_count", 0),
        }
        for r in results_list if not r.success
    ]
    
    results_dict = {
        "success": success,
        "total_expectations": len(results_list),
        "passed": passed,
        "failed": failed,
        "failed_expectations": failed_expectations,
    }
    
    print("\n" + "=" * 60)
    if success:
        print("✅ DATA QUALITY CHECK PASSED")
    else:
        print("❌ DATA QUALITY CHECK FAILED")
        if fail_on_error:
            raise ValueError(
                f"Data quality check failed! {failed} expectation(s) failed."
            )
    print("=" * 60)
    
    return success, results_dict


def run_checkpoint_on_csv(
    csv_path: str,
    fail_on_error: bool = True,
) -> Tuple[bool, Dict[str, Any]]:
    """
    Run validation on a CSV file.
    
    Args:
        csv_path: Path to the CSV file
        fail_on_error: If True, raises exception on validation failure
    
    Returns:
        Tuple of (success: bool, results: dict)
    """
    print(f"📁 Loading data from: {csv_path}")
    df = pd.read_csv(csv_path)
    return validate_user_profiles(df, fail_on_error=fail_on_error)


if __name__ == "__main__":
    import sys
    from pathlib import Path
    
    csv_path = Path(__file__).parent.parent / "data" / "user_profiles.csv"
    
    if len(sys.argv) > 1:
        csv_path = Path(sys.argv[1])
    
    try:
        success, results = run_checkpoint_on_csv(str(csv_path), fail_on_error=False)
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)
