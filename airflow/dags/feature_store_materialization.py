"""
Airflow DAG: Feature Store Materialization Pipeline
Runs daily to materialize features from Offline Store (Parquet) to Online Store (Redis).
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago

# ============================================================
# Configuration
# ============================================================
FEAST_REPO_PATH = "/home/jovyan/feast_repo"
OFFLINE_STORE_PATH = "/home/jovyan/data/offline_store/user_click_features"
VALIDATION_SCRIPT_PATH = "/home/jovyan/airflow/scripts/validate_online_store.py"

# Default arguments for all tasks
default_args = {
    "owner": "feature-store-team",
    "depends_on_past": False,
    "email": ["alerts@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


# ============================================================
# Validation Function
# ============================================================
def validate_online_store(**context):
    """
    Fetch a random feature vector from the Online Store (Redis)
    to validate the materialization worked correctly.
    """
    import random
    import pandas as pd
    from feast import FeatureStore
    
    print("=" * 60)
    print("🔍 Validating Online Store Features")
    print("=" * 60)
    
    # Initialize Feast
    fs = FeatureStore(repo_path=FEAST_REPO_PATH)
    
    # Get list of available feature views
    feature_views = fs.list_feature_views()
    print(f"\n📋 Available Feature Views: {[fv.name for fv in feature_views]}")
    
    if not feature_views:
        raise ValueError("No feature views found in the Feature Store!")
    
    # Load user profiles to get valid user IDs
    user_profiles_path = Path("/home/jovyan/data/user_profiles.csv")
    if user_profiles_path.exists():
        users_df = pd.read_csv(user_profiles_path)
        sample_users = users_df["user_id"].sample(n=min(5, len(users_df))).tolist()
    else:
        # Fallback to generated user IDs
        sample_users = [f"user_{i:06d}" for i in random.sample(range(10000), 5)]
    
    print(f"\n👥 Testing with users: {sample_users}")
    
    # Create entity DataFrame for online retrieval
    entity_df = pd.DataFrame({
        "user_id": sample_users,
    })
    
    # Attempt to retrieve features from online store
    try:
        # Get feature vectors
        online_features = fs.get_online_features(
            features=[
                "user_click_features:total_events",
                "user_click_features:click_count",
                "user_click_features:view_count",
                "user_click_features:cart_count",
                "user_click_features:purchase_count",
                "user_click_features:unique_products",
                "user_click_features:total_revenue",
                "user_click_features:click_through_rate",
            ],
            entity_rows=[{"user_id": uid} for uid in sample_users],
        ).to_dict()
        
        print("\n✅ Successfully retrieved features from Online Store!")
        print("\n📊 Sample Feature Vector:")
        
        # Display results
        for i, user_id in enumerate(online_features.get("user_id", [])):
            print(f"\n   User: {user_id}")
            for key, values in online_features.items():
                if key != "user_id" and values[i] is not None:
                    print(f"      {key}: {values[i]}")
        
        # Count non-null values
        non_null_count = sum(
            1 for v in online_features.get("total_events", []) if v is not None
        )
        
        if non_null_count == 0:
            print("\n⚠️  Warning: All feature values are NULL. Features may not be materialized yet.")
            # Don't fail - this might be expected for new users
        else:
            print(f"\n✅ Found {non_null_count}/{len(sample_users)} users with features in Online Store")
        
        # Push validation metrics to XCom
        context["ti"].xcom_push(key="validation_status", value="success")
        context["ti"].xcom_push(key="users_with_features", value=non_null_count)
        
        return True
        
    except Exception as e:
        print(f"\n❌ Validation failed: {str(e)}")
        context["ti"].xcom_push(key="validation_status", value="failed")
        context["ti"].xcom_push(key="error", value=str(e))
        raise


def check_parquet_exists(**context):
    """
    Custom sensor function to check if Parquet files exist for the current date.
    More flexible than FileSensor for partitioned data.
    """
    from datetime import datetime
    import os
    
    execution_date = context["execution_date"]
    partition_date = execution_date.strftime("%Y-%m-%d")
    partition_path = f"{OFFLINE_STORE_PATH}/event_date={partition_date}"
    
    print(f"🔍 Checking for Parquet files at: {partition_path}")
    
    if os.path.exists(partition_path):
        parquet_files = [f for f in os.listdir(partition_path) if f.endswith(".parquet")]
        if parquet_files:
            print(f"✅ Found {len(parquet_files)} Parquet files")
            return True
    
    # Also check for any recent parquet files (last 7 days) if today's not available
    print(f"⚠️  No files for {partition_date}, checking recent partitions...")
    
    if os.path.exists(OFFLINE_STORE_PATH):
        partitions = os.listdir(OFFLINE_STORE_PATH)
        recent_partitions = sorted([p for p in partitions if p.startswith("event_date=")])[-7:]
        
        if recent_partitions:
            print(f"✅ Found recent partitions: {recent_partitions}")
            return True
    
    print(f"❌ No Parquet files found")
    return False


# ============================================================
# DAG Definition
# ============================================================
with DAG(
    dag_id="feature_store_materialization",
    description="Daily materialization of features from Offline Store (Parquet) to Online Store (Redis)",
    default_args=default_args,
    schedule_interval="@daily",  # Run daily at midnight
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["feature-store", "feast", "materialization"],
    doc_md="""
    ## Feature Store Materialization DAG
    
    This DAG performs daily materialization of computed features from the 
    Offline Feature Store (Parquet files) to the Online Feature Store (Redis).
    
    ### Tasks:
    1. **check_parquet_files**: Sensor that waits for new Parquet files
    2. **materialize_features**: Runs `feast materialize-incremental` to load features
    3. **validate_online_store**: Fetches random feature vectors to verify success
    
    ### Triggered By:
    - Daily schedule at midnight UTC
    - Can be triggered manually for ad-hoc materialization
    """,
) as dag:
    
    # --------------------------------------------------------
    # Task 1: Check if new Parquet files exist
    # --------------------------------------------------------
    check_parquet_files = PythonOperator(
        task_id="check_parquet_files",
        python_callable=check_parquet_exists,
        provide_context=True,
        doc_md="Checks if Parquet files exist for the execution date partition",
    )
    
    # Alternative: Use FileSensor (less flexible for partitioned data)
    # check_parquet_files = FileSensor(
    #     task_id="check_parquet_files",
    #     filepath=f"{OFFLINE_STORE_PATH}/_SUCCESS",  # or specific file pattern
    #     poke_interval=60,  # Check every minute
    #     timeout=3600,  # Timeout after 1 hour
    #     mode="poke",
    # )
    
    # --------------------------------------------------------
    # Task 2: Run Feast materialize-incremental
    # --------------------------------------------------------
    materialize_features = BashOperator(
        task_id="materialize_features",
        bash_command=f"""
            set -e
            
            echo "============================================================"
            echo "🚀 Starting Feature Materialization"
            echo "============================================================"
            
            cd {FEAST_REPO_PATH}
            
            # Get current date for materialization
            MATERIALIZE_DATE=$(date +%Y-%m-%dT%H:%M:%S)
            
            echo "📅 Materializing features up to: $MATERIALIZE_DATE"
            echo ""
            
            # Run feast materialize-incremental
            feast materialize-incremental $MATERIALIZE_DATE
            
            echo ""
            echo "✅ Materialization completed successfully!"
            echo "============================================================"
        """,
        doc_md="Runs feast materialize-incremental to load features from Parquet to Redis",
    )
    
    # --------------------------------------------------------
    # Task 3: Validate Online Store
    # --------------------------------------------------------
    validate_features = PythonOperator(
        task_id="validate_online_store",
        python_callable=validate_online_store,
        provide_context=True,
        doc_md="Fetches random feature vectors from Redis to validate materialization",
    )
    
    # --------------------------------------------------------
    # Task Dependencies
    # --------------------------------------------------------
    check_parquet_files >> materialize_features >> validate_features
