"""
Feast Entity and Feature View Definitions for ML Feature Store
Simplified version - only user profile features for now.
Click features will be added once Spark streaming creates the Parquet files.
"""

from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource, ValueType
from feast.types import Float64, Int64, String

# ============================================================
# Entities
# ============================================================

user_entity = Entity(
    name="user",
    join_keys=["user_id"],
    value_type=ValueType.STRING,
    description="A user of the e-commerce platform",
)

# ============================================================
# Data Sources
# ============================================================

# User profiles (batch source - Parquet) with absolute path
user_profiles_source = FileSource(
    name="user_profiles_source",
    path="/home/onlyahad/Desktop/ML Feature Store/data/user_profiles.parquet",
    timestamp_field="signup_date",
    description="Static user profile data from batch generator",
)

# ============================================================
# Feature Views
# ============================================================

# User profile features (static/batch)
user_profile_features = FeatureView(
    name="user_profile_features",
    entities=[user_entity],
    ttl=timedelta(days=365),  # Static data, refresh yearly
    schema=[
        Field(name="age", dtype=Int64),
        Field(name="country", dtype=String),
        Field(name="subscription_tier", dtype=String),
        Field(name="activity_weight", dtype=Float64),
    ],
    source=user_profiles_source,
    online=True,
    description="Static user profile features from batch data",
    tags={"team": "feature-store", "source": "batch"},
)

# NOTE: User click features will be added once Spark streaming
# produces Parquet files at data/offline_store/user_click_features/
# See features_streaming.py for the full definition
