"""
Batch Data Generator for ML Feature Store
Generates static user profiles CSV with realistic distributions.
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

from faker import Faker

# Initialize Faker with seed for reproducibility
fake = Faker()
Faker.seed(42)
random.seed(42)

# Configuration
NUM_USERS = 10_000
OUTPUT_DIR = Path(__file__).parent.parent / "data"
OUTPUT_FILE = OUTPUT_DIR / "user_profiles.csv"

# Realistic distributions
COUNTRIES = [
    ("United States", 0.35),
    ("United Kingdom", 0.12),
    ("Germany", 0.10),
    ("Canada", 0.08),
    ("France", 0.07),
    ("Australia", 0.06),
    ("India", 0.08),
    ("Brazil", 0.05),
    ("Japan", 0.04),
    ("Other", 0.05),
]

SUBSCRIPTION_TIERS = [
    ("free", 0.60),       # 60% free users
    ("basic", 0.25),      # 25% basic
    ("premium", 0.12),    # 12% premium
    ("enterprise", 0.03), # 3% enterprise
]

# Age distribution (skewed towards younger demographics for e-commerce)
AGE_RANGES = [
    ((18, 24), 0.20),
    ((25, 34), 0.35),
    ((35, 44), 0.20),
    ((45, 54), 0.12),
    ((55, 64), 0.08),
    ((65, 80), 0.05),
]


def weighted_choice(choices: List[tuple]) -> Any:
    """Select from weighted choices."""
    items, weights = zip(*choices)
    return random.choices(items, weights=weights, k=1)[0]


def generate_age() -> int:
    """Generate age with realistic distribution."""
    age_range = weighted_choice(AGE_RANGES)
    return random.randint(age_range[0], age_range[1])


def generate_signup_date() -> str:
    """
    Generate signup date with realistic distribution.
    More recent signups are more likely (exponential-ish distribution).
    """
    # Range: from 3 years ago to today
    days_ago = int(random.expovariate(1 / 365) % 1095)  # ~3 years max
    signup_date = datetime.now() - timedelta(days=days_ago)
    return signup_date.strftime("%Y-%m-%d")


def generate_activity_weight(tier: str, age: int, signup_days: int) -> float:
    """
    Generate activity weight for each user.
    This determines how likely they are to generate events in the stream.
    
    Premium users are more active, younger users too, newer signups are more engaged.
    """
    tier_weights = {"free": 0.3, "basic": 0.6, "premium": 0.9, "enterprise": 1.0}
    
    # Base weight from tier
    weight = tier_weights.get(tier, 0.3)
    
    # Age factor (younger = slightly more active)
    if age < 35:
        weight *= 1.2
    elif age > 55:
        weight *= 0.7
    
    # Recency factor (newer users more engaged, but very new might not be)
    if signup_days < 30:
        weight *= 0.5  # Still learning
    elif signup_days < 180:
        weight *= 1.3  # Peak engagement
    elif signup_days > 730:
        weight *= 0.6  # Declining engagement
    
    # Add some randomness
    weight *= random.uniform(0.5, 1.5)
    
    return min(weight, 2.0)  # Cap at 2.0


def generate_user_profiles() -> List[Dict[str, Any]]:
    """Generate all user profiles with realistic distributions."""
    users = []
    
    for i in range(NUM_USERS):
        user_id = f"user_{i:06d}"  # Consistent format: user_000000 to user_009999
        age = generate_age()
        country = weighted_choice(COUNTRIES)
        signup_date = generate_signup_date()
        tier = weighted_choice(SUBSCRIPTION_TIERS)
        
        # Calculate days since signup for activity weight
        signup_datetime = datetime.strptime(signup_date, "%Y-%m-%d")
        signup_days = (datetime.now() - signup_datetime).days
        
        activity_weight = generate_activity_weight(tier, age, signup_days)
        
        users.append({
            "user_id": user_id,
            "age": age,
            "country": country,
            "signup_date": signup_date,
            "subscription_tier": tier,
            "activity_weight": round(activity_weight, 3),  # For stream generator
        })
    
    return users


def save_to_csv(users: List[Dict[str, Any]], filepath: Path) -> None:
    """Save users to CSV file."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    fieldnames = ["user_id", "age", "country", "signup_date", "subscription_tier", "activity_weight"]
    
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(users)
    
    print(f"✅ Generated {len(users):,} user profiles")
    print(f"📁 Saved to: {filepath}")


def print_statistics(users: List[Dict[str, Any]]) -> None:
    """Print distribution statistics for verification."""
    print("\n📊 Data Distribution Statistics:")
    print("-" * 50)
    
    # Country distribution
    country_counts = {}
    for user in users:
        country_counts[user["country"]] = country_counts.get(user["country"], 0) + 1
    
    print("\n🌍 Country Distribution:")
    for country, count in sorted(country_counts.items(), key=lambda x: -x[1])[:5]:
        print(f"   {country}: {count:,} ({count/len(users)*100:.1f}%)")
    
    # Tier distribution
    tier_counts = {}
    for user in users:
        tier_counts[user["subscription_tier"]] = tier_counts.get(user["subscription_tier"], 0) + 1
    
    print("\n💎 Subscription Tier Distribution:")
    for tier in ["free", "basic", "premium", "enterprise"]:
        count = tier_counts.get(tier, 0)
        print(f"   {tier}: {count:,} ({count/len(users)*100:.1f}%)")
    
    # Age statistics
    ages = [user["age"] for user in users]
    print(f"\n👤 Age Statistics:")
    print(f"   Mean: {sum(ages)/len(ages):.1f}")
    print(f"   Min: {min(ages)}, Max: {max(ages)}")
    
    # Activity weight distribution
    weights = [user["activity_weight"] for user in users]
    print(f"\n⚡ Activity Weight Statistics:")
    print(f"   Mean: {sum(weights)/len(weights):.2f}")
    print(f"   Min: {min(weights):.2f}, Max: {max(weights):.2f}")
    
    # High activity users (weight > 1.0)
    high_activity = sum(1 for w in weights if w > 1.0)
    print(f"   High activity (>1.0): {high_activity:,} ({high_activity/len(users)*100:.1f}%)")


def main():
    """Main entry point."""
    print("🚀 Starting Batch Data Generation")
    print("=" * 50)
    
    users = generate_user_profiles()
    save_to_csv(users, OUTPUT_FILE)
    print_statistics(users)
    
    print("\n" + "=" * 50)
    print("✨ Batch generation complete!")
    print(f"   Users can be loaded with: pd.read_csv('{OUTPUT_FILE}')")


if __name__ == "__main__":
    main()
