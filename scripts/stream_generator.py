"""
Stream Data Generator for ML Feature Store
Generates high-volume clickstream events and pushes to Kafka.
Users with higher activity_weight generate more events (realistic distribution drift).
"""

import argparse
import csv
import json
import random
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Initialize Faker
fake = Faker()

# Configuration
DEFAULT_EVENTS_PER_SECOND = 10
DEFAULT_KAFKA_BOOTSTRAP = "localhost:29094"  # External host access
DEFAULT_TOPIC = "user_clicks"
USER_PROFILES_PATH = Path(__file__).parent.parent / "data" / "user_profiles.csv"

# Event types with probabilities
EVENT_TYPES = [
    ("view", 0.50),      # 50% views
    ("click", 0.30),     # 30% clicks
    ("cart", 0.15),      # 15% add to cart
    ("purchase", 0.05),  # 5% purchases
]

# Product categories with realistic distributions
PRODUCT_CATEGORIES = [
    ("electronics", 0.25),
    ("clothing", 0.20),
    ("home", 0.15),
    ("books", 0.12),
    ("sports", 0.10),
    ("beauty", 0.08),
    ("toys", 0.05),
    ("food", 0.05),
]

# Session behavior
SESSION_DURATION_MINUTES = (5, 45)  # Min-max session duration
EVENTS_PER_SESSION = (3, 30)        # Min-max events per session


class GracefulKiller:
    """Handle graceful shutdown on SIGINT/SIGTERM."""
    kill_now = False
    
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
    
    def exit_gracefully(self, *args):
        print("\n🛑 Shutdown signal received. Finishing current batch...")
        self.kill_now = True


class UserPool:
    """Manages user selection with activity-based weighting."""
    
    def __init__(self, profiles_path: Path):
        self.users: List[Dict[str, Any]] = []
        self.weights: List[float] = []
        self.load_profiles(profiles_path)
    
    def load_profiles(self, path: Path) -> None:
        """Load user profiles from CSV."""
        if not path.exists():
            print(f"⚠️  User profiles not found at {path}")
            print("   Run batch_generator.py first to create user profiles.")
            print("   Generating synthetic user IDs for now...")
            self._generate_synthetic_users()
            return
        
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.users.append({
                    "user_id": row["user_id"],
                    "country": row["country"],
                    "tier": row["subscription_tier"],
                })
                self.weights.append(float(row["activity_weight"]))
        
        print(f"✅ Loaded {len(self.users):,} user profiles")
        print(f"   Weighted selection enabled (high-activity users generate more events)")
    
    def _generate_synthetic_users(self) -> None:
        """Generate synthetic users if profiles don't exist."""
        for i in range(10_000):
            self.users.append({
                "user_id": f"user_{i:06d}",
                "country": "Unknown",
                "tier": "free",
            })
            self.weights.append(random.uniform(0.3, 1.5))
    
    def get_random_user(self) -> Dict[str, Any]:
        """Get a random user weighted by activity level."""
        return random.choices(self.users, weights=self.weights, k=1)[0]


class StreamGenerator:
    """Generates clickstream events and pushes to Kafka."""
    
    def __init__(
        self,
        kafka_bootstrap: str,
        topic: str,
        events_per_second: int,
        user_pool: UserPool,
    ):
        self.topic = topic
        self.events_per_second = events_per_second
        self.user_pool = user_pool
        self.producer: Optional[KafkaProducer] = None
        self.events_sent = 0
        self.errors = 0
        self.start_time = None
        
        self._connect_kafka(kafka_bootstrap)
    
    def _connect_kafka(self, bootstrap_servers: str) -> None:
        """Connect to Kafka with retries."""
        max_retries = 5
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    acks="all",
                    retries=3,
                    max_in_flight_requests_per_connection=1,
                )
                print(f"✅ Connected to Kafka at {bootstrap_servers}")
                return
            except KafkaError as e:
                print(f"⚠️  Kafka connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    print(f"   Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
        
        print("❌ Failed to connect to Kafka. Exiting.")
        sys.exit(1)
    
    def _generate_product_id(self, category: str) -> str:
        """Generate a product ID within a category."""
        # Products per category (some categories have more products)
        products_per_category = {
            "electronics": 500,
            "clothing": 1000,
            "home": 300,
            "books": 2000,
            "sports": 400,
            "beauty": 600,
            "toys": 250,
            "food": 150,
        }
        
        max_id = products_per_category.get(category, 200)
        product_num = random.randint(1, max_id)
        return f"{category[:3].upper()}_{product_num:05d}"
    
    def _weighted_choice(self, choices: List[tuple]) -> Any:
        """Select from weighted choices."""
        items, weights = zip(*choices)
        return random.choices(items, weights=weights, k=1)[0]
    
    def generate_event(self) -> Dict[str, Any]:
        """Generate a single clickstream event."""
        user = self.user_pool.get_random_user()
        event_type = self._weighted_choice(EVENT_TYPES)
        category = self._weighted_choice(PRODUCT_CATEGORIES)
        product_id = self._generate_product_id(category)
        
        # More specific event data
        event = {
            "event_id": fake.uuid4(),
            "user_id": user["user_id"],
            "product_id": product_id,
            "product_category": category,
            "event_type": event_type,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "session_id": fake.uuid4()[:8],  # Short session ID
            "device_type": random.choice(["mobile", "desktop", "tablet"]),
            "user_country": user["country"],
            "user_tier": user["tier"],
        }
        
        # Add event-specific fields
        if event_type == "view":
            event["view_duration_sec"] = random.randint(3, 120)
        elif event_type == "click":
            event["click_position"] = random.randint(1, 20)
            event["referrer"] = random.choice(["search", "recommendation", "homepage", "email", "direct"])
        elif event_type == "cart":
            event["quantity"] = random.choices([1, 2, 3, 4, 5], weights=[0.6, 0.2, 0.1, 0.05, 0.05])[0]
        elif event_type == "purchase":
            event["quantity"] = random.choices([1, 2, 3], weights=[0.7, 0.2, 0.1])[0]
            event["price"] = round(random.uniform(9.99, 499.99), 2)
        
        return event
    
    def send_event(self, event: Dict[str, Any]) -> bool:
        """Send event to Kafka."""
        try:
            future = self.producer.send(
                self.topic,
                key=event["user_id"],
                value=event,
            )
            # Don't block on every message - let Kafka batch them
            return True
        except KafkaError as e:
            self.errors += 1
            return False
    
    def run(self, duration_seconds: Optional[int] = None) -> None:
        """Run the stream generator."""
        killer = GracefulKiller()
        self.start_time = time.time()
        interval = 1.0 / self.events_per_second
        
        print(f"\n🚀 Starting stream generation")
        print(f"   Topic: {self.topic}")
        print(f"   Rate: {self.events_per_second} events/sec")
        if duration_seconds:
            print(f"   Duration: {duration_seconds} seconds")
        print(f"   Press Ctrl+C to stop\n")
        
        last_stats_time = time.time()
        
        try:
            while not killer.kill_now:
                # Check duration limit
                if duration_seconds and (time.time() - self.start_time) >= duration_seconds:
                    print("\n⏱️  Duration limit reached.")
                    break
                
                # Generate and send event
                event = self.generate_event()
                if self.send_event(event):
                    self.events_sent += 1
                
                # Print stats every 5 seconds
                if time.time() - last_stats_time >= 5:
                    self._print_stats()
                    last_stats_time = time.time()
                
                # Rate limiting
                time.sleep(interval)
        
        finally:
            self._shutdown()
    
    def _print_stats(self) -> None:
        """Print current statistics."""
        elapsed = time.time() - self.start_time
        rate = self.events_sent / elapsed if elapsed > 0 else 0
        print(f"📊 Events: {self.events_sent:,} | Rate: {rate:.1f}/sec | Errors: {self.errors}")
    
    def _shutdown(self) -> None:
        """Clean shutdown."""
        print("\n🔄 Flushing remaining events...")
        if self.producer:
            self.producer.flush(timeout=10)
            self.producer.close()
        
        elapsed = time.time() - self.start_time
        print(f"\n✨ Stream generation complete!")
        print(f"   Total events: {self.events_sent:,}")
        print(f"   Duration: {elapsed:.1f} seconds")
        print(f"   Average rate: {self.events_sent / elapsed:.1f} events/sec")
        print(f"   Errors: {self.errors}")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate clickstream events and push to Kafka",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-r", "--rate",
        type=int,
        default=DEFAULT_EVENTS_PER_SECOND,
        help="Events per second",
    )
    parser.add_argument(
        "-d", "--duration",
        type=int,
        default=None,
        help="Duration in seconds (None = run indefinitely)",
    )
    parser.add_argument(
        "-b", "--bootstrap",
        type=str,
        default=DEFAULT_KAFKA_BOOTSTRAP,
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "-t", "--topic",
        type=str,
        default=DEFAULT_TOPIC,
        help="Kafka topic name",
    )
    parser.add_argument(
        "-p", "--profiles",
        type=str,
        default=str(USER_PROFILES_PATH),
        help="Path to user profiles CSV",
    )
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    
    print("=" * 60)
    print("🌊 Clickstream Event Generator")
    print("=" * 60)
    
    # Load user profiles
    user_pool = UserPool(Path(args.profiles))
    
    # Create and run generator
    generator = StreamGenerator(
        kafka_bootstrap=args.bootstrap,
        topic=args.topic,
        events_per_second=args.rate,
        user_pool=user_pool,
    )
    
    generator.run(duration_seconds=args.duration)


if __name__ == "__main__":
    main()
