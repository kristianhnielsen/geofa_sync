from shapely.geometry import Point
from database.utils import clone_databases
from sync.vk import VK, make_datetime
from sync.db_sync import DatabaseSync, SyncConfig
from sync.utils import DatabaseConfig
from dotenv import load_dotenv
from datetime import datetime, timedelta


def sync_new_objects_using_database_sync():
    """
    Example workflow: Sync new objects from VK to GeoFA using DatabaseSync class.

    This demonstrates the complete sync process using the DatabaseSync abstraction:
    1. Configure database paths
    2. Use DatabaseSync.sync_new_objects() to handle the entire workflow
    3. Results are returned in a SyncResult object
    """
    print("=" * 80)
    print("VK → GeoFA Sync Workflow (Using DatabaseSync Class)")
    print("=" * 80)

    # Configure database paths
    sync_config = SyncConfig(vk_db_path="vk.gpkg", geofa_db_path="geofa.gpkg")

    # Calculate date threshold (looking back 720 days for test data)
    days_back = 720
    threshold_date = datetime.now() - timedelta(days=days_back)
    start_date = make_datetime(
        threshold_date.year, threshold_date.month, threshold_date.day, 0, 0, 0
    )

    print(f"\nSearching for VK objects created since: {start_date}")
    print("-" * 80)

    # Use DatabaseSync context manager to handle connections
    with DatabaseSync(sync_config) as db_sync:
        # Sync all new objects (without GeoFA IDs) created since start_date
        result = db_sync.sync_new_objects(since_date=start_date, verbose=True)

    # The sync result contains all the statistics
    print(f"\n✓ Sync complete!")
    print(f"  Total objects: {result.total_objects}")
    print(f"  Already synced: {result.already_synced}")
    print(f"  Newly synced: {result.newly_synced}")
    print(f"  Errors: {result.errors}")


def test_database_sync():
    """
    Comprehensive test of the DatabaseSync class functionality.

    This demonstrates:
    1. Database setup (cloning)
    2. Creating a test object
    3. Syncing new objects using DatabaseSync
    4. Viewing sync results
    """
    print("=" * 80)
    print("Testing DatabaseSync Class")
    print("=" * 80)

    # Step 1: Clone databases (setup test environment)
    print("\n[1/3] Setting up test databases...")
    clone_databases()

    # Step 2: Create a test dummy object (without GeoFA ID)
    print("\n[2/3] Creating test dummy object for sync demonstration...")
    vk = VK(config=DatabaseConfig(db_path="vk.gpkg"))
    vk.create_dummy_object(5800, Point(0, 0), oprettet=make_datetime(2025, 10, 1))
    vk.close()

    # Step 3: Sync using DatabaseSync class
    print("\n[3/3] Running sync with DatabaseSync class...")
    sync_new_objects_using_database_sync()


def main():
    """
    Main entry point for the GeoFA sync application.

    This demonstrates the DatabaseSync class which provides a clean,
    object-oriented interface for synchronizing data between VK and GeoFA databases.
    """
    print("GeoFA Sync Application")
    print("=" * 80)

    # Run comprehensive test of DatabaseSync
    test_database_sync()

    print("\n" + "=" * 80)
    print("Test completed successfully!")
    print("=" * 80)


if __name__ == "__main__":
    load_dotenv()
    main()
