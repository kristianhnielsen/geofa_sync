from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from datetime import datetime
import pandas as pd

from .vk import VK
from .geofa import GeoFA
from .utils import DatabaseConfig


@dataclass
class SyncConfig:
    """Configuration for database synchronization."""

    vk_db_path: str
    """Path to the VK GeoPackage database file."""

    geofa_db_path: str
    """Path to the GeoFA GeoPackage database file."""


@dataclass
class SyncResult:
    """Result of a synchronization operation."""

    total_objects: int
    """Total number of objects processed."""

    already_synced: int
    """Number of objects that already had GeoFA IDs."""

    newly_synced: int
    """Number of objects successfully synced."""

    errors: int
    """Number of errors encountered during sync."""

    sync_details: List[Dict[str, Any]]
    """Detailed results for each synced object."""


class DatabaseSync:
    """
    Manages bi-directional synchronization between VK and GeoFA databases.

    This class handles the core synchronization logic:
    - VK is the data master (changes originate here)
    - GeoFA is the ID master (generates Object IDs)
    - Sync flow: VK → GeoFA → back to VK with IDs

    Key synchronization operations:
    1. sync_new_objects: Create new VK objects in GeoFA and update VK with IDs
    2. sync_updated_objects: Push VK data changes to existing GeoFA objects
    3. sync_updated_at: Sync objects modified since a specific timestamp
    """

    def __init__(self, config: SyncConfig):
        """
        Initialize the DatabaseSync with configuration.

        Args:
            config: SyncConfig containing database paths
        """
        self.config = config
        self.vk: Optional[VK] = None
        self.geofa: Optional[GeoFA] = None

    def __enter__(self):
        """Context manager entry - establish database connections."""
        vk_config = DatabaseConfig(db_path=self.config.vk_db_path)
        gfa_config = DatabaseConfig(db_path=self.config.geofa_db_path)

        self.vk = VK(vk_config)
        self.geofa = GeoFA(gfa_config)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close database connections."""
        if self.vk:
            self.vk.close()
        if self.geofa:
            self.geofa.close()

    def _get_layer_name_from_temakode(self, temakode: int) -> str:
        """
        Get the GeoFA layer name for a given temakode.

        Args:
            temakode: The theme code (5800=point, 5801=polygon, 5802=line)

        Returns:
            Layer name string (e.g., "GeoFA_5800_fac_pkt")
        """
        suffix = "pkt" if temakode == 5800 else "fl" if temakode == 5801 else "li"
        return f"GeoFA_{temakode}_fac_{suffix}"

    def _filter_objects_without_id(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter DataFrame to only include objects that need syncing to GeoFA.

        Objects need syncing if their objekt_id is null, empty, or whitespace.

        Args:
            df: DataFrame with VK objects

        Returns:
            Filtered DataFrame containing only objects needing sync
        """
        return df[df["objekt_id"].isna() | (df["objekt_id"].str.strip() == "")].copy()

    def _create_objects_in_geofa(
        self, objects_df: pd.DataFrame, verbose: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Create objects in GeoFA and return sync results.

        Args:
            objects_df: DataFrame of VK objects to create in GeoFA
            verbose: Whether to print progress messages

        Returns:
            List of sync results with FID, layer_name, new_objekt_id, etc.
        """
        if self.geofa is None:
            raise RuntimeError("GeoFA connection not established. Use context manager.")

        if verbose:
            print(f"\nCreating {len(objects_df)} objects in GeoFA")
            print("-" * 80)

        sync_results = []

        for idx, row in objects_df.iterrows():
            try:
                # Extract object details
                temakode = int(row["temakode"])
                geometry = row["geometry"]
                layer_name = self._get_layer_name_from_temakode(temakode)
                navn = row.get("navn", "Unknown")

                # Create in GeoFA
                new_gfa_id = self.geofa.create_object(temakode, geometry)

                # Store result for updating VK
                sync_results.append(
                    {
                        "fid": idx,  # DataFrame index is the FID
                        "layer_name": layer_name,
                        "new_objekt_id": new_gfa_id,
                        "navn": navn,
                        "temakode": temakode,
                    }
                )

                if verbose:
                    print(f"  ✓ Created {temakode} '{navn}' → {new_gfa_id}")

            except Exception as e:
                if verbose:
                    print(f"  ✗ Error creating object {idx}: {e}")
                continue

        return sync_results

    def _update_vk_with_geofa_ids(
        self, sync_results: List[Dict[str, Any]], verbose: bool = True
    ) -> tuple[int, int]:
        """
        Update VK objects with their GeoFA-generated object IDs.

        Args:
            sync_results: List of sync results from _create_objects_in_geofa
            verbose: Whether to print progress messages

        Returns:
            Tuple of (success_count, error_count)
        """
        if self.vk is None:
            raise RuntimeError("VK connection not established. Use context manager.")

        if verbose:
            print(f"\nUpdating VK with GeoFA object IDs")
            print("-" * 80)

        success_count = 0
        error_count = 0

        for result in sync_results:
            try:
                self.vk.update_objekt_id(
                    result["layer_name"], result["fid"], result["new_objekt_id"]
                )
                success_count += 1

                if verbose:
                    print(f"  ✓ Updated FID {result['fid']} in {result['layer_name']}")

            except Exception as e:
                error_count += 1
                if verbose:
                    print(f"  ✗ Error updating FID {result['fid']}: {e}")

        return success_count, error_count

    def sync_new_objects(
        self, since_date: Optional[datetime] = None, verbose: bool = True
    ) -> SyncResult:
        """
        Sync new objects from VK to GeoFA.

        This method handles objects that exist in VK but don't yet have a GeoFA Object ID.
        The workflow is:
        1. Get all objects created in VK since the specified date (or all objects)
        2. Filter for objects without a GeoFA ID (objekt_id is null/empty)
        3. Create corresponding objects in GeoFA
        4. Update VK with the GeoFA-generated object IDs

        Args:
            since_date: Optional datetime to filter objects created after this date.
                       If None, processes all objects without GeoFA IDs.
            verbose: Whether to print progress messages during sync

        Returns:
            SyncResult object containing sync statistics and details
        """
        if self.vk is None:
            raise RuntimeError("VK connection not established. Use context manager.")

        if verbose:
            print("\n" + "=" * 80)
            print("Syncing New Objects: VK → GeoFA")
            print("=" * 80)

        # Step 1: Get objects from VK
        if since_date:
            if verbose:
                print(f"\nRetrieving objects from VK created since: {since_date}")
            vk_objects = self.vk.get_objects_by_date(since_date)
        else:
            if verbose:
                print("\nRetrieving all objects from VK")
            # Get all objects (implementation depends on VK class capabilities)
            vk_objects = self.vk.get_objects_by_date(
                datetime(2000, 1, 1)
            )  # Far past date to get all

        if verbose:
            print(f"Found {len(vk_objects)} total objects")

        if len(vk_objects) == 0:
            return SyncResult(
                total_objects=0,
                already_synced=0,
                newly_synced=0,
                errors=0,
                sync_details=[],
            )

        # Step 2: Filter for objects without GeoFA ID
        if verbose:
            print("\nIdentifying objects that need GeoFA IDs")

        needs_sync = self._filter_objects_without_id(vk_objects)
        already_synced = len(vk_objects) - len(needs_sync)

        if verbose:
            print(f"Objects already synced: {already_synced}")
            print(f"Objects needing sync: {len(needs_sync)}")

        if len(needs_sync) == 0:
            return SyncResult(
                total_objects=len(vk_objects),
                already_synced=already_synced,
                newly_synced=0,
                errors=0,
                sync_details=[],
            )

        # Step 3: Create objects in GeoFA
        sync_details = self._create_objects_in_geofa(needs_sync, verbose)

        # Step 4: Update VK with GeoFA IDs
        success_count, error_count = self._update_vk_with_geofa_ids(
            sync_details, verbose
        )

        result = SyncResult(
            total_objects=len(vk_objects),
            already_synced=already_synced,
            newly_synced=success_count,
            errors=error_count,
            sync_details=sync_details,
        )

        if verbose:
            self._print_sync_summary(result)

        return result

    def sync_updated_objects(self, object_ids: List[str], verbose: bool = True):
        """
        Sync updated data from VK to GeoFA for specific objects.

        This method handles objects that already have GeoFA IDs but have been
        modified in VK and need their data pushed to GeoFA.

        Args:
            object_ids: List of GeoFA object IDs to sync
            verbose: Whether to print progress messages during sync

        Returns:
            SyncResult object containing sync statistics and details

        Note:
            This method assumes the objects already exist in both databases
            and only updates the data content, not the IDs.
        """
        pass

    def sync_updated_at(self, since_datetime: datetime, verbose: bool = True):
        """
        Sync objects that have been updated in VK since a specific timestamp.

        This method identifies all VK objects modified after the given datetime
        and syncs their updated data to GeoFA. It handles both new objects
        (without GeoFA IDs) and existing objects (with GeoFA IDs).

        Args:
            since_datetime: Datetime threshold - sync objects modified after this time
            verbose: Whether to print progress messages during sync

        Returns:
            SyncResult object containing sync statistics and details
        """
        pass

    def _print_sync_summary(self, result: SyncResult) -> None:
        """
        Print a summary of the sync operation.

        Args:
            result: SyncResult object to summarize
        """
        print("\n" + "=" * 80)
        print("Sync Summary")
        print("=" * 80)
        print(f"Total objects processed: {result.total_objects}")
        print(f"Already synced: {result.already_synced}")
        print(f"Newly synced: {result.newly_synced}")
        print(f"Errors: {result.errors}")
        print("\n✓ Sync operation complete!")
        print("=" * 80)
