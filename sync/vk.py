from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import geopandas as gpd
from typing import Optional, Union
from datetime import datetime
import pandas as pd

from .utils import DatabaseConfig


def make_datetime(
    year: int,
    month: int = 1,
    day: int = 1,
    hour: int = 0,
    minute: int = 0,
    second: int = 0,
) -> datetime:
    """
    Helper function to create a timezone-aware datetime in UTC.

    Args:
        year: Year (e.g., 2024)
        month: Month (1-12), defaults to 1
        day: Day (1-31), defaults to 1
        hour: Hour (0-23), defaults to 0
        minute: Minute (0-59), defaults to 0
        second: Second (0-59), defaults to 0

    Returns:
        datetime: Timezone-aware datetime in UTC

    Examples:
        # Simple date
        make_datetime(2024, 6, 1)  # June 1, 2024 00:00:00 UTC

        # With time
        make_datetime(2024, 6, 30, 23, 59, 59)  # June 30, 2024 23:59:59 UTC

        # Year only
        make_datetime(2024)  # January 1, 2024 00:00:00 UTC
    """
    return datetime(
        year, month, day, hour, minute, second, tzinfo=pd.Timestamp.now("UTC").tzinfo
    )


class VK:
    """
    A class to interact with the VK (Vejle Kommune) database.

    Handles database connections and operations for the Vejle Kommune geospatial database.
    """

    def __init__(self, config: DatabaseConfig):
        """
        Initializes the VK database connection.

        Args:
            config (DatabaseConfig): Database configuration containing path and driver info.
        """
        self.config = config
        self.engine: Optional[Engine] = None
        self._connect()

    def _connect(self):
        """
        Establishes a connection to the GeoPackage database using SQLAlchemy.
        This is an internal method.
        """
        try:
            # Create SQLAlchemy engine for GeoPackage
            connection_string = f"sqlite:///{self.config.db_path}"
            self.engine = create_engine(connection_string)
            print(f"Connected to VK database at: {self.config.db_path}")
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise

    def get_layer(self, layer_name: str) -> gpd.GeoDataFrame:
        """
        Reads a layer from the GeoPackage database.

        Args:
            layer_name (str): Name of the layer to read (e.g., 'GeoFA_5800_fac_pkt').

        Returns:
            gpd.GeoDataFrame: The layer data as a GeoDataFrame.
        """
        try:
            gdf = gpd.read_file(self.config.db_path, layer=layer_name)
            print(f"Successfully read layer '{layer_name}' with {len(gdf)} features.")
            return gdf
        except Exception as e:
            print(f"Error reading layer '{layer_name}': {e}")
            raise

    def get_objects_by_date(
        self,
        start: datetime,
        end: Optional[datetime] = None,
        layer_name: Optional[str] = None,
    ) -> gpd.GeoDataFrame:
        """
        Gets all objects created within a datetime range.

        Args:
            start (datetime): Start datetime (inclusive). Objects created on or after this time.
                Use make_datetime() helper to create timezone-aware datetimes.
            end (datetime, optional): End datetime (inclusive). If None, gets all objects from start onwards.
                Use make_datetime() helper to create timezone-aware datetimes.
            layer_name (str, optional): Specific layer to query. If None, queries all layers.

        Returns:
            gpd.GeoDataFrame: GeoDataFrame containing objects created within the datetime range.

        Note:
            Datetimes are automatically converted to UTC if timezone-naive.
            For best results, use the make_datetime() helper function.

        Examples:
            # Recommended: Use make_datetime() helper
            vk.get_objects_by_date(make_datetime(2024, 6, 1))

            # Get objects in a date range
            vk.get_objects_by_date(make_datetime(2024, 6, 1), make_datetime(2024, 6, 30))

            # Get objects from specific layer only
            vk.get_objects_by_date(make_datetime(2024), layer_name='GeoFA_5800_fac_pkt')
        """
        try:
            # Define all VK layers
            all_layers = [
                "GeoFA_5800_fac_pkt",
                "GeoFA_5801_fac_fl",
                "GeoFA_5802_fac_li",
            ]

            # Use specific layer if provided, otherwise use all layers
            layers_to_query = [layer_name] if layer_name else all_layers

            # Ensure datetimes are timezone-aware (UTC)
            start = (
                make_datetime(
                    start.year,
                    start.month,
                    start.day,
                    start.hour,
                    start.minute,
                    start.second,
                )
                if start is not None
                else start
            )
            end = (
                make_datetime(
                    end.year, end.month, end.day, end.hour, end.minute, end.second
                )
                if end is not None
                else end
            )

            # Build filter description for logging
            if end is None:
                filter_desc = f"since {start}"
            else:
                filter_desc = f"between {start} and {end}"

            results = []
            for layer in layers_to_query:
                gdf = self.get_layer(layer)

                # Filter by 'oprettet' (created) timestamp
                if "oprettet" in gdf.columns:
                    # Ensure the column is datetime type
                    if not pd.api.types.is_datetime64_any_dtype(gdf["oprettet"]):
                        gdf["oprettet"] = pd.to_datetime(gdf["oprettet"])

                    # Apply filter based on whether end date is provided
                    if end is None:
                        filtered = gdf[gdf["oprettet"] >= start]
                    else:
                        filtered = gdf[
                            (gdf["oprettet"] >= start)
                            & (gdf["oprettet"] <= end)
                            & (gdf["cvr_kode"] == 29189900)
                        ]

                    if len(filtered) > 0:
                        results.append(filtered)
                        print(
                            f"  Found {len(filtered)} objects in '{layer}' created {filter_desc}"
                        )
                else:
                    print(f"  Warning: 'oprettet' column not found in layer '{layer}'")

            # Combine results from all layers
            if results:
                combined = gpd.GeoDataFrame(
                    pd.concat(results, ignore_index=True), crs=results[0].crs
                )
                print(f"\nTotal objects created {filter_desc}: {len(combined)}")
                return combined
            else:
                print(f"\nNo objects found created {filter_desc}")
                # Return empty GeoDataFrame with schema from first layer
                empty_gdf = self.get_layer(layers_to_query[0])
                return empty_gdf.iloc[0:0]

        except Exception as e:
            print(f"Error getting objects by date: {e}")
            raise

    def update_objekt_id(self, layer_name: str, fid: int, new_objekt_id: str) -> None:
        """
        Updates the objekt_id for a specific object in the VK database.

        This is typically used after creating a new object in GeoFA to sync the
        GeoFA-generated UUID back to the VK database.

        Args:
            layer_name (str): Name of the layer (e.g., 'GeoFA_5800_fac_pkt').
            fid (int): The feature ID (primary key) of the object to update.
            new_objekt_id (str): The new objekt_id (UUID) to set.

        Examples:
            # Update a specific object's GeoFA ID
            vk.update_objekt_id('GeoFA_5800_fac_pkt', fid=123, new_objekt_id='abc-def-123')

            # Typical sync workflow
            vk_obj = vk.get_layer('GeoFA_5800_fac_pkt').iloc[0]  # Get object
            fid = vk_obj.name  # Get the fid (index)
            gfa_id = geofa.create_object(...)  # Create in GeoFA, get UUID
            vk.update_objekt_id('GeoFA_5800_fac_pkt', fid, gfa_id)  # Sync back to VK
        """
        try:
            # Read the entire layer
            gdf = gpd.read_file(self.config.db_path, layer=layer_name)

            # Check if fid exists
            if fid not in gdf.index:
                raise ValueError(
                    f"FID {fid} not found in layer '{layer_name}'. "
                    f"Valid range: 0 to {len(gdf)-1}"
                )

            # Update the objekt_id for the specific row
            gdf.loc[fid, "objekt_id"] = new_objekt_id

            # Write back to the database (overwrites the layer)
            gdf.to_file(
                self.config.db_path, layer=layer_name, driver=self.config.driver
            )

            print(
                f"Updated objekt_id for FID {fid} in '{layer_name}' to: {new_objekt_id}"
            )

        except Exception as e:
            print(f"Error updating objekt_id: {e}")
            raise

    def create_dummy_object(
        self,
        temakode: int,
        geometry,
        navn: str = "Test Object",
        clear_objekt_id: bool = True,
        oprettet: Optional[datetime] = None,
    ) -> int:
        """
        Creates a new dummy/test object in the VK database.

        This is useful for testing sync workflows without modifying real data.
        The object is created with minimal required fields and can optionally
        have its objekt_id cleared to simulate a new object needing sync.

        Args:
            temakode (int): The theme code (5800=point, 5801=polygon, 5802=line).
            geometry: Shapely geometry object (Point, Polygon, or LineString).
            navn (str): Name for the test object. Defaults to "Test Object".
            clear_objekt_id (bool): If True, sets objekt_id to empty string to simulate
                a new object that needs GeoFA ID. If False, generates a temporary UUID.
                Defaults to True.
            oprettet (Optional[datetime]): The creation date for the object. If None,
                uses the current timestamp. Defaults to None.

        Returns:
            int: The FID (feature ID) of the newly created object.

        Examples:
            # Create a test point without GeoFA ID (for testing sync)
            from shapely.geometry import Point
            fid = vk.create_dummy_object(5800, Point(10.0, 55.0))

            # Create a test polygon with temporary ID
            from shapely.geometry import Polygon
            fid = vk.create_dummy_object(
                5801,
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                navn="Test Area",
                clear_objekt_id=False
            )

            # Create a test line for sync testing
            from shapely.geometry import LineString
            fid = vk.create_dummy_object(
                5802,
                LineString([(0, 0), (1, 1), (2, 0)]),
                navn="Test Route"
            )

            # Create object with specific creation date
            fid = vk.create_dummy_object(
                5800,
                Point(10.0, 55.0),
                navn="Old Object",
                oprettet=make_datetime(2023, 1, 15)
            )
        """
        import uuid
        from datetime import datetime

        try:
            # Determine layer name based on temakode
            if temakode == 5800:
                layer_name = "GeoFA_5800_fac_pkt"
                temanavn = "Friluftsliv facilitet - punkt"
            elif temakode == 5801:
                layer_name = "GeoFA_5801_fac_fl"
                temanavn = "Friluftsliv facilitet - flade"
            elif temakode == 5802:
                layer_name = "GeoFA_5802_fac_li"
                temanavn = "Friluftsliv facilitet - linje"
            else:
                raise ValueError(
                    f"Invalid temakode: {temakode}. Must be 5800, 5801, or 5802."
                )

            # Read existing layer to get schema
            gdf = gpd.read_file(self.config.db_path, layer=layer_name)

            # Create new row with minimal required fields
            now_utc = pd.Timestamp.now("UTC")

            # Use provided oprettet date or default to now
            oprettet_date = oprettet if oprettet is not None else now_utc
            # Convert to pandas Timestamp if it's a datetime
            if isinstance(oprettet_date, datetime):
                oprettet_date = pd.Timestamp(oprettet_date)

            new_row = {
                "temakode": temakode,
                "temanavn": temanavn,
                "objekt_id": "" if clear_objekt_id else str(uuid.uuid4()),
                "versions_id": str(uuid.uuid4()),
                "systid_fra": now_utc,
                "systid_til": pd.NaT,
                "oprettet": oprettet_date,
                "cvr_kode": 29189900,  # Vejle Kommune CVR
                "cvr_navn": "Vejle Kommune",
                "kommunekode": 575,  # Vejle
                "bruger_id": "test_user",
                "oprindkode": 1,
                "oprindelse": "Vejle Kommune",
                "statuskode": 3,
                "status": "Endelig",
                "off_kode": 1,
                "offentlig": "Ja",
                "noegle": None,
                "note": f"Test object created at {now_utc}",
                "navn": navn,
                "geometry": geometry,
            }

            # Create GeoDataFrame for the new row
            new_gdf = gpd.GeoDataFrame([new_row], crs=gdf.crs)

            # Match dtypes to avoid issues
            for col in new_gdf.columns:
                if col in gdf.columns and col != "geometry":
                    try:
                        new_gdf[col] = new_gdf[col].astype(
                            gdf[col].dtype, errors="ignore"
                        )
                    except:
                        pass

            # Append to existing data
            combined = pd.concat([gdf, new_gdf], ignore_index=True)
            combined_gdf = gpd.GeoDataFrame(combined, crs=gdf.crs)

            # Write back to database
            combined_gdf.to_file(
                self.config.db_path, layer=layer_name, driver=self.config.driver
            )

            # The new FID is the last index
            new_fid = len(gdf)  # 0-indexed, so this is the next available index

            objekt_status = (
                "without objekt_id"
                if clear_objekt_id
                else f"with objekt_id: {new_row['objekt_id']}"
            )
            print(
                f"Created dummy object in '{layer_name}' {objekt_status}\n"
                f"  FID: {new_fid}\n"
                f"  Name: {navn}\n"
                f"  Temakode: {temakode}"
            )

            return new_fid

        except Exception as e:
            print(f"Error creating dummy object: {e}")
            raise

    def close(self):
        """Closes the database connection."""
        if self.engine:
            self.engine.dispose()
            print("VK Database connection closed.")

    def __enter__(self):
        """Context manager entry point."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point - ensures connection is closed."""
        self.close()
        return False  # Don't suppress exceptions


if __name__ == "__main__":
    # Example usage with context manager
    config = DatabaseConfig(db_path="vk.gpkg")

    with VK(config) as vk:
        # Test 1: Reading all three layers
        print("\n" + "=" * 60)
        print("Test 1: VK Database Connection")
        print("=" * 60)

        layers = ["GeoFA_5800_fac_pkt", "GeoFA_5801_fac_fl", "GeoFA_5802_fac_li"]

        for layer_name in layers:
            print(f"\n{'-'*60}")
            gdf = vk.get_layer(layer_name)
            print(f"Temakode values: {gdf['temakode'].unique()}")
            print(f"Shape: {gdf.shape}")
            print(f"Geometry type: {gdf.geometry.geom_type.unique()}")

        # Test 2: Get objects created since a specific date
        print("\n" + "=" * 60)
        print("Test 2: Get Objects Created Since Date (using helper)")
        print("=" * 60)

        since_date = make_datetime(2022, 6, 1)
        print(f"\nSearching for objects created since: {since_date}")
        recent_objects = vk.get_objects_by_date(since_date)

        if len(recent_objects) > 0:
            print(f"\nSample of recent objects:")
            print(recent_objects[["objekt_id", "temakode", "oprettet"]].head(5))

        # Test 3: Get objects created within a date range
        print("\n" + "=" * 60)
        print("Test 3: Get Objects Created Between Dates (using helper)")
        print("=" * 60)

        start_date = make_datetime(2024, 4, 1)
        end_date = make_datetime(2024, 6, 30, 23, 59, 59)
        print(f"\nSearching for objects created between: {start_date} and {end_date}")
        range_objects = vk.get_objects_by_date(start_date, end_date)

        if len(range_objects) > 0:
            print(f"\nSample of objects in range:")
            print(range_objects[["objekt_id", "temakode", "oprettet"]].head(5))

        # Test 4: Get objects from specific layer only
        print("\n" + "=" * 60)
        print("Test 4: Get Objects from Specific Layer (using helper)")
        print("=" * 60)

        year_start = make_datetime(2024)  # Defaults to Jan 1, 00:00:00
        print(f"\nSearching for point objects created since: {year_start}")
        points_only = vk.get_objects_by_date(
            year_start, layer_name="GeoFA_5800_fac_pkt"
        )

        if len(points_only) > 0:
            print(f"\nFound {len(points_only)} point objects")
            print(f"All are temakode: {points_only['temakode'].unique()}")

    print("\n" + "=" * 60)
    print("Context manager automatically closed the connection.")
    print("=" * 60)
