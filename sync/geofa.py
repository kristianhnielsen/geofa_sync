from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import geopandas as gpd
from typing import Optional, Any, Dict
import uuid
from shapely.geometry import (
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
)

from .utils import DatabaseConfig


class GeoFA:
    """
    A class to interact with the GeoFA database.

    Handles database connections and operations for the GeoFA geospatial database.
    """

    def __init__(self, config: DatabaseConfig):
        """
        Initializes the GeoFA database connection.

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
            print(f"Connected to GeoFA database at: {self.config.db_path}")
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise

    def get_layer(self, layer_name: str) -> gpd.GeoDataFrame:
        """
        Reads a layer from the GeoPackage database.

        Args:
            layer_name (str): Name of the layer to read (e.g., '5800_fac_pkt').

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

    def _infer_layer_from_geometry(self, geometry) -> str:
        """
        Infers the appropriate layer name based on geometry type.

        Args:
            geometry: A Shapely geometry object.

        Returns:
            str: The layer name suffix (pkt, fl, or li).
        """
        geom_type = geometry.geom_type

        # Map geometry types to layer suffixes
        if geom_type in ["Point", "MultiPoint"]:
            return "5800_fac_pkt"
        elif geom_type in ["Polygon", "MultiPolygon"]:
            return "5801_fac_fl"
        elif geom_type in ["LineString", "MultiLineString"]:
            return "5802_fac_li"
        else:
            raise ValueError(f"Unsupported geometry type: {geom_type}")

    def create_object(self, temakode: int, geometry) -> str:
        """
        Creates a new object in the GeoFA database.

        Args:
            temakode (int): The temakode value (5800, 5801, or 5802).
            geometry: A Shapely geometry object.

        Returns:
            str: The newly generated object ID (UUID).
        """
        try:
            # Generate a new UUID for the object
            new_object_id = str(uuid.uuid4())

            # Infer the correct layer based on geometry type
            layer_name = self._infer_layer_from_geometry(geometry)

            # Read the existing layer to get the schema
            gdf = self.get_layer(layer_name)

            # Create a new row with minimal required fields
            new_row: Dict[str, Any] = {col: None for col in gdf.columns}
            new_row["objekt_id"] = new_object_id
            new_row["temakode"] = temakode
            new_row["geometry"] = geometry

            # Set temanavn based on temakode
            temanavn_map = {
                5800: "t_5800_fac_pkt_t",
                5801: "t_5801_fac_fl_t",
                5802: "t_5802_fac_li_t",
            }
            new_row["temanavn"] = temanavn_map.get(temakode)

            # Append the new row to the GeoDataFrame
            new_gdf = gpd.GeoDataFrame([new_row], crs=gdf.crs)

            # Match dtypes to avoid FutureWarning
            for col in new_gdf.columns:
                if col != "geometry" and col in gdf.columns:
                    new_gdf[col] = new_gdf[col].astype(gdf[col].dtype, errors="ignore")

            # Concatenate with matched dtypes
            import pandas as pd

            updated_gdf = gpd.GeoDataFrame(
                pd.concat([gdf, new_gdf], ignore_index=True), crs=gdf.crs
            )

            # Write back to the database
            updated_gdf.to_file(self.config.db_path, layer=layer_name, driver="GPKG")

            print(f"Created new object with ID: {new_object_id}")
            print(f"  Temakode: {temakode}")
            print(f"  Layer: {layer_name}")
            print(f"  Geometry type: {geometry.geom_type}")

            return new_object_id

        except Exception as e:
            print(f"Error creating object: {e}")
            raise

    def close(self):
        """Closes the database connection."""
        if self.engine:
            self.engine.dispose()
            print("GeoFA Database connection closed.")

    def __enter__(self):
        """Context manager entry point."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point - ensures connection is closed."""
        self.close()
        return False  # Don't suppress exceptions


if __name__ == "__main__":
    # Example usage with context manager
    config = DatabaseConfig(db_path="geofa.gpkg")

    with GeoFA(config) as geofa:
        # Test creating new objects with different geometry types
        print("\n" + "=" * 60)
        print("Testing create_object function")
        print("=" * 60)

        # Create a point object (temakode 5800)
        point_geom = Point(1000000, 7000000)
        point_id = geofa.create_object(temakode=5800, geometry=point_geom)

        print("\n" + "-" * 60)

        # Create a polygon object (temakode 5801)
        polygon_geom = Polygon(
            [
                (1000000, 7000000),
                (1000100, 7000000),
                (1000100, 7000100),
                (1000000, 7000100),
            ]
        )
        polygon_id = geofa.create_object(temakode=5801, geometry=polygon_geom)

        print("\n" + "-" * 60)

        # Create a line object (temakode 5802)
        line_geom = LineString(
            [(1000000, 7000000), (1000100, 7000100), (1000200, 7000200)]
        )
        line_id = geofa.create_object(temakode=5802, geometry=line_geom)

        print("\n" + "=" * 60)
        print(f"Created 3 new objects:")
        print(f"  Point ID: {point_id}")
        print(f"  Polygon ID: {polygon_id}")
        print(f"  Line ID: {line_id}")

    # Connection automatically closed when exiting the 'with' block
    print("Context manager automatically closed the connection.")
