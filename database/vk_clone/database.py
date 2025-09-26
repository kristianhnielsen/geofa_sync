from dataclasses import dataclass
import geopandas as gpd
import json
import os


@dataclass
class GeodatabaseClonerConfig:
    schema_files: list
    gdb_path: str
    output_path: str


class GeodatabaseCloner:
    """
    A class to clone feature classes from a File Geodatabase to a GeoPackage.
    """

    def __init__(self, schema_files: list[str], gdb_path: str, output_path: str):
        """
        Initializes the GeodatabaseCloner.

        Args:
            schema_files (list): A list of paths to the JSON schema files.
            gdb_path (str): The path to the source .gdb folder.
            output_path (str): The path for the destination GeoPackage file.
        """
        self.schema_files = schema_files
        self.gdb_path = gdb_path
        self.output_path = output_path

    def _clone_layer(self, json_schema_path):
        """
        Clones a single feature class layer. This is an internal method.

        Args:
            json_schema_path (str): The path to the JSON schema for a single layer.
        """
        try:
            with open(json_schema_path, "r") as f:
                schema = json.load(f)

            table_name = schema["name"].split(".")[-1]
            print(f"--- Processing layer: {table_name} ---")

            print(f"Reading from {self.gdb_path}...")
            gdf = gpd.read_file(self.gdb_path, layer=table_name)
            print(f"Successfully read {len(gdf)} features.")

            print(f"Writing layer '{table_name}' to {self.output_path}...")
            gdf.to_file(self.output_path, layer=table_name, driver="GPKG")
            print(f"Successfully cloned layer: {table_name}\n")

        except Exception as e:
            print(f"An error occurred while processing {json_schema_path}: {e}\n")
            print("Troubleshooting tips:")
            print(f"- Ensure the layer '{table_name}' exists in your geodatabase.")
            print("- Check that file paths are correct.")

    def run_clone(self):
        """
        Executes the main cloning process for all specified schema files.
        """
        if not os.path.exists(self.gdb_path):
            print("---")
            print(f"WARNING: Geodatabase folder not found at '{self.gdb_path}'.")
            print("Please check the 'geodatabase_folder' variable.")
            print("---")
            return

        if os.path.exists(self.output_path):
            os.remove(self.output_path)
            print(f"Removed existing clone at '{self.output_path}'.")

        print(f"\nStarting geodatabase clone process...")

        for schema_file in self.schema_files:
            if os.path.exists(schema_file):
                self._clone_layer(schema_file)
            else:
                print(f"WARNING: Schema file not found, skipping: {schema_file}\n")

        print("-----------------------------------------")
        print("Database clone process completed!")
        print(f"You can find your new database with all layers at: {self.output_path}")


if __name__ == "__main__":

    config = GeodatabaseClonerConfig(
        schema_files=[
            r"database\vk_clone\schema\geofa_5800_fac_pkt.json",
            r"database\vk_clone\schema\geofa_5801_fac_fl.json",
            r"database\vk_clone\schema\geofa_5802_fac_li.json",
        ],
        gdb_path=r"database\vk_clone\friluftslivs.gdb",
        output_path=r"vk.gpkg",
    )

    cloner = GeodatabaseCloner(
        schema_files=config.schema_files,
        gdb_path=config.gdb_path,
        output_path=config.output_path,
    )
    cloner.run_clone()
