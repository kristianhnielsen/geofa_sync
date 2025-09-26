import geopandas as gpd
import fiona
import os


def clone_geofa_database(output_path: str | None = None):
    geofa_gpkg_files = [
        r"database\geofa_clone\data\5800_fac_pkt.gpkg",
        r"database\geofa_clone\data\5801_fac_fl.gpkg",
        r"database\geofa_clone\data\5802_fac_li.gpkg",
    ]
    output_file = output_path if output_path else "geofa.gpkg"

    if os.path.exists(output_file):
        os.remove(output_file)
        print(f"Removed existing output file: {output_file}")

    for gpkg_file in geofa_gpkg_files:
        layers = fiona.listlayers(gpkg_file)

        for layer in layers:
            gdf = gpd.read_file(gpkg_file, layer=layer)

            gdf.to_file(output_file, layer=layer, driver="GPKG")

    print(f"All layers cloned to: {output_file}")
