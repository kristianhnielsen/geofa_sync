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

            # Use a unique layer name based on the source file
            # Extract layer name from filename: 5800_fac_pkt, 5801_fac_fl, 5802_fac_li
            layer_name = os.path.basename(gpkg_file).replace(".gpkg", "")

            print(
                f"Cloning {len(gdf)} features to layer '{layer_name}' in {output_file}"
            )
            gdf.to_file(output_file, layer=layer_name, driver="GPKG")

    print(f"All layers cloned to: {output_file}")
