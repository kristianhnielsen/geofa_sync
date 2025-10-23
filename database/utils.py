from .vk_clone.database import (
    GeodatabaseCloner,
    GeodatabaseClonerConfig,
)

from .geofa_clone.database import clone_geofa_database


def clone_databases(
    vk_output_path: str | None = None, geofa_output_path: str | None = None
):
    clone_vk_database(output_path=vk_output_path)
    clone_geofa_database(output_path=geofa_output_path)


def clone_vk_database(output_path: str | None = None):
    config = GeodatabaseClonerConfig(
        schema_files=[
            r"data_test\vk\schema\geofa_5800_fac_pkt.json",
            r"data_test\vk\schema\geofa_5801_fac_fl.json",
            r"data_test\vk\schema\geofa_5802_fac_li.json",
        ],
        gdb_path=r"data_test\vk\friluftslivs.gdb",
        output_path=output_path if output_path else r"vk.gpkg",
    )

    cloner = GeodatabaseCloner(
        schema_files=config.schema_files,
        gdb_path=config.gdb_path,
        output_path=config.output_path,
    )

    cloner.run_clone()


if __name__ == "__main__":
    clone_databases()
