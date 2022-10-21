import argparse
import os.path
from em_tasks.export import (
    export_normalized_uint8,
    export_uint16,
    get_files,
    load_ser_file,
    process_metadata,
)
from em_tasks.stitch import stitch_tiles
from pathlib import Path
from prefect import task, unmapped, flow, get_run_logger

TILE_CONF_NAME = "TileConfiguration.txt"


@task()
def get_files_task(input_dir, filename_filter):
    return get_files(input_dir=input_dir, filename_filter=filename_filter, logger=get_run_logger())


@task()
def load_ser_file_task(ser_file):
    return load_ser_file(ser_file=ser_file, logger=get_run_logger())


@task()
def export_uint16_task(data, pixel_size, save_dir, basename, prefix="16bit"):
    export_uint16(data=data, pixel_size=pixel_size, save_dir=save_dir, basename=basename, prefix=prefix)


@task()
def export_normalized_uint8_task(data, pixel_size, save_dir, basename, prefix, intensity_range):
    export_normalized_uint8(data=data,
                            pixel_size=pixel_size,
                            save_dir=save_dir,
                            basename=basename,
                            prefix=prefix,
                            intensity_range=intensity_range
                            )


@task()
def process_metadata_task(metadata_list, save_dir, prefixes, filename):
    process_metadata(metadata_list=metadata_list, save_dir=save_dir, filename=filename,
                     prefixes=prefixes, logger=get_run_logger())


@task()
def stitch_tiles_task(input_dir, tileconf_filename, save_path=None):
    stitch_tiles(input_dir, tileconf_filename, save_path, logger=get_run_logger())


@task()
def export_task(ser_file, save_dir, intensity_range):
    metadata, data, pixel_size = load_ser_file(ser_file=ser_file)
    basename = Path(ser_file).stem
    export_uint16(data=data, pixel_size=pixel_size, save_dir=save_dir, basename=basename)
    export_normalized_uint8(data=data, pixel_size=pixel_size, save_dir=save_dir, basename=basename,
                            intensity_range=intensity_range)
    metadata['image_file_name'] = basename + '.tif'
    metadata['pixel_size'] = pixel_size[0] * 1000 * 1000
    return metadata


@task()
def path_join_task(folder, file):
    return os.path.join(folder, file)


@flow(name="Export and Stitch .ser data")
def export_and_stitch(input_dir=r'/path/to/input/dir', filename_filter="*.ser", save_dir=r'/path/to/output/dir',
                      intensity_range=1000):
    files = get_files_task(input_dir=input_dir, filename_filter=filename_filter)
    metadata_list = export_task.map(ser_file=files, save_dir=unmapped(save_dir),
                                    intensity_range=unmapped(intensity_range))
    # save_position_file from metadata
    metadata_task = process_metadata_task(metadata_list=metadata_list, save_dir=save_dir, prefixes=["8bit", "16bit"],
                                          filename=TILE_CONF_NAME)
    # stitch tiles
    subdir = path_join_task(save_dir, "8bit")
    stitch_tiles_task(input_dir=subdir, tileconf_filename=TILE_CONF_NAME,
                      wait_for=[metadata_task])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir", type=str)
    parser.add_argument("--save_dir", type=str)
    args = parser.parse_args()
    export_and_stitch(input_dir=args.input_dir, save_dir=args.save_dir)
