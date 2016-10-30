"""
The Spark-based workflow for orthorectification using several different approaches.
The pipelines can be built with any pyspark version >1.6 or simplespark
"""

from utils import  NamedLambda, FieldSelector, namedtuple
import numpy as np
import time

# labeling class types
url_id = namedtuple('url', ['url'])
src_tile_id = namedtuple('source_tile_position', ['img_id', 'z', 'x', 'y', 'type'])
dest_tile_id = namedtuple('target_tile_position', ['img_id', 'z', 'x', 'y', 'type'])
tile_item = namedtuple('tile_data',
                       ['request', 'target_tile_id', 'source_tile_id', 'source_tile_data', 'out_tile_data'])

# image settings

IM_W, IM_H = 60000, 60000
TILE_W, TILE_H = 2000, 2000

# should the images be downscaled
DS_FACTOR = 1
TILE_FACTOR = int(IM_W / TILE_W)

IM_W, IM_H = int(IM_W / DS_FACTOR), int(IM_H / DS_FACTOR)
TILE_W, TILE_H = int(IM_W / TILE_FACTOR), int(IM_H / TILE_FACTOR)

TILES_PER_IMAGE = int(IM_W / TILE_W * IM_H / TILE_H)

print('%d @ %dx%d' % (TILES_PER_IMAGE, TILE_W, TILE_H))

DELAY_SCALE = 0 # use to specify the multiplier for all delays (0 to turn off)

def stack(arr_list, axis=0):
    """
    since numpy 1.8.2 does not have the stack command
    """
    assert axis == 0, "Only works for axis 0"
    return np.vstack(map(lambda x: np.expand_dims(x, 0), arr_list))


def get_output_tiles(req_str):
    return [(req_str, src_tile_id(req_str, -1, x, y, "out")) for x in np.arange(0, IM_W, TILE_W)
            for y in np.arange(0, IM_H, TILE_H)]


def calc_input_tiles(o_tile, n_size=1, delay=0):
    """
    A simple 3x3 neighborhood with a delay
    :param o_tile: the output tile to start with
    :param n_size: the size of the square to iterate through (1 -> 3x3, 2 -> 5x5, ...)
    :param delay: the amount of time to pause before returning (simulate a rest api call)
    :return: List[(output_tile, input_tile)]
    """
    time.sleep(DELAY_SCALE * delay)
    return [(o_tile, dest_tile_id(
        img_id=o_tile.img_id,
        z=o_tile.z,
        type="in",
        x=ix,
        y=iy))
            for ix in np.arange(o_tile.x - n_size * TILE_W, o_tile.x + (n_size + 1) * TILE_W, TILE_W) if ix > 0
            for iy in np.arange(o_tile.y - n_size * TILE_H, o_tile.y + (n_size + 1) * TILE_H, TILE_H) if iy > 0]


def pull_input_tile(i_tile, delay=2.5):
    """
    Generates a random, but unique 8 bit input tile given the source tile id i_tile
    """
    assert i_tile.type.find("in") == 0
    time.sleep(DELAY_SCALE * delay)
    np.random.seed(i_tile.__hash__() % 4294967295)  # should make process deterministic
    return np.random.uniform(-127, 127, size=(TILE_W, TILE_H, 4)).astype(np.int16)


RECON_TIME = 1.0


def partial_reconstruction(src_tile, targ_tile, tile_data, delay=RECON_TIME):
    time.sleep(DELAY_SCALE * delay)
    out_data = tile_data.copy()
    out_data[out_data > 20] = 0
    return out_data


def combine_reconstructions(many_slices):
    """
    Bring a number of partial reconstructions together
    """
    return np.sum(stack(many_slices, 0), 0)


def full_reconstruction(src_tiles, pr_delay=RECON_TIME / 2):
    """
    Run the full reconstruction as one step
    """
    out_images = [partial_reconstruction(src.source_tile_id, src.target_tile_id, src.source_tile_data, delay=pr_delay)
                  for src in src_tiles]
    return combine_reconstructions(out_images)


# intermediate spark functions
def ti_full_reconstruction(x):
    k, src_tiles = x
    return k, full_reconstruction(src_tiles)


def grp_tile_read(x):
    src_tile_id, tile_items = x
    tile_data = pull_input_tile(src_tile_id)
    return [i._replace(source_tile_data=tile_data) for i in tile_items]


def ti_partial_reconstruction(in_tile_item):
    return in_tile_item._replace(
        out_tile_data=partial_reconstruction(in_tile_item.source_tile_id,
                                             in_tile_item.target_tile_id,
                                             in_tile_item.source_tile_data),
        source_tile_data=None  # throw out the old data
    )


def ti_partial_collect(x):
    k, in_tile_items = x  # don't need the key
    assert len(in_tile_items) > 0, "Cannot provide empty partial collecton set"
    iti_list = list(in_tile_items)
    all_part_recon = map(lambda x: x.out_tile_data, in_tile_items)

    return (k, combine_reconstructions(all_part_recon))


# pipelines to build

def _build_input(in_sc, reqs):
    req_rdd = in_sc.parallelize(reqs)
    out_tile_rdd = req_rdd.flatMap(get_output_tiles).repartition(100)
    shuffle_tile_fields = NamedLambda("Shuffle Tile Fields", lambda x: tile_item(x[0], x[1][0], x[1][1], None, None))
    all_tile_rdd = out_tile_rdd.flatMapValues(calc_input_tiles).map(shuffle_tile_fields)
    return all_tile_rdd
def build_naive_pipe(in_sc, reqs):
    all_tile_rdd = _build_input(in_sc, reqs)
    # parallel reading of the data
    read_fcn = NamedLambda("pull_input_tile", lambda i: i._replace(source_tile_data = pull_input_tile(i.source_tile_id)))
    all_tile_rdd_data = all_tile_rdd.map(read_fcn)
    # parallel combining of the tiles
    recon_tiles_rdd = all_tile_rdd_data.groupBy(FieldSelector('target_tile_id')).map(ti_full_reconstruction)
    return recon_tiles_rdd

def build_grpread_pipe(in_sc, reqs):
    all_tile_rdd = _build_input(in_sc, reqs)
    single_read_tiles_rdd = all_tile_rdd.groupBy(FieldSelector('source_tile_id')).flatMap(grp_tile_read)
    gr_recon_tiles_rdd = single_read_tiles_rdd.groupBy(FieldSelector('target_tile_id')).map(ti_full_reconstruction)
    return gr_recon_tiles_rdd

def build_partialrecon_pipe(in_sc, reqs):
    all_tile_rdd = _build_input(in_sc, reqs)
    # group together all files by source tile and then read that source tile and put in into every item
    single_read_tiles_rdd = all_tile_rdd.groupBy(FieldSelector('source_tile_id')).flatMap(grp_tile_read)
    # run a partial reconstruction on every item
    partial_recon_tiles_rdd = single_read_tiles_rdd.map(ti_partial_reconstruction)
    # combine all the partial reconstructions to create the final reconstruction
    full_recon_tiles_rdd = partial_recon_tiles_rdd.groupBy(FieldSelector('target_tile_id')).map(ti_partial_collect)
    return full_recon_tiles_rdd

# the IO components
from io import BytesIO
def compress_tile(in_tile):
    out_stream = BytesIO()
    np.savez_compressed(out_stream, out_tile = in_tile)
    out_stream.seek(0) # restart at beginning
    return b''.join(out_stream.readlines())

import tempfile

def write_tiles(kvarg):
    img_info, tile_list = kvarg
    out_data = b''
    out_file = tempfile.NamedTemporaryFile(suffix = 'jp2', prefix = str(img_info))
    for tile_info, tile_data in tile_list:
        out_file.write(tile_data)
    return (img_info, out_file.name)


# the first approximations for these output components
def build_full_pipe(in_sc, reqs, read_pipe = build_grpread_pipe):
    full_recon_tiles_rdd = build_grpread_pipe(in_sc, reqs)
    all_image_rdd = full_recon_tiles_rdd.mapValues(compress_tile).groupBy(NamedLambda("Select Field : img_id",lambda x: x[0].img_id))
    return all_image_rdd.map(write_tiles)

def build_parallelio_pipe(in_sc, reqs, read_pipe = build_grpread_pipe):
    """
    A pipe for saving the output in parallel blocks rather than one by one
    """
    full_recon_tiles_rdd = build_grpread_pipe(in_sc, reqs)
    all_image_rdd = full_recon_tiles_rdd.mapValues(compress_tile).groupBy(NamedLambda("Select Field : img_id",lambda x: x[0].img_id))
    return all_image_rdd.saveAsPickleFile('test_output_pkl')