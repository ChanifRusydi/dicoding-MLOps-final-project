import os
import sys

from absl import logging
from tfx.orchestration import metadata, pipeline
from tfx.orchestration.beam.beam_dag_runner import BeamDagRunner

PIPELINE_NAME = 'chanif1516-pipeline'
DATA_ROOT = "data"
TRANSFORM_MODULE_FILE = "modules/transform.py"
TRAINER_MODULE_FILE = "modules/trainer.py"

OUTPUT_DIR = "output"
SERVING_MODEL_DIR = os.path.join(OUTPUT_DIR, "serving_model")
PIPELINE_ROOT = os.path.join(OUTPUT_DIR, 'pipelines', PIPELINE_NAME)
METADATA_PATH = os.path.join(OUTPUT_DIR, 'metadata', PIPELINE_NAME, 'metadata.db')

def create_pipeline(components, pipeline_root:str) -> pipeline.Pipeline:
    beam_args = [
        '--direct_running_mode=multi_processing',
        '--direct_num_workers=0',   
    ]
    return pipeline.Pipeline(
        pipeline_name=PIPELINE_NAME,
        pipeline_root=pipeline_root,
        components=components,
        enable_cache=True,
        metadata_connection_config=metadata.sqlite_metadata_connection_config(METADATA_PATH),
        beam_pipeline_args=beam_args
    )

if __name__ == '__main__':
    from modules.components import create_components
    logging.set_verbosity(logging.INFO)
    components = create_components(
        DATA_ROOT,
        TRANSFORM_MODULE_FILE,
        TRAINER_MODULE_FILE,
        SERVING_MODEL_DIR
    )
    pipeline_create = create_pipeline(components, PIPELINE_ROOT)
    BeamDagRunner().run(pipeline=pipeline_create)