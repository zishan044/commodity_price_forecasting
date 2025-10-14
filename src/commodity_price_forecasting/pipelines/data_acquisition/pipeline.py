from kedro.pipeline import Node, Pipeline
from .nodes import process_all_files


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            Node(
                func=process_all_files,
                inputs=None,
                outputs="raw_data",
                name="data_acquisition_node",
            )
        ]
    )