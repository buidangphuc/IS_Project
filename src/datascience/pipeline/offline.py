from datascience.components.data_ingestion import run as ingest
from datascience.components.data_validation import run as validate
from datascience.components.data_transformation import run as transform
from datascience.components.model_trainer import run as train
from datascience.components.index_builder import run as build_index
from datascience.components.offline_topn import run as precompute_topn

def run_all():
    ingest()
    validate()
    transform()
    train()
    build_index()
    precompute_topn()

if __name__ == "__main__":
    run_all()
