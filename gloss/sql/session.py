from .readwriter import DataFrameReader
import uuid
from pyarrow.fs import *

class SparkSession(object):

    def __init__(self):
        self.conf = {}
        self._stages = []
        self.jobs = []
        self.conf = {
            "spark.executor.cores": 4,
            "spark.sql.shuffle.partitions": 100,
            "staging_dir": "/tmp/gloss_staging"
        }
        self._id = uuid.uuid4()
        self._dfs = []

    def __del__(self):
        fs = LocalFileSystem()
        try:
            staging_dir = self.conf["staging_dir"]
            fs.delete_dir(f"{staging_dir}/session={self._id}")
        except FileNotFoundError:
            pass
    
    def _registerDF(self, df):
        self._dfs.append(df)
        return len(self._dfs) - 1

    def _registerStage(self, stage):
        self._stages.append(stage)
        return len(self._stages) - 1

    @property
    def read(self):
        return DataFrameReader(self)