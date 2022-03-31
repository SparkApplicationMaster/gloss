from asyncore import write
from distutils.command.clean import clean
from functools import reduce
from operator import length_hint
import sys
from pyarrow.dataset import field
import pyarrow.compute as pc
import pyarrow as pa
import builtins
from copy import deepcopy
import uuid
import gc
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

import pyarrow.parquet as parq
import pyarrow.orc as porc
from pyarrow.fs import *

class DataFrame(object):
    def __init__(self, sparkSession, ds, special=[["read",{}]], op_groups=[[["read",{}]]], stages=[], other_dfs=[]): #, tables=None):
        self._ds = ds
        self._op_groups = op_groups
        self._special = special
        self.sparkSession = sparkSession
        self._stages = stages
        self._id = self.sparkSession._registerDF(self)
        self._other_dfs = deepcopy(other_dfs)

        #self._tables = tables
        #self._projections = []

        #self._add_projection({col: field(col) for col in self.schema.names})
    #def _add_projection(self, projection):
    #    self._projections.append(projection)

    #def _stack_projections(self):
    #    projection = {}
    #    for p in self._projections:
            # TODO: replace with real projection engine
    #        projection.update(p)
    #    return projection

    #@property
    #def schema(self):
        # TODO: convert to pyspark StructType
    #    return self._ds.schema

    def parallel_exec(self, task_func, batches_tables, **kwargs):
            def func_with_kwargs(p):
                return task_func(t=p[1], i=p[0], **kwargs)

            with ThreadPoolExecutor(max_workers=self.sparkSession.conf["spark.executor.cores"]) as executor:
                return list(executor.map(func_with_kwargs, enumerate(batches_tables)))

    def optimize_stage(self, ops):
        try:
            limit_index = [i for i, x in enumerate(ops) if x[0] == "limit_local"][0]
            preferred_index = limit_index
            index = limit_index
            if limit_index > 0:
                for index in range(limit_index, -1, -1):
                    if ops[index][0] != "where":
                        preferred_index = index
                    else:
                        break

                if (preferred_index < limit_index):
                    ops.insert(preferred_index, ops.pop(limit_index))
        except IndexError:
            pass

        return ops
    
    def execute(self):
        print("self_id:", self._id)
        for id in self._other_dfs:
            df = self.sparkSession._dfs[id]
            print("execute for df:", df._id)
            df.execute()
            print("finished df:", df._id)

        batches_tables = None
        special = self._special
        num_partitions = -1
        write_options = None

        stage_num_global = 0
        stage_num_df = 0
        for spec, ops in zip(special, self._op_groups):
            stage_num_global = self.sparkSession._registerStage(spec[0])
            self._stages.append(stage_num_global)
            stage_num_df = len(self._stages) - 1
            print("stage_num:", stage_num_df)
            ops = self.optimize_stage(ops)
            if spec[0] == "read":
                
                batches_tables = list(self._ds.get_fragments())
                if len(batches_tables) and isinstance(batches_tables[0], pa._dataset_parquet.ParquetFileFragment):
                    print("fragments:", len(batches_tables))
                    import builtins
                    import math
                    batches_tables = reduce(
                        lambda a, b: a + b, 
                        [[b.subset(row_group_ids=(list(range(i * 10, builtins.min(b.num_row_groups, (i + 1) * 10))))) for i in range(int(b.num_row_groups / 10))] for b in batches_tables]
                    )
                print("fragments:", len(batches_tables))
                #print(dir(batches_tables[0]))
            
            elif spec[0] == "exch":
                if spec[1]["type"] == "SinglePartition":
                    if num_partitions != 1:
                        batches_tables = [0]
                    
                elif spec[1]["type"] in {"RoundRobin", "HashPartitioning"}:
                    batches_tables = range(0, spec[1]["n"])
                
                num_partitions = len(batches_tables)
            
            if spec[1].get("write", False):
                write_options = {
                    "uid": uuid.uuid4()
                }
                for op in ops:
                    op_name = op[0]
                    op_kwargs = op[1]

                    if op_name == "write_option":
                        write_options[op_kwargs["key"]] = op_kwargs["value"]

                prepare_save(**write_options)
            
            #if spec[1].get("clean_tmp", False):
                #clean_tmp_shuffle(stage_num_df)

            print("stage_change:", spec)
            
            for op in ops:
                print(op)

            batches_tables = self.parallel_exec(task_func, batches_tables, stage_num=stage_num_df, ops=ops, num_partitions=num_partitions, write_options=write_options)
            #print(len(batches_tables))
            #pa.default_memory_pool().release_unused()
                
        return batches_tables

    def add_op(self, op, **kwargs):
        op_groups = deepcopy(self._op_groups)
        special = deepcopy(self._special)
        if op in {"exch"}:
            kwargs["staging_dir"] = self.sparkSession.conf.get("staging_dir")
            kwargs["session_id"] = self.sparkSession._id
            kwargs["df_id"] = self._id
            kwargs["shuffle_stage"] = uuid.uuid4()
            op_groups[-1].append(["shuffle_write", kwargs])
            special[-1][1]["clean_tmp"] = True
            #print("from_stage:", kwargs["from_stage"])
            op_groups.append([["shuffle_read", kwargs]])
            special.append([op, kwargs])
        else:
            if op == "write":
                special[-1][1]["write"] = True
            op_groups[-1].append([op, kwargs])

        return DataFrame(
            self.sparkSession,
            self._ds, 
            special=special,
            op_groups=op_groups, 
            other_dfs=self._other_dfs
            #tables=self._tables
        )

    def join(self, other, cols, how="inner"):
        other_repart = other.repartition(*cols)
        self_repart = self.repartition(*cols)
        other_shuffle_read = other_repart._special.pop()
        other_repart._op_groups.pop()
        self_repart._special[-1][1]["df_id_2"] = other_shuffle_read[1]["df_id"]
        self_repart._special[-1][1]["shuffle_stage_2"] = other_shuffle_read[1]["shuffle_stage"]
        self_repart._op_groups[-1][0][0] = "shuffle_join"
        self_repart._op_groups[-1][0][1]["df_id_2"] = other_shuffle_read[1]["df_id"]
        self_repart._op_groups[-1][0][1]["shuffle_stage_2"] = other_shuffle_read[1]["shuffle_stage"]
        self_repart._op_groups[-1][0][1]["how"] = how

        #print(self_repart._op_groups[-1][0])
        self_repart._other_dfs.append(other_repart._id)
        #print(self._id)
        #print(self_repart._id)
        #print(other_repart._id)
        return self_repart


    def withColumn(
        self, 
        name, #: str 
        column #: Expression
    ):
        #self._add_projection({name: exp})
        #try:
        #    index = self._table.column_names.index(name)
            #self._table = self._table.set_column(index, name, exp)
        #except ValueError:
            #self._table = self._table.append_column(name, exp)
            #field("test1") - 1
            
            #print(column.name)
            #print(t.column_names)
            #print(dict((c, "t.column({})".format(c)) for c in t.column_names))

        return self.add_op("withColumn", name=name, column=column)

    def where(self, condition):
        return self.add_op("where", condition=condition)

    def select(self, *cols):
        return self.add_op("select", cols=cols)
    
    def drop(self, *cols):
        return self.add_op("drop", cols=cols)
    
    def count(self):

        self.sparkSession.jobs.append("count")

        for ops in self._op_groups:
            print(ops[0])
        if 1 == len(self._special) and 0 == sum([sum([1 for op in ops if op[0] == "where"]) for ops in self._op_groups]):
            print("this")
            return self._ds.count_rows()

        df = self.add_op("count")
        
        return builtins.sum(df.execute())
            

    def limit(self, n):
        df = self\
            .add_op("limit_local", limit=n)\
            .add_op("exch", type="SinglePartition")\
            .add_op("limit_global", limit=n)

        return df

    def repartition(self, *args):
        n = None
        start_arg = 0
        hash_cols = None
        type = "RoundRobin"

        if isinstance(args[0], int):
            n = args[0]
            start_arg = 1
            if n == 1:
                type = "SinglePartition"

        if len(args) > start_arg:
            hash_cols = list(args[start_arg:])
            type = "HashPartitioning"
            if not n:
                n = self.sparkSession.conf.get("spark.sql.shuffle.partitions")

        df = self\
            .add_op("exch", type=type, n=n, hash_cols=hash_cols)\

        return df

    def toPandas(self):
        self.sparkSession.jobs.append("toPandas")

        df = self\
            .add_op("exch", type="SinglePartition")\
            .add_op("toPandas")

        tables = list(df.execute())

        pdf = tables[0].to_pandas()

        return pdf

    @property
    def write(self):
        return self.add_op("write")

    def format(self, format):
        return self.option(key="format", value=format)

    def mode(self, mode):
        return self.option(key="mode", value=mode)

    def partitionBy(self, *cols):
        return self.option(key="partitionBy", value=cols)

    def option(self, key, value):
        return self.add_op("write_option", key=key, value=value)

    def save(self, path):
        self.sparkSession.jobs.append("save")

        df = self\
            .option("path", path)\
            .add_op("save")
        
        return builtins.sum(df.execute())

def clean_tmp_shuffle(stage_num):
    fs = LocalFileSystem()
    try:
        print(f"delete /tmp/gloss_staging/stage={stage_num}")
        fs.delete_dir(f"/tmp/gloss_staging/stage={stage_num}")
        print(f"cleaned shuffle stage: {stage_num}")
    except FileNotFoundError:
        pass

import numpy as np
import pandas as pd
import pyarrow.dataset as ds

def with_column(table, col_name, col_value):
    if col_name in table.column_names:
        return table.set_column(table.column_names.index(col_name), col_name, col_value)
    else:
        return table.append_column(col_name, col_value)

def with_row_number(table, col_name, start_index=1):
    return with_column(table, col_name, pa.Array.from_pandas(np.arange(table.num_rows) + start_index))

def with_hash_mod(table, col_name, hash_cols, mod_by):
    return with_column(table, col_name, pa.Array.from_pandas(np.mod(reduce(
        lambda a, b: np.add(a, b), 
        map(lambda key: pd.util.hash_array(table.column(key).to_pandas().to_numpy()), hash_cols)
    ), mod_by)))

def shuffle_read(staging_dir, session_id, df_id, stage_num, i, num_partitions=1):
    #print(f"shuffle_read_{i}")
    staging_full_path = f"{staging_dir}/session={session_id}/df={df_id}/stage={stage_num}"
    return parq.read_table(staging_full_path, filters=[("in_task", "=", i)]).drop(["out_task", "in_task"])

        
def shuffle_write(staging_dir, session_id, df_id, stage_num, t, i, num_partitions=1, hash_cols=None):
    #print(f"shuffle_write_{i}")
    fs = LocalFileSystem()

    staging_full_path = f"{staging_dir}/session={session_id}/df={df_id}/stage={stage_num}"

    try:
        if (fs.get_file_info(staging_full_path).type == FileType.Directory):
            print("dir exists")
            #return
        #fs.create_dir(staging_full_path)
        #fs.create_dir(f"{staging_full_path}/out_task={i}")

        if not hash_cols:
            t = with_row_number(t, "in_task")
            hash_cols = ["in_task"]

        t = with_hash_mod(t, "in_task", hash_cols=hash_cols, mod_by=num_partitions)
        print(f"shuffle_write_{i}", t.num_rows) 

        #print(t.column_names)
        part_schema = t.select(["in_task"]).schema
        partitioning = ds.partitioning(part_schema, flavor="hive")
        ds.write_dataset(t, f"{staging_full_path}/out_task={i}", partitioning=partitioning, filesystem=fs, 
        min_rows_per_group=10000, max_rows_per_group=100000, max_rows_per_file=1000000000, format="parquet", existing_data_behavior="overwrite_or_ignore")

    except Exception as e:
        print(e)
        raise(e)

def prepare_save(**kwargs):

    filesystem = "local"
    mode = kwargs.get("mode", "error")
    path = kwargs.get("path")

    if filesystem == "local":
        fs = LocalFileSystem()

    if mode == "overwrite":
        try:
            fs.delete_dir(path)
        except FileNotFoundError:
            pass

    fs.create_dir(path)       

def save_write(table, **kwargs):
    uid = kwargs["uid"]
    format = kwargs.get("format", "parquet")
    compression = kwargs.get("compression", "snappy")
    partitionBy = kwargs.get("partitionBy")
    mode = kwargs.get("mode", "error")
    
    path = kwargs.get("path")

    index = kwargs["index"]
    
    if format == "parquet":
        FileFormat = ds.ParquetFileFormat()
    elif format == "orc":
        FileFormat = ds.OrcFileFormat()
    elif format == "csv":
        FileFormat = ds.CsvFileFormat()
    
    if index == 0:
        print("save started")

    #kwargs = {}
    #if (format == "parquet"):
    #    kwargs["row_group_size"] = 10000
    
    if table.num_rows:
        if partitionBy:
            part_schema = table.select(partitionBy).schema
            partitioning = ds.partitioning(part_schema, flavor="hive")
        else:
            partitioning = None

        file_options=FileFormat.make_write_options(compression=compression)

        bracket_i = "{i}"
        ds.write_dataset(table, path, f"part-{bracket_i}{str(index).zfill(4)}-{uid}.{compression}.{format}", 
        format=format, partitioning=partitioning, min_rows_per_group=10000, max_rows_per_group=100000000, max_rows_per_file=1000000000, file_options=file_options, existing_data_behavior="overwrite_or_ignore")
        #writer.write_table(table, f"{path}/part-{str(index).zfill(5)}-{uid}.{compression}.{format}", compression=compression, **kwargs)
        print("save:", index)

def join_arrow_tables(a, b, join_keys):
    i_tbl = a.select(join_keys + a.drop(join_keys).column_names)
    j_tbl = b.drop(join_keys)

    return pa.Table.from_arrays(i_tbl.columns + j_tbl.columns, names=i_tbl.column_names + j_tbl.column_names)

def join_task(t1, t2, join_keys, how="inner"):
    adf = with_row_number(t1.select(join_keys), "_join_rn_left", 0).combine_chunks().to_pandas()
    bdf = with_row_number(t2.select(join_keys), "_join_rn_right", 0).combine_chunks().to_pandas()

    join_res = pd.merge(
        adf,
        bdf,
        how=how,
        on=join_keys,
        left_on=None,
        right_on=None,
        left_index=False,
        right_index=False,
        sort=False,
        suffixes=("_x", "_y"),
        copy=True,
        indicator=False,
        validate=None,
    )

    return join_arrow_tables(t1.take(pa.Array.from_pandas(join_res["_join_rn_left"])), t2.take(pa.Array.from_pandas(join_res["_join_rn_right"])), join_keys)


def task_func(i, t, **kwargs):
    stage_num = kwargs["stage_num"]
    num_partitions = kwargs["num_partitions"]
    ops = kwargs["ops"]
    write_options = kwargs.get("write_options", {})

    for op in ops:
        op_name = op[0]
        op_kwargs = op[1]

        if op_name == "read":
            #print(type(t))
            if isinstance(t, pa._dataset.Fragment):
                t = t.to_table()
            if isinstance(t, pa._dataset.TaggedRecordBatch):
                t = t.record_batch
            if isinstance(t, pa.RecordBatch):
                t = pa.Table.from_batches([t])

        elif op_name == "withColumn":
            name = op_kwargs["name"]
            column = op_kwargs["column"]
            t = t.append_column(name, eval(column.name.format(**dict((c, "t.column(\"{}\")".format(c)) for c in t.column_names))))

        elif op_name == "where":
            condition = op_kwargs["condition"]
            #.format(**dict((c, "t.column(\"{}\")".format(c)) for c in t.column_names))
            #print(condition.name)
            #exit(1)
            t = t.filter(eval(condition.name.format(**dict((c, "t.column(\"{}\")".format(c)) for c in t.column_names))))

        elif op_name == "select":
            cols = op_kwargs["cols"]
            t = t.select(cols)

        elif op_name == "drop":
            cols = op_kwargs["cols"]
            t = t.drop(list(set(cols).intersection(set(t.column_names))))

        elif op_name == "limit_local" or op_name == "limit_global":
            limit = op_kwargs["limit"]
            if isinstance(t, pa._dataset.Fragment):
                t = t.head(num_rows=limit)
            else:
                t = t.slice(length=limit)

        elif op_name == "count":
            cnt = t.num_rows
            #del t
            return cnt

        elif op_name == "save":
            write_options["index"] = i
            save_write(t, **write_options)
            cnt = t.num_rows
            return cnt
        
        elif op_name == "shuffle_write":
            if op_kwargs["type"] == "SinglePartition" and num_partitions == 1:
                pass
            else:
                staging_dir = op_kwargs["staging_dir"]
                session_id = op_kwargs["session_id"]
                df_id = op_kwargs["df_id"]
                to_stage = op_kwargs["shuffle_stage"]

                shuffle_write(staging_dir, session_id, df_id, to_stage, t, i, op_kwargs.get("n", 1), hash_cols=op_kwargs.get("hash_cols"))
                return i
        elif op_name == "shuffle_read":
            # TODO: remote scans from other nodes
            if not isinstance(t, pa.Table):
                #print("from_stage:", from_stage)
                #fs = LocalFileSystem()
                try:
                    from_stage = op_kwargs["shuffle_stage"]
                    staging_dir = op_kwargs["staging_dir"]
                    session_id = op_kwargs["session_id"]
                    df_id = op_kwargs["df_id"]
                    # TODO: not read whole table, read partitioned dataset by batches on one node
                    t = shuffle_read(staging_dir, session_id, df_id, from_stage, i, op_kwargs.get("n", 1))
                    print("shuffle_read: ", t.num_rows)
                except Exception as e:
                    print(e)
                    raise e
                #print(t.column_names)
            else:
                print("fuck")
        elif op_name == "shuffle_join":
            #print(op_kwargs)
            #print("op_kwargs", op_kwargs)
            staging_dir = op_kwargs["staging_dir"]
            session_id = op_kwargs["session_id"]
            df_id = op_kwargs["df_id"]
            # TODO: not read whole table, read partitioned dataset by batches on one node
            t = shuffle_read(staging_dir, session_id, op_kwargs["df_id"], op_kwargs["shuffle_stage"], i, op_kwargs.get("n", 1))
            t2 = shuffle_read(staging_dir, session_id, op_kwargs["df_id_2"], op_kwargs["shuffle_stage_2"], i, op_kwargs.get("n", 1))
            #print("join lengths:", i, t.num_rows, t2.num_rows)
            t = join_task(t, t2, op_kwargs["hash_cols"], op_kwargs["how"])
            #print("join lengths:", i, t.num_rows)
            
            #raise NotImplementedError("shuffle_join")
            
    return t