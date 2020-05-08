from datetime import date
from calendar import monthrange

from datetime import datetime, date

import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import col as c
from pyspark.sql.functions import trim, count, sum, avg, stddev, lit
from pyspark.sql.types import StringType, IntegerType
import json

from Utils.Utils import Utils


class AnalyticBaseTable:
    """This is the AnalyticBaseTableJob component
    Contains all the logic to load tables and process them
    with queries in order to generate an Analytic Base Table
    """

    def __init__(self, sql_context):
        """Constructor, initialize all the attribute variables

        Args:
            this_year: Year number, Integer
            this_month: Month number, Integer
            params: Dictionary with the required parameters for the execution of the job
        """
        
        self.sql_context = sql_context
        
        params_path = "/var/sds/homes/MB70967/workspace/garanti/conf/params.json"
        self.path = '/intelligence/infinz/analytic/users/MB70967/workspace/garanti/t_orv_garanti_abt'
        
        with open(params_path) as params_json:
            params = json.load(params_json)

        self.bins = params['bines']
        self.commerces = params['commerces']
        
        self.table_path_tla1872_targets = params["input"]["params_tla1872_targets"]["table_path"]
        self.table_label_tla1872_targets = params["input"]["params_tla1872_targets"]["table_label"]
        self.partition_by_tla1872_targets = params["input"]["params_tla1872_targets"]["partition_by"]
        self.fields_tla1872_targets = params["input"]["params_tla1872_targets"]["fields"]
        self.read_type_tla1872_targets = params["input"]["params_tla1872_targets"]["read_type"]
        
        self.tla1872_recurrents_worktable = None
        self.tla1872_targets_worktable = None
        self.tla1872_worktable = None
        self.tla117_worktable = None
        self.tla543_worktable = None

        print("AnalyticBaseTable initiated")
    
    def getTargets(self):
        targets = self.tla1872_targets_worktable.where(F.col("commerce_affiliation_id").isin(self.commerces)) \
                                                .select("customer_id").distinct().withColumn("target",lit(1)) \
                                                .persist()
        
        return(targets)
    
    def getPanTargets(self):
        pan_targets = self.tla1872_targets_worktable.where(F.col("commerce_affiliation_id").isin(self.commerces) &\
                                                           F.col("pan_id").substr(1,6).isin(self.bins)) \
                                                    .select("customer_id").distinct().withColumn("pan_target",lit(1)) \
                                                    .persist()
        
        return(pan_targets)

    def getRecurrents(self):
        recurrents = self.tla1872_recurrents_worktable.where(F.col("commerce_affiliation_id").isin(self.commerces)) \
                            .select("customer_id").distinct().withColumn("recurrent",lit(1)) \
                            .persist()
        
        return(recurrents)        
