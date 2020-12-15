import csv
import fnmatch
import gc
import glob
import io
import os
import time
import pytest

import zipfile
import pandas as pd
import boto3
import pyarrow.csv as pv
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from PSPSProject.src.Repository.dbqueries import queries
from PSPSProject.src.ReusableFunctions.awsfunctions import download_file_from_S3, download_dir_from_S3
from PSPSProject.src.ReusableFunctions.baseclass import BaseClass, exceptionRowCount
from PSPSProject.src.ReusableFunctions.commonfunctions import logfilepath, deleteFiles, downloadsfolderPath, readData, \
    deleteFolder, create_folder
from PSPSProject.src.ReusableFunctions.databasefunctions import queryresults_get_alldata, queryresults_fetchone

from PSPSProject.src.Tests.conftest import downloadsfolder, testDatafilePath, s3config, dirctorypath

VAR_TESTCASENAME = os.path.basename(__file__)
global VAR_COUNT, VAR_TESTENV
VAR_COUNT = 0
VAR_TESTENV = ""
VAR_COUNT = BaseClass().InitializeExecution(VAR_TESTCASENAME, VAR_COUNT, VAR_TESTENV)


@pytest.mark.skipif(not VAR_COUNT, reason="Excluded from regression suite")
@pytest.mark.regression
class TestDefaultDeviceValidation(BaseClass):
    def test_defaultdevicevalidation(self):
        try:
            var_row = VAR_COUNT[0]
            var_os = VAR_COUNT[2]
        except(ValueError, Exception):
            exceptionrow = exceptionRowCount()
            var_row = exceptionrow
        log = self.getLogger(logfilepath, VAR_TESTCASENAME)
        deleteFiles(downloadsfolder, ".csv")
        var_tp_array = []
        var_execution_flag = ''
        final_assert = []
        log.info("Starting Validation")

        var_nooftps = readData(testDatafilePath, "Data", var_row, 6)
        timeplace = readData(testDatafilePath, "Data", var_row, 7)

        if timeplace is None or timeplace == "":
            print("Version should take from UI")
        else:
            var_tp_array.append(timeplace)

        # Get the latest table filepaths from db
        get_defaulttable_db = queries.get_activetablename % 's3-defaultmanagement-circuits'
        lst_table_details = queryresults_fetchone(get_defaulttable_db)
        filename_defaulttable = lst_table_details
        log.info("Filename for Default Management active table is : " + str(filename_defaulttable))
        log.info("-----------------------------------------------------------------------------------------------")
        filename = filename_defaulttable.split('/')[-1]
        print(filename)
        # Download Default Circuits parquet file
        s3 = boto3.client('s3')
        s3_resource = boto3.resource("s3")
        s3_bucketname = s3config()['datastorebucketname']
        BUCKET_PATH = s3config()['defaultmangementpath']
        profilename = s3config()['profile_name']
        localpath = downloadsfolderPath + "\\defaultmanagement-circuits" + "\\" + filename
        defaultmngtcircuits = downloadsfolderPath + "\\defaultmanagement-circuits"
        deleteFolder(defaultmngtcircuits)
        create_folder(defaultmngtcircuits)
        download_file_from_S3(s3_bucketname, BUCKET_PATH, filename, localpath, profilename)
        log.info("Downloaded defaultmanagement-circuits parquet file from S3")

        # Get Timeplace UID and Timeplace ID for the required timeplace
        i = 0
        for each in var_tp_array:
            get_timeplace_db = queries.get_timeplace % each
            lst_tp_details = queryresults_get_alldata(get_timeplace_db)
            var_tp_uid = lst_tp_details[0][0]
            var_tp_id = lst_tp_details[0][1]
            var_meteorology_path = lst_tp_details[0][19]
            log.info("Timeplace UID for timeplace: " + str(each) + " is: " + str(var_tp_uid))
            log.info("Timeplace ID for timeplace: " + str(each) + " is: " + str(var_tp_id))
            log.info("Output Meteorology path is : " + str(var_meteorology_path))
            log.info("-----------------------------------------------------------------------------------------------")

            # Download parent circuits file for the timeplace from S3 bucket
            filename = str(var_tp_id) + "/" + str(var_tp_uid) + "/meteorology/meteorology_" + str(var_tp_uid) + "/"
            # filename = "131/151/circuits/circuits_151/"
            s3 = boto3.client('s3')
            s3_resource = boto3.resource("s3")
            s3_bucketname = s3config()['datastorebucketname']
            BUCKET_PATH = s3config()['tpbucketpath']
            path = BUCKET_PATH + filename
            profilename = s3config()['profile_name']
            local_folder = downloadsfolderPath + "meteorology_" + str(var_tp_uid)
            deleteFolder(local_folder)
            download_dir_from_S3(path, s3_bucketname, profilename, local_folder)
            log.info("Downloaded meteorology circuits(parent) parquet file from S3")

            spark = SparkSession.builder.appName("Timeplace-Creation") \
                .config('spark.driver.memory', '10g') \
                .config("spark.cores.max", "6") \
                .config('spark.yarn.appMasterEnv.ARROW_PRE_0_15_IPC_FORMAT', 1) \
                .config('spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT', 1) \
                .config("spark.sql.execution.arrow.enabled", "true") \
                .config("spark.sql.catalogImplementation", "in-memory") \
                .getOrCreate()
            log.info("Spark session connected")

            # read meteorology csv and convert to parquet file
            meteorology_input = local_folder + '/' + path
            meteorology_input = os.listdir(meteorology_input)
            var_input_file_cvs = meteorology_input[0]
            var_input_file_cvs = local_folder + '/' + path + var_input_file_cvs
            df = spark.read.csv(var_input_file_cvs, header=True)
            df.repartition(1).write.mode('overwrite').parquet(downloadsfolderPath + '\\meteorologyoutput')

            df_defaultmngtcircuits = spark.read.parquet(defaultmngtcircuits)
            df_defaultmngtcircuits.createOrReplaceTempView("defaultmngtcircuits")
            tempfolder = r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\df_defaultmngtcircuits"
            df_defaultmngtcircuits.coalesce(1).write.option("header", "true").format("csv").mode(
                "overwrite").save(tempfolder)
            df_defaultmngtcircuits.cache()
            df_meteorology = spark.read.parquet(downloadsfolderPath + '\\meteorologyoutput')
            df_meteorology.createOrReplaceTempView("meteorologycircuits")
            defaultcircuits = spark.sql("""SELECT DISTINCT mc.fireindex, mc.circuitid, mc.opnum, mc.devicetype, 
            mc.circuitname, mc.min_branch, mc.max_branch, mc.treelevel, mc.order_num, mc.substationname, mc.operable, 
            mc.consider, 'Y' as defaultdevice, 'intersectedcircuit' as action from meteorologycircuits as mc, defaultmngtcircuits as dc where mc.circuitid = dc.circuitId and mc.opnum = 
            dc.sourceIsolationDevice and mc.devicetype = dc.sourceIsolationDeviceType""")
            defaultcircuits.createOrReplaceTempView("tempdefaultcircuits")
            tempfolder = r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\tempdefaultcircuits"
            defaultcircuits.coalesce(1).write.option("header", "true").format("csv").mode(
                "overwrite").save(tempfolder)

            # Remove circuits if already present in intersected circuits list
            df_defaultcircuits_intersected = spark.sql("""SELECT  mc.fireindex, mc.circuitid, mc.opnum, 
            mc.devicetype, mc.circuitname, mc.min_branch, mc.max_branch, mc.treelevel, mc.order_num, 
            mc.substationname, mc.operable, mc.consider, tc.defaultdevice, tc.action from meteorologycircuits as mc 
            LEFT JOIN tempdefaultcircuits as tc on mc.circuitid = tc.circuitid AND mc.opnum = 
            tc.opnum AND mc.devicetype = tc.devicetype """)
            df_defaultcircuits_intersected.createOrReplaceTempView("defaultcircuits_intersected")
            tempfolder = r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\defaultcircuits_intersected"
            df_defaultcircuits_intersected.coalesce(1).write.option("header", "true").format("csv").mode(
                "overwrite").save(tempfolder)

            


