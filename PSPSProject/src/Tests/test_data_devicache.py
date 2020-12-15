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
from PSPSProject.src.ReusableFunctions.databasefunctions import queryresults_get_alldata

from PSPSProject.src.Tests.conftest import downloadsfolder, testDatafilePath, s3config, dirctorypath

VAR_TESTCASENAME = os.path.basename(__file__)
global VAR_COUNT, VAR_TESTENV
VAR_COUNT = 0
VAR_TESTENV = ""
VAR_COUNT = BaseClass().InitializeExecution(VAR_TESTCASENAME, VAR_COUNT, VAR_TESTENV)


@pytest.mark.skipif(not VAR_COUNT, reason="Excluded from regression suite")
@pytest.mark.regression
class TestDeviceCache(BaseClass):
    def test_devicecache(self):
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
        '''
        # Download all the required data files from S3- PSPSDataSync folder
        s3 = boto3.client('s3')
        s3_resource = boto3.resource("s3")
        s3_bucketname = s3config()['pspsdatabucketname']
        BUCKET_PATH = s3config()['pspsdatasyncpath']
        profilename = s3config()['profile_name']
        # Download feedernetworktrace-transformers parquet file
        filename = "feederNetwork_transformer.parquet"
        localpath = downloadsfolderPath + "\\feederNetwork_transformer" + "\\" + filename
        feederNetwork_transformer = downloadsfolderPath + "\\feederNetwork_transformer"
        deleteFolder(feederNetwork_transformer)
        create_folder(feederNetwork_transformer)
        download_file_from_S3(s3_bucketname, BUCKET_PATH, filename, localpath, profilename)
        log.info("Downloaded feederNetwork_transformer parquet file from S3")
        '''
        # Get Timeplace UID and Timeplace ID for the required timeplace
        i = 0
        for each in var_tp_array:
            get_timeplace_db = queries.get_timeplace % each
            lst_tp_details = queryresults_get_alldata(get_timeplace_db)
            var_tp_uid = lst_tp_details[0][0]
            var_tp_id = lst_tp_details[0][1]
            var_devicecache_path = lst_tp_details[0][16]
            log.info("Timeplace UID for timeplace: " + str(each) + " is: " + str(var_tp_uid))
            log.info("Timeplace ID for timeplace: " + str(each) + " is: " + str(var_tp_id))
            log.info("Output Devicecache path is : " + str(var_devicecache_path))
            log.info("-----------------------------------------------------------------------------------------------")

            # var_devicecache_path = "s3://psps-datastore-dev/reports/timeplacecreation/157/177/devicecache/devicecache_177"
            # filename = var_devicecache_path.strip("s3://psps-datastore-dev/reports/timeplacecreation/")

            # Download circuits file for the timeplace from S3 bucket
            filename = str(var_tp_id) + "/" + str(var_tp_uid) + "/circuits/circuits_" + str(var_tp_uid) + "/"
            # filename = "131/151/circuits/circuits_151/"
            s3 = boto3.client('s3')
            s3_resource = boto3.resource("s3")
            s3_bucketname = s3config()['datastorebucketname']
            BUCKET_PATH = s3config()['tpbucketpath']
            path = BUCKET_PATH + filename
            profilename = s3config()['profile_name']
            local_folder = downloadsfolderPath + "circuits_" + str(var_tp_uid)
            deleteFolder(local_folder)
            download_dir_from_S3(path, s3_bucketname, profilename, local_folder)
            log.info("Downloaded circuits parquet file from S3")

            devicecachefilename = str(var_tp_id) + "/" + str(var_tp_uid) + "/devicecache/devicecache_" + str(
                var_tp_uid) + "/"
            # filename = "131/151/circuits/circuits_151/"
            s3 = boto3.client('s3')
            s3_resource = boto3.resource("s3")
            s3_bucketname = s3config()['datastorebucketname']
            BUCKET_PATH = s3config()['tpbucketpath']
            devicecachepath = BUCKET_PATH + devicecachefilename
            profilename = s3config()['profile_name']
            local_folder = downloadsfolderPath + "devicecachecircuits_" + str(var_tp_uid)
            deleteFolder(local_folder)
            download_dir_from_S3(devicecachepath, s3_bucketname, profilename, local_folder)
            log.info("Downloaded devicecache circuits parquet file from S3")

            # var_tp_uid = '163'
            # var_tp_id = '143'

            spark = SparkSession.builder.appName("Timeplace-Creation") \
                .config('spark.driver.memory', '10g') \
                .config("spark.cores.max", "6") \
                .config('spark.yarn.appMasterEnv.ARROW_PRE_0_15_IPC_FORMAT', 1) \
                .config('spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT', 1) \
                .config("spark.sql.execution.arrow.enabled", "true") \
                .config("spark.sql.catalogImplementation", "in-memory") \
                .getOrCreate()
            log.info("Spark session connected")

            ciruitsfilepath = os.path.join(
                downloadsfolderPath + "\\circuits_" + str(var_tp_uid) + "\\reports\\timeplacecreation\\" + str(
                    var_tp_id) + "\\" + str(var_tp_uid) + "\\circuits\\circuits_" + str(var_tp_uid))
            df_timeplacecircuits = spark.read.parquet(ciruitsfilepath)
            df_timeplacecircuits.createOrReplaceTempView("timeplace_circuits")
            tempfolder = r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\dc_circuitinfo"
            df_timeplacecircuits.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save(
                tempfolder)

            devicecacheciruitsfilepath = os.path.join(
                downloadsfolderPath + "\\devicecachecircuits_" + str(
                    var_tp_uid) + "\\reports\\timeplacecreation\\" + str(
                    var_tp_id) + "\\" + str(var_tp_uid) + "\\devicecache\\devicecache_" + str(var_tp_uid))
            df_devicecachecircuits = spark.read.parquet(devicecacheciruitsfilepath)
            df_devicecachecircuits.createOrReplaceTempView("devicecachecircuits")
            tempfolder = r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\dc_devicecachecircuits"
            df_timeplacecircuits.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save(
                tempfolder)

            # Read FeederNetwork_Transformer table
            feedertransformerdataloc = downloadsfolderPath + "\\feederNetwork_transformer"
            df_feedertransformerdataloc = spark.read.parquet(feedertransformerdataloc)
            df_feedertransformerdataloc.createOrReplaceTempView("feedertransformer")

            # Do the Downstream trace and get transformers
            df_temptransformersinfo = spark.sql(""" SELECT 
                                                                c.circuitinfo_uid,
                                                                c.timeplace_foreignkey,
                                                                c.fireindex,
                                                                ff.FEEDERID AS circuitid,
                                                                c.circuitname,
                                                                ff.substationname,
                                                                c.transmissionimpact,
                                                                c.division,
                                                                ff.MIN_BRANCH AS source_min_branch,
                                                                ff.MAX_BRANCH AS source_max_branch,
                                                                ff.ORDER_NUM AS source_order_num,
                                                                ff.TREELEVEL AS source_treelevel,
                                                                c.additional_max_branch,
                                                                c.additional_min_branch,
                                                                c.additional_order_num,
                                                                c.additional_treelevel,
                                                                ff.ASSETTYPE AS source_isolation_device,
                                                                c.additional_isolation_device,
                                                                ff.OPERATINGNUMBER AS source_isolation_device_type,
                                                                c.additional_isolation_device_type,
                                                                c.flag,
                                                                ff.FEEDERFEDBY AS parentfeederfedby,
                                                                c.circuitid as parentcircuitid,
                                                                c.tempgenname,
                                                                c.comments,
                                                                ff.TO_LINE_GLOBALID,
                                                                '' AS from_circuitid,
                                                                ff.TO_FEATURE_GLOBALID,
                                                                1 as parentloop
                                                            FROM 
                                                                feederFull fv, 
                                                                feederFull ff, 
                                                                circuits AS c 
                                                                WHERE 
                                                                fv.FEEDERID = LPAD(c.circuitid,9,'0')
                                                                AND fv.MIN_BRANCH >= c.source_min_branch
                                                                AND fv.MAX_BRANCH <= c.source_max_branch
                                                                AND fv.TREELEVEL >= c.source_treelevel
                                                                AND fv.ORDER_NUM <= c.source_order_num
                                                                AND (fv.TO_LINE_GLOBALID IS NOT NULL AND fv.TO_LINE_GLOBALID <> '') 
                                                                AND ff.to_feature_globalid = fv.to_line_globalId""")
