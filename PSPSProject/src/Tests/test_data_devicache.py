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

from PSPSProject.src.Pages.DefaultManagement import DefaultManagement
from PSPSProject.src.Pages.HomePage import HomePage
from PSPSProject.src.Pages.TimePlacePage import TimePlacePage
from PSPSProject.src.Repository.dbqueries import queries
from PSPSProject.src.ReusableFunctions.awsfunctions import download_file_from_S3, download_dir_from_S3
from PSPSProject.src.ReusableFunctions.baseclass import BaseClass, exceptionRowCount
from PSPSProject.src.ReusableFunctions.commonfunctions import logfilepath, deleteFiles, downloadsfolderPath, readData, \
    deleteFolder, create_folder
from PSPSProject.src.ReusableFunctions.databasefunctions import queryresults_get_alldata, queryresults_fetchone, \
    queryresults_get_data

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
        defmanagement = DefaultManagement(self.driver)
        eventmanagement = TimePlacePage(self.driver)
        homepage = HomePage(self.driver)
        deleteFiles(downloadsfolder, ".csv")
        var_tp_array = []
        var_execution_flag = ''
        final_assert = []
        log.info("Starting Validation")

        var_nooftps = readData(testDatafilePath, "Data", var_row, 6)
        timeplace = readData(testDatafilePath, "Data", var_row, 7)
        var_dmfile = readData(testDatafilePath, "Data", var_row, 9)
        scopetpname = readData(testDatafilePath, "Data", var_row, 10)

        # if var_dmfile is None or var_dmfile == "":
        #     log.info("default circuits are fetched from system")
        # else:
        #     status = defmanagement.dm_uploadfile(var_dmfile)
        #     log.info("Default circuits are uploaded from UI and status of upload is: " + status)
        for i in range(var_nooftps):
            if i > 0:
                self.driver.refresh()
                time.sleep(3)
            if timeplace is None or timeplace == "":
                homepage.navigate_eventManagement()
                var_tpcreation = eventmanagement.TimePlaceCreation(scopetpname)
                timeplace = var_tpcreation[1]
                get_status = queries.get_tp_status % timeplace
                status = queryresults_get_data(get_status)
                if status == "Failed":
                    log.error("Timeplace creation Failed")
                elif status == "Completed":
                    log.info("Timeplace creation is successful and status is: " + status)
                log.info("Timeplace creation is successful and timeplacename is: " + var_tpcreation[1])
                var_tp_array.append(timeplace)
            else:
                log.info("Timeplace details are : " + timeplace)
                var_tp_array.append(timeplace)

        # Get the latest feeder transformers table filepath from db
        get_feederNetwork_transformertable_db = queries.get_activetablename % 's3-feedernetworktrace-transformers'
        lst_table_details1 = queryresults_fetchone(get_feederNetwork_transformertable_db)
        filename_feedertransformerstable = lst_table_details1
        log.info("Filename for Feeder Network - transformers active table is : " + str(filename_feedertransformerstable))
        log.info("-----------------------------------------------------------------------------------------------")
        feedertransformerstablefilename = filename_feedertransformerstable.split(s3config()['envpath'])[-1]
        print(feedertransformerstablefilename)

        # Download feederdevices parquet file from S3- PSPSDataSync folder
        s3 = boto3.client('s3')
        s3_resource = boto3.resource("s3")
        s3_bucketname = s3config()['pspsdatabucketname']
        feedertransformersBUCKET_PATH = feedertransformerstablefilename
        profilename = s3config()['profile_name']
        feedertransformer = downloadsfolderPath + "\\feederNetwork_transformer"
        deleteFolder(feedertransformer)
        create_folder(feedertransformer)
        download_dir_from_S3(feedertransformersBUCKET_PATH, s3_bucketname, profilename, feedertransformer)
        log.info("Downloaded feederNetwork_transformer parquet file from S3")

        # Get Timeplace UID and Timeplace ID for the required timeplace
        i = 0
        for each in var_tp_array:
            get_timeplace_db = queries.get_timeplace % each
            lst_tp_details = queryresults_get_alldata(get_timeplace_db)
            var_tp_uid = lst_tp_details[0][0]
            var_tp_id = lst_tp_details[0][1]
            var_devicecache_path = lst_tp_details[0][16]
            var_scopeversion = lst_tp_details[0][20]
            log.info("Timeplace UID for timeplace: " + str(each) + " is: " + str(var_tp_uid))
            log.info("Timeplace ID for timeplace: " + str(each) + " is: " + str(var_tp_id))
            log.info("Meteorology Scopeversionname is: " + str(var_scopeversion))
            log.info("Output Devicecache path is : " + str(var_devicecache_path))
            log.info("-----------------------------------------------------------------------------------------------")

            # Download circuits file for the timeplace from S3 bucket
            filename = str(var_tp_id) + "/" + str(var_tp_uid) + "/circuits/circuits_" + str(var_tp_uid) + "/"
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
            tempfolder = r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\devfile_circuitinfo"
            df_timeplacecircuits.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save(
                tempfolder)
            log.info("Fetched circuit info parquet file from S3")

            devicecacheciruitsfilepath = os.path.join(
                downloadsfolderPath + "\\devicecachecircuits_" + str(
                    var_tp_uid) + "\\reports\\timeplacecreation\\" + str(
                    var_tp_id) + "\\" + str(var_tp_uid) + "\\devicecache\\devicecache_" + str(var_tp_uid))
            df_devicecachecircuits = spark.read.parquet(devicecacheciruitsfilepath)
            df_devicecachecircuits.createOrReplaceTempView("devicecachecircuits")
            tempfolder = r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\devfile_devicecachecircuits"
            df_devicecachecircuits.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save(
                tempfolder)
            log.info("Fetched devicecache parquet file from S3")

            # Read FeederNetwork_Transformer table
            # feedertransformerdataloc = downloadsfolderPath + "\\feederNetwork_transformer"
            df_feedertransformerdataloc = spark.read.parquet(feedertransformer + '/' + feedertransformersBUCKET_PATH)
            df_feedertransformerdataloc.createOrReplaceTempView("feedertransformer")
            tempfolder = r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\S3_feedertransformerdata"
            df_feedertransformerdataloc.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save(
                tempfolder)

            df_tempdevicecachedata = spark.sql("""SELECT DISTINCT '' as devicecache_uid, c.timeplace_foreignkey, ft.FEEDERID as circuitId, ft.TO_FEATURE_GLOBALID as transformer_primarymeterguid, '' as flag, ft.CGC12, c.source_isolation_device, c.source_isolation_device_type, c.additional_isolation_device, c.additional_isolation_device_type, c.circuitinfo_uid, '' as concat_circuitinfo_uid
            from timeplace_circuits as c inner join feedertransformer ft
                                            on ft.FEEDERID = LPAD(c.circuitid,9,'0')
                                            AND ft.TREELEVEL >= c.source_treelevel
                                            AND ft.ORDER_NUM <= c.source_order_num
                                            AND ft.MAX_BRANCH <= c.source_max_branch
                                            AND ft.MIN_BRANCH >= c.source_min_branch
                                            AND ft.TO_FEATURE_FCID IN (1001,1014)""")
            df_tempdevicecachedata.createOrReplaceTempView("tempdevicecachedata")
            tempfolder = r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\df_tempdevicecachedata"
            df_tempdevicecachedata.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save(
                tempfolder)
            log.info("Downstream tracing to identify transformers is completed")

            # Store df_tempdevicecachedata csv to dataframe
            tempfolder = downloadsfolderPath + '\\df_tempdevicecachedata'
            devicecachecircuits = os.listdir(tempfolder)
            for file in devicecachecircuits:
                if file.endswith('csv'):
                    break
            finaldevicecachecircuitscsv = downloadsfolderPath + '\\df_tempdevicecachedata' + '/' + file
            finaldevicecachecircuits_actual = pd.read_csv(finaldevicecachecircuitscsv)

            # Store devfile_devicecachecircuits csv to dataframe
            tempdevicecachedata = downloadsfolderPath + '\\devfile_devicecachecircuits'
            devicecachecircuits_expected = os.listdir(tempdevicecachedata)
            for file1 in devicecachecircuits_expected:
                if file1.endswith('csv'):
                    break
            finaldevicecachecircuitscsv_expected = downloadsfolderPath + '\\devfile_devicecachecircuits' + '/' + file1
            finaldevicecachecircuits_expected = pd.read_csv(finaldevicecachecircuitscsv_expected)
            finaldevicecachecircuits_actual = finaldevicecachecircuits_actual.drop_duplicates(
                subset=['circuitId', 'transformer_primarymeterguid'],
                keep='first')
            finalcircuitsfolder = downloadsfolderPath + "\\Autofile_devicecachecircuits"
            deleteFolder(finalcircuitsfolder)
            create_folder(finalcircuitsfolder)
            finaldevicecachecircuits_actual.to_csv(finalcircuitsfolder + '/finaldc_actual.csv')

            # log.info("Expected device cache circuits are stored in finaldc_expected folder")
            actualdccount = finaldevicecachecircuits_actual.shape[0]
            log.info("Automation file Count of records is: " + str(actualdccount))
            expecteddccount = finaldevicecachecircuits_expected.shape[0]
            log.info("Dev file Count of records is: " + str(expecteddccount))
            if str(actualdccount) == str(expecteddccount):
                log.info("Total transformers count matched between Actual and Expected Device cache report and count is: " + str(actualdccount))
                df1 = pd.merge(finaldevicecachecircuits_expected, finaldevicecachecircuits_actual,
                               on=['circuitId', 'transformer_primarymeterguid'], how='outer', indicator=True)
                df1 = df1[df1['_merge'] != 'both']
                if len(df1) == 0:
                    log.info("All the transformer_primarymeterguid matched")
                else:
                    mismatchtransformers = downloadsfolderPath + "\\mismatchtransformers"
                    deleteFolder(mismatchtransformers)
                    create_folder(mismatchtransformers)
                    df1.to_csv(mismatchtransformers + '/mismatchtransformers.csv')
                    log.error("All the transformer_primarymeterguid not matched and mismatched transformers are: " + str(df1))
                    final_assert.append(False)
            else:
                log.error("Total transformers count not matched between Actual and Expected Device cache report")
                final_assert.append(False)
                df1 = pd.merge(finaldevicecachecircuits_expected, finaldevicecachecircuits_actual,
                               on=['circuitId', 'transformer_primarymeterguid'], how='outer', indicator=True)
                df1 = df1[df1['_merge'] != 'both']
                if len(df1) == 0:
                    log.info("All the transformer_primarymeterguid matched")
                else:
                    mismatchtransformers = downloadsfolderPath + "\\mismatchtransformers"
                    deleteFolder(mismatchtransformers)
                    create_folder(mismatchtransformers)
                    df1.to_csv(mismatchtransformers + '/mismatchtransformers.csv')
                    log.error(
                        "All the transformer_primarymeterguid not matched and mismatched transformers are: " + str(df1))
                    final_assert.append(False)
        if var_execution_flag == 'fail':
            log.error("Execution failed: Errors found in execution!!")
            assert False
        log.info("----------------------------------------------------------------------------------------------")
        log.info("*************AUTOMATION EXECUTION COMPLETED*************")
