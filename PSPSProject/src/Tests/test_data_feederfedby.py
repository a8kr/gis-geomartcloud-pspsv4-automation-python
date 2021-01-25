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
    deleteFolder, create_folder, getCurrentTime
from PSPSProject.src.ReusableFunctions.databasefunctions import queryresults_get_alldata, queryresults_fetchone
from PSPSProject.src.Tests.conftest import downloadsfolder, testDatafilePath, s3config, dirctorypath

VAR_TESTCASENAME = os.path.basename(__file__)
global VAR_COUNT, VAR_TESTENV
VAR_COUNT = 0
VAR_TESTENV = ""
VAR_COUNT = BaseClass().InitializeExecution(VAR_TESTCASENAME, VAR_COUNT, VAR_TESTENV)


@pytest.mark.skipif(not VAR_COUNT, reason="Excluded from regression suite")
@pytest.mark.regression
class TestFeederFedBy(BaseClass):
    def test_feederfedby(self):
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
        tpid = readData(testDatafilePath, "Data", var_row, 10)

        for i in range(var_nooftps):
            if i > 0:
                self.driver.refresh()
                time.sleep(3)
            if timeplace is None or timeplace == "":
                homepage.navigate_eventManagement()
                var_tpcreation = eventmanagement.TimePlaceCreation(tpid)
                timeplace = var_tpcreation[3]
                timetaken = var_tpcreation[1]
                log.info("Timeplace creation is successful and timeplacename is: " + var_tpcreation[3])
                log.info("Time taken to create Timeplace is: " + var_tpcreation[1])
                var_tp_array.append(timeplace)
            else:
                log.info("Timeplace details are : " + timeplace)
                var_tp_array.append(timeplace)

        # Get the latest feederNetwork_priugconductor table filepath from db
        get_feederNetwork_priugconductor_db = queries.get_activetablename % 's3-feedernetworktrace-priugconductor'
        lst_table_details1 = queryresults_fetchone(get_feederNetwork_priugconductor_db)
        filename_feeder_priugconductor_dbtable = lst_table_details1
        log.info("Filename for FfeederNetwork_priugconductor active table is : " + str(filename_feeder_priugconductor_dbtable))
        log.info("-----------------------------------------------------------------------------------------------")
        feederNetwork_priugconductorfilename = filename_feeder_priugconductor_dbtable.split(s3config()['envpath'])[-1]
        print(feederNetwork_priugconductorfilename)

        # Download all the required data files from S3- PSPSDataSync folder
        s3 = boto3.client('s3')
        s3_resource = boto3.resource("s3")
        s3_bucketname = s3config()['pspsdatabucketname']
        feeder_priugconductorBUCKET_PATH = feederNetwork_priugconductorfilename
        profilename = s3config()['profile_name']
        feedernetwork_ugdata = downloadsfolderPath + "\\feedernetwork_ugdata"
        deleteFolder(feedernetwork_ugdata)
        create_folder(feedernetwork_ugdata)
        download_dir_from_S3(feeder_priugconductorBUCKET_PATH, s3_bucketname, profilename, feedernetwork_ugdata)
        log.info("Downloaded feederNetwork_priugconductor parquet file from S3")

        # Get the latest feederNetwork_priohconductor table filepath from db
        get_feederNetwork_priohconductor_db = queries.get_activetablename % 's3-feedernetworktrace-priohconductor'
        lst_table_details2 = queryresults_fetchone(get_feederNetwork_priohconductor_db)
        filename_feeder_priohconductor_dbtable = lst_table_details2
        log.info("Filename for FfeederNetwork_priohconductor active table is : " + str(filename_feeder_priohconductor_dbtable))
        log.info("-----------------------------------------------------------------------------------------------")
        feederNetwork_priohconductorfilename = filename_feeder_priohconductor_dbtable.split(s3config()['envpath'])[-1]
        print(feederNetwork_priohconductorfilename)

        # Download all the required data files from S3- PSPSDataSync folder
        s3 = boto3.client('s3')
        s3_resource = boto3.resource("s3")
        s3_bucketname = s3config()['pspsdatabucketname']
        feeder_priohconductorBUCKET_PATH = feederNetwork_priohconductorfilename
        profilename = s3config()['profile_name']
        feedernetwork_ohdata = downloadsfolderPath + "\\feedernetwork_ohdata"
        deleteFolder(feedernetwork_ohdata)
        create_folder(feedernetwork_ohdata)
        download_dir_from_S3(feeder_priohconductorBUCKET_PATH, s3_bucketname, profilename, feedernetwork_ohdata)
        log.info("Downloaded feederNetwork_priohconductor parquet file from S3")

        # Get the latest feedernetworktrace-devices table filepath from db
        get_feederNetwork_devices_db = queries.get_activetablename % 's3-feedernetworktrace-devices'
        lst_table_details3 = queryresults_fetchone(get_feederNetwork_devices_db)
        filename_feeder_devices_dbtable = lst_table_details3
        log.info("Filename for feedernetworktrace-devices active table is : " + str(filename_feeder_devices_dbtable))
        log.info("-----------------------------------------------------------------------------------------------")
        feederNetwork_devicesfilename = filename_feeder_devices_dbtable.split(s3config()['envpath'])[-1]
        print(feederNetwork_devicesfilename)

        # Download all the required data files from S3- PSPSDataSync folder
        s3 = boto3.client('s3')
        s3_resource = boto3.resource("s3")
        s3_bucketname = s3config()['pspsdatabucketname']
        feeder_devicesBUCKET_PATH = feederNetwork_devicesfilename
        profilename = s3config()['profile_name']
        feedernetwork_devicesdata = downloadsfolderPath + "\\feedernetwork_devicesdata"
        deleteFolder(feedernetwork_devicesdata)
        create_folder(feedernetwork_devicesdata)
        download_dir_from_S3(feeder_devicesBUCKET_PATH, s3_bucketname, profilename, feedernetwork_devicesdata)
        log.info("Downloaded feedernetworktrace-devices parquet file from S3")

        # Get Timeplace UID and Timeplace ID for the required timeplace
        i = 0
        for each in var_tp_array:
            get_timeplace_db = queries.get_timeplace % each
            lst_tp_details = queryresults_get_alldata(get_timeplace_db)
            var_tp_uid = lst_tp_details[0][0]
            var_tp_id = lst_tp_details[0][1]
            var_tpid = lst_tp_details[0][21]
            log.info("Timeplace UID for timeplace: " + str(each) + " is: " + str(var_tp_uid))
            log.info("Timeplace ID for timeplace: " + str(each) + " is: " + str(var_tp_id))
            log.info("Meteorology Timeplace ID is: " + str(var_tpid))
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
            tempfolder = downloadsfolderPath + '\\actualtimeplace_circuits'
            df_timeplacecircuits.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save(
                tempfolder)

            df_timeplacecircuits.cache()
            Actual_feederfedcircuits = spark.sql("""SELECT timeplace_foreignkey, circuitid, circuitname, 
            substationname, transmissionimpact,division,source_min_branch,source_max_branch,source_order_num,
            source_treelevel,additional_max_branch,additional_min_branch,additional_order_num,additional_treelevel,
            source_isolation_device,additional_isolation_device,source_isolation_device_type,
            additional_isolation_device_type,flag,parentfeederfedby,parentcircuitid,tempgenname,comments FROM 
            timeplace_circuits where parentcircuitid is not null and parentcircuitid <> '' """)
            Actual_feederfedcircuits.createOrReplaceTempView("Actual_feederfedcircuits")
            tempfolder = downloadsfolderPath + '\\Actual_feederfedcircuits'
            Actual_feederfedcircuits.coalesce(1).write.option("header", "true").format("csv").mode(
                "overwrite").save(tempfolder)
            log.info(
                "Actual Feederfed by circuits from circuits file are stored in 'Actual_feederfedcircuits.csv' file")
            Actual_feederfedcircuits.cache()

            if Actual_feederfedcircuits.count() == 0:
                print('No feederfed by circuits found')
                log.info('No feederfed by circuits found')
            else:
                print('Feederfed by circuits found')
                log.info('Feederfed by circuits found')
                df_filteredcircuits = spark.sql(
                    """ SELECT * from timeplace_circuits where parentcircuitid is null or parentcircuitid = '' """)
                df_filteredcircuits.createOrReplaceTempView("circuits")
                tempfolder = downloadsfolderPath + '\\circuits'
                df_filteredcircuits.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save(
                    tempfolder)

                # Read Feeder tables
                df_feederOHhconductor = spark.read.parquet(feedernetwork_ohdata + '/' + feeder_priohconductorBUCKET_PATH)
                df_feederOHhconductor.createOrReplaceTempView("feederOHhconductor")
                df_feederUGconductor = spark.read.parquet(feedernetwork_ugdata + '/' + feeder_priugconductorBUCKET_PATH)
                df_feederUGconductor.createOrReplaceTempView("feederUGhconductor")
                df_feederdevices = spark.read.parquet(feedernetwork_devicesdata + '/' + feeder_devicesBUCKET_PATH)
                df_feederdevices.createOrReplaceTempView("feederdevices")
                log.info("Read all parquet files and create temp tables is completed")

                feederFull = spark.sql(
                    """SELECT * from feederUGhconductor UNION SELECT * from feederOHhconductor""")
                feederFull.createOrReplaceTempView("feederFull")
                tempfolder = downloadsfolderPath + '\\feederFull'
                feederFull.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save(tempfolder)

                # Do the Downstream trace and get parent circuits
                df_tempCircuitInfoTableName = spark.sql(""" SELECT 
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
                df_tempCircuitInfoTableName.createOrReplaceTempView("tempCircuitInfoTableName")
                tempfolder = downloadsfolderPath + '\\downstream_parent'
                df_tempCircuitInfoTableName.coalesce(1).write.option("header", "true").format("csv").mode(
                    "overwrite").save(tempfolder)
                df_tempCircuitInfoTableName.createOrReplaceTempView("tempCircuitInfoTableName")

                # Do downstream tracing and get Child and grand childs (tocircuits)
                df_tempCircuitInfoTableName = get_feedfedby_circuits(df_tempCircuitInfoTableName, spark)
                df_tempCircuitInfoTableName.createOrReplaceTempView("tempCircuitInfoTableName")
                tempfolder = downloadsfolderPath + '\\downstream_child'
                df_tempCircuitInfoTableName.coalesce(1).write.option("header", "true").format("csv").mode(
                    "overwrite").save(tempfolder)
                df_tempCircuitInfoTableName.cache()

                # Do upstream tracing
                final_circuitTable = spark.sql("""  SELECT 
                                                        ce.circuitinfo_uid,
                                                        ce.timeplace_foreignkey,
                                                        '' as fireindex,
                                                        fv.feederid AS circuitid,
                                                        fv.circuitname,
                                                        fv.substationname,
                                                        ce.transmissionimpact,
                                                        ce.division,
                                                        fv.min_branch AS source_min_branch,
                                                        fv.max_branch AS source_max_branch,
                                                        fv.order_num AS source_order_num,
                                                        fv.treelevel AS source_treelevel,
                                                        ce.additional_max_branch,
                                                        ce.additional_min_branch,
                                                        ce.additional_order_num,
                                                        ce.additional_treelevel,
                                                        fv.operatingnumber AS source_isolation_device,
                                                        ce.additional_isolation_device,
                                                        fv.assettype AS source_isolation_device_type,
                                                        ce.additional_isolation_device_type,
                                                        ce.flag,
                                                        fv.feederfedby AS parentfeederfedby,
                                                        ce.parentcircuitid,
                                                        ce.tempgenname,
                                                        ce.comments,
                                                        '' AS to_line_globalId,
                                                        '' AS from_circuitid,
                                                        fv.TO_FEATURE_GLOBALID,
                                                        '' AS parentloop                                                    
                                                FROM   
                                                    tempCircuitInfoTableName  AS ce, 
                                                    feederdevices  AS fv  
                                                WHERE   fv.feederid =  ce.circuitid
                                                    AND fv.MIN_BRANCH <= ce.source_min_branch  
                                                    AND fv.MAX_BRANCH >= ce.source_max_branch
                                                    AND fv.TREELEVEL <= ce.source_treelevel
                                                    AND fv.ORDER_NUM >= ce.source_order_num
                                                    AND fv.to_feature_fcid IN ( 1003, 1005, 998, 997 )
                                                ORDER BY fv.TREELEVEL ASC  """)
                final_circuitTable.createOrReplaceTempView("final_circuitTable")
                tempfolder = downloadsfolderPath + '\\upstreamdevices'
                final_circuitTable.coalesce(1).write.option("header", "true").format("csv").mode(
                    "overwrite").save(tempfolder)
                final_circuitTable.cache()
                log.info("Feeder fed by logic completed and circuits list stored in 'feederfedcircuits.csv' file")

                # Get Duplicate records which match from Circuits list and feederfedcircuits list
                duplicaterecords = spark.sql("""SELECT f.timeplace_foreignkey, f.fireindex, f.circuitid, f.circuitname, 
                                f.substationname, f.transmissionimpact, f.division, f.source_min_branch,f.source_max_branch,
                                f.source_order_num,f.source_treelevel,f.additional_max_branch,f.additional_min_branch,
                                f.additional_order_num,f.additional_treelevel,f.source_isolation_device,
                                f.additional_isolation_device,f.source_isolation_device_type,f.additional_isolation_device_type,
                                f.flag,f.parentfeederfedby,f.parentcircuitid,f.tempgenname,f.comments from 
                                final_circuitTable as f Inner Join circuits as c on LPAD(c.circuitid,9,'0') = LPAD(f.circuitid,9,'0') and 
                                c.source_isolation_device=f.source_isolation_device and c.source_isolation_device_type = 
                                f.source_isolation_device_type """)
                duplicaterecords.createOrReplaceTempView("duplicaterecords")
                tempfolder = downloadsfolderPath + '\\duplicaterecords'
                duplicaterecords.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save(
                    tempfolder)
                duplicaterecords.cache()

                # Remove Duplicate records and get Expected Feeder fed by circuits list
                Expected_feederfedcircuits = spark.sql("""Select timeplace_foreignkey, circuitid, circuitname, substationname, transmissionimpact, division,
                               source_min_branch,source_max_branch,source_order_num,source_treelevel,additional_max_branch,additional_min_branch,
                               additional_order_num,additional_treelevel,source_isolation_device,additional_isolation_device,source_isolation_device_type,
                               additional_isolation_device_type,flag,parentfeederfedby,parentcircuitid,tempgenname,comments from final_circuitTable
                                EXCEPT
                                Select timeplace_foreignkey, circuitid, circuitname, substationname, transmissionimpact, division,
                               source_min_branch,source_max_branch,source_order_num,source_treelevel,additional_max_branch,additional_min_branch,
                               additional_order_num,additional_treelevel,source_isolation_device,additional_isolation_device,source_isolation_device_type,
                               additional_isolation_device_type,flag,parentfeederfedby,parentcircuitid,tempgenname,comments from duplicaterecords """)
                Expected_feederfedcircuits.createOrReplaceTempView("Expected_feederfedcircuits")
                tempfolder = downloadsfolderPath + '\\Expected_feederfedcircuits'
                Expected_feederfedcircuits.coalesce(1).write.option("header", "true").format("csv").mode(
                    "overwrite").save(tempfolder)
                log.info("Remove Duplicate records which match from Circuits list and feederfedcircuits list")
                log.info(
                    "Expected Feederfed by circuits from circuits file are stored in 'Expected_feederfedcircuits.csv' file")

                df_mismatched = spark.sql(
                    """ SELECT * FROM Expected_feederfedcircuits EXCEPT SELECT * FROM Actual_feederfedcircuits""")
                if df_mismatched.count() == 0:
                    log.info('Actual and Expected feederfed by circuits matched')
                else:
                    log.error('Actual and Expected feederfed by circuits not matched')
                    df_mismatched.createOrReplaceTempView("df_mismatched")
                    tempfolder = downloadsfolderPath + '\\mismatchedcircuits'
                    df_mismatched.coalesce(1).write.option("header", "true").format("csv").mode(
                        "overwrite").save(tempfolder)
                    log.info("Mismatch circuits are stored in 'mismatchedcircuits.csv' file")

            spark.catalog.dropTempView("timeplace_circuits")
            spark.catalog.dropTempView("Actual_feederfedcircuits")
            spark.catalog.dropTempView("circuits")
            spark.catalog.dropTempView("feederOHhconductor")
            spark.catalog.dropTempView("feederUGhconductor")
            spark.catalog.dropTempView("feederdevices")
            spark.catalog.dropTempView("feederFull")
            spark.catalog.dropTempView("tempCircuitInfoTableName")
            spark.catalog.dropTempView("final_circuitTable")
            spark.catalog.dropTempView("Expected_feederfedcircuits")
            spark.catalog.dropTempView("df_mismatched")
            log.info("Dropped all temp tables")
            spark.stop()
            log.info("Spark Session closed")


def get_feedfedby_circuits(df, spark):
    try:
        checkChidCiruits = True
        while checkChidCiruits:
            df.createOrReplaceTempView("tempCircuitInfoTableName")
            df_tempCircuitTable = spark.sql(""" SELECT 
                                                c.circuitinfo_uid,
                                                c.timeplace_foreignkey,
                                                c.fireindex,
                                                ff.feederid AS circuitid,
                                                c.circuitname,
                                                ff.substationname,
                                                c.transmissionimpact,
                                                c.division,
                                                ff.min_branch AS source_min_branch,
                                                ff.max_branch AS source_max_branch,
                                                ff.order_num AS source_order_num,
                                                ff.treelevel AS source_treelevel,
                                                c.additional_max_branch,
                                                c.additional_min_branch,
                                                c.additional_order_num,
                                                c.additional_treelevel,
                                                ff.assettype AS source_isolation_device,
                                                c.additional_isolation_device,
                                                ff.operatingnumber AS source_isolation_device_type,
                                                c.additional_isolation_device_type,
                                                c.flag,
                                                ff.feederfedby AS parentfeederfedby,
                                                c.circuitid as parentcircuitid,
                                                c.tempgenname,
                                                c.comments,
                                                ff.to_line_globalId ,
                                                '' AS from_circuitid,
                                                ff.TO_FEATURE_GLOBALID,
                                                1 as parentloop
                                            FROM 
                                                feederFull fv, 
                                                feederFull ff, 
                                                tempCircuitInfoTableName AS c 
                                                WHERE 
                                                    fv.feederid = LPAD(c.circuitid,9,'0')
                                                AND fv.MIN_BRANCH >= c.source_min_branch
                                                AND fv.MAX_BRANCH <= c.source_max_branch
                                                AND fv.TREELEVEL >= c.source_treelevel
                                                AND fv.ORDER_NUM <= c.source_order_num
                                                AND (fv.to_line_globalid IS NOT NULL AND fv.to_line_globalid <> '') 
                                                AND ff.to_feature_globalid = fv.to_line_globalId AND c.parentloop = 1""")
            df_tempCircuitTable.createOrReplaceTempView("tempCircuitInfoTableName1")
            df = spark.sql(""" SELECT  circuitinfo_uid,timeplace_foreignkey,fireindex,circuitid,circuitname,substationname,transmissionimpact,division,
                               source_min_branch,source_max_branch,source_order_num,source_treelevel,additional_max_branch,additional_min_branch,
                               additional_order_num,additional_treelevel,source_isolation_device,additional_isolation_device,source_isolation_device_type,
                               additional_isolation_device_type,flag,parentfeederfedby,parentcircuitid,tempgenname,comments,to_line_globalId ,from_circuitid,
                               TO_FEATURE_GLOBALID,0 as parentloop from tempCircuitInfoTableName """)
            df.createOrReplaceTempView("tempCircuitInfoTableName")
            if df_tempCircuitTable.count() == 0:
                checkChidCiruits = False
            df = df.union(df_tempCircuitTable)
        return df
    except Exception as e:
        print(str(e))
