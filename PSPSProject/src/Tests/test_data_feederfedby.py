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
class TestFeederFedBy(BaseClass):
    def test_feederfedby(self):
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
        # Download feederNetwork_priugconductor parquet file
        filename = "feederNetwork_priugconductor.parquet"
        localpath = downloadsfolderPath + "\\feedernetwork_ugdata" + "\\" + filename
        feedernetwork_ugdata = downloadsfolderPath + "\\feedernetwork_ugdata"
        deleteFolder(feedernetwork_ugdata)
        create_folder(feedernetwork_ugdata)
        download_file_from_S3(s3_bucketname, BUCKET_PATH, filename, localpath, profilename)
        log.info("Downloaded feederNetwork_priugconductor parquet file from S3")
        # Download feederNetwork_priohconductor parquet file
        filename1 = "feederNetwork_priohconductor.parquet"
        localpath1 = downloadsfolderPath + "\\feedernetwork_ohdata" + "\\" + filename1
        feedernetwork_ohdata = downloadsfolderPath + "\\feedernetwork_ohdata"
        deleteFolder(feedernetwork_ohdata)
        create_folder(feedernetwork_ohdata)
        download_file_from_S3(s3_bucketname, BUCKET_PATH, filename1, localpath1, profilename)
        log.info("Downloaded feederNetwork_priohconductor parquet file from S3")
        # Download feederNetwork_priohconductor parquet file
        filename2 = "feederNetwork_device.parquet"
        localpath2 = downloadsfolderPath + "\\feedernetwork_devicesdata" + "\\" + filename2
        feedernetwork_devicesdata = downloadsfolderPath + "\\feedernetwork_devicesdata"
        deleteFolder(feedernetwork_devicesdata)
        create_folder(feedernetwork_devicesdata)
        download_file_from_S3(s3_bucketname, BUCKET_PATH, filename2, localpath2, profilename)
        log.info("Downloaded feederNetwork_device parquet file from S3")
        '''
        # Get Timeplace UID and Timeplace ID for the required timeplace
        i = 0
        for each in var_tp_array:

            get_timeplace_db = queries.get_timeplace % each
            lst_tp_details = queryresults_get_alldata(get_timeplace_db)
            var_tp_uid = lst_tp_details[0][0]
            var_tp_id = lst_tp_details[0][1]
            log.info("Timeplace UID for timeplace: " + str(each) + " is: " + str(var_tp_uid))
            log.info("Timeplace ID for timeplace: " + str(each) + " is: " + str(var_tp_id))
            log.info("-----------------------------------------------------------------------------------------------")
            '''
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
            '''
            # var_tp_uid = "169"
            # var_tp_id = "149"
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
            tempfolder = r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\actualtimeplace_circuits"
            df_timeplacecircuits.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save(
                tempfolder)

            Actual_feederfedcircuits = spark.sql("""SELECT timeplace_foreignkey, fireindex, circuitid, circuitname, 
            substationname, transmissionimpact,division,source_min_branch,source_max_branch,source_order_num,
            source_treelevel,additional_max_branch,additional_min_branch,additional_order_num,additional_treelevel,
            source_isolation_device,additional_isolation_device,source_isolation_device_type,
            additional_isolation_device_type,flag,parentfeederfedby,parentcircuitid,tempgenname,comments FROM 
            timeplace_circuits where parentfeederfedby <> '' """)
            Actual_feederfedcircuits.createOrReplaceTempView("Actual_feederfedcircuits")
            tempfolder = r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\Actual_feederfedcircuits"
            Actual_feederfedcircuits.coalesce(1).write.option("header", "true").format("csv").mode(
                "overwrite").save(tempfolder)
            log.info("Actual Feederfed by circuits from circuits file are stored in 'Actual_feederfedcircuits.csv' file")

            if Actual_feederfedcircuits.count() == 0:
                print('No feederfed by circuits found')
                log.info('No feederfed by circuits found')
            else:
                print('Feederfed by circuits found')
                log.info('Feederfed by circuits found')
                df_filteredcircuits = spark.sql(
                    """ SELECT * from timeplace_circuits where parentfeederfedby is null or parentfeederfedby = '' """)
                df_filteredcircuits.createOrReplaceTempView("circuits")
                tempfolder = r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\circuits"
                df_filteredcircuits.coalesce(1).write.option("header", "true").format("csv").mode(
                    "overwrite").save(tempfolder)

                # Read Feeder tables
                feederohdataloc = downloadsfolderPath + "\\feedernetwork_ohdata" + "\\feederNetwork_priohconductor.parquet"
                feederugdataloc = downloadsfolderPath + "\\feedernetwork_ugdata" + "\\feederNetwork_priugconductor.parquet"
                feederdevicedataloc = downloadsfolderPath + "\\feedernetwork_devicesdata" + "\\feederNetwork_device.parquet"
                df_feederOHhconductor = spark.read.parquet(feederohdataloc)
                df_feederOHhconductor.createOrReplaceTempView("feederOHhconductor")
                df_feederUGconductor = spark.read.parquet(feederugdataloc)
                df_feederUGconductor.createOrReplaceTempView("feederUGhconductor")
                df_feederdevices = spark.read.parquet(feederdevicedataloc)
                df_feederdevices.createOrReplaceTempView("feederdevices")
                feederFull = spark.sql(
                    """SELECT * from feederUGhconductor UNION SELECT * from feederOHhconductor""")
                feederFull.createOrReplaceTempView("feederFull")

                # Do the Downstream trace and get parent circuits
                df_tempCircuitInfoTableName = spark.sql(""" SELECT 
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
                                    circuits AS c 
                                    WHERE 
                                        fv.feederid   = c.circuitid
                                    AND fv.MIN_BRANCH >= c.source_min_branch
                                    AND fv.MAX_BRANCH <= c.source_max_branch
                                    AND fv.TREELEVEL >= c.source_treelevel
                                    AND fv.ORDER_NUM <= c.source_order_num
                                    AND (fv.to_line_globalid IS NOT NULL AND fv.to_line_globalid <> '') 
                                    AND ff.to_feature_globalid = fv.to_line_globalId""")
                df_tempCircuitInfoTableName.createOrReplaceTempView("tempCircuitInfoTableName")
                # Do Tracing and Get Child and grand childs (tocircuits)
                df_tempCircuitInfoTableName = get_feedfedby_circuits(df_tempCircuitInfoTableName, spark)
                df_tempCircuitInfoTableName.createOrReplaceTempView("tempCircuitInfoTableName")
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
                                                WHERE   fv.feederid     =  ce.circuitid
                                                    AND fv.MIN_BRANCH <= ce.source_min_branch  
                                                    AND fv.MAX_BRANCH >= ce.source_max_branch
                                                    AND fv.TREELEVEL <= ce.source_treelevel
                                                    AND fv.ORDER_NUM >= ce.source_order_num
                                                    AND fv.to_feature_fcid IN ( 1003, 1005, 998, 997 )
                                                ORDER BY fv.TREELEVEL ASC  """)
                final_circuitTable.createOrReplaceTempView("final_circuitTable")
                spark.sql(""" SELECT * from tempCircuitInfoTableName 
                                    UNION
                                    SELECT * from final_circuitTable
                                """).createOrReplaceTempView("final_circuitTable")
                tempfolder = r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\feederfedcircuits"
                final_circuitTable.coalesce(1).write.option("header", "true").format("csv").mode(
                    "overwrite").save(tempfolder)
                log.info("Feeder fed by logic completed and circuits list stored in 'feederfedcircuits.csv' file")
                # Remove duplicates:

                final_circuitTable = spark.sql(""" SELECT f.timeplace_foreignkey, f.fireindex, f.circuitid, f.circuitname, f.substationname, f.transmissionimpact,f.division,
                               f.source_min_branch,f.source_max_branch,f.source_order_num,f.source_treelevel,f.additional_max_branch,f.additional_min_branch,
                               f.additional_order_num,f.additional_treelevel,f.source_isolation_device,f.additional_isolation_device,f.source_isolation_device_type,
                               f.additional_isolation_device_type,f.flag,f.parentfeederfedby,f.parentcircuitid,f.tempgenname,f.comments from final_circuitTable as f Full Outer Join circuits as c
                               on c.circuitid = f.circuitid and c.source_isolation_device=f.source_isolation_device
                                And c.source_isolation_device_type = f.source_isolation_device_type """)
                final_circuitTable.createOrReplaceTempView("final_circuitTable")
                tempfolder = r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\duplicatesremoved"
                final_circuitTable.coalesce(1).write.option("header", "true").format("csv").mode(
                    "overwrite").save(tempfolder)
                log.info("Feeder fed by logic completed and circuits list stored in 'feederfedcircuits.csv' file")

                Expected_feederfedcircuits = spark.sql(""" SELECT timeplace_foreignkey, fireindex, circuitid, circuitname, substationname, transmissionimpact,division,
                               source_min_branch,source_max_branch,source_order_num,source_treelevel,additional_max_branch,additional_min_branch,
                               additional_order_num,additional_treelevel,source_isolation_device,additional_isolation_device,source_isolation_device_type,
                               additional_isolation_device_type,flag,parentfeederfedby,parentcircuitid,tempgenname,comments FROM final_circuitTable""")
                Expected_feederfedcircuits.createOrReplaceTempView("Expected_feederfedcircuits")
                tempfolder = r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\Expected_feederfedcircuits"
                Expected_feederfedcircuits.coalesce(1).write.option("header", "true").format("csv").mode(
                    "overwrite").save(tempfolder)
                log.info("Expected Feeder fed by circuits are stored in 'Expected_feederfedcircuits.csv' file")

                df_mismatched = spark.sql(
                    """ SELECT * FROM Expected_feederfedcircuits EXCEPT SELECT * FROM Actual_feederfedcircuits""")
                if df_mismatched.count() == 0:
                    log.info('Actual and Expected feederfed by circuits matched')
                else:
                    log.error('Actual and Expected feederfed by circuits not matched')
                    df_mismatched.createOrReplaceTempView("df_mismatched")
                    tempfolder = r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\mismatchedcircuits"
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
                                                    fv.feederid   = c.circuitid
                                                AND fv.MIN_BRANCH >= c.source_min_branch
                                                AND fv.MAX_BRANCH <= c.source_max_branch
                                                AND fv.TREELEVEL >= c.source_treelevel
                                                AND fv.ORDER_NUM <= c.source_order_num
                                                AND (fv.to_line_globalid IS NOT NULL AND fv.to_line_globalid <> '') 
                                                AND ff.to_feature_globalid = fv.to_line_globalId AND  c.parentloop = 1""")
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
