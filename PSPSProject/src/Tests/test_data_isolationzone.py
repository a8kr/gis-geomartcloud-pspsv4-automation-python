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
class TestIsolationZone(BaseClass):
    def test_isolationzone(self):
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
                var_tp_array.append(timeplace)

        # Get the latest ssd-isolationzone table filepath from db
        get_ssd_isolationzonetable_db = queries.get_activetablename % 's3-ssd-isolationzone'
        lst_table_details1 = queryresults_fetchone(get_ssd_isolationzonetable_db)
        filename_ssd_isolationzonetable = lst_table_details1
        log.info("Filename for ssd-isolationzone active table is : " + str(filename_ssd_isolationzonetable))
        log.info("-----------------------------------------------------------------------------------------------")
        ssd_isolationzonetablefilename = filename_ssd_isolationzonetable.split(s3config()['envpath'])[-1]
        print(ssd_isolationzonetablefilename)

        # Download ssd-isolationzone parquet file from S3- PSPSDataSync folder
        s3 = boto3.client('s3')
        s3_resource = boto3.resource("s3")
        s3_bucketname = s3config()['pspsdatabucketname']
        ssd_isolationzoneBUCKET_PATH = ssd_isolationzonetablefilename
        profilename = s3config()['profile_name']
        ssd_isolationzone = downloadsfolderPath + "\\ssd_isolationzone"
        deleteFolder(ssd_isolationzone)
        create_folder(ssd_isolationzone)
        download_dir_from_S3(ssd_isolationzoneBUCKET_PATH, s3_bucketname, profilename, ssd_isolationzone)
        log.info("Downloaded ssd_isolationzone parquet file from S3")

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

            isozonefilename = str(var_tp_id) + "/" + str(var_tp_uid) + "/isolationZone/isolationZone_" + str(
                var_tp_uid) + "/"
            s3 = boto3.client('s3')
            s3_resource = boto3.resource("s3")
            s3_bucketname = s3config()['datastorebucketname']
            BUCKET_PATH = s3config()['tpbucketpath']
            isozonepath = BUCKET_PATH + isozonefilename
            profilename = s3config()['profile_name']
            local_folder = downloadsfolderPath + "isolationZonecircuits_" + str(var_tp_uid)
            deleteFolder(local_folder)
            download_dir_from_S3(isozonepath, s3_bucketname, profilename, local_folder)
            log.info("Downloaded isolation Zone circuits parquet file from S3")

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

            isolationzonefilepath = os.path.join(
                downloadsfolderPath + "\\isolationZonecircuits_" + str(
                    var_tp_uid) + "\\reports\\timeplacecreation\\" + str(
                    var_tp_id) + "\\" + str(var_tp_uid) + "\\isolationZone\\isolationZone_" + str(var_tp_uid))
            df_isozonecircuits = spark.read.parquet(isolationzonefilepath)
            df_isozonecircuits.createOrReplaceTempView("isolationzonecircuits")
            tempfolder = r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\dc_isolationzonecircuits"
            df_isozonecircuits.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save(
                tempfolder)

            # Read ssd-isolationzone table
            # feedertransformerdataloc = downloadsfolderPath + "\\feederNetwork_transformer"
            df_ssd_isolationzonedataloc = spark.read.parquet(ssd_isolationzone + '/' + ssd_isolationzoneBUCKET_PATH)
            df_ssd_isolationzonedataloc.createOrReplaceTempView("ssd_isolationzone")
            tempfolder = r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\ssd_isolationzonedata"
            df_ssd_isolationzonedataloc.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save(
                tempfolder)

            df_tempcircuits = spark.sql("""Select circuitinfo_uid, timeplace_foreignkey, circuitid, circuitname, 
            substationname, transmissionimpact, division, circuitid || '-' || source_isolation_device || 
            '-' || source_isolation_device_type as isolationzonename, source_min_branch, source_max_branch, 
            source_order_num, source_treelevel, additional_max_branch, additional_min_branch, additional_order_num, 
            additional_treelevel, source_isolation_device, additional_isolation_device, source_isolation_device_type, 
            additional_isolation_device_type, flag, parentfeederfedby, parentcircuitid, tempgenname, comments 
            from timeplace_circuits""")
            df_tempcircuits.createOrReplaceTempView("timeplace_circuits")
            tempfolder = r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\dc_tempcircuitsdata"
            df_tempcircuits.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save(
                tempfolder)

            df_tempisozonedata = spark.sql("""SELECT DISTINCT '' as isolationzone_uid, '' as concat_circuitinfo_uid, 
            c.circuitinfo_uid, c.timeplace_foreignkey, iz.circuitid as circuitId, c.circuitname, 
            c.source_isolation_device, c.source_isolation_device_type, c.deviceoperatingnumber, c.devicetype, 
            c.additional_isolation_device, c.additional_isolation_device_type, c.isolationzonename, '' as flag, 
            c.transmissionimpact, iz.restorationzonename, iz.tier2ohmiles, iz.tier3ohmiles, iz.totalohmiles, iz.tier2ugmiles, 
            iz.tier3ugmiles, iz.totalugmiles, iz.tier2ohpoles, iz.tier3ohpoles, iz.totalpoles, iz.shape, '' as impacttype from 
            timeplace_circuits as c inner join ssd_isolationzone iz 
            on iz.isolationzone = c.isolationzonename 
            AND LPAD(iz.circuitid,9,'0') = LPAD(c.circuitid,9,'0') 
            AND iz.TREELEVEL >= c.source_treelevel 
            AND iz.ORDER_NUM <= c.source_order_num 
            AND iz.MAX_BRANCH <= c.source_max_branch 
            AND iz.MIN_BRANCH >= c.source_min_branch""")
            df_tempisozonedata.createOrReplaceTempView("tempisolationzonedata")
            tempfolder = r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\df_tempisolationzonedata"
            df_tempisozonedata.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save(
                tempfolder)

