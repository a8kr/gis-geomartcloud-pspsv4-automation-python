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

        # Get the latest feeder device table filepaths from db
        get_feederdevicestable_db = queries.get_activetablename % 's3-feedernetworktrace-devices'
        lst_table_details1 = queryresults_fetchone(get_feederdevicestable_db)
        filename_feederdevicestable = lst_table_details1
        log.info("Filename for Feeder Network Trace - Devices active table is : " + str(filename_feederdevicestable))
        log.info("-----------------------------------------------------------------------------------------------")
        feederdevicestablefilename = filename_feederdevicestable.split(s3config()['devpath'])[-1]
        print(feederdevicestablefilename)

        # Download Default Circuits parquet file
        s3 = boto3.client('s3')
        s3_resource = boto3.resource("s3")
        s3_bucketname = s3config()['datastorebucketname']
        BUCKET_PATH = s3config()['defaultmangementpath']
        profilename = s3config()['profile_name']
        defaultmanagementcircuitslocalpath = downloadsfolderPath + "\\defaultmanagement-circuits" + "\\" + filename
        defaultmngtcircuits = downloadsfolderPath + "\\defaultmanagement-circuits"
        deleteFolder(defaultmngtcircuits)
        create_folder(defaultmngtcircuits)
        download_file_from_S3(s3_bucketname, BUCKET_PATH, filename, defaultmanagementcircuitslocalpath, profilename)
        log.info("Downloaded defaultmanagement-circuits parquet file from S3")

        # Download feederdevices parquet file
        s3 = boto3.client('s3')
        s3_resource = boto3.resource("s3")
        s3_bucketname = s3config()['pspsdatabucketname']
        feederdevicesBUCKET_PATH = feederdevicestablefilename
        profilename = s3config()['profile_name']
        feederdevices = downloadsfolderPath + "\\feederdevices"
        deleteFolder(feederdevices)
        create_folder(feederdevices)
        download_dir_from_S3(feederdevicesBUCKET_PATH, s3_bucketname, profilename, feederdevices)
        log.info("Downloaded feederdevices parquet file from S3")

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
            log.info("Converted Intermediate meteorology csv to parquet file")

            df_defaultmngtcircuits = spark.read.parquet(defaultmngtcircuits)
            df_defaultmngtcircuits.createOrReplaceTempView("defaultmngtcircuits")
            tempfolder = r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\df_defaultmngtcircuits"
            df_defaultmngtcircuits.coalesce(1).write.option("header", "true").format("csv").mode(
                "overwrite").save(tempfolder)
            df_defaultmngtcircuits.cache()
            df_meteorology = spark.read.parquet(downloadsfolderPath + '\\meteorologyoutput')
            df_meteorology.createOrReplaceTempView("meteorologycircuits")
            df_meteorology = spark.sql(""" SELECT row_number() over (order by circuitid desc, treelevel desc, order_num asc, max_branch asc, min_branch desc) as circuit_uid,* from meteorologycircuits """)
            df_meteorology.createOrReplaceTempView("meteorologycircuits")
            tempfolder = downloadsfolderPath + '\\meteorologycircuits'
            df_meteorology.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save(tempfolder)
            log.info(
                "List of meteorologycircuits after adding UID")

            # Join Parent circuits with defaultcircuits csv and get default circuits
            defaultcircuits = spark.sql("""SELECT mc.circuit_uid, mc.fireindex, mc.circuitid, mc.opnum, mc.devicetype, 
            mc.circuitname, mc.min_branch, mc.max_branch, mc.treelevel, mc.order_num, mc.substationname, mc.operable, 
            mc.consider, '' as OPERABLE_TRANS_PMETER_COUNT, 'Y' as defaultdevice, 'intersectedcircuit' as action 
            from meteorologycircuits as mc INNER JOIN defaultmngtcircuits as dc on mc.circuitid = dc.circuitId and mc.opnum = 
            dc.sourceIsolationDevice and mc.devicetype = dc.sourceIsolationDeviceType""")
            defaultcircuits.createOrReplaceTempView("tempdefaultcircuits")
            tempfolder = downloadsfolderPath + '\\tempdefaultcircuits'
            defaultcircuits.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save(tempfolder)
            log.info("List of default devices from intersected circuits are identified and output csv is stored in tempdefaultcircuits csv")

            # Remove circuits if already present in intersected circuits list
            df_defaultcircuits_intersected = spark.sql("""SELECT mc.circuit_uid, mc.fireindex, mc.circuitid, mc.opnum, 
            mc.devicetype, mc.circuitname, mc.min_branch, mc.max_branch, mc.treelevel, mc.order_num, 
            mc.substationname, mc.operable, mc.consider,'' as OPERABLE_TRANS_PMETER_COUNT, tc.defaultdevice, tc.action from meteorologycircuits as mc 
            LEFT JOIN tempdefaultcircuits as tc on mc.circuitid = tc.circuitid AND mc.opnum = 
            tc.opnum AND mc.devicetype = tc.devicetype """)
            df_defaultcircuits_intersected.createOrReplaceTempView("defaultcircuits_intersected")
            tempfolder = downloadsfolderPath + '\\defaultcircuits_intersected'
            df_defaultcircuits_intersected.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save(tempfolder)
            log.info("Removal of circuits if already present in intersected circuits list is done and output csv is stored in defaultcircuits_intersected csv")

            # Get the Non Default devices list from above table created
            df_nondefaultdevices = spark.sql("""SELECT * from  defaultcircuits_intersected where defaultdevice is null or defaultdevice = ''""")
            df_nondefaultdevices.createOrReplaceTempView("nondefaultdevices")
            tempfolder = downloadsfolderPath + '\\temp_nondefaultcircuits'
            df_nondefaultdevices.coalesce(1).write.option("header", "true").format("csv").mode(
                "overwrite").save(tempfolder)
            log.info("Non Default devices are filtered and output csv is stored in temp_nondefaultcircuits csv")

            # Read Feederdevice parquet file
            df_feederdevices = spark.read.parquet(feederdevices + '/' + feederdevicesBUCKET_PATH)
            df_feederdevices.createOrReplaceTempView("feederdevices")
            log.info("Read feederdevice parquet files and creating temp table is completed")

            # Do tracing and get upstream devices, remove non operable devices
            df_upstreamdevices = spark.sql("""SELECT DISTINCT c.circuit_uid, c.fireindex, f.feederid as circuitid, f.operatingnumber as opnum,
            f.assettype as devicetype, f.circuitname, f.min_branch, f.max_branch, f.treelevel, f.order_num, 
            f.substationname, c.consider, f.operable, f.OPERABLE_TRANS_PMETER_COUNT, c.defaultdevice, c.action 
            from nondefaultdevices as c inner join feederdevices f
                                            on c.circuitid = f.feederid
                                            AND f.treelevel <= c.treelevel
                                            AND f.order_num >= c.order_num
                                            AND f.max_branch >= c.max_branch
                                            AND f.min_branch <= c.min_branch""")
            df_upstreamdevices.createOrReplaceTempView("upstreamdevices")
            tempfolder = downloadsfolderPath + '\\temp_upstreamdevices'
            df_upstreamdevices.coalesce(1).write.option("header", "true").format("csv").mode(
                "overwrite").save(tempfolder)
            log.info("Upstream Tracing and removal of non operable is done and output csv is stored in temp_upstreamdevices csv")

            df_remnonoperableupstreamdevices = spark.sql("""SELECT * from upstreamdevices where operable in ('M','Y')""")
            df_remnonoperableupstreamdevices.createOrReplaceTempView("remnonoperableupstreamdevices")
            tempfolder = downloadsfolderPath + '\\df_remnonoperableupstreamdevices'
            df_remnonoperableupstreamdevices.coalesce(1).write.option("header", "true").format("csv").mode(
                "overwrite").save(tempfolder)

            # Join upstreamdevices with default devices csv and get default circuits
            defaultcircuits_upstream = spark.sql("""SELECT DISTINCT c.circuit_uid, c.fireindex, c.circuitid, c.opnum,
            c.devicetype, c.circuitname, c.min_branch, c.max_branch, c.treelevel, c.order_num, 
            c.substationname, c.consider, c.operable, c.OPERABLE_TRANS_PMETER_COUNT, 'Y' as defaultdevice, 'upstream' as action 
            from remnonoperableupstreamdevices as c INNER JOIN defaultmngtcircuits as dc on c.circuitid = dc.circuitId 
            and c.opnum = dc.sourceIsolationDevice and c.devicetype = dc.sourceIsolationDeviceType""")
            defaultcircuits_upstream.createOrReplaceTempView("tempupstreamdefaultcircuits")
            tempfolder = downloadsfolderPath + '\\tempupstreamdefaultcircuits'
            defaultcircuits_upstream.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save(tempfolder)
            log.info("List of default devices from upstream circuits are identified and output csv is stored in tempupstreamdefaultcircuits csv")

            # Remove circuits if already present in upstream circuits list
            df_defaultcircuits_upstream = spark.sql("""SELECT c.circuit_uid, c.fireindex, c.circuitid, c.opnum,
            c.devicetype, c.circuitname, c.min_branch, c.max_branch, c.treelevel, c.order_num, 
            c.substationname, c.operable, c.consider, c.OPERABLE_TRANS_PMETER_COUNT, tc.defaultdevice, tc.action from remnonoperableupstreamdevices as c 
            LEFT JOIN tempupstreamdefaultcircuits as tc on 
            c.circuitid = tc.circuitid 
            AND c.opnum = tc.opnum 
            AND c.devicetype = tc.devicetype """)
            df_defaultcircuits_upstream.createOrReplaceTempView("defaultcircuits_upstream")
            tempfolder = downloadsfolderPath + '\\defaultcircuits_upstream'
            df_defaultcircuits_upstream.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save(tempfolder)
            log.info("Removal of circuits if already present in upstream circuits list is done and output csv is stored in defaultcircuits_upstream csv")

            # Join Default intersected and defaultcircuits_upstream
            df_defaultcircuits_union = spark.sql("""SELECT * from tempdefaultcircuits 
            UNION
            SELECT * from defaultcircuits_upstream""")
            df_defaultcircuits_union.createOrReplaceTempView("defaultcircuits_union")
            df_defaultcircuits_union = spark.sql("""SELECT DISTINCT * from defaultcircuits_union order by circuitid asc, order_num asc""")
            df_defaultcircuits_union.createOrReplaceTempView("defaultcircuits_union")
            tempfolder = downloadsfolderPath + '\\defaultcircuits_union'
            df_defaultcircuits_union.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save(
                tempfolder)

            # Order by circuits
            df_defaultcircuits_order = spark.sql(
                """SELECT DISTINCT * from defaultcircuits_union order by circuitid desc, treelevel desc, order_num asc, max_branch asc, min_branch desc""")
            df_defaultcircuits_order.createOrReplaceTempView("defaultcircuits_order")
            tempfolder = downloadsfolderPath + '\\defaultcircuits_order'
            df_defaultcircuits_order.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save(
                tempfolder)

            # Store Defaultcircuits_order csv to dataframe
            tempfolder = downloadsfolderPath + '\\defaultcircuits_order'
            defaultcircuits_order = os.listdir(tempfolder)
            for file in defaultcircuits_order:
                if file.endswith('csv'):
                    break
            defaultcircuits_order_csv = downloadsfolderPath + '\\defaultcircuits_order' + '/' + file
            defaultcircuits_order = pd.read_csv(defaultcircuits_order_csv)
            







            
            















