import os
import datetime
import time

import pytest
import pandas as pd
import boto3

from pyspark.sql import SparkSession
from PSPSProject.src.Pages.DefaultManagement import DefaultManagement
from PSPSProject.src.Pages.HomePage import HomePage
from PSPSProject.src.Repository.uilocators import locators
from PSPSProject.src.Repository.statictext import textMessage
from PSPSProject.src.Repository.dbqueries import queries
from PSPSProject.src.ReusableFunctions.baseclass import BaseClass, exceptionRowCount
from PSPSProject.src.ReusableFunctions.databasefunctions import *
from PSPSProject.src.ReusableFunctions.awsfunctions import *
from PSPSProject.src.ReusableFunctions.commonfunctions import logfilepath, deleteFiles, readData, getCSVrowCount, \
    getCurrentTime
from PSPSProject.src.ReusableFunctions.uiactions import UI_Element_Actions
from PSPSProject.src.Tests.conftest import downloadsfolder, testDatafilePath, testDatafolderPath
from PSPSProject.src.Tests.conftest import downloadsfolder, testDatafilePath, s3config, dirctorypath

from PSPSProject.src.Repository.dbqueries import queries
from PSPSProject.src.ReusableFunctions.awsfunctions import download_file_from_S3, download_dir_from_S3
from PSPSProject.src.ReusableFunctions.baseclass import BaseClass, exceptionRowCount
from PSPSProject.src.ReusableFunctions.commonfunctions import logfilepath, deleteFiles, downloadsfolderPath, readData, \
    deleteFolder, create_folder
from PSPSProject.src.ReusableFunctions.databasefunctions import queryresults_get_alldata

VAR_TESTCASENAME = os.path.basename(__file__)
global VAR_COUNT, VAR_TESTENV
VAR_COUNT = 0
VAR_TESTENV = ""
VAR_COUNT = BaseClass().InitializeExecution(VAR_TESTCASENAME, VAR_COUNT, VAR_TESTENV)


@pytest.mark.skipif(not VAR_COUNT, reason="Excluded from regression suite")
@pytest.mark.regression
class TestDefaultManagementPositive(BaseClass):
    def test_defaultmanagement_positive(self):
        try:
            var_row = VAR_COUNT[0]
            var_os = VAR_COUNT[2]
        except(ValueError, Exception):
            exceptionrow = exceptionRowCount()
            var_row = exceptionrow
        log = self.getLogger(logfilepath, VAR_TESTCASENAME)
        homepage = HomePage(self.driver)
        defmanagement = DefaultManagement(self.driver)
        uielements = UI_Element_Actions(self.driver)
        deleteFiles(downloadsfolder, ".csv")
        var_execution_flag = ''
        final_assert = []
        log.info("Starting Validation")
        if var_os == "MAC-OS":
            homepage.SignOn()
            log.info("Successfully entered user id & password:")
            print("Successfully entered user id & password:")

        homepage.navigate_defaultManagement()
        uielements.Click(locators.dm_uplaodfile)
        log.info("Clicked on Upload a file button")
        var_uploadfilename = readData(testDatafilePath, "Main", var_row, 8)
        var_totalcircuits = getCSVrowCount(testDatafolderPath, var_uploadfilename)
        var_uploadsuccess = defmanagement.dm_fileupload(testDatafolderPath, var_uploadfilename)
        if var_uploadsuccess == 'Validation success':
            log.info("Successfully uploaded circuits: " + var_uploadsuccess)
        else:
            log.error("Upload circuits failed: " + var_uploadsuccess)
            final_assert.append(False)
        self.driver.find_element_by_xpath(locators.dm_upload_btn).click()
        time.sleep(0.5)
        var_ccount = uielements.getValue(locators.grid_totalcircuits)
        var_circuits = var_ccount.split()
        if int(var_circuits[0]) == var_totalcircuits:
            log.info("Total Circuits count matched between Circuits grid & Uploaded circuits file!")
        else:
            log.error("Total Circuits count not matched between Circuits grid & Uploaded circuits file!")
            final_assert.append(False)

        var_dmgridcolumnnames = readData(testDatafilePath, "Main", var_row, 11)
        dm_Gridheader = defmanagement.dm_Gridheader(var_dmgridcolumnnames)
        if dm_Gridheader[0] == dm_Gridheader[1]:
            log.info("Default Management Grid header displayed as expected")
        else:
            log.error("Default Management Grid header NOT displayed as expected")
            final_assert.append(False)

        var_flag = uielements.iselementEnabled(locators.dm_save_btn)
        if var_flag:
            log.info("Save button is enabled after the valid circuit file uploaded")
            uielements.Click(locators.dm_save_btn)
            log.info("Click on Save button")
            while True:
                try:
                    var_message = uielements.getValue(locators.dm_status_message)
                    if var_message in textMessage.upload_file_in_progress_message:
                        continue
                    else:
                        break
                except:
                    break

            var_message = uielements.getValue(locators.dm_status_message)
            if var_message in textMessage.upload_file_successfully:
                log.info("Message 'Default devices data uploaded successfully' validation passed")
            else:
                log.error("Message 'Default devices data uploaded successfully' validation failed")
                final_assert.append(False)
        else:
            log.error("Save button is not enabled after the valid circuit file uploaded")
            final_assert.append(False)

        # Verify message retained in the Default management screen on navigating to other pages and returning back
        homepage.navigate_eventManagement()
        homepage.navigate_defaultManagement()
        var_message = uielements.getValue(locators.dm_status_message)
        if var_message in textMessage.upload_file_successfully:
            log.info("Verify 'Default devices data uploaded successfully' message retained page on navigating to "
                     "other pages and returning passed")
        else:
            log.error("Verify 'Default devices data uploaded successfully' message retained page on navigating to "
                      "other pages and returning failed")
            final_assert.append(False)
        if var_execution_flag == 'fail':
            log.error("Execution failed: Errors found in execution!!")
            assert False

        # Validate database for S3 file path
        # Get the latest table filepaths from db
        get_defaulttable_db = queries.get_activetablename % 's3-defaultmanagement-circuits'
        lst_table_details = queryresults_fetchone(get_defaulttable_db)
        filename_defaulttable_s3 = lst_table_details
        log.info("Filename for Default Management active table from S3 is : " + str(filename_defaulttable_s3))
        log.info("-----------------------------------------------------------------------------------------------")

        var_today = str(datetime.date.today())
        var_year = var_today.split('-')[0]
        var_month = var_today.split('-')[1]
        var_day = var_today.split('-')[2]
        var_date_validate = var_month + "_" + var_day + "_" + var_year

        # Validate that currect date present in the S3 path
        if str(var_date_validate) in str(filename_defaulttable_s3):
            log.info("Validate S3 path in database passed and latest uploaded file has the current date: " + str(
                filename_defaulttable_s3))
        else:
            log.error("Latest uploaded file doesnt have current date in the file uploaded")
            final_assert.append(False)

        # Validate if Uploaded default circuits from UI is same as in S3 bucket parquet file
        # Download Default Circuits parquet file
        dcfilename = filename_defaulttable_s3.split('/')[-1]
        s3 = boto3.client('s3')
        s3_resource = boto3.resource("s3")
        s3_bucketname = s3config()['datastorebucketname']
        BUCKET_PATH = s3config()['defaultmangementpath']
        profilename = s3config()['profile_name']
        defaultmanagementcircuitslocalpath = downloadsfolderPath + "\\defaultmanagement-circuits" + "\\" + dcfilename
        defaultmngtcircuits = downloadsfolderPath + "\\defaultmanagement-circuits"
        deleteFolder(defaultmngtcircuits)
        create_folder(defaultmngtcircuits)
        download_file_from_S3(s3_bucketname, BUCKET_PATH, dcfilename, defaultmanagementcircuitslocalpath, profilename)
        log.info("Downloaded defaultmanagement-circuits parquet file from S3")

        spark = SparkSession.builder.appName("Timeplace-Creation") \
            .config('spark.driver.memory', '10g') \
            .config("spark.cores.max", "6") \
            .config('spark.yarn.appMasterEnv.ARROW_PRE_0_15_IPC_FORMAT', 1) \
            .config('spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT', 1) \
            .config("spark.sql.execution.arrow.enabled", "true") \
            .config("spark.sql.catalogImplementation", "in-memory") \
            .getOrCreate()
        log.info("Spark session connected")
        df_defaultmngtcircuits = spark.read.parquet(defaultmngtcircuits)
        df_defaultmngtcircuits.createOrReplaceTempView("defaultmngtcircuits")
        tempfolder = downloadsfolderPath + '\\df_defaultmngtcircuits'
        df_defaultmngtcircuits.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save(tempfolder)

        # Store Defaultcircuits csv to dataframe
        defaultcircuits = os.listdir(tempfolder)
        for file in defaultcircuits:
            if file.endswith('csv'):
                break
        defaultcircuits_csv = downloadsfolderPath + '\\df_defaultmngtcircuits' + '/' + file
        defaultcircuits_actual = pd.read_csv(defaultcircuits_csv)

        defaultcircuitsexpected_csv = testDatafolderPath + '\\' + var_uploadfilename
        defaultcircuits_expected = pd.read_csv(defaultcircuitsexpected_csv)
        # Rename Columns
        defaultcircuits_expected.rename(
            columns={'Circuit name': 'circuitName', 'Circuit ID': 'circuitId', 'Source Is.D': 'sourceIsolationDevice',
                     'Source Is.D Type': 'sourceIsolationDeviceType', 'Substation': 'substation'},
            inplace=True)
        defaultcircuits_expected['sourceIsolationDevice'] = defaultcircuits_expected['sourceIsolationDevice'].fillna('CB', inplace=False)

        df1 = pd.merge(defaultcircuits_actual, defaultcircuits_expected, on=['circuitName', 'circuitId', 'sourceIsolationDevice', 'sourceIsolationDeviceType', 'substation'], how='outer', indicator=True)
        df1 = df1[df1['_merge'] != 'both']
        if len(df1) == 0:
            log.info("Uploaded default circuits from UI is same as in S3 bucket parquet file")
        else:
            mismatchdefaultcircuits = downloadsfolderPath + "\\mismatchdefaultcircuits"
            deleteFolder(mismatchdefaultcircuits)
            create_folder(mismatchdefaultcircuits)
            df1.to_csv(mismatchdefaultcircuits + '/mismatchdefaultcircuits.csv')
            log.error("Uploaded default circuits from UI is not same as in S3 bucket parquet file: " + str(df1))
            final_assert.append(False)
        if var_execution_flag == 'fail':
            log.error("Execution failed: Errors found in execution!!")
            assert False
        log.info("----------------------------------------------------------------------------------------------")
        log.info("*************AUTOMATION EXECUTION COMPLETED*************")
