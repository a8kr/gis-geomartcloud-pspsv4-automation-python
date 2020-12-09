import os
import datetime
import time

import pytest
import pandas as pd
import boto3

from PSPSProject.src.Pages.DefaultManagement import DefaultManagement
from PSPSProject.src.Pages.HomePage import HomePage
from PSPSProject.src.Repository.uilocators import locators
from PSPSProject.src.Repository.statictext import textMessage
from PSPSProject.src.Repository.dbqueries import queries
from PSPSProject.src.ReusableFunctions.baseclass import BaseClass, exceptionRowCount
from PSPSProject.src.ReusableFunctions.databasefunctions import *
from PSPSProject.src.ReusableFunctions.awsfunctions import *
from PSPSProject.src.ReusableFunctions.commonfunctions import logfilepath, deleteFiles, readData, getCSVrowCount, getCurrentTime
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
        final_assert = []
        log.info("Starting Validation")
        if var_os == "MAC-OS":
            homepage.SignOn()
            log.info("Successfully entered user id & password:")
            print("Successfully entered user id & password:")

        # filename = "s3://psps-datastore-dev/reports/defaultmanagement/defaultmanagement_circuit_12_04_2020_11_54_12.parquet"
        # var_tp_uid = "169"

        # s3 = boto3.client('s3')
        # s3_resource = boto3.resource("s3")
        # s3_bucketname = s3config()['datastorebucketname']
        # #BUCKET_PATH = s3config()['tpbucketpath']
        # #path = BUCKET_PATH + filename
        # path = s3_bucketname + filename
        # profilename = s3config()['profile_name']
        # local_folder = downloadsfolderPath + "circuits_" + str(var_tp_uid)
        # deleteFolder(local_folder)
        # var_res = download_dir_from_S3(path, s3_bucketname, profilename, local_folder)
        # print (var_res)
        # log.info("Downloaded circuits parquet file from S3")

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

        # Validate database for S3 file path
        var_s3_path_db = queryresults_get_one(queries.get_activetablename)
        log.info("Get S3 file path from DB: " + str(var_s3_path_db))

        var_today = str(datetime.date.today())
        var_year = var_today.split('-')[0]
        var_month = var_today.split('-')[1]
        var_day = var_today.split('-')[2]
        var_date_validate = var_month + "_" + var_day + "_" + var_year

        # Validate that currect date present in the S3 path
        if str(var_date_validate) in str(var_s3_path_db):
            log.info("Validate S3 path in database passed: " + str(var_s3_path_db))

        # Verify message retained in the Default management screen on navigating to other pages and returning back
        homepage.navigate_eventManagement()
        homepage.navigate_defaultManagement()
        var_message = uielements.getValue(locators.dm_status_message)
        if var_message in textMessage.upload_file_successfully:
            log.info("Verify 'Default devices data uploaded successfully' message retained page on navigating to other pages and returning passed")
        else:
            log.error("Verify 'Default devices data uploaded successfully' message retained page on navigating to other pages and returning failed")
            final_assert.append(False)

        log.info("*************AUTOMATION EXECUTION COMPLETED*************")




