import os
import time

import pandas as pd
import pytest

from PSPSProject.src.Pages.DefaultManagement import DefaultManagement
from PSPSProject.src.Pages.HomePage import HomePage
from PSPSProject.src.Repository.uilocators import locators
from PSPSProject.src.Repository.statictext import textMessage
from PSPSProject.src.ReusableFunctions.baseclass import BaseClass, exceptionRowCount
from PSPSProject.src.ReusableFunctions.commonfunctions import logfilepath, deleteFiles, readData, testDatafolderPath, \
    convertExcelToTextIndex, convertTexttoCSV, getMostRecent_downloaded_File, getCSVrowCount
from PSPSProject.src.ReusableFunctions.uiactions import UI_Element_Actions
from PSPSProject.src.Tests.conftest import downloadsfolder, testDatafilePath, testDatafolderPath

VAR_TESTCASENAME = os.path.basename(__file__)
global VAR_COUNT, VAR_TESTENV
VAR_COUNT = 0
VAR_TESTENV = ""
VAR_COUNT = BaseClass().InitializeExecution(VAR_TESTCASENAME, VAR_COUNT, VAR_TESTENV)


@pytest.mark.skipif(not VAR_COUNT, reason="Excluded from regression suite")
@pytest.mark.regression
class TestDefaultManagementNegative(BaseClass):
    def test_defaultmanagement_negative(self):
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

        homepage.navigate_defaultManagement()
        var_flag = uielements.iselementEnabled(locators.dm_uplaodfile)
        if var_flag:
            log.info("Upload file button is enabled by default in Default Management Page")
        else:
            log.error("Upload file button is not enabled by default in Default Management Page")
            final_assert.append(False)

        var_flag = uielements.iselementEnabled(locators.dm_save_btn)
        if not var_flag:
            log.info("Save button is disabled by default in Default Management Page")
        else:
            log.error("Save button is not disabled by default in Default Management Page")
            final_assert.append(False)

        var_dmgridcolumnnames = readData(testDatafilePath, "Main", var_row, 10)
        var_flag = defmanagement.dm_Gridheader(var_dmgridcolumnnames)
        if var_flag:
            log.info("Default Management Grid header displayed as expected")
        else:
            log.error("Default Management Grid header NOT displayed as expected")
            final_assert.append(False)

        # # Upload invalid files -- apart from csv
        # var_filename = readData(testDatafilePath, "Main", var_row, 9)
        # var_filename = var_filename.split(',')
        # nooffiles = len(var_filename)
        # for i in range(nooffiles):
        #     uielements.Click(locators.dm_uplaodfile)
        #     log.info("Clicked on Upload a file button")
        #     var_uploadsuccess = defmanagement.dm_fileupload(testDatafolderPath, var_filename[i])
        #     if var_uploadsuccess == 'Allowed file is .csv':
        #         log.info("Error message is displayed for incorrect file type: " + var_uploadsuccess +" and file uploaded is: " +var_filename[i])
        #     else:
        #         log.error("Error message is not displayed for incorrect file type: " + var_uploadsuccess +" and file uploaded is: " +var_filename[i])
        #         final_assert.append(False)
        #     uielements.Click(locators.dm_close_uploadpopup)
        #
        # # Invalid files
        # log.info("Start file validation regression")
        # for i in range(0, 16):
        #     log.info("Create cvs file from Valid_Invalid_Circuits.xlsx file tab: " + str(i))
        #     var_row_num = i
        #     var_row_num = var_row_num + 2
        #     filePath = os.path.join(testDatafolderPath, "dm_Valid_Invalid_Circuits.xlsx")
        #     var_error_message_file = readData(filePath, "Error_Message", var_row_num, 1)
        #     log.info("Validate error message in the log file: " + var_error_message_file)
        #     print(i)
        #     convertExcelToTextIndex(testDatafolderPath + '/dm_Valid_Invalid_Circuits.xlsx', i,
        #                    testDatafolderPath + '/dm_Valid_Invalid_Circuits.txt')
        #     convertTexttoCSV(testDatafolderPath + '/dm_Valid_Invalid_Circuits.txt',
        #                  testDatafolderPath + '/dm_Valid_Invalid_Circuits.csv')
        #     uielements.Click(locators.dm_uplaodfile)
        #     log.info("Click on Upload file button")
        #     var_uploadFileName = "dm_Valid_Invalid_Circuits.csv"
        #     var_error_message = "File validation failed."
        #     defmanagement.dm_validatefile(testDatafolderPath, var_uploadFileName, var_error_message)
        #     log.info("Validate File validation failed message")
        #     uielements.Click(locators.dm_validationlink)
        #     log.info("Click on Validation log link")
        #     # Read most recent file from download folder
        #     var_error_log = getMostRecent_downloaded_File()
        #     # Get the Error message from file
        #     col_list = ["Error message"]
        #     var_file_message = pd.read_csv(var_error_log, usecols=col_list)
        #     if open(var_error_log).read().find(var_error_message_file):
        #         print("Error message for verified for failed file tab:  " + str(i) + "message: " + var_file_message)
        #         log.info("Error message for verified for failed file tab:  " + str(i) + "message: " + var_file_message)
        #     else:
        #         log.error("Error message not displayed properly: " + var_file_message)
        #         final_assert.append(False)
        #
        #     uielements.Click(locators.dm_close_uploadpopup)
        #     log.info("Close upload file modal")


        # Invalid files for PV-1153
        log.info("Start file validation regression")
        for i in range(17, 18):
            log.info("Create cvs file from Valid_Invalid_Circuits.xlsx file tab: " + str(i))
            var_row_num = i
            var_row_num = var_row_num + 2
            filePath = os.path.join(testDatafolderPath, "dm_Valid_Invalid_Circuits.xlsx")
            var_error_message_file = readData(filePath, "Error_Message", var_row_num, 1)

            log.info("Validate error message in the log file: " + var_error_message_file)
            print(i)
            convertExcelToTextIndex(testDatafolderPath + '/dm_Valid_Invalid_Circuits.xlsx', i,
                           testDatafolderPath + '/dm_Valid_Invalid_Circuits.txt')
            convertTexttoCSV(testDatafolderPath + '/dm_Valid_Invalid_Circuits.txt',
                         testDatafolderPath + '/dm_Valid_Invalid_Circuits.csv')
            uielements.Click(locators.dm_uplaodfile)
            log.info("Click on Upload file button")
            var_uploadFileName = "dm_Valid_Invalid_Circuits.csv"
            var_totalcircuits = getCSVrowCount(testDatafolderPath, var_uploadFileName)

            var_uploadsuccess = defmanagement.dm_fileupload(testDatafolderPath, var_uploadFileName)
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

            uielements.Click(locators.dm_save_btn)

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
            if var_message in textMessage.upload_file_validation_error_message:
                log.info("Message 'Circuit validation failed' validation passed")
                uielements.Click(locators.dm_validationlink_bottom)
                log.info("Click on Validation Log link")
            else:
                log.error("Message 'Circuit validation failed' validation failed")
                final_assert.append(False)

            # Read most recent file from download folder
            var_error_log = getMostRecent_downloaded_File()
            # Get the Error message from file
            col_list = ["Error message"]
            var_file_message = pd.read_csv(var_error_log, usecols=col_list)

            if open(var_error_log).read().find(var_error_message_file):
                print("Error message for verified for failed file tab:  " + str(i) + "message: " + var_file_message)
                log.info("Error message for verified for failed file tab:  " + str(i) + "message: " + var_file_message)
            else:
                log.error("Error message not displayed properly: " + var_file_message)
                final_assert.append(False)

        # Verify error message retained in the Default management screen on navigating to other pages and returning back
        homepage.navigate_eventManagement()
        homepage.navigate_defaultManagement()
        var_message = uielements.getValue(locators.dm_status_message)
        if var_message in textMessage.upload_file_validation_error_message:
            log.info("Verify error message retained page on navigating to other pages and returning passed")
        else:
            log.error("Verify error message retained page on navigating to other pages and returning failed")
            final_assert.append(False)




        log.info("*************AUTOMATION EXECUTION COMPLETED*************")




