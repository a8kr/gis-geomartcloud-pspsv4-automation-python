import os
import time

import pandas as pd
import pytest

from PSPSProject.src.Pages.DefaultManagement import DefaultManagement
from PSPSProject.src.Pages.HomePage import HomePage
from PSPSProject.src.Pages.EventPage import EventPage
from PSPSProject.src.Repository.uilocators import locators
from PSPSProject.src.Repository.statictext import textMessage
from PSPSProject.src.ReusableFunctions.baseclass import BaseClass, exceptionRowCount
from PSPSProject.src.ReusableFunctions.commonfunctions import logfilepath, deleteFiles, readData, testDatafolderPath, \
    convertExcelToTextIndex, convertTexttoCSV, getMostRecent_downloaded_File, getCSVrowCount
from PSPSProject.src.ReusableFunctions.uiactions import UI_Element_Actions
from PSPSProject.src.Tests.conftest import downloadsfolder, testDatafilePath, testDatafolderPath
from PSPSProject.src.ReusableFunctions.commonfunctions import *
from PSPSProject.src.Repository.dbqueries import queries
from PSPSProject.src.ReusableFunctions.databasefunctions import *

VAR_TESTCASENAME = os.path.basename(__file__)
global VAR_COUNT, VAR_TESTENV
VAR_COUNT = 0
VAR_TESTENV = ""
VAR_COUNT = BaseClass().InitializeExecution(VAR_TESTCASENAME, VAR_COUNT, VAR_TESTENV)


@pytest.mark.skipif(not VAR_COUNT, reason="Excluded from regression suite")
@pytest.mark.regression
class TestDefaultManagementNegative(BaseClass):
    def test_eventmanagement_circuit_validation(self):
        try:
            var_row = VAR_COUNT[0]
            var_os = VAR_COUNT[2]
        except(ValueError, Exception):
            exceptionrow = exceptionRowCount()
            var_row = exceptionrow
        log = self.getLogger(logfilepath, VAR_TESTCASENAME)
        homepage = HomePage(self.driver)
        eventpage = EventPage(self.driver)
        defmanagement = DefaultManagement(self.driver)
        uielements = UI_Element_Actions(self.driver)
        deleteFiles(downloadsfolder, ".csv")
        var_execution_flag = ''
        final_assert = []

        log.info("----------------------------------------------------------------------------------------------")
        log.info("-------------------------------- Starting Validation------------------------------------------")
        if var_os == "MAC-OS":
            homepage.SignOn()
            log.info("Successfully entered user id & password:")
            print("Successfully entered user id & password:")

        homepage.navigate_eventManagement()
        log.info("Select Event Management menu")
        var_flag = uielements.iselementEnabled(locators.new_timeplace_browse_button)
        if var_flag:
            log.info("Upload file button is enabled by default")
        else:
            log.error("Upload file button is not enabled by default")
            final_assert.append(False)

        # # Valid Circuit validation
        # log.info("Start Circuit error validation")
        # var_uploadFileName = "em_Valid_Circuits.csv"
        # var_error_message = "Validation success."
        # eventpage.tp_validatefile_message(testDatafolderPath, var_uploadFileName, var_error_message)
        # log.info("Validate File success message")
        #
        # var_Timestamp = getCurrentTime()
        # var_tp_name_internal = "Auto_TP_" + var_Timestamp
        # var_tp_name_external = "Ext_Auto_TP_" + var_Timestamp
        # log.info("TP name: " + var_tp_name_internal)
        #
        # # Enter TP names
        # uielements.setText(var_tp_name_internal, locators.new_timeplace_internal_name)
        # uielements.setText(var_tp_name_external, locators.new_timeplace_external_name)
        # log.info("Enter TP names")
        #
        # uielements.Click(locators.new_timeplace_create_button)
        # log.info("Click on Create button")
        # time.sleep(5)
        # while True:
        #     try:
        #         var_status_message = uielements.getValue(locators.new_timeplace_validation_message)
        #         if var_status_message in "Time place creation in progress.":
        #             continue
        #         else:
        #             break
        #     except:
        #         break
        # log.info("Wait for 'Time place creation in progress' completion " )
        #
        # if uielements.iselementDisplayed(locators.view_time_place_grid_header) == True:
        #     log.info("TP created and navigated to View TP tab")
        #
        # # Get TP DB UID
        # get_new_tp_db = queries.get_timeplace % var_tp_name_internal
        # lst_table_details = queryresults_fetchone(get_new_tp_db)
        # if lst_table_details:
        #     log.info("TP record created in DB: uid is " + str(lst_table_details))
        #
        #  # Navigate to New TP tab
        # uielements.Click(locators.new_time_place_new_tab)
        #
        #
        # # Clear form
        # uielements.Click(locators.new_timeplace_clear_button)
        # uielements.Click(locators.new_timeplace_clear_modal_yes_button)
        #
        # # Refresh form
        # uielements.Click(locators.new_event_tab)
        # uielements.Click(locators.new_time_place_new_tab)


        # File Errors Validations
        log.info("Start files error validation")
        var_uploadFileName = "em_Valid_Invalid_Circuits_logfiles.xlsx"
        var_error_message = "Invalid File! Allowed file is either a csv or a zip file."
        eventpage.tp_validatefile_message(testDatafolderPath, var_uploadFileName, var_error_message)
        log.info("Validate error message for zip and cvs files only")

        # Clear form
        uielements.Click(locators.new_timeplace_clear_button)
        uielements.Click(locators.new_timeplace_clear_modal_yes_button)

        # Refresh form
        uielements.Click(locators.new_event_tab)
        uielements.Click(locators.new_time_place_new_tab)

        for i in range(23, 27):
            log.info("Create cvs file from em_Valid_Invalid_Circuits_logfiles.xlsx file tab: " + str(i))
            var_row_num = i
            var_row_num = var_row_num + 2
            filePath = os.path.join(testDatafolderPath, "em_Valid_Invalid_Circuits_logfiles.xlsx")
            var_error_message_file = readData(filePath, "Error_Message", var_row_num, 1)
            log.info("Validate error message in the log file: " + var_error_message_file)
            print(i)
            convertExcelToTextIndex(testDatafolderPath + '/em_Valid_Invalid_Circuits_logfiles.xlsx', i,
                           testDatafolderPath + '/em_Valid_Invalid_Circuits_logfiles.txt')
            convertTexttoCSV(testDatafolderPath + '/em_Valid_Invalid_Circuits_logfiles.txt',
                         testDatafolderPath + '/em_Valid_Invalid_Circuits_logfiles.csv')

            var_uploadFileName = "em_Valid_Invalid_Circuits_logfiles.csv"
            var_error_message = "File validation failed."
            eventpage.tp_validatefile(testDatafolderPath, var_uploadFileName, var_error_message)
            log.info("Validate File validation failed message")
            uielements.Click(locators.new_timeplace_fileupload_error_link)
            log.info("Click on Validation log link")

            # Read most recent file from download folder
            var_error_log = getMostRecent_downloaded_File()
            # Get the Error message from file
            col_list = ["error message"]
            var_file_message = pd.read_csv(var_error_log, usecols=col_list)
            if open(var_error_log).read().find(var_error_message_file):
               print("Error message for verified for failed file tab:  " + str(i) + "message: " + var_file_message)
            else:
               log.error("Error message not displayed properly: " + var_file_message)
               final_assert.append(False)

            # Clear form
            uielements.Click(locators.new_timeplace_clear_button)
            uielements.Click(locators.new_timeplace_clear_modal_yes_button)

            # Refresh form
            uielements.Click(locators.new_event_tab)
            uielements.Click(locators.new_time_place_new_tab)


        # # Circuit Errors Messages Validations
        # log.info("Start Circuit messages error validation")
        # for i in range(0, 7):
        #     log.info("Create cvs file from em_Valid_Invalid_Circuits_message.xlsx file tab: " + str(i))
        #     var_row_num = i
        #     var_row_num = var_row_num + 2
        #     filePath = os.path.join(testDatafolderPath, "em_Valid_Invalid_Circuits_message.xlsx")
        #     var_error_message_file = readData(filePath, "Error_Message", var_row_num, 1)
        #     log.info("Validate error message in the log file: " + var_error_message_file)
        #     print(i)
        #     convertExcelToTextIndex(testDatafolderPath + '/em_Valid_Invalid_Circuits_message.xlsx', i,
        #                    testDatafolderPath + '/em_Valid_Invalid_Circuits_message.txt')
        #     convertTexttoCSV(testDatafolderPath + '/em_Valid_Invalid_Circuits_message.txt',
        #                  testDatafolderPath + '/em_Valid_Invalid_Circuits_message.csv')
        #
        #     var_uploadFileName = "em_Valid_Invalid_Circuits_message.csv"
        #     var_error_message = "One or more columns are missing in the file. Expected column names are: FIA, Circuit ID, Circuit name, source_isolation_device, source_isolation_device_type, additional_isolation_device, additional_isolation_device_type, Flag, tempgenname, inc, substationname, transmissionimpact, Comments (note: column names are not case sensitive)"
        #     eventpage.tp_validatefile_message(testDatafolderPath, var_uploadFileName, var_error_message_file)
        #     log.info("Validate File validation failed message")
        #
        #     # Clear form
        #     uielements.Click(locators.new_timeplace_clear_button)
        #     uielements.Click(locators.new_timeplace_clear_modal_yes_button)
        #
        #     # Refresh form
        #     uielements.Click(locators.new_event_tab)
        #     uielements.Click(locators.new_time_place_new_tab)

        # Circuit Errors Validations
        log.info("Start Circuit error validation")
        for i in range(5, 8):
            log.info("Create cvs file from em_Valid_Invalid_Circuits.xlsx file tab: " + str(i))
            var_row_num = i
            var_row_num = var_row_num + 2
            filePath = os.path.join(testDatafolderPath, "em_Valid_Invalid_Circuits.xlsx")
            var_error_message_file = readData(filePath, "Error_Message", var_row_num, 1)
            log.info("Validate error message in the log file: " + var_error_message_file)
            print(i)
            convertExcelToTextIndex(testDatafolderPath + '/em_Valid_Invalid_Circuits.xlsx', i,
                           testDatafolderPath + '/em_Valid_Invalid_Circuits.txt')
            convertTexttoCSV(testDatafolderPath + '/em_Valid_Invalid_Circuits.txt',
                         testDatafolderPath + '/em_Valid_Invalid_Circuits.csv')

            var_uploadFileName = "em_Valid_Invalid_Circuits.csv"
            var_error_message = "Validation success."
            eventpage.tp_validatefile_message(testDatafolderPath, var_uploadFileName, var_error_message)
            log.info("Validate File success message")

            var_Timestamp = getCurrentTime()
            var_tp_name_internal = "Auto_TP_" + var_Timestamp
            var_tp_name_external = "Auto_TP_" + var_Timestamp

            # Enter TP names
            uielements.setText(var_tp_name_internal, locators.new_timeplace_internal_name)
            uielements.setText(var_tp_name_external, locators.new_timeplace_external_name)
            log.info("Enter TP names")

            uielements.Click(locators.new_timeplace_create_button)
            log.info("Click on Create button")
            time.sleep(5)
            while True:
                try:
                    var_status_message = uielements.getValue(locators.new_timeplace_validation_message)
                    if var_status_message in "Time place creation in progress.":
                        continue
                    else:
                        break
                except:
                    break
            log.info("Wait for 'Time place creation in progress' completion " )

            # Validate 'Circuit validation failed! See log for failed records.'
            var_status_message = uielements.getValue(locators.new_timeplace_validation_message)
            if var_status_message in "Circuit validation failed! See log for failed records.":
                log.info("Validate 'Circuit validation failed' message exists")
                uielements.Click(locators.new_timeplace_validation_error_link)
                log.info("Click on Validation error message")


            # Read most recent file from download folder
            var_error_log = getMostRecent_downloaded_File()
            # Get the Error message from file
            col_list = ["error message"]
            var_file_message = pd.read_csv(var_error_log, usecols=col_list)
            if open(var_error_log).read().find(var_error_message_file):
               print("Error message for verified for failed file tab:  " + str(i) + "message: " + var_file_message)
            else:
               log.error("Error message not displayed properly: " + var_file_message)
               final_assert.append(False)

            # Clear form
            uielements.Click(locators.new_timeplace_clear_button)
            uielements.Click(locators.new_timeplace_clear_modal_yes_button)

            # Refresh form
            uielements.Click(locators.new_event_tab)
            uielements.Click(locators.new_time_place_new_tab)


        log.info("----------------------------------------------------------------------------------------------")
        log.info("*************AUTOMATION EXECUTION COMPLETED*************")
