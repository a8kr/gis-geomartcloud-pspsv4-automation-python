import os
import time

import pandas as pd
import pytest

from PSPSProject.src.Pages.DefaultManagement import DefaultManagement
from PSPSProject.src.Pages.HomePage import HomePage
from PSPSProject.src.Pages.TimePlacePage import TimePlacePage
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
    def test_timeplace_edit(self):
        try:
            var_row = VAR_COUNT[0]
            var_os = VAR_COUNT[2]
        except(ValueError, Exception):
            exceptionrow = exceptionRowCount()
            var_row = exceptionrow
        log = self.getLogger(logfilepath, VAR_TESTCASENAME)
        homepage = HomePage(self.driver)
        timeplacepage = TimePlacePage (self.driver)
        eventpage = EventPage(self.driver)
        defmanagement = DefaultManagement(self.driver)
        uielements = UI_Element_Actions(self.driver)
        deleteFiles(downloadsfolder, ".csv")
        var_execution_flag = ''
        final_assert = []

        log.info("----------------------------------------------------------------------------------------------")
        log.info("Starting Validation")

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

        # Valid Circuit validation
        log.info("Start Circuit error validation")
        var_uploadFileName = "em_Valid_Circuits.csv"
        var_error_message = "Validation success."
        eventpage.tp_validatefile_message(testDatafolderPath, var_uploadFileName, var_error_message)
        log.info("Validate File success message")

        var_Timestamp = getCurrentTime()
        var_tp_name_internal = "Auto_TP_" + var_Timestamp
        var_tp_name_external = "Ext_Auto_TP_" + var_Timestamp
        log.info("TP name: " + var_tp_name_internal)

        # Enter TP names
        uielements.setText(var_tp_name_internal, locators.new_timeplace_internal_name)
        uielements.setText(var_tp_name_external, locators.new_timeplace_external_name)
        log.info("Enter TP names")

        # Metadata validations
        var_date_current = getTimeAddedvalue(0, 0)

        uielements.Click(locators.new_timeplace_md_start_time)
        uielements.setText(var_date_current, locators.new_timeplace_md_start_time)
        log.info("Enter Estimated Shut Off Start Time")

        uielements.Click(locators.new_timeplace_md_end_time)
        uielements.setText(getTimeAddedvalue(0, -1), locators.new_timeplace_md_end_time)
        log.info("Enter incorrect Estimated Shut Off end Time")
        var_md_end_time_error = uielements.getValue(locators.new_timeplace_md_end_time_message)
        if "End date/time cannot be before start time" in var_md_end_time_error:
            log.info("Validate error message 'End date/time cannot be before start time'")
        else:
            log.error("Validate error message 'End date/time cannot be before start time'")

        uielements.Click(locators.new_timeplace_md_etor)
        uielements.Click(locators.new_timeplace_md_end_time)
        uielements.setText(getTimeAddedvalue(0, 1), locators.new_timeplace_md_end_time)
        log.info("Enter Estimated Shut Off end Time")

        uielements.Click(locators.new_timeplace_md_all_clear)
        uielements.setText(getTimeAddedvalue(0, 2), locators.new_timeplace_md_all_clear)
        log.info("Enter All clear time")

        uielements.Click(locators.new_timeplace_md_etor)
        uielements.setText(getTimeAddedvalue(0, 4), locators.new_timeplace_md_etor)
        log.info("Enter etor time")

        uielements.setText("Test Automation Time place", locators.new_timeplace_md_comment)
        log.info("Enter metadata comment ")

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
        log.info("Wait for 'Time place creation in progress' completion ")

        # # ---- for existing TP
        # var_tp_name_internal = "Auto_TP_20210329_232226_Updated"
        # var_tp_name_external = "Ext_Auto_TP_20210329_232226_Updated"
        # uielements.Click(locators.view_timeplace_tab)
        # # -----------------------------------------

        if uielements.iselementDisplayed(locators.view_timeplace_search) == True:
            log.info("TP created and navigated to View TP tab")
            uielements.setText(var_tp_name_internal, locators.view_timeplace_search)
            log.info("Search for TP :" + var_tp_name_internal)
        time.sleep(2)

        uielements.Click(locators.view_time_place_edit_icon_1)
        log.info("Click on TP action menu icon")
        uielements.Click(locators.view_time_place_menu_update)
        log.info("Click on Update menu item")

        if uielements.iselementDisplayed(locators.view_time_place_file_browse) == True:
            log.info("Navigate to Edit TP - Upload file tab")

        var_uploadFileName = "em_Valid_Circuits.csv"
        var_error_message = "Validation success."
        eventpage.tp_edit_validatefile_message(testDatafolderPath, var_uploadFileName, var_error_message)
        log.info("Validate File success message")


        # var_Timestamp = getCurrentTime()
        var_tp_name_internal_updated = var_tp_name_internal + "_Updated"
        var_tp_name_external_updated = var_tp_name_external + "_Updated"
        log.info("Updated TP name: " + var_tp_name_internal_updated)

        # Enter Updated TP names
        uielements.setText(var_tp_name_internal_updated, locators.view_timeplace_internal_name)
        uielements.setText(var_tp_name_external_updated, locators.view_timeplace_external_name)
        log.info("Enter Updated TP names")


        # Metadata validations

        uielements.Click(locators.view_timeplace_md_start_time)
        uielements.setText(getTimeAddedvalue(0, 0), locators.view_timeplace_md_start_time)
        log.info("Enter Estimated Shut Off Start Time")

        uielements.Click(locators.view_timeplace_md_end_time)
        uielements.setText(getTimeAddedvalue(0, -1), locators.view_timeplace_md_end_time)
        log.info("Enter incorrect Estimated Shut Off end Time")
        # var_md_end_time_error = uielements.getValue(locators.view_timeplace_md_end_time_message)
        # if "End date/time cannot be before start time" in var_md_end_time_error:
        #     log.info("Validate error message 'End date/time cannot be before start time'")
        # else:
        #     log.error("Validate error message 'End date/time cannot be before start time'")

        uielements.Click(locators.view_timeplace_md_etor)
        uielements.Click(locators.view_timeplace_md_end_time)
        uielements.setText(getTimeAddedvalue(0, 1), locators.view_timeplace_md_end_time)
        log.info("Enter Estimated Shut Off end Time")

        uielements.Click(locators.view_timeplace_md_all_clear)
        uielements.setText(getTimeAddedvalue(0, 2), locators.view_timeplace_md_all_clear)
        log.info("Enter All clear time")

        uielements.Click(locators.view_timeplace_md_etor)
        uielements.setText(getTimeAddedvalue(0, 4), locators.view_timeplace_md_etor)
        log.info("Enter etor time")

        uielements.setText("Updated comment for Test Automation Time place", locators.view_timeplace_md_comment)
        log.info("Enter metadata comment ")

        uielements.Click(locators.view_timeplace_update_button)
        log.info("Click on Update button")
        time.sleep(5)
        while True:
            try:
                var_status_message = uielements.getValue(locators.view_timeplace_validation_message)
                if var_status_message in "Time place update in progress.":
                    continue
                else:
                    break
            except:
                break
        log.info("Wait for 'Time place update in progress' completion ")

        time.sleep(5)

        if uielements.iselementDisplayed(locators.view_timeplace_search) == True:
            log.info("TP created and navigated to View TP tab")
            uielements.setText(var_tp_name_internal_updated, locators.view_timeplace_search)
            log.info("Search for Updated TP :" + var_tp_name_internal_updated)

        if uielements.iselementDisplayed(locators.view_time_place_edit_icon_1) == True:
            log.info("TP update passed: " + var_tp_name_internal_updated)
        else:
            log.error("TP update failed: " + var_tp_name_internal_updated)


        # var_browser = self.driver.current_url
        # newbrowserURL = "window.open(" + "'" + var_browser + "','new window')"
        # self.driver.execute_script(newbrowserURL)
        # varhandles = self.driver.window_handles
        # self.driver.switch_to.window(varhandles[1])
        # homepage.navigate_eventManagement()
        #
        # self.driver.switch_to.window(varhandles[0])



        log.info("----------------------------------------------------------------------------------------------")
        log.info("*************AUTOMATION EXECUTION COMPLETED*************")
