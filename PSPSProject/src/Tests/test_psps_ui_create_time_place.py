import os
import time

import pytest

from PSPSProject.src.Pages.DefaultManagement import DefaultManagement
from PSPSProject.src.Pages.TimePlacePage import TimePlacePage
from PSPSProject.src.Pages.HomePage import HomePage
from PSPSProject.src.Pages.EventPage import EventPage
from PSPSProject.src.Repository.uilocators import locators
from PSPSProject.src.Repository.dbqueries import queries
from PSPSProject.src.ReusableFunctions.baseclass import BaseClass, exceptionRowCount
from PSPSProject.src.ReusableFunctions.commonfunctions import logfilepath, deleteFiles, readData, getCurrentTime
from PSPSProject.src.ReusableFunctions.uiactions import UI_Element_Actions
from PSPSProject.src.Tests.conftest import downloadsfolder, testDatafilePath, testDatafolderPath
from PSPSProject.src.ReusableFunctions.databasefunctions import *

VAR_TESTCASENAME = os.path.basename(__file__)
global VAR_COUNT, VAR_TESTENV
VAR_COUNT = 0
VAR_TESTENV = ""
VAR_COUNT = BaseClass().InitializeExecution(VAR_TESTCASENAME, VAR_COUNT, VAR_TESTENV)


@pytest.mark.skipif(not VAR_COUNT, reason="Excluded from regression suite")
@pytest.mark.regression
class TestDefaultManagementPositive(BaseClass):
    def test_create_time_place(self):
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

        # CSV file upload
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

        if uielements.iselementDisplayed(locators.view_time_place_grid_header) == True:
            log.info("TP created and navigated to View TP tab")

        # Get TP DB UID
        get_new_tp_db = queries.get_timeplace % var_tp_name_internal
        lst_table_details = queryresults_fetchone(get_new_tp_db)
        if lst_table_details:
            log.info("TP record created in DB: uid is " + str(lst_table_details))

         # Navigate to New TP tab
        uielements.Click(locators.new_time_place_new_tab)

        # Clear form
        uielements.Click(locators.new_timeplace_clear_button)
        uielements.Click(locators.new_timeplace_clear_modal_yes_button)

        # Refresh form
        uielements.Click(locators.new_event_tab)
        uielements.Click(locators.new_time_place_new_tab)

        # Polygon file upload
        var_uploadFileName = "18MAY2020_TP2.zip"
        var_error_message = "Validation success."
        log.info("Upload Polygon file: " + var_uploadFileName)
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

        if uielements.iselementDisplayed(locators.view_time_place_grid_header) == True:
            log.info("TP created and navigated to View TP tab")

        # Get TP DB UID
        get_new_tp_db = queries.get_timeplace % var_tp_name_internal
        lst_table_details = queryresults_fetchone(get_new_tp_db)
        if lst_table_details:
            log.info("TP record created in DB: uid is " + str(lst_table_details))

        # Navigate to New TP tab
        uielements.Click(locators.new_time_place_new_tab)

        # Clear form
        uielements.Click(locators.new_timeplace_clear_button)
        uielements.Click(locators.new_timeplace_clear_modal_yes_button)

        # Refresh form
        uielements.Click(locators.new_event_tab)
        uielements.Click(locators.new_time_place_new_tab)

        # Create TP by PSPS scope
        var_psps_scope = "2021-09-08-V-04"

        uielements.Click(locators.psps_scope_tab)
        log.info("Clicked on View PSPS Scope tab")
        time.sleep(5)
        uielements.setText(var_psps_scope, locators.psps_scope_search)
        uielements.Click(locators.psps_scope_checkbox_1)
        log.info("Enter Scope time place: " + var_psps_scope)
        uielements.Click(locators.psps_scope_next_button)
        log.info("Click Next button")
        uielements.Click(locators.psps_scope_edit_icon)
        log.info("Click edit icon")

        var_Timestamp = getCurrentTime()
        var_tp_name_internal = "Auto_TP_" + var_Timestamp
        var_tp_name_external = "Ext_Auto_TP_" + var_Timestamp
        log.info("TP name: " + var_tp_name_internal)

        # Enter TP names
        uielements.setText(var_tp_name_internal, locators.psps_scope_tp_name_field)
        uielements.setText(var_tp_name_external, locators.psps_scope_tp_external_name_field)
        log.info("Enter TP names")

        uielements.Click(locators.new_timeplace_create_button)
        log.info("Click on Create button")
        time.sleep(5)
        while True:
            try:
                var_status_message = uielements.getValue(locators.psps_scope_status)
                if var_status_message in "Time place creation in progress.":
                    continue
                else:
                    break
            except:
                break
        log.info("Wait for 'Time place creation in progress' completion ")

        time.sleep(5)

        if uielements.iselementDisplayed(locators.view_time_place_grid_header) == True:
            log.info("TP created and navigated to View TP tab")

        # Get TP DB UID
        get_new_tp_db = queries.get_timeplace % var_tp_name_internal
        lst_table_details = queryresults_fetchone(get_new_tp_db)
        if lst_table_details:
            log.info("TP record created in DB: uid is " + str(lst_table_details))

        # if uielements.iselementEnabled(locators.view_psps_scope_modal_next_button) == False:
        #     log.info("Validate that Next button disabled by default")
        # uielements.Click(locators.view_psps_scope_modal_grid_1st_checkbox)
        # log.info("Select 1st check box in View PSPS scope grid")
        # if uielements.iselementEnabled(locators.view_psps_scope_modal_next_button) == True:
        #     log.info("Validate that Next button enable after selecting time place")
        # uielements.Click(locators.view_psps_scope_modal_grid_2nd_checkbox)
        # log.info("Select 2nd check box in View PSPS scope grid")
        # uielements.Click(locators.view_psps_scope_modal_next_button)
        # log.info("Clicked on Next button")
        #
        # if uielements.iselementEnabled(locators.view_psps_scope_modal_status_red_cross) == True:
        #     log.info("Validate that red cross icon displayed")
        #
        # var_timeplace_created = eventmanagement.CreateTimePlace()
        # log.info("Time place name: " + var_timeplace_created)

        log.info("*************AUTOMATION EXECUTION COMPLETED*************")
