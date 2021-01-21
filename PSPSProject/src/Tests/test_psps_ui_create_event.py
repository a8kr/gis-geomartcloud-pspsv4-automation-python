import os
import time

import pytest

from PSPSProject.src.Pages.DefaultManagement import DefaultManagement
from PSPSProject.src.Pages.TimePlacePage import TimePlacePage
from PSPSProject.src.Pages.HomePage import HomePage
from PSPSProject.src.Pages.EventPage import EventPage
from PSPSProject.src.Repository.uilocators import locators
from PSPSProject.src.ReusableFunctions.baseclass import BaseClass, exceptionRowCount
from PSPSProject.src.ReusableFunctions.commonfunctions import logfilepath, deleteFiles, readData, getCurrentTime
from PSPSProject.src.ReusableFunctions.uiactions import UI_Element_Actions
from PSPSProject.src.Tests.conftest import downloadsfolder, testDatafilePath, testDatafolderPath

VAR_TESTCASENAME = os.path.basename(__file__)
global VAR_COUNT, VAR_TESTENV
VAR_COUNT = 0
VAR_TESTENV = ""
VAR_COUNT = BaseClass().InitializeExecution(VAR_TESTCASENAME, VAR_COUNT, VAR_TESTENV)


@pytest.mark.skipif(not VAR_COUNT, reason="Excluded from regression suite")
@pytest.mark.regression
class TestDefaultManagementPositive(BaseClass):
    def test_create_new_event(self):
        try:
            var_row = VAR_COUNT[0]
            var_os = VAR_COUNT[2]
        except(ValueError, Exception):
            exceptionrow = exceptionRowCount()
            var_row = exceptionrow
        log = self.getLogger(logfilepath, VAR_TESTCASENAME)
        homepage = HomePage(self.driver)
        eventpage = EventPage(self.driver)
        eventmanagement = TimePlacePage(self.driver)
        uielements = UI_Element_Actions(self.driver)
        deleteFiles(downloadsfolder, ".csv")
        log.info("************* START TEST CASE EXECUTION *************")
        log.info("Starting Validation")
        if var_os == "MAC-OS":
            homepage.SignOn()
            log.info("Successfully entered user id & password:")
            print("Successfully entered user id & password:")

        homepage.navigate_eventManagement()
        log.info("Select Event Management menu")

        var_timestamp = getCurrentTime()
        var_event_name = "Event" + var_timestamp
        log.info("Event name: " + var_event_name)

        var_event_external_name = var_timestamp + " Ext"
        log.info("Event External name: " + var_event_external_name)

        uielements.Click(locators.new_event_tab)
        log.info("Clicked New Event tab")

        var_timeplace = "Timeplace_20210113_145743"
        log.info("Event time place: " + var_timeplace)

        var_event_comment = "Automation event"
        log.info("Event comment: " + var_event_comment)

        while True:
            try:
                var_message = uielements.getValue(locators.new_event_status_message)
                if var_message in "Fetching all completed time places.":#textMessa""ge.upload_file_in_progress_message:
                    continue
                else:
                    break
            except:
                break

        var_view_tpgridcolumnnames = readData(testDatafilePath, "Main", var_row, 14)
        view_tp_gridheader = eventmanagement.ValidateGridheader(var_view_tpgridcolumnnames,locators.new_event_grid_header)
        if view_tp_gridheader == True:
            log.info("New Event Time Places grid header displayed as expected")

        if uielements.iselementEnabled(locators.new_event_next_button) == False:
            log.info("Validate that Next button disabled by default")

        uielements.setText(var_event_name, locators.new_event_name)
        log.info("Enter event name to validate Next button status")

        uielements.Click(locators.new_event_grid_1st_checkbox)
        log.info("Select 1st check box in event time places grid")

        if uielements.iselementEnabled(locators.view_psps_scope_modal_next_button) == True:
            log.info("Validate that Next button enable after selecting time place")

        uielements.Click(locators.new_event_grid_2nd_checkbox)
        log.info("Select 2nd check box in New event time places grid")

        # Navigate to Edit event tab
        uielements.Click(locators.edit_event_tab)
        time.sleep(3)
        var_edit_event_gridcolumnnames = readData(testDatafilePath, "Main", var_row, 12)
        view_tp_gridheader = eventmanagement.ValidateGridheader(var_edit_event_gridcolumnnames,
                                                                locators.edit_event_grid_header)
        if view_tp_gridheader == True:
            log.info("Edit Event grid header displayed as expected")

        # Verify Edit button status
        if uielements.iselementDisplayed(locators.edit_event_edit_button) == True:
            log.info("Validate that Edit button disabled by default")
        uielements.Click(locators.edit_event_grid_cell_top)
        if uielements.iselementEnabled(locators.edit_event_edit_button) == True:
            log.info("Validate that Edit button enabled after selecting check box")

        var_event_total = uielements.getValue(locators.edit_event_total)
        if "1 selected of" in var_event_total:
            log.info("Validate text for total events")



        # Create new time place
        scopetpname = readData(testDatafilePath, "Data", var_row, 7)
        var_tpcreation = eventmanagement.TimePlaceCreation(scopetpname)
        var_timeplace = var_tpcreation[1]
        log.info("Event time place: " + var_timeplace)

        var_event_create = eventpage.createEvent_single_tp(var_event_name, var_timeplace, var_event_external_name, var_event_comment)
        if var_event_create == True:
            log.info("New event created successfully")



        log.info("*************AUTOMATION EXECUTION COMPLETED*************")










