import os
import time

import pytest

from PSPSProject.src.Pages.DefaultManagement import DefaultManagement
from PSPSProject.src.Pages.TimePlacePage import TimePlacePage
from PSPSProject.src.Pages.HomePage import HomePage
from PSPSProject.src.Pages.EventPage import EventPage
from PSPSProject.src.Repository.uilocators import locators
from PSPSProject.src.ReusableFunctions.baseclass import BaseClass, exceptionRowCount
from PSPSProject.src.ReusableFunctions.commonfunctions import logfilepath, deleteFiles, readData, getCSVrowCount
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
    def test_home_mapview(self):
        try:
            var_row = VAR_COUNT[0]
            var_os = VAR_COUNT[2]
        except(ValueError, Exception):
            exceptionrow = exceptionRowCount()
            var_row = exceptionrow
        log = self.getLogger(logfilepath, VAR_TESTCASENAME)
        final_assert = []
        homepage = HomePage(self.driver)
        eventpage = EventPage(self.driver)
        eventmanagement = TimePlacePage(self.driver)
        uielements = UI_Element_Actions(self.driver)
        deleteFiles(downloadsfolder, ".csv")
        log.info("Starting Validation")
        if var_os == "MAC-OS":
            homepage.SignOn()
            log.info("Successfully entered user id & password:")
            print("Successfully entered user id & password:")

        homepage.navigate_defaultManagement()
        log.info("SuccessfullyNavigated to Default Management screen")
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

        homepage.navigate_eventManagement()
        log.info("Successfully Navigated to Event Management screen")

        var_tpgridcolumnnames = readData(testDatafilePath, "Main", var_row, 11)
        new_tp_gridheader = eventmanagement.ValidateGridheader(var_tpgridcolumnnames,
                                                               locators.new_time_place_grid_header)
        if new_tp_gridheader == True:
            log.info("New Time Place Grid header displayed as expected")

        homepage.navigate_Home()
        log.info("Successfully Navigated to Home screen")

        var_event_dropdown = uielements.iselementDisplayed(locators.home_event_dropdown)
        if var_event_dropdown:
            log.info("Home page event dropdown displayed as expected")
        else:
            log.error("Home page event dropdown not displayed as expected")
            final_assert.append(False)

        log.info("*************AUTOMATION EXECUTION COMPLETED*************")










