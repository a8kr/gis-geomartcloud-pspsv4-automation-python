import os
import time

import pytest

from PSPSProject.src.Pages.DefaultManagement import DefaultManagement
from PSPSProject.src.Pages.TimePlacePage import TimePlacePage
from PSPSProject.src.Pages.HomePage import HomePage
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
    def test_create_time_place(self):
        try:
            var_row = VAR_COUNT[0]
            var_os = VAR_COUNT[2]
        except(ValueError, Exception):
            exceptionrow = exceptionRowCount()
            var_row = exceptionrow
        log = self.getLogger(logfilepath, VAR_TESTCASENAME)
        homepage = HomePage(self.driver)
        eventmanagement = TimePlacePage(self.driver)
        uielements = UI_Element_Actions(self.driver)
        deleteFiles(downloadsfolder, ".csv")
        log.info("Starting Validation")
        if var_os == "MAC-OS":
            homepage.SignOn()
            log.info("Successfully entered user id & password:")
            print("Successfully entered user id & password:")

        homepage.navigate_eventManagement()
        log.info("Select Event Management menu")

        var_tpgridcolumnnames = readData(testDatafilePath, "Main", var_row, 11)
        new_tp_gridheader = eventmanagement.ValidateGridheader(var_tpgridcolumnnames,locators.new_time_place_grid_header)
        if new_tp_gridheader == True:
            log.info("New Time Place Grid header displayed as expected")

        uielements.Click(locators.new_time_place_view_psps_scope_button)
        log.info("Clicked on View PSPS Scope button")

        var_view_tpgridcolumnnames = readData(testDatafilePath, "Main", var_row, 14)
        view_tp_gridheader = eventmanagement.ValidateGridheader(var_view_tpgridcolumnnames,locators.view_psps_scope_modal_grid_header)
        if view_tp_gridheader == True:
            log.info("View PSPS Scope grid header displayed as expected")


        if uielements.iselementEnabled(locators.view_psps_scope_modal_next_button) == False:
            log.info("Validate that Next button disabled by default")
        uielements.Click(locators.view_psps_scope_modal_grid_1st_checkbox)
        log.info("Select 1st check box in View PSPS scope grid")
        if uielements.iselementEnabled(locators.view_psps_scope_modal_next_button) == True:
            log.info("Validate that Next button enable after selecting time place")
        uielements.Click(locators.view_psps_scope_modal_grid_2nd_checkbox)
        log.info("Select 2nd check box in View PSPS scope grid")
        uielements.Click(locators.view_psps_scope_modal_next_button)
        log.info("Clicked on Next button")

        if uielements.iselementEnabled(locators.view_psps_scope_modal_status_red_cross) == True:
            log.info("Validate that red cross icon displayed")

        var_timeplace_created = eventmanagement.CreateTimePlace()
        log.info("Time place name: " + var_timeplace_created)

        log.info("*************AUTOMATION EXECUTION COMPLETED*************")










