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

        homepage.navigate_manageTempGen()
        log.info("Select Manage Template Gen menu")
        var_flag = uielements.iselementEnabled(locators.new_timeplace_browse_button)
        if var_flag:
            log.info("Upload file button is enabled by default")
        else:
            log.error("Upload file button is not enabled by default")
            final_assert.append(False)

        # CSV file upload
        log.info("Start Circuit error validation")
        var_uploadFileName = "manageTempGen_Valid_Circuits.csv"
        var_error_message = "Validation success."
        eventpage.tp_validatefile_message(testDatafolderPath, var_uploadFileName, var_error_message)
        log.info("Validate File success message")

        uielements.Click(locators.new_timeplace_save_button)
        log.info("Click on Save button")
        time.sleep(5)
        while True:
            try:
                var_status_message = uielements.getValue(locators.new_timeplace_validation_message)
                if var_status_message in "Tempgen save is in progress.":
                    continue
                else:
                    break
            except:
                break
        log.info("Wait for 'Time place creation in progress' completion ")

        time.sleep(10)
        if uielements.iselementDisplayed(locators.new_timeplace_saved_message) == True:
            log.info("Temp Gen created successfully")

        time.sleep(5)
        if uielements.iselementEnabled(locators.new_timeplace_save_button) == True:
            log.info("The save button is enabled")
