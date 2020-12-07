import os
import time

import pytest

from PSPSProject.src.Pages.DefaultManagement import DefaultManagement
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
        else:
            log.error("Save button is not enabled after the valid circuit file uploaded")
            final_assert.append(False)

        if False in final_assert:
            log.error("One of Test Case Execution Failed")
        else:
            log.info("All Test Cases Executed successfully ")

        if var_execution_flag == 'fail':
            log.error("Execution failed: Errors found in execution!!")
            assert False
        log.info("----------------------------------------------------------------------------------------------")
        log.info("*************AUTOMATION EXECUTION COMPLETED*************")
