import os
import time
import pytest

from PSPSProject.src.Pages.DefaultManagement import DefaultManagement
from PSPSProject.src.Pages.HomePage import HomePage
from PSPSProject.src.Repository.uilocators import locators
from PSPSProject.src.Repository.statictext import textMessage
from PSPSProject.src.ReusableFunctions.baseclass import BaseClass, exceptionRowCount
from PSPSProject.src.ReusableFunctions.commonfunctions import *
from PSPSProject.src.ReusableFunctions.uiactions import UI_Element_Actions
from PSPSProject.src.Tests.conftest import testDatafilePath, dirctorypath, testDatafolderPath

VAR_TESTCASENAME = os.path.basename(__file__)
global VAR_COUNT, VAR_TESTENV
VAR_COUNT = 0
VAR_TESTENV = ""
VAR_COUNT = BaseClass().InitializeExecution(VAR_TESTCASENAME, VAR_COUNT, VAR_TESTENV)


@pytest.mark.skipif(not VAR_COUNT, reason="Excluded from regression suite")
@pytest.mark.regression
class TestViewEditVersion(BaseClass):
    def test_create_version_search_positive(self):
        try:
            var_row = VAR_COUNT[0]
            var_os = VAR_COUNT[2]
        except(ValueError, Exception):
            exceptionrow = exceptionRowCount()
            var_row = exceptionrow
        if var_os == "MAC-OS":
            var_logfilefolder = logfilefolder_mac
        else:
            var_logfilefolder = logfilefolder
        logfilepath = os.path.join(dirctorypath, var_logfilefolder)
        log = self.getLogger(logfilepath, VAR_TESTCASENAME)
        homepage = HomePage(self.driver)
        defmanagement = DefaultManagement(self.driver)
        uielements = UI_Element_Actions(self.driver)
        var_execution_flag = ''
        final_assert = []
        if var_os == "MAC-OS":
            homepage.SignOn()
            log.info("Successfully entered user id & password:")
            print("Successfully entered user id & password:")
        log.info("PSPS Landing Page displayed!!")
        homepage.navigate_defaultManagement()
        log.info("Starting Validation")

        #uielements.Click(locators.dm_search_dd)
        var_dm_searchdd = readData(testDatafilePath, "Main", var_row, 10)
        ddvallist = defmanagement.get_Searchddvalue(var_dm_searchdd)
        if ddvallist[0] == ddvallist[1]:
            log.info("Successfully validated Search by dropdown & selected value:")
        else:
            log.error("Validation failed for Search by dropdown & selected value:")
            final_assert.append(False)

        # Positive flow
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
        filePath = os.path.join(testDatafolderPath, "dm_Circuits_Search_Data.xlsx")

        for num in range(1, 7):
            var_data_index = num
            var_search_input_data = readData(filePath, "Circuits_Search", 2, var_data_index)
            var_index = num + 2
            uielements.setText(var_search_input_data, locators.dm_search_input)
            uielements.Click(locators.dm_search_dd)
            self.driver.find_element_by_xpath("//*[@id='menu-']/div[3]/ul/li[" + str(var_index) + "]").click()
            var_index_cell = num + 1
            var_table_value = self.driver.find_element_by_xpath(
                "//div[@class='ReactVirtualized__Grid__innerScrollContainer']//div[1]//div[" + str(
                    var_index_cell) + "]//div[1]").text
            if str(var_table_value) == str(var_search_input_data):
                log.info("Verified search value for index:" + str(num) + " and value is: " + str(var_search_input_data))
            else:
                log.error("Search value doesnot match " + str(var_search_input_data))
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
