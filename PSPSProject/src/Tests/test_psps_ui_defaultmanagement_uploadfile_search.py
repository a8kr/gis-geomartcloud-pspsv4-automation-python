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
        final_assert = []
        if var_os == "MAC-OS":
            homepage.SignOn()
            log.info("Successfully entered user id & password:")
            print("Successfully entered user id & password:")
        log.info("PSPS Landing Page displayed!!")
        homepage.navigate_defaultManagement()
        log.info("Starting Validation")

        uielements.Click(locators.dm_search_dd)
        var_dm_searchdd = readData(testDatafilePath, "Main", var_row, 10)
        ddvallist = defmanagement.get_Searchddvalue(var_dm_searchdd)
        if ddvallist[0] == ddvallist[1]:
            log.info("Successfully validated Search by dropdown & selected value:")
        else:
            log.error("Validation failed for Search by dropdown & selected value:")
            final_assert.append(False)



