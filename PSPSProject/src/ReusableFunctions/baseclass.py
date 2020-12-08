import inspect
import logging
import os
import sys
import pytest
from pathlib import Path


from PSPSProject.src.ReusableFunctions.commonfunctions import getRowCount, readData

p = Path(os.getcwd())
dirctorypath = p.parent.parent
testdatafolder = "testData"
testDatafile = "testData.xlsx"
globalparamfile = "GlobalParameter.xlsx"
testDatafolderPath = os.path.join(dirctorypath, testdatafolder)
testDatafilePath = os.path.join(testDatafolderPath, testDatafile)
globalparamfilepath = os.path.join(os.path.join(dirctorypath, testdatafolder), globalparamfile)


@pytest.mark.usefixtures("setup")
class BaseClass:
    def InitializeExecution(self, var_testCaseName, VAR_TESTENV, VAR_Browser):
        global var_testEnv
        global var_eRowCount
        global var_browsertype
        var_FileName = var_testCaseName

        if 'test_psps_ui' in var_testCaseName:
            var_data = "Main"
        elif 'test_api' in var_testCaseName:
            var_data = "Data"
        elif 'test_data' in var_testCaseName:
            var_data = "Data"
        elif 'test_EP' in var_testCaseName:
            var_data = "EP"
        elif 'test_EW' in var_testCaseName:
            var_data = "EW"

        var_TotalRows = getRowCount(testDatafilePath, var_data)
        var_TotalRows = var_TotalRows + 1
        for i in range(4, var_TotalRows):
            var_Execute = readData(testDatafilePath, var_data, i, 1)
            var_TestScriptName = readData(testDatafilePath, var_data, i, 2)
            if var_TestScriptName.upper() == var_testCaseName.upper() and var_Execute.upper() == "N":
                return False
            else:
                if var_Execute.upper() == "N":
                    continue
                else:
                    if var_TestScriptName.upper() == var_testCaseName.upper() and var_Execute.upper() == "Y":
                        var_TestEnvironment = readData(testDatafilePath, var_data, i, 5)
                        var_BrowserType = readData(testDatafilePath, var_data, i, 4)
                        var_CountNum = i
                        var_testEnv = var_TestEnvironment
                        var_browsertype = var_BrowserType
                        if var_BrowserType == "Chrome-MAC-Default" or var_BrowserType == "Chrome-MAC-Headless":
                            var_BrowserType = "MAC-OS"
                        return [var_CountNum, var_TestEnvironment, var_BrowserType]
                    else:
                        if var_TestScriptName.upper() != var_testCaseName.upper() and var_Execute.upper() == "Y":
                            var_TestEnvironment = readData(testDatafilePath, var_data, i, 5)
                            var_testEnv = var_TestEnvironment
                            # continue
                        else:
                            var_TestEnvironment = readData(testDatafilePath, var_data, i, 5)
                            var_testEnv = var_TestEnvironment
                            var_eRowCount = i
                            sys.exit()

    def getLogger(self, logfilePath, var_testCaseName):
        loggerfileName = logfilePath + var_testCaseName + ".log"
        loggerName = inspect.stack()[1][3]
        logger = logging.getLogger(loggerName)
        fileHandler = logging.FileHandler(loggerfileName)
        formatter = logging.Formatter("%(asctime)s :%(levelname)s : %(name)s :%(message)s")
        fileHandler.setFormatter(formatter)
        logger.addHandler(fileHandler)  # filehandler object
        logger.setLevel(logging.DEBUG)
        return logger


def testEnvironment():
    global var_testEnv
    var_evn = var_testEnv
    var_browser = var_browsertype
    return [var_evn, var_browser]


def exceptionRowCount():
    global var_eRowCount
    var_erow = var_eRowCount
    return var_erow
