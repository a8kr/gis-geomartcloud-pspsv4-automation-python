import os
import sys
import time
import pytest

from selenium import webdriver
from pathlib import Path
from configparser import ConfigParser

from PSPSProject.src.ReusableFunctions.baseclass import testEnvironment

sys.path.append(os.path.join(os.path.dirname(__file__), "...", "..."))

p = Path(os.getcwd())
dirctorypath = p.parent.parent
testdatafolder = "testData"
downloadsfolder = "downloads"
testDatafile = "testData.xlsx"
globalparamfile = "GlobalParameter.xlsx"
testDatafolderPath = os.path.join(dirctorypath, testdatafolder)
downloadfolderpath = os.path.join(dirctorypath, downloadsfolder)
testDatafilePath = os.path.join(testDatafolderPath, testDatafile)
globalparamfilepath = os.path.join(os.path.join(dirctorypath, testdatafolder), globalparamfile)
driver = None


@pytest.fixture(scope="class")
def setup(request):
    global driver
    var_testCaseName = ""
    CountNum = 0
    # try:
    #    os.system("taskkill /f /im chromedriver.exe")
    # except:
    #    None
    try:
        TESTENV = testEnvironment()
    except(ValueError, Exception):
        TESTENV = "TEST"
    # Chrome Browser
    dirpath = p.parent.parent
    driverfolder = "drivers\\chromedriver.exe"
    vartest = os.path.join(p.parent.parent, "downloads")
    chromeOptions = webdriver.ChromeOptions()
    prefs = {"download.default_directory": vartest}
    chromeOptions.add_experimental_option("prefs", prefs)
    if TESTENV[1] == "Chrome-Headless":
        chromeOptions.add_argument("--headless")
        print("*********Chrome Headless Browser started*************")
    if TESTENV[1] == "Chrome-MAC-Default":
        driverfolder = "drivers\\chromedriver"
        driverfolder = os.path.join("drivers", "chromedriver")
        print("*********Chrome Running in MAC OS*************")
    if TESTENV[1] == "Chrome-MAC-Headless":
        driverfolder = "drivers\\chromedriver"
        driverfolder = os.path.join("drivers", "chromedriver")
        print("*********Chrome Running in MAC OS*************")
        chromeOptions.add_argument("--headless")
        print("*********Chrome Headless Browser started in MAC OS*************")
    chromedriver = os.path.join(os.path.join(dirpath, driverfolder))
    driver = webdriver.Chrome(executable_path=chromedriver, chrome_options=chromeOptions)
    driver.implicitly_wait(10)
    driver.maximize_window()
    if TESTENV[0].upper() == "QA":
        URL = ""
    elif TESTENV[0].upper() == "TEST":
        URL = "https://pspsviewer4tst.nonprod.pge.com"
    elif TESTENV[0].upper() == "PROD":
        URL = ""
    elif TESTENV[0].upper() == "DEV":
        URL = "https://pspsviewer4dev.nonprod.pge.com/"
    else:
        URL = "https://pspsviewer4dev.nonprod.pge.com/"
    print(URL)
    driver.get(URL)
    time.sleep(1)
    request.cls.driver = driver
    yield
    driver.close()
    driver.quit()


@pytest.mark.hookwrapper
def pytest_runtest_makereport(item):
    """
        Extends the PyTest Plugin to take and embed screenshot in html report, whenever test fails.
        :param item:
        """
    pytest_html = item.config.pluginmanager.getplugin('html')
    outcome = yield
    report = outcome.get_result()
    extra = getattr(report, 'extra', [])

    if report.when == 'call' or report.when == "setup":
        xfail = hasattr(report, 'wasxfail')
        if (report.skipped and xfail) or (report.failed and not xfail) or report.passed:
            file_name = report.nodeid.replace("::", "_") + ".png"
            _capture_screenshot(file_name)
            if file_name:
                html = '<div><img src="%s" alt="screenshot" style="width:304px;height:228px;" ' \
                       'onclick="window.open(this.src)" align="right"/></div>' % file_name
                extra.append(pytest_html.extras.html(html))
        report.extra = extra


def _capture_screenshot(name):
    driver.get_screenshot_as_file(name)


# READ DATABASE DETAILS
def config(filename='database.ini'):
    try:
        env = testEnvironment()
        if env[0].upper() == "QA":
            section = 'postgresql-qa'
        elif env[0].upper() == "TEST":
            section = 'postgresql-test'
        elif env[0].upper() == "DEV":
            section = 'postgresql-dev'
        elif env[0].upper() == "PROD":
            section = 'postgresql-prod'
        else:
            section = 'postgresql-test'
    except(ValueError, Exception):
        section = 'postgresql-test'
    parser = ConfigParser(interpolation=None)
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return db


# READ S3bucket DETAILS
def s3config(filename='s3bucket.ini'):
    try:
        env = testEnvironment()
        if env[0].upper() == "QA":
            section = 's3bucket-qa'
        elif env[0].upper() == "TEST":
            section = 's3bucket-test'
        elif env[0].upper() == "DEV":
            section = 's3bucket-dev'
        elif env[0].upper() == "PROD":
            section = 's3bucket-prod'
        else:
            section = 's3bucket-test'
    except(ValueError, Exception):
        section = 's3bucket-test'
    parser = ConfigParser(interpolation=None)
    parser.read(filename)
    s3 = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            s3[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return s3


# READ API DETAILS
def APIconfig(filename='apiconfig.ini'):
    try:
        env = testEnvironment()
        if env[0].upper() == "QA":
            section = 'apidetails-qa'
        elif env[0].upper() == "TEST":
            section = 'apidetails-test'
        elif env[0].upper() == "DEV":
            section = 'apidetails-dev'
        elif env[0].upper() == "PROD":
            section = 'apidetails-prod'
        else:
            section = 'apidetails-test'
    except(ValueError, Exception):
        section = 'apidetails-test'
    parser = ConfigParser(interpolation=None)
    parser.read(filename)
    s3 = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            s3[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return s3


def config_met(filename='database.ini'):
    try:
        env = testEnvironment()
        if env[0].upper() == "QA":
            section = ''
        elif env[0].upper() == "TEST":
            section = 'postgresql-mettstdb'
        elif env[0].upper() == "DEV":
            section = 'postgresql-metdevdb'
        elif env[0].upper() == "PROD":
            section = ''
        else:
            section = 'postgresql-mettstdb'
    except(ValueError, Exception):
        section = 'postgresql-test'
    parser = ConfigParser(interpolation=None)
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return db


