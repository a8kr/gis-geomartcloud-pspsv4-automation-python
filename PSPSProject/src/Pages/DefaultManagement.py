import os
import time

from PSPSProject.src.Repository.uilocators import locators
from PSPSProject.src.ReusableFunctions.uiactions import UI_Element_Actions


class DefaultManagement:
    def __init__(self, driver):
        self.driver = driver

    def dm_fileupload(self, filepath, filename):
        time.sleep(0.5)
        try:
            self.filepath = filepath
            self.file = os.path.join(filepath, filename)
            var_textFilename = self.driver.find_element_by_id(locators.uploadFile_id).get_attribute('value')
            assert var_textFilename == ""
            self.driver.find_element_by_id(locators.uploadFile_id).send_keys(self.file)
            time.sleep(0.5)
            message = self.driver.find_element_by_xpath(locators.dm_upload_validation).get_attribute(
                'innerHTML')
            return message
        except:
            assert False

    def dm_Gridheader(self, var_gridColumnNames):
        val = self.driver.find_elements_by_xpath(locators.dm_grid_headers)
        valuelist = val[0].text.split("\n")
        var_gridnames = var_gridColumnNames.split(',')
        return valuelist, var_gridnames

    def get_Searchddvalue(self, dropdownvalues):
        uielements = UI_Element_Actions(self.driver)
        dropdownValues_exp = dropdownvalues.split(',')
        dmddlist = self.driver.find_elements_by_xpath("//*[@id='menu-']/div[3]/ul")
        dropdownlist = dmddlist[0].text.split("\n")
        length = len(dropdownlist)
        length = length + 1
        for i in range(2, length):
            self.driver.find_element_by_xpath("//*[@id='menu-']/div[3]/ul/li[" + str(i) + "]").click()
            time.sleep(0.5)
            uielements.clickondropdown(locators.dm_search_dd)
            time.sleep(0.5)
        uielements.Click("//*[@id='menu-']/div[3]/ul/li[2]")
        return dropdownValues_exp, dropdownlist

    def dm_validatefile(self, filepath, filename, ErrorMessage):
        time.sleep(0.5)
        try:
            self.filepath = filepath
            self.file = os.path.join(filepath, filename)
            var_textFilename = self.driver.find_element_by_id(locators.uploadFile_id).get_attribute('value')
            assert var_textFilename == ""
            self.driver.find_element_by_id(locators.uploadFile_id).send_keys(self.file)
            time.sleep(0.5)
            message = self.driver.find_element_by_xpath(locators.dm_upload_validation).get_attribute(
                'innerHTML')
            assert message == ErrorMessage
            print("Error message displayed properly")
            time.sleep(0.5)
            return message
        except:
            assert False


