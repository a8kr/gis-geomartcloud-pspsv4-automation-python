import os
import time

from PSPSProject.src.Pages.HomePage import HomePage
from PSPSProject.src.Repository.uilocators import locators
from PSPSProject.src.Repository.statictext import textMessage
from PSPSProject.src.ReusableFunctions.uiactions import UI_Element_Actions
from PSPSProject.src.ReusableFunctions.commonfunctions import *



class EventPage:
    def __init__(self, driver):
        self.driver = driver



    def createEvent_single_tp(self, Event, timeplace, externalname, comment):
        uielements = UI_Element_Actions(self.driver)
        uielements.Click(locators.new_event_tab)
        while True:
            try:
                var_message = uielements.getValue(locators.new_event_status_message)
                if var_message in textMessage.event_create_status_fetching:
                    continue
                else:
                    break
            except:
                break

        uielements.setText(Event, locators.new_event_name)
        uielements.setText(timeplace, locators.new_event_search)
        uielements.Click(locators.new_event_grid_1st_checkbox)
        uielements.Click(locators.new_event_next_button)

        # Enter metadata
        print("Enter event metadata")
        uielements.setText(comment, locators.new_event_metadata_modal_comment)
        uielements.setText(externalname, locators.new_event_metadata_modal_external_name)
        uielements.Click(locators.new_event_metadata_back_save)
        print("Click Save button on metadata modal")

        # Wait for Event status
        while True:
            try:
                var_status = uielements.getValue(locators.new_event_status_message)
                if var_status in textMessage.event_create_status_created:
                    continue
                else:
                    break
            except:
                break

        # Verify if just created event on Event edit page
        uielements.setText(Event, locators.edit_event_search)
        var_cell = uielements.getValue(locators.edit_event_grid_cell_1st)
        print("Verify if just created event on Event edit page")

        if var_cell in Event:
            return True
        else:
            return False

    def new_tp_gridheader(self, var_gridColumnNames):
        val = self.driver.find_elements_by_xpath(locators.new_time_place_grid_header)
        valuelist = val[0].text.split("\n")
        var_gridnames = var_gridColumnNames.split(',')
        return valuelist, var_gridnames

    def ValidateGridheader(self, var_gridColumnNames, Element):
        val = self.driver.find_elements_by_xpath(Element)
        valuelist = val[0].text.split("\n")
        var_gridnames = var_gridColumnNames.split(',')
        for i in var_gridnames:
            for j in valuelist:
                if i == j:
                    break
        return True

    def CreateTimePlace(self):

        uielements = UI_Element_Actions(self.driver)
        time.sleep(0.5)
        if uielements.iselementEnabled(locators.view_psps_scope_modal_expand_icon) == False:
            print("Validate that Create button disabled by default")
        var_Timestamp = getCurrentTime()
        var_timeplace = "Timeplace_" + var_Timestamp
        uielements.Click(locators.view_psps_scope_modal_expand_icon)
        uielements.setText(var_timeplace, locators.view_psps_scope_modal_internal_time_place_name)
        time.sleep(0.5)
        var_Timestamp = getCurrentTime()
        uielements.setText(var_Timestamp, locators.view_psps_scope_modal_external_time_place_name)

        if uielements.iselementDisplayed(locators.view_psps_scope_modal_status_green_done) == True:
            print("Validate that status icon changed to green done")

        if uielements.iselementEnabled(locators.view_psps_scope_modal_create_button) == True:
            print("Validate that Create button enabled after required field populated")
            uielements.Click(locators.view_psps_scope_modal_create_button)

        var_create_status = uielements.getValue(locators.view_psps_scope_modal_status_message)
        if textMessage.create_time_place_message in var_create_status:
            print(var_create_status)
            print("'Time place creation in progress' status validated")

        if uielements.iselementEnabled(locators.view_psps_scope_modal_create_button) == False:
            print("Validate that Create button disabled after click on Create time place")

        if uielements.iselementEnabled(locators.view_psps_scope_modal_back_button) == False:
            print("Validate that Back button disabled after click on Create time place")

        return var_timeplace

    def circuit_fileupload(self, filepath, filename):
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

    def tp_validatefile(self, filepath, filename, ErrorMessage):
        uielements = UI_Element_Actions(self.driver)
        time.sleep(0.5)
        try:
            self.filepath = filepath
            self.file = os.path.join(filepath, filename)
            var_textFilename = self.driver.find_element_by_id(locators.uploadFile_id).get_attribute('value')
            assert var_textFilename == ""
            self.driver.find_element_by_id(locators.uploadFile_id).send_keys(self.file)
            time.sleep(0.5)
            message = self.driver.find_element_by_xpath(locators.new_timeplace_fileupload_error).get_attribute(
                'innerHTML')
            assert ErrorMessage == message
            print("Error message displayed properly")
            time.sleep(0.5)
            return message
        except:
            assert False

    def tp_validatefile_message(self, filepath, filename, ErrorMessage):
        uielements = UI_Element_Actions(self.driver)
        time.sleep(0.5)
        try:
            self.filepath = filepath
            self.file = os.path.join(filepath, filename)
            var_textFilename = self.driver.find_element_by_id(locators.uploadFile_id).get_attribute('value')
            assert var_textFilename == ""
            self.driver.find_element_by_id(locators.uploadFile_id).send_keys(self.file)
            time.sleep(0.5)
            message = self.driver.find_element_by_xpath(locators.new_timeplace_fileupload_error).get_attribute(
                'innerHTML')
            assert str(ErrorMessage) in str(message)
            print("Error message displayed properly")
            time.sleep(0.5)
            return message
        except:
            assert False

    def tp_edit_validatefile_message(self, filepath, filename, ErrorMessage):
        uielements = UI_Element_Actions(self.driver)
        time.sleep(0.5)
        try:
            self.filepath = filepath
            self.file = os.path.join(filepath, filename)
            var_textFilename = self.driver.find_element_by_id(locators.uploadFile_id).get_attribute('value')
            assert var_textFilename == ""
            self.driver.find_element_by_id(locators.uploadFile_id).send_keys(self.file)
            time.sleep(0.5)
            message = self.driver.find_element_by_xpath(locators.view_time_place_fileupload_error).get_attribute(
                'innerHTML')
            assert ErrorMessage == message
            print("Error message displayed properly")
            time.sleep(0.5)
            return message
        except:
            assert False

    def eventm_uploadfile(self, filename):
        time.sleep(0.5)
        uielements = UI_Element_Actions(self.driver)
        homepage = HomePage(self.driver)
        homepage.navigate_defaultManagement()
        uielements.Click(locators.dm_uplaodfile)
        var_totalcircuits = getCSVrowCount(testDatafolderPath, filename)
        var_uploadsuccess = self.circuit_fileupload(testDatafolderPath, filename)
        if var_uploadsuccess == 'Validation success':
            print("Successfully uploaded circuits: " + var_uploadsuccess)
        else:
            print("Upload circuits failed: " + var_uploadsuccess)
        self.driver.find_element_by_xpath(locators.dm_upload_btn).click()
        time.sleep(0.5)
        uielements.Click(locators.dm_save_btn)
        while True:
            try:
                var_message = uielements.getValue(locators.dm_status_message)
                if var_message in textMessage.upload_file_in_progress_message:
                    continue
                else:
                    break
            except:
                break
        var_message = uielements.getValue(locators.dm_status_message)
        if var_message in textMessage.upload_file_successfully:
            assert True, "Message 'Default devices data uploaded successfully' validation passed"
        else:
            assert False, "Message 'Default devices data uploaded successfully' validation failed"
        return var_message





