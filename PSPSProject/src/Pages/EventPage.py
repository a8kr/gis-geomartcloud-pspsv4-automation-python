import os
import time

from PSPSProject.src.Repository.uilocators import locators
from PSPSProject.src.Repository.statictext import textMessage
from PSPSProject.src.ReusableFunctions.uiactions import UI_Element_Actions
from PSPSProject.src.ReusableFunctions.commonfunctions import *



class EventPage:
    def __init__(self, driver):
        self.driver = driver



    def createEvent_bysingleVersion(self, Event, timeplace):
        uielements = UI_Element_Actions(self.driver)
        time.sleep(2)
        uielements.Click(locators.new_event_tab)
        time.sleep(5)
        uielements.setText(Event, locators.new_event_name)
        time.sleep(0.15)
        uielements.setText(timeplace, locators.new_event_search)
        uielements.Click(locators.new_event_grid_1st_checkbox)
        #uielements.Click(locators.clearSearch_xpath)
        time.sleep(0.15)
        # varflag = isElementChecked(self, locators.versionName_checkbox_xpath)
        # if not varflag:
        #     assert False, "Version name checkbox is NOT Selected after clicking on select checkbox"
        uielements.Click(locators.new_event_n)
        #varEventCreationList = self.ClickOn_Event_SaveButton()
        #return varEventCreationList
        return True

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




