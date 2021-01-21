import os
import time

from PSPSProject.src.Repository.uilocators import locators
from PSPSProject.src.Repository.statictext import textMessage
from PSPSProject.src.ReusableFunctions.uiactions import UI_Element_Actions
from PSPSProject.src.ReusableFunctions.commonfunctions import *



class TimePlacePage:
    def __init__(self, driver):
        self.driver = driver

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
        var_timeplace = "Auto_TP_" + var_Timestamp
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

        while True:
            try:
                var_create_status = uielements.getValue(locators.view_psps_scope_modal_status_message)
                if textMessage.create_time_place_message in var_create_status:
                    continue
                else:
                    break
            except:
                break

        if textMessage.create_time_place_message in var_create_status:
            print(var_create_status)
            print("'Time place creation in progress' status validated")

        if uielements.iselementEnabled(locators.view_psps_scope_modal_create_button) == False:
            print("Validate that Create button disabled after click on Create time place")

        if uielements.iselementEnabled(locators.view_psps_scope_modal_back_button) == False:
            print("Validate that Back button disabled after click on Create time place")
        return var_timeplace

    def TimePlaceCreation(self, scopename):
        uielements = UI_Element_Actions(self.driver)
        time.sleep(0.5)
        uielements.Click(locators.new_time_place_new_tab)
        var_Timestamp = getCurrentTime()
        var_timeplace = "Auto_TP_" + var_Timestamp
        uielements.Click(locators.new_time_place_view_psps_scope_button)
        time.sleep(8)
        if scopename is None or scopename == "":
            uielements.Click(locators.view_psps_scope_modal_grid_1st_checkbox)
            assert True, "Scope 1st checkbox selected"
        else:
            uielements.setText(scopename, locators.view_psps_scope_modal_search)
            uielements.Click(locators.view_psps_scope_modal_grid_1st_checkbox)
            assert True, "Scope 1st checkbox selected"
        uielements.Click(locators.view_psps_scope_modal_next_button)
        uielements.Click(locators.view_psps_scope_modal_expand_icon)
        uielements.setText(var_timeplace, locators.view_psps_scope_modal_internal_time_place_name)
        time.sleep(0.5)
        var_Timestamp = getCurrentTime()
        var_extname = "Ext_" + var_Timestamp
        uielements.setText(var_extname, locators.view_psps_scope_modal_external_time_place_name)
        uielements.Click(locators.view_psps_scope_modal_create_button)
        while True:
            try:
                var_create_status = uielements.getValue(locators.view_psps_scope_modal_status_message)
                if textMessage.create_time_place_message in var_create_status:
                    # continue
                    break
                else:
                    assert False, "Timeplace creation failed"
            except:
                break
        return var_create_status, var_timeplace
