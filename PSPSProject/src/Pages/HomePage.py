import os
import time

from PSPSProject.src.Repository.uilocators import locators
from PSPSProject.src.ReusableFunctions.uiactions import UI_Element_Actions


class HomePage:
    def __init__(self, driver):
        self.driver = driver

    def SignOn(self):
        try:
            uielements = UI_Element_Actions(self.driver)
            uielements.iselementDisplayed(locators.userID)
            uielements.setText(os.environ.get('LAN_USER'), locators.userID)
            uielements.setText(os.environ.get('LAN_PASS'), locators.password)
            uielements.Click(locators.signOnbutton)
        except:
            assert False, "Failed to click on SignOn button!!"

    def navigate_defaultManagement(self):
        try:
            uielements = UI_Element_Actions(self.driver)
            uielements.iselementDisplayed(locators.PSPS_Dropdown_menu)
            uielements.Click(locators.PSPS_Dropdown_menu)
            uielements.Click(locators.PSPS_List_Select_DefaultManagement)
        except:
            assert False, "Failed to navigate to Deafult Managment"

    def navigate_eventManagement(self):
        try:
            uielements = UI_Element_Actions(self.driver)
            uielements.iselementDisplayed(locators.PSPS_Dropdown_menu)
            uielements.Click(locators.PSPS_Dropdown_menu)
            uielements.Click(locators.PSPS_List_Select_EventManagement)
        except:
            assert False, "Failed to navigate to Deafult Managment"

    def navigate_ExternalPortal(self):
        try:
            uielements = UI_Element_Actions(self.driver)
            uielements.iselementDisplayed(locators.PSPS_Dropdown_menu)
            uielements.Click(locators.PSPS_Dropdown_menu)
            uielements.Click(locators.PSPS_List_Select_ExternalPortal)
        except:
            assert False, "Failed to navigate to Deafult Managment"

    def navigate_Home(self):
        try:
            uielements = UI_Element_Actions(self.driver)
            uielements.iselementDisplayed(locators.PSPS_Dropdown_menu)
            uielements.Click(locators.PSPS_Dropdown_menu)
            uielements.Click(locators.PSPS_List_Select_Home)
        except:
            assert False, "Failed to navigate to Deafult Managment"


