import time


class UI_Element_Actions:
    def __init__(self, driver):
        self.driver = driver
        driver = self.driver

    # Method desc: Verify UI element is displayed on the page
    # Usage: function name with 1 parameter (UI Element xpath/id/name/css)
    # returns boolean value

    def iselementDisplayed(self, Element):
        try:
            if self.driver.find_element_by_xpath(Element).is_displayed():
                return True
        except(ValueError, Exception):
            return False

    # Method desc: Set value to InputTextBox
    # Usage: function name with 2 arguments ( 1. UI Element xpath/id/name/css. 2: Value to be passed)

    def setText(self, value, element):
        time.sleep(1)
        self.driver.find_element_by_xpath(element).clear()
        self.driver.find_element_by_xpath(element).click()
        self.driver.find_element_by_xpath(element).send_keys(value)
        time.sleep(1)

    # Method desc: click on UI element
    # Usage: function name with 1 parameter (UI Element xpath/id/name/css)

    def Click(self, Element):
        time.sleep(1)
        self.driver.find_element_by_xpath(Element).click()
        time.sleep(1)

    # Method desc: Verify UI element is enabled on the page
    # Usage: function name with 1 parameter (UI Element xpath/id/name/css)
    # returns boolean value
    def iselementEnabled(self, Element):
        try:
            if self.driver.find_element_by_xpath(Element).is_enabled():
                return True
            else:
                return False
        except(ValueError, Exception):
            return False

    # Method desc: Get text of an UI element
    # Usage: function name with 1 parameter (UI Element xpath/id/name/css)
    # returns value/text
    def getValue(self, Element):
        time.sleep(1)
        if self.driver.find_element_by_xpath(Element).is_displayed():
            time.sleep(0.5)
            varValue = self.driver.find_element_by_xpath(Element).get_attribute('innerHTML')
            varValue = self.driver.find_element_by_xpath(Element).text
            time.sleep(0.5)
            # innerHTML
        return varValue

    def clickondropdown(self, Element):
        self.driver.find_element_by_xpath(Element).click()
