class locators:
    # SignOn Page
    userID = "//*[@id='username']"
    password = "//*[@id='password']"
    signOnbutton = "/html/body/div/div[2]/div/form/div[6]"

    # Home Page
    PSPS_Dropdown_menu = "//*[@id='root']/header/div/button"

    # DefaultManagement
    PSPS_List_Select_DefaultManagement = "/html/body/div[2]/div[3]/div[2]/ul/div[3]/div[2]/span"
    dm_uplaodfile = "//*[@id='root']/div[1]/div[2]/div[1]/div[1]/span[1]"
    dm_save_btn = "//*[@id='root']/div[1]/div[2]/div[3]/div/button[2]"
    dm_upload_validation = "/html/body/div[2]/div[3]/div/div[3]/span[4]"
    dm_upload_btn = "/html/body/div[2]/div[3]/div/div[3]/span[2]/button/span[1]"
    grid_totalcircuits = "//*[@id='root']/div[1]/div[2]/div[2]/div/div/div[2]/div/span[2]"
    uploadFile_id = "FileInput"
    dm_grid_headers = "//*[@id='root']/div[1]/div[2]/div[2]/div/div/div[1]/div/div[2]/div/div[1]/div/div[1]"
    dm_search_dd = "//*[@id='root']/div[1]/div[2]/div[2]/div/div/div[1]/div/div[1]/div/div[2]/div/div/div"
    dm_close_uploadpopup = "/html/body/div[2]/div[3]/div/div[1]/button/span[1]"
    dm_validationlink = "/html/body/div[2]/div[3]/div/div[3]/span[5]/span/p/a"
    dm_search_input = "//*[@id='root']/div[1]/div[2]/div[2]/div/div/div[1]/div/div[1]/div/div[1]/div/div/div/input"
    dm_validationlink_bottom = "//*[@id='root']/div[1]/div[2]/div[3]/div/div/p[2]/a"
    dm_status_message = "//*[@id='root']/div[1]/div[2]/div[3]/div/div/p"


    # New time place tab
    PSPS_List_Select_EventManagement = "/html/body/div[2]/div[3]/div[2]/ul/div[2]/div[2]/span"
    new_time_place_new_tab = "//span[contains(text(),'NEW TIME PLACE')]"
    new_time_place_edit_tab = "//span[contains(text(),'VIEW & EDIT TIME PLACES')]"
    new_time_place_new_event_tab = "//span[contains(text(),'New Event')]"
    new_time_place_edit_event_tab = "//span[contains(text(),'View & Edit Event')]"
    new_time_place_view_psps_scope_button = "//span[@class='MuiChip-label MuiChip-labelSmall' and contains(text(),'View PSPS scope')]"
    new_time_place_grid_header = "//*[@id='root']/div[1]/div/div[3]/div/div/div[5]/div/div/div[1]/div/div[2]/div/div[1]/div/div[1]"
    new_timeplace_records = "//*[@id='root']/div[1]/div/div[3]/div/div[2]/div/div[1]/div/div/div[3]/div/span/text()[1]"

    # New time place modal
    view_psps_scope_modal_search = "//input[@class='MuiInputBase-input MuiInput-input MuiInputBase-inputAdornedStart MuiInputBase-inputAdornedEnd']"
    view_psps_scope_modal_search_clear = "//button[@class='MuiButtonBase-root MuiIconButton-root']"
    view_psps_scope_modal_grid_header = "//*[@id='root']/div[1]/div/div[3]/div/div[2]/div/div[1]/div/div/div[2]/div/div/div/div[1]/div/div[1]"
    view_psps_scope_modal_grid_1st_checkbox = "//div[1]/div[1]/div/span/span[1]/input"
    view_psps_scope_modal_grid_2nd_checkbox = "//div[2]/div[1]/div/span/span[1]/input"
    view_psps_scope_modal_next_button = "//span[contains(text(),'NEXT')]"
    view_psps_scope_modal_cancel_button = "//span[contains(text(),'CANCEL')]"
    view_psps_scope_modal_status_red_cross = "//div[@class='selected-scope-line-item-cross']"
    view_psps_scope_modal_status_green_done = "//div[@class='selected-scope-line-item-done']"
    view_psps_scope_modal_expand_icon = "//div[@class='MuiButtonBase-root MuiIconButton-root MuiAccordionSummary-expandIcon MuiIconButton-edgeEnd']"
    view_psps_scope_modal_collapsed_icon = "//div[@class='MuiButtonBase-root MuiIconButton-root MuiAccordionSummary-expandIcon Mui-expanded MuiIconButton-edgeEnd']"
    view_psps_scope_modal_internal_time_place_name = "//*[@id='root']/div[1]/div/div[3]/div/div[2]/div/div[1]/div/div[2]/div[2]/div/div/div/div/div/div[1]/div/div[1]/div/div/div/input"
    view_psps_scope_modal_external_time_place_name = "//*[@id='root']/div[1]/div/div[3]/div/div[2]/div/div[1]/div/div[2]/div[2]/div/div/div/div/div/div[1]/div/div[2]/div/div/div/input"
    view_psps_scope_modal_create_button = "//span[contains(text(),'CREATE')]"
    view_psps_scope_modal_back_button = "//span[contains(text(),'BACK')]"
    view_psps_scope_modal_status_message = "//*[@id='root']/div[1]/div/div[3]/div/div[2]/div/div[2]/div/div/p"

    # New Event tab
    new_event_tab = "//span[contains(text(),'New Event')]"
    new_event_search = "//input[@class='MuiInputBase-input MuiInput-input MuiInputBase-inputAdornedStart MuiInputBase-inputAdornedEnd']"
    new_event_search_clear ="//button[@class='MuiButtonBase-root MuiIconButton-root']"
    new_event_name = "//input[@class='MuiInputBase-input MuiFilledInput-input MuiInputBase-inputMarginDense MuiFilledInput-inputMarginDense']"
    new_event_fields_list = "//div[@class='MuiSelect-root MuiSelect-select MuiSelect-selectMenu MuiInputBase-input MuiInput-input']"
    new_event_fields_list_all_fields = "//*[@id='menu-']/div[3]/ul/li[2]"
    new_event_fields_list_time_places = "//*[@id='menu-']/div[3]/ul/li[3]"
    new_event_fields_list_stage = "//*[@id='menu-']/div[3]/ul/li[4]"
    new_event_fields_list_status = "//*[@id='menu-']/div[3]/ul/li[5]"
    new_event_fields_list_tied_to_event = "//*[@id='menu-']/div[3]/ul/li[6]"
    new_event_fields_list_created_by = "//*[@id='menu-']/div[3]/ul/li[7]"
    new_event_fields_list_last_modified = "//*[@id='menu-']/div[3]/ul/li[8]"
    new_event_fields_list_external_name = "//*[@id='menu-']/div[3]/ul/li[9]"
    new_event_grid_header = "//*[@id='root']/div[1]/div/div[3]/div/div[3]/div/div/div[1]/div/div[2]/div/div[1]/div/div[1]"
    new_event_grid_1st_checkbox = "//div[1]/div[1]/div/span/span[1]/input"
    new_event_grid_2nd_checkbox = "//div[2]/div[1]/div/span/span[1]/input"
    new_event_total_timeplaces = "//*[@id='root']/div[1]/div/div[3]/div/div[3]/div/div/div[2]/div/text()"
    new_event_status_message = "//*[@id='root']/div[1]/div/div[3]/div/div[4]/div/div/p"
    new_event_next_button = "//span[contains(text(),'NEXT')]"

    #"//*[@id='root']/div[1]/div[2]/div[3]/div/div/p"





