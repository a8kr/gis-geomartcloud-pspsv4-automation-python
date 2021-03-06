class locators:
    # SignOn Page
    userID = "//*[@id='username']"
    password = "//*[@id='password']"
    signOnbutton = "/html/body/div/div[2]/div/form/div[6]"

    # Home Page
    PSPS_Dropdown_menu = "//*[@id='root']/header/div/button"
    PSPS_List_Select_Home = "/html/body/div[2]/div[3]/div[2]/ul/div[1]/div[2]"
    home_event_dropdown = "//*[@id='root']/div[1]/div/div[1]/div/div[1]/div/div/div"


    # DefaultManagement
    PSPS_List_Select_DefaultManagement = "/html/body/div[2]/div[3]/div[2]/ul/div[3]/div[2]/span"
    dm_uplaodfile = "//*[@id='root']/div[1]/div[2]/div[1]/div[1]/span[1]"
    dm_save_btn = "//*[@id='root']/div[1]/div[2]/div[3]/div/button[2]"
    dm_upload_validation = "/html/body/div[3]/div[3]/div/div[3]/span[4]"
    dm_upload_btn = "/html/body/div[3]/div[3]/div/div[3]/span[2]/button"
    grid_totalcircuits = "//*[@id='root']/div[1]/div[2]/div[2]/div/div/div[2]/div/span[2]"
    uploadFile_id = "FileInput"
    dm_grid_headers = "//*[@id='root']/div[1]/div[2]/div[2]/div/div/div[1]/div/div/div[2]/div/div[1]/div/div[1]"
    dm_search_dd = "//*[@id='root']/div[1]/div[2]/div[2]/div/div/div[1]/div/div[1]/div/div[2]/div/div/div"
    dm_close_uploadpopup = "/html/body/div[3]/div[3]/div/div[1]/button"
    dm_validationlink = "/html/body/div[3]/div[3]/div/div[3]/span[5]/span/p/a"
    dm_search_input = "//*[@id='root']/div[1]/div[2]/div[2]/div/div/div[1]/div/div/div[1]/div/div[1]/div/div/div/input"
    dm_validationlink_bottom = "//*[@id='root']/div[1]/div[2]/div[3]/div/div/p[2]/a"
    dm_status_message = "//*[@id='root']/div[1]/div[2]/div[3]/div/div/p"

    # PSPS scope
    psps_scope_tab = "//span[contains(text(),'VIEW PSPS SCOPE')]"
    psps_scope_search = "//*[@id='root']/div[1]/div/div[3]/div[2]/div/div/div[1]/div/div/div[1]/div/div[1]/div/div/div/input"
    psps_scope_checkbox_1 = "//*[@id='root']/div[1]/div/div[3]/div[2]/div/div/div[1]/div/div/div[2]/div/div/div/div/div[1]/div/div[2]/div/div[1]/div[1]/div/span"
    psps_scope_next_button = "//span[contains(text(),'NEXT')]"
    psps_scope_edit_icon = "//*[@id='root']/div[1]/div/div[3]/div[2]/div/div/div[1]/div/div/div[2]/div/table/tbody/tr[1]/td[8]/button"
    psps_scope_tp_name_field = "//*[@id='root']/div[1]/div/div[3]/div[2]/div/div/div[1]/div/div/div[2]/div/table/tbody/tr[2]/td/div/div/div/div/div/div[1]/div/div/div/input"
    psps_scope_tp_external_name_field = "//*[@id='root']/div[1]/div/div[3]/div[2]/div/div/div[1]/div/div/div[2]/div/table/tbody/tr[2]/td/div/div/div/div/div/div[2]/div/div/div/input"
    psps_scope_status = "//*[@id='root']/div[1]/div/div[3]/div[2]/div/div/div[2]/div/div/p"


    # New time place tab
    PSPS_List_Select_EventManagement = "//div[3]/div[2]/ul/div[2]/div[2]"
    PSPS_List_Select_ManageTempGen = "//div[3]/div[2]/ul/div[4]/div[2]"
    new_time_place_new_tab = "//span[contains(text(),'NEW TIME PLACE')]"
    new_time_place_edit_tab = "//span[contains(text(),'VIEW & EDIT TIME PLACES')]"
    new_time_place_new_event_tab = "//span[contains(text(),'New Event')]"
    new_time_place_edit_event_tab = "//span[contains(text(),'View & Edit Event')]"
    new_time_place_view_psps_scope_button = "//span[@class='MuiChip-label MuiChip-labelSmall' and contains(text(),'View PSPS scope')]"
    new_time_place_grid_header = "//*[@id='root']/div[1]/div/div[3]/div/div/div[5]/div/div/div[1]/div/div[2]/div/div[1]/div/div[1]"
    new_timeplace_records = "//*[@id='root']/div[1]/div/div[3]/div/div[2]/div/div[1]/div/div/div[3]/div/span/text()[1]"
    new_timeplace_circuit_file_field = "//*[@id='root']/div[1]/div/div[3]/div[2]/div/div[1]/div/div[1]/div/div/div/input"
    new_timeplace_browse_button = "//span[contains(text(),'BROWSE')]"
    new_timeplace_clear_button = "//span[contains(text(),'CLEAR')]"
    new_timeplace_browse_button = "//span[contains(text(),'BROWSE')]"
    new_timeplace_create_button = "//span[contains(text(),'CREATE')]"
    new_timeplace_save_button = "// span[contains(text(), 'SAVE')]"
    #new_timeplace_fileupload_error = "//*[@id='root']/div[1]/div/div[3]/div[2]/div/div[1]/div/div[2]/div/p"
    new_timeplace_fileupload_error = "//label[ @for='FileInput']//following::p[1]"
    new_timeplace_fileupload_error_file = "//*[@id='root']/div[1]/div/div[3]/div[2]/div/div[1]/div/div[2]/div/p[1]"
    new_timeplace_fileupload_error_link = "//*[@id='root']/div[1]/div/div[3]/div[2]/div/div[1]/div/div[2]/div/p[2]/a"
    new_timeplace_clear_modal_no_button = "/html/body/div[2]/div[3]/div/div[3]/button[1]"
    new_timeplace_clear_modal_yes_button = "/html/body/div[2]/div[3]/div/div[3]/button[2]"
    new_timeplace_validation_error_link = "//*[@id='root']/div[1]/div/div[3]/div[2]/div/div[3]/div/div/p[2]/a"
    #new_timeplace_validation_message = "//*[@id='root']/div[1]/div/div[3]/div[2]/div/div[3]/div/div/p[1]"
    new_timeplace_validation_message = "//div[@role='progressbar']//following::p[1]"
    new_timeplace_saved_message = "//*[text()='Tempgen save completed.']"
    new_timeplace_internal_name = "//*[@id='root']/div[1]/div/div[3]/div[2]/div/div[2]/div/div/div[2]/div/div[1]/div/div/div/input"
    new_timeplace_external_name = "//*[@id='root']/div[1]/div/div[3]/div[2]/div/div[2]/div/div/div[2]/div/div[2]/div/div/div/input"

    new_timeplace_radio_watch = "//div/div[2]/div/div/div[2]/div/div[3]/fieldset/div/label[1]/span[1]"
    new_timeplace_radio_warning = "//div/div[2]/div/div/div[2]/div/div[3]/fieldset/div/label[2]/span[1]"

    new_timeplace_md_start_time = "//div/div[2]/div/div/div[2]/div/div[4]/div/div[1]/div/div/div/input"
    new_timeplace_md_end_time = "//div/div[2]/div/div/div[2]/div/div[4]/div/div[2]/div/div/div/input"
    new_timeplace_md_etor = "//div/div[2]/div/div/div[2]/div/div[5]/div/div[2]/div/div/div/input"
    new_timeplace_md_all_clear = "//div/div[2]/div/div/div[2]/div/div[5]/div/div[1]/div/div/div/input"
    new_timeplace_md_comment = "//div/div[2]/div/div/div[2]/div/div[6]/div/div/div/div/textarea"

    new_timeplace_cancel_button = "//span[contains(text(),'CANCEL')]"
    new_timeplace_save_button = "//span[contains(text(),'SAVE')]"

    new_timeplace_md_end_time_message = "//*[@id='root']/div[1]/div/div[3]/div[2]/div/div[2]/div/div/div[2]/div/div[4]/div/div[2]/div/div/p"



    # Time place View & Edit Tab
    view_timeplace_tab = "//*[@id='root']/div[1]/div/div[2]/div/div/div/button[2]"
    view_timeplace_search = "//div[1]/div/div/div[1]/div/div[1]/div/div/div/input"
    view_time_place_grid_header = "//*[@id='root']/div[1]/div/div[3]/div/div/div[1]/div/div/div[1]/div/div/div[2]/div/div[1]/div/div[1]"
    view_time_place_upload_file_tab = "//span[contains(text(),'UPLOAD A FILE')]"
    view_time_place_view_scope_tab = "//span[contains(text(),'VIEW PSPS SCOPE')]"
    view_time_place_edit_icon_1 = "//div/div[1]/div/div[2]/div/div[1]/div[1]/div/button"
    view_time_place_edit_icon_2 = "//div/div[1]/div/div[2]/div/div[2]/div[1]/div/button"
    view_time_place_menu_download = "//div[@id='simple-menu']/div[3]/ul[1]/li[1]"
    view_time_place_menu_update = "//div[@id='simple-menu']/div[3]/ul[1]/li[2]"
    view_time_place_name = "//div/div[3]/div/div[2]/div/div[1]/div/div/div/input"
    view_time_place_file_input ="//div/div[3]/div/div[2]/div/div[2]/div/div[1]/div/div/div/input"
    view_time_place_file_browse = "//div/div[2]/div/div[2]/div/div[2]/div/label/span"
    view_time_place_fileupload_error = "//*[@id='root']/div[1]/div/div[3]/div/div[2]/div/div[2]/div/div[2]/div/p"
    view_timeplace_internal_name = "//div/div[2]/div/div[3]/div/div/div[2]/div/div[1]/div/div/div/input"
    view_timeplace_external_name = "//div/div[2]/div/div[3]/div/div/div[2]/div/div[2]/div/div/div/input"
    view_timeplace_radio_watch = "//div/div[2]/div/div[3]/div/div/div[2]/div/div[3]/fieldset/div/label[1]/span[1]"
    view_timeplace_radio_warning = "//div/div[2]/div/div[3]/div/div/div[2]/div/div[3]/fieldset/div/label[2]/span[1]"
    view_timeplace_md_start_time = "//div/div[2]/div/div[3]/div/div/div[2]/div/div[4]/div/div[1]/div/div/div/input"
    view_timeplace_md_end_time = "//div/div[2]/div/div[3]/div/div/div[2]/div/div[4]/div/div[2]/div/div/div/input"
    view_timeplace_md_etor = "//div/div[2]/div/div[3]/div/div/div[2]/div/div[5]/div/div[2]/div/div/div/input"
    view_timeplace_md_all_clear = "//div/div[2]/div/div[3]/div/div/div[2]/div/div[5]/div/div[1]/div/div/div/input"
    view_timeplace_md_comment = "//div/div[2]/div/div[3]/div/div/div[2]/div/div[6]/div/div/div/div/textarea"
    view_timeplace_cancel_button = "//span[contains(text(),'CANCEL')]"
    view_timeplace_save_button = "//span[contains(text(),'SAVE')]"
    view_timeplace_update_button = "//span[contains(text(),'UPDATE')]"
    view_timeplace_fileupload_error_link ="//*[@id='root']/div[1]/div/div[3]/div/div[2]/div/div[2]/div/div[2]/div/p[2]"
    view_timeplace_validation_message = "//*[@id='root']/div[1]/div/div[3]/div/div[2]/div/div[4]/div/div/p"
    view_timeplace_validation_scope_message = "//*[@id='root']/div[1]/div/div[3]/div/div[2]/div/div/div[3]/div/div/p"
    view_timeplace_validation_error_link = "//*[@id='root']/div[1]/div/div[3]/div/div[2]/div/div[4]/div/div/p[2]/a"
    view_timeplace_scope_radiobutton_1 = "//div/div[2]/div/div[1]/div[1]/div/div/label/span[1]/span[1]/input"


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
    view_psps_scope_modal_status_message = "//*[@id='root']/div[1]/div/div[3]/div/div[2]/div/div[2]/div/div"
    view_psps_scope_modal_dropdown = "//*[@id='root']/div[1]/div/div[3]/div/div[2]/div/div[1]/div/div/div[1]/div/div[2]/div/div"
    view_psps_scope_modal_dd_timeplaceid = "//*[@id='menu-']/div[3]/ul/li[3]"

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
    new_event_grid_header = "//*[@id='root']/div[1]/div/div[3]/div/div[3]/div/div/div[1]/div/div/div[2]/div/div[1]/div/div[1]"
    new_event_grid_1st_checkbox = "//div[1]/div[1]/div/span/span[1]/input"
    new_event_grid_2nd_checkbox = "//div[2]/div[1]/div/span/span[1]/input"
    new_event_total_timeplaces = "//*[@id='root']/div[1]/div/div[3]/div/div[3]/div/div/div[2]/div/text()"
    new_event_status_message = "//*[@id='root']/div[1]/div/div[3]/div/div[4]/div/div/p"
    new_event_next_button = "//span[contains(text(),'NEXT')]"
    new_event_metadata_modal_comment = "//textarea[@class='MuiInputBase-input MuiFilledInput-input MuiInputBase-inputMultiline MuiFilledInput-inputMultiline MuiInputBase-inputMarginDense MuiFilledInput-inputMarginDense']"
    new_event_metadata_modal_external_name = "/html/body/div[3]/div[3]/div/div[2]/div/div[2]/div/div/div/input"
    new_event_metadata_back_button = "//span[contains(text(),'BACK')]"
    new_event_metadata_save_button = "//span[contains(text(),'SAVE')]"


    # Edit Event
    edit_event_tab = "//span[contains(text(),'View & Edit Event')]"
    edit_event_search = "//input[@class='MuiInputBase-input MuiInput-input MuiInputBase-inputAdornedStart MuiInputBase-inputAdornedEnd']"
    edit_event_search_clear = "//button[@class='MuiButtonBase-root MuiIconButton-root']"
    edit_event_grid_header = "//*[@id='root']/div[1]/div/div[3]/div/div[3]/div/div/div[1]/div/div/div[2]/div/div[1]/div/div[1]"
    edit_event_grid_cell_1st = "//*[@id='root']/div[1]/div/div[3]/div/div[1]/div/div/div[1]/div/div/div[2]/div/div[1]/div/div[2]/div/div[1]/div[1]/div/button"
    edit_event_grid_cell_top = "//*[@id='root']/div[1]/div/div[3]/div/div[1]/div/div/div[1]/div/div[2]/div/div[1]/div/div[2]/div/div[1]/div[1]/div/span/span[1]/input"
    edit_event_edit_button = "//*[@id='root']/div[1]/div/div[3]/div/div[1]/div/div/div[2]/div/div[1]/span[1]"

    edit_event_total = "//*[@id='root']/div[1]/div/div[3]/div/div[1]/div/div/div[2]/div/span[2]"

    # External Portal
    PSPS_List_Select_ExternalPortal = "/html/body/div[2]/div[3]/div[2]/ul/div[4]/div[2]/span"
    ep_publish_tab = "//*[@id='root']/div[1]/div/div[2]/div/div/div/button/span[1]"


