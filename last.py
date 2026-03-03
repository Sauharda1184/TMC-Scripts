# Imports all packages used in the program.
from tkinter import IntVar, StringVar, END
import tkinter as tk
import tkinter.font as tkFont
import bs4 
import requests as rq
import pandas as pd
import re
import datetime
import pytz
import traceback
import sys


# Creates a simple user interface.
class App:
    def __init__(self, root):
       
        # Sets the title of the window that opens.
        root.title("API Data Import Program")

        # Sets the size of the window that opens.
        width = 450
        height = 250

        # Formats the window, so that it fits to our desired width/height.
        screenwidth = root.winfo_screenwidth()
        screenheight = root.winfo_screenheight()
        alignstr = "%dx%d+%d+%d" % (width, height, (screenwidth - width) / 2, (screenheight - height) / 2)
        root.geometry(alignstr)

        # Disables resizing of the window.
        root.resizable(width = False, height = False)

        # Allows us to use our entered values in the code below.
        global var1
        global label_text
        global status_text
        global intersection_name_entry
        global ip_address_entry
        global start_date_entry
        global end_date_entry
        global status_message
        global custom_direction_mapping

        # Creates some placeholder values, to be assigned values later.
        var1 = IntVar()
        label_text = StringVar()
        status_text = StringVar()
        
        # Initialize default direction mapping
        custom_direction_mapping = {
            1: "EB",
            2: "WB", 
            3: "NB",
            4: "SB",
            "TRT": "EB",
            "ADV": "WB",
            "TI": "NB"
        }

        # Determines the font of all items within our window.
        ft = tkFont.Font(family = "Times", size = 11)

        # Creates a label that prompts the user to enter a name for the file.
        intersection_label = tk.Label(root, font = ft, justify = "left",
                                    text = "Enter a file name: ")
        intersection_label.place(x = 15, y = 10, height = 30)

        # Creates a text box for the user to enter a file name.
        intersection_name_entry = tk.Entry(root, borderwidth = "1.5px", font = ft, justify = "left", 
                                         text = "")
        intersection_name_entry.place(x = 140, y = 10, width = 240, height = 30)

        # Creates a label that prompts the user to enter the IP address of the intersection.
        ip_address_label = tk.Label(root, font = ft, justify = "left", 
                                             text = "IP address of intersection: ")
        ip_address_label.place(x = 15, y = 50, height = 30)

        # Creates a text box for the user to enter a IP address.
        ip_address_entry = tk.Entry(root, borderwidth = "1.5px", font = ft, justify = "left", 
                                     text = "")
        ip_address_entry.place(x = 190, y = 50, width = 190, height = 30)

        # Creates a label for the user to enter a start date.
        start_date_label = tk.Label(root, font = ft, justify = "left", 
                                text = "Start date (yyyy-mm-dd): ")
        start_date_label.place(x = 15, y = 90, height = 30)

        # Creates a text box for the user to enter a start date.
        start_date_entry = tk.Entry(root, borderwidth = "1.5px", font = ft, justify = "left", 
                                     text = "")
        start_date_entry.place(x = 190, y = 90, width = 190, height = 30)

        # Creates a label for the user to enter a end date.
        end_date_label = tk.Label(root, font = ft, justify = "left",
                                    text = "End date (yyyy-mm-dd): ")
        end_date_label.place(x = 15, y = 125, height = 30)

        # Creates a text box for the user to enter a end date.
        end_date_entry = tk.Entry(root, borderwidth = "1.5px", font = ft, justify = "left", 
                                     text = "") 
        end_date_entry.place(x = 190, y = 125, width = 190, height = 30)

        # Creates a button to allow the user to clear all data entered.
        clear_button = tk.Button(root, font = ft, justify = "center",
                                relief = "raised",
                                text = "Clear",
                                command = self.clear_button_command)
        clear_button.place(x = 390, y = 10, height = 145)

        # Creates a label for the status display.
        status_label = tk.Label(root, font = ft, justify = "left",
                              text = "Status:")
        status_label.place(x = 15, y = 170, height = 30)

        # Creates a dynamic label, that tells the user the status of the program.
        # If any errors occur in data validation, this will tell them the issue.
        status_text.set("")
        status_message = tk.Label(root, font = ft, justify = "left", 
                                relief = "sunken", state = "disabled",
                                text = status_text.get())
        status_message.place(x = 70, y = 170, width = 365, height=30)

        # Creates a button to customize camera direction mappings
        mapping_button = tk.Button(root, font = ft, justify = "center",
                                relief = "raised",
                                text = "Directions...",
                                command = self.show_mapping_dialog)
        mapping_button.place(x = 120, y = 210, width = 70, height = 30)

        # Creates a button to allow the user to submit the information. 
        submit_button = tk.Button(root, font = ft, justify = "center",
                                relief = "raised",
                                text = "Submit",
                                command = self.Submit_button_command)
        submit_button.place(x = 200, y = 210, width = 50, height = 30)

        #Creates a checkbox that a user can press to bypass empty fields.
        bypass_checkbox = tk.Checkbutton(root, font = ft, justify = "left", 
                                             variable = var1, onvalue = 1, offvalue = 0, 
                                             text = "Bypass")
        bypass_checkbox.place(x = 15, y = 210, width = 65, height = 30)

        # Inserts the first part of the IP address to lessen user input.
        ip_address_entry.insert(0, "192.168.")

    # Deletes all entered information when the 'clear' button is pressed.
    def clear_button_command(self):
        intersection_name_entry.delete(0, END)
        ip_address_entry.delete(0, END)
        ip_address_entry.insert(0, "192.168.")
        start_date_entry.delete(0, END)
        end_date_entry.delete(0, END)
        status_text.set("")
        status_message["text"] = status_text.get()

    # Shows a dialog to configure custom zone name mappings
    def show_mapping_dialog(self):
        mapping_dialog = tk.Toplevel()
        mapping_dialog.title("Configure Camera Direction Mappings")
        
        # Set up dialog dimensions
        width = 300
        height = 250
        screenwidth = mapping_dialog.winfo_screenwidth()
        screenheight = mapping_dialog.winfo_screenheight()
        alignstr = "%dx%d+%d+%d" % (width, height, (screenwidth - width) / 2, (screenheight - height) / 2)
        mapping_dialog.geometry(alignstr)
        
        # Set the font
        ft = tkFont.Font(family = "Times", size = 11)
        
        # Instructions label
        instructions = tk.Label(mapping_dialog, font = ft, justify = "left",
                              text = "Assign directions to camera positions.\nThis will be prepended to zone names.")
        instructions.place(x = 20, y = 10, width = 260, height = 40)
        
        # Camera 1 mapping
        cam1_label = tk.Label(mapping_dialog, font = ft, text = "Camera 1:")
        cam1_label.place(x = 40, y = 60, width = 80, height = 25)
        
        cam1_entry = tk.Entry(mapping_dialog, font = ft)
        cam1_entry.place(x = 130, y = 60, width = 60, height = 25)
        cam1_entry.insert(0, "EB")
        
        # Camera 2 mapping
        cam2_label = tk.Label(mapping_dialog, font = ft, text = "Camera 2:")
        cam2_label.place(x = 40, y = 95, width = 80, height = 25)
        
        cam2_entry = tk.Entry(mapping_dialog, font = ft)
        cam2_entry.place(x = 130, y = 95, width = 60, height = 25)
        cam2_entry.insert(0, "WB")
        
        # Camera 3 mapping
        cam3_label = tk.Label(mapping_dialog, font = ft, text = "Camera 3:")
        cam3_label.place(x = 40, y = 130, width = 80, height = 25)
        
        cam3_entry = tk.Entry(mapping_dialog, font = ft)
        cam3_entry.place(x = 130, y = 130, width = 60, height = 25)
        cam3_entry.insert(0, "NB")
        
        # Camera 4 mapping
        cam4_label = tk.Label(mapping_dialog, font = ft, text = "Camera 4:")
        cam4_label.place(x = 40, y = 165, width = 80, height = 25)
        
        cam4_entry = tk.Entry(mapping_dialog, font = ft)
        cam4_entry.place(x = 130, y = 165, width = 60, height = 25)
        cam4_entry.insert(0, "SB")
        
        # Save function
        def save_mappings():
            # Create a global dictionary to store mappings
            global custom_direction_mapping
            custom_direction_mapping = {
                1: cam1_entry.get(),
                2: cam2_entry.get(),
                3: cam3_entry.get(),
                4: cam4_entry.get()
            }
            status_text.set(f"Camera direction mappings saved")
            status_message["text"] = status_text.get()
            mapping_dialog.destroy()
        
        # Save button
        save_button = tk.Button(mapping_dialog, font = ft, text = "Save", command = save_mappings)
        save_button.place(x = 100, y = 210, width = 60, height = 30)
        
        # Cancel button
        cancel_button = tk.Button(mapping_dialog, font = ft, text = "Cancel", 
                                 command = mapping_dialog.destroy)
        cancel_button.place(x = 170, y = 210, width = 60, height = 30)

    # Helper function to print errors to terminal
    def print_error(self, error_msg, exception=None):
        """Print error message to terminal with full traceback if available"""
        print("\n" + "="*60, file=sys.stderr)
        print("ERROR:", error_msg, file=sys.stderr)
        if exception:
            print("\nFull traceback:", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
        print("="*60 + "\n", file=sys.stderr)

    # Determines what happens when the "submit" button is clicked.
    # The rest of the program follows.
    def Submit_button_command(self):

        # Determine if the bypass checkbox has been activated.
        if var1.get() == 1:
            bypass = 1
        if var1.get() == 0:
            bypass = 0

        # Gets a value for all of the user inputs.
        intersection_name = intersection_name_entry.get()
        start_date = start_date_entry.get()
        end_date = end_date_entry.get()

        # Gets a value for the ip address user input, removing anything not characterizaed as a number or '.'
        ip_address = ip_address_entry.get()
        ip_address = re.sub('[^\d|\.]','', ip_address)
        ip_address_entry.delete(0, END)
        ip_address_entry.insert(0, ip_address)

        # Sets a filler value to stop the program if an error is detected.
        nextstep = "yes"

        # Validates all of the user entry fields.       
        while True: 

            # Checks if the IP address entry is empty.
            if ip_address == "192.168.":
                error_msg = "You have not entered any IP address."
                print(f"\nERROR: {error_msg}\n", file=sys.stderr)
                status_text.set(error_msg)
                status_message["text"] = status_text.get()
                nextstep = "no"
                break

            # Checks if the IP address entry is too short.
            if len(ip_address) <= 10:
                error_msg = "The IP Address is too short."
                print(f"\nERROR: {error_msg}\n", file=sys.stderr)
                status_text.set(error_msg)
                status_message["text"] = status_text.get()
                nextstep = "no"
                break

            # Checks if the IP address entry is too long and removes extra characters.
            if len(ip_address) >= 16:
                status_text.set("The IP address is too long.")
                status_message["text"] = status_text.get()
                ip_address_entry.delete(15, END)
                ip_address_entry.delete(11)
                ip_address_entry.insert(11, ".") 
                b = ip_address_entry.get()
                b = b.replace('..', '.')
                ip_address_entry.delete(0, END)
                ip_address_entry.insert(0, b)
                nextstep = "no"
                break

            # Checks if the user has forgotten to enter a third period and removes extra characters.
            if '.' not in ip_address[8:]:
                status_text.set("Your IP address does not contain a third period.")
                ip_address_entry.insert(11, ".")
                ip_address_entry.delete(15, END)
                b = ip_address_entry.get()
                b = b.replace('..', '.')
                ip_address_entry.delete(0, END)
                ip_address_entry.insert(0, b)
                status_message["text"] = status_text.get()
                nextstep = "no"
                break

            # Checks to see if the user has accidentally finished their IP address with a period.
            if ip_address[-1] == ".":
                status_text.set("The last digit of your IP address is a period.")
                status_message["text"] = status_text.get()
                nextstep = "no"
                break
            
            try:
                # Checks to see if the user has entered a third octet greater than 256.
                int(ip_address[8:11])
                if int(ip_address[8:11]) >= 256:
                    status_text.set("The third octet of your IP Address is out of range.")
                    status_message["text"] = status_text.get()
                    nextstep = "no"
                    break
                
                # Checks to make sure that the user has entered only 3 numbers in their third octet.
                if(ip_address[11]) != ".":
                    status_text.set("Your third octet contains too many numbers.")
                    status_message["text"] = status_text.get()
                    ip_address_entry.delete(11, END)
                    ip_address_entry.insert(11, ".")
                    b = ip_address_entry.get()
                    b = b.replace('..', '.')
                    ip_address_entry.delete(0, END)
                    ip_address_entry.insert(0, b)
                    nextstep = "no"
                    break

            except ValueError:
                if ip_address[9:11] == ".":
                    pass

            try:
                # Checks to see if the user has entered a fourth octet greater than 256.
                int(ip_address[-3:])
                if int(ip_address[-3:]) >= 256:
                    status_text.set("The fourth octet of your IP Address is out of range.")
                    status_message["text"] = status_text.get()
                    nextstep = "no"
                    break

            except ValueError:
                if ip_address[-3:] == ".":
                    pass

            # Replaces the fifth and eighth value of the start date with a dash.
            l = list(start_date)
            l[4:5] = ["-"]
            l[7:8] = ["-"]
            s = ''.join(l)
            s = s.replace('--', '')

            start_date_entry.delete(0, END)
            start_date_entry.insert(0, s)
            start_date_entry.delete(10, END)
            
            # Replaces the fifth and eighth value of the end date with a dash.
            l = list(end_date)
            l[4:5] = ["-"]
            l[7:8] = ["-"]
            e = ''.join(l)
            e = e.replace('--', '')
                
            end_date_entry.delete(0, END)
            end_date_entry.insert(0, e)
            end_date_entry.delete(10, END)
            break

        while nextstep == "yes":
            try:
                # Attempts to turn the start date into integer parts, returning an error if their are letters or empty.
                int(start_date[0:4])
                int(start_date[5:7])
                int(start_date[8:10])

                # Returns an error if the day is greater than 31 or less than 1.
                if nextstep == "yes" and int(start_date[8:10]) == 0 or int(start_date[8:10]) >= 32:
                    error_msg = "The starting day is incorrect."
                    print(f"\nERROR: {error_msg}\n", file=sys.stderr)
                    status_text.set(error_msg)
                    status_message["text"] = status_text.get()
                    nextstep = "no"

                # Returns an error if the month is greater than 12 or less than 1.
                if nextstep == "yes" and int(start_date[5:7]) == 0 or int(start_date[5:7]) >= 13:
                    error_msg = "The starting month is incorrect."
                    print(f"\nERROR: {error_msg}\n", file=sys.stderr)
                    status_text.set(error_msg)
                    status_message["text"] = status_text.get()
                    nextstep = "no"

                # Returns an error if the year is greater than 2099 or less than 1990.
                if int(start_date[0:4]) >= 2099 or int(start_date[0:4]) <= 1990:
                    error_msg = "The start year is outside the camera's scope."
                    print(f"\nERROR: {error_msg}\n", file=sys.stderr)
                    status_text.set(error_msg)
                    status_message["text"] = status_text.get()
                    nextstep = "no"

                break

            except ValueError as e:
                error_msg = "The start date is empty or non-numerical."
                self.print_error(error_msg, e)
                status_text.set(error_msg)
                status_message["text"] = status_text.get()
                nextstep = "no"
                break
        

        while nextstep == "yes":
            try:
                # Attempts to turn the end date into integer parts, returning an error if their are letters or empty.
                int(end_date[0:4])
                int(end_date[5:7])
                int(end_date[8:10])
                
                # Returns an error if the day is greater than 31 or less than 1.
                if nextstep == "yes" and int(end_date[8:10]) == 0 or int(end_date[8:10]) >= 32:
                    status_text.set("The ending day is incorrect.")
                    status_message["text"] = status_text.get()
                    nextstep = "no"

                # Returns an error if the month is greater than 12 or less than 1.
                if nextstep == "yes" and int(end_date[5:7]) == 0 or int(end_date[5:7]) >= 13:
                    status_text.set("The ending month is incorrect.")
                    status_message["text"] = status_text.get()
                    nextstep = "no"

                # Returns an error if the year is greater than 2099 or less than 1990.
                if int(end_date[0:4]) >= 2099 or int(end_date[0:4]) <= 1990:
                    status_text.set("The ending year is outside the camera's scope.")
                    status_message["text"] = status_text.get()
                    nextstep = "no"

                # Returns an error if the ending day is prior to the starting day.
                if int(start_date[0:4]) == int(end_date[0:4]) and int(start_date[5:7]) == int(end_date[5:7]) and int(int(start_date[8:10]) > int(end_date[8:10])):
                    status_text.set("The ending day is prior to the starting day.")
                    status_message["text"] = status_text.get()
                    nextstep = "no"
        
                # Returns an error if the ending month is prior to the starting month.
                if int(start_date[0:4]) == int(end_date[0:4]) and int(start_date[5:7]) > int(end_date[5:7]):
                    status_text.set("The ending month is prior to the starting month.")
                    status_message["text"] = status_text.get()
                    nextstep = "no"
            
                # Returns an error if the ending year is prior to the starting year.
                if int(start_date[0:4]) > int(end_date[0:4]):
                    status_text.set("The ending year is prior to the starting year.")
                    status_message["text"] = status_text.get()
                    nextstep = "no"

                # Returns an error if the intersection name is blank.
                if intersection_name == (""):
                    status_text.set("The intersection name is blank. Please enter one.")
                    status_message["text"] = status_text.get()
                    nextstep = "no"
                break

            # Returns an error if the end date is empty.
            except ValueError as e:
                error_msg = "The end date is empty or non-numerical."
                self.print_error(error_msg, e)
                status_text.set(error_msg)
                status_message["text"] = status_text.get()
                nextstep = "no"
                break



        # This function converts across time zones, where dt is the datetime to be changed,
        # tz1 is the timezone to be changed from, and tz2 is the timezone the datetime is changed to.
        def convert_datetime_timezone(dt, tz1, tz2):
            tz1 = pytz.timezone(tz1)
            tz2 = pytz.timezone(tz2)

            dt = datetime.datetime.strptime(dt,"%Y-%m-%d %H:%M:%S")
            dt = tz1.localize(dt)
            dt = dt.astimezone(tz2)
            dt = dt.strftime("%Y-%m-%d %H:%M:%S")
            return dt

        # This function maps camera directions and non-standard zone names to standard directional formats
        def map_zone_to_direction(zone_name, camera_number, direction_list):
            # If the zone already starts with a standard direction, return it as is
            if zone_name.startswith(("EB", "WB", "NB", "SB")):
                return zone_name
            
            # Use camera numbers to determine directional prefixes
            # Get the directional prefix from custom mapping based on camera number
            if 'custom_direction_mapping' in globals() and camera_number in custom_direction_mapping:
                # Simply prepend the direction mapped to this camera number to the zone name
                return custom_direction_mapping[camera_number] + zone_name
            
            # Fallback to default camera number mapping if no custom mapping
            default_camera_mapping = {1: "EB", 2: "WB", 3: "NB", 4: "SB"}
            if camera_number in default_camera_mapping:
                return default_camera_mapping[camera_number] + zone_name
            
            # If all else fails, return the original zone name
            return zone_name

        # This converts the end date to one day after the end date entered.
        # The program will end when the start day is the same as the end date.
        end_date = pd.to_datetime(end_date) + pd.DateOffset(days=1)
        end_date = str(end_date)
        end_date = end_date[:10]
        
        # Variables used in the proceeding while loop.
        direction_list = []
        direction_number = 1

        # Prepare date for direction detection - use user's start_date
        # Convert start_date to GMT format for API call
        direction_detection_start = convert_datetime_timezone(start_date + " 00:00:00", "US/Central", "Etc/GMT+0")
        direction_detection_start = direction_detection_start.replace(" ", "T")
        direction_detection_end = pd.to_datetime(start_date) + pd.DateOffset(days=1)
        direction_detection_end = str(direction_detection_end)
        direction_detection_end = direction_detection_end[:10]
        direction_detection_end = convert_datetime_timezone(direction_detection_end + " 00:00:00", "US/Central", "Etc/GMT+0")
        direction_detection_end = direction_detection_end.replace(" ", "T")

        try:
            while direction_number < 5  and nextstep == "yes":
                # Goes to the data source according to the user input.
                retro = ("http://" + ip_address_entry.get()  + "/api/v1/cameras/" + str(direction_number) + 
                    "/bin-statistics?start-time=" + direction_detection_start + "&end-time=" + direction_detection_end)

                # Reads the webpage and retrieves all text on the page.
                try:
                    fix = rq.get(retro, timeout=10)
                except rq.exceptions.RequestException as req_err:
                    raise Exception(f"Network error connecting to camera {direction_number}: {str(req_err)}")
                
                # Check if the request was successful
                if fix.status_code != 200:
                    raise Exception(f"HTTP {fix.status_code} from camera {direction_number}: {fix.reason}. URL: {retro}")

                # Check if response has content
                if not fix.text or len(fix.text.strip()) == 0:
                    raise Exception(f"Empty response from camera {direction_number}. No data available for date range.")

                # Transfers all text from the webpage to a readable format.
                soup2 = bs4.BeautifulSoup(fix.text, "lxml")
                fix = soup2.get_text()
                
                # Check if parsed text has enough content
                if not fix or len(fix) < 15:
                    raise Exception(f"Invalid response format from camera {direction_number}. Response too short or malformed.")

                # Determines only the direction to append to zone names.
                fix_parts = fix.split(",")
                if len(fix_parts) < 2:
                    raise Exception(f"Invalid response format from camera {direction_number}. Expected comma-separated data.")
                
                fix = fix_parts[1]
                if len(fix) < 14:
                    raise Exception(f"Invalid response format from camera {direction_number}. Direction data too short.")
                
                fix = fix[12:14]
                direction_list.append(fix)

                # Adds one to direction_number, and continues the while loop until all 4 directions are covered.
                direction_number += 1

        except IndexError as idx_err:
            # IndexError means the response format was unexpected - treat as data format error
            if bypass == 1:
                direction_number += 1
                direction_list.append("NA")
                while direction_number < 5  and nextstep == "yes":
                    try:
                        # Goes to the data source according to the user input.
                        retro = ("http://" + ip_address_entry.get()  + "/api/v1/cameras/" + str(direction_number) + 
                            "/bin-statistics?start-time=" + direction_detection_start + "&end-time=" + direction_detection_end)

                        # Reads the webpage and retrieves all text on the page.
                        fix = rq.get(retro, timeout=10)
                        
                        # Check if the request was successful
                        if fix.status_code != 200 or not fix.text:
                            direction_list.append("NA")
                            direction_number += 1
                            continue

                        # Transfers all text from the webpage to a readable format.
                        soup2 = bs4.BeautifulSoup(fix.text, "lxml")
                        fix = soup2.get_text()
                        
                        if not fix or len(fix) < 15:
                            direction_list.append("NA")
                            direction_number += 1
                            continue

                        # Determines only the direction to append to zone names.
                        fix_parts = fix.split(",")
                        if len(fix_parts) < 2 or len(fix_parts[1]) < 14:
                            direction_list.append("NA")
                            direction_number += 1
                            continue
                            
                        fix = fix_parts[1][12:14]
                        direction_list.append(fix)
                    except:
                        direction_list.append("NA")
                    
                    # Adds one to direction_number, and continues the while loop until all 4 directions are covered.
                    direction_number = direction_number + 1
            
            # If there are only three cameras, then this will make sure an error is not raised
            # and instead we will continue with three directions.
            if direction_number == 4:
                pass
            
            elif bypass == 1:
                pass

            else:
                # If the URL doesn't work, then this error will be raised.
                error_msg = f"Camera {direction_number} returned invalid data format. Check IP address and date range. Error: {str(idx_err)}"
                self.print_error(error_msg, idx_err)
                status_text.set(error_msg)
                status_message["text"] = status_text.get()
                nextstep = "no"
        
        except ValueError as val_err:
            # If there are only three cameras, then this will make sure an error is not raised
            # and instead we will continue with three directions.
            if direction_number == 4:
                pass

            else: 
                # If less than 2 cameras are found, then raises an error.
                error_msg = "The IP address couldn't be found, or a camera is down."
                self.print_error(error_msg, val_err)
                status_text.set(error_msg)
                status_message["text"] = status_text.get()
                nextstep = "no"
        
        except Exception as e:
            # Handle network errors, HTTP errors, and other exceptions
            error_msg = str(e)
            
            # Print error to terminal
            self.print_error(f"Error detecting camera directions: {error_msg}", e)
            
            if bypass == 1:
                # If bypass is enabled, continue with default directions
                while len(direction_list) < 4:
                    direction_list.append("NA")
                # Still show a warning
                status_text.set(f"Warning: Some cameras had issues. Using bypass mode. Error: {error_msg}")
                status_message["text"] = status_text.get()
            else:
                if "HTTP" in error_msg or "Network error" in error_msg:
                    status_text.set(f"{error_msg}. Check IP address ({ip_address_entry.get()}) and ensure cameras are accessible.")
                elif "Empty response" in error_msg or "Invalid response" in error_msg:
                    status_text.set(f"{error_msg}. Try a different date range or check if cameras have data for {start_date}.")
                elif "timeout" in error_msg.lower() or "Connection" in error_msg:
                    status_text.set(f"Network error: {error_msg}. Check IP address and network connection.")
                else:
                    status_text.set(f"Error detecting camera directions: {error_msg}")
                status_message["text"] = status_text.get()
                nextstep = "no"

        while end_date != start_date and nextstep == "yes":
            # Sets the file name as the name of the intersection, plus the date for which data is collected.
            file_name = intersection_name + " " + start_date

            # This converts the start of the day to central time.
            start_of_day = convert_datetime_timezone(start_date + " 00:00:00", "US/Central", "Etc/GMT+0")
            start_of_day = start_of_day.replace(" ", "T")

            # This calculates 24 hours from the start date, to capture data for the entire day.
            end_of_day = pd.to_datetime(start_date) + pd.DateOffset(days=1)
            end_of_day = str(end_of_day)

            # This converts the end of the day to central time.
            end_of_day = convert_datetime_timezone(end_of_day, "US/Central", "Etc/GMT+0")
            end_of_day = end_of_day.replace(" ", "T")

            # Creates a placeholder value for counting cameras.
            camera_number = 1 


            try:
                # Runs a loop that retrieves information from each directional camera.
                while camera_number == 1 and nextstep == "yes":
                    # Goes to the data source according to the user input.
                    data_source = ("http://" + ip_address_entry.get()  + "/api/v1/cameras/" + str(camera_number) + 
                    "/bin-statistics?start-time=" + start_of_day + "&end-time=" + end_of_day)

                    # Reads the webpage and retrieves all text on the page.
                    try:
                        response = rq.get(data_source, timeout=10)
                    except rq.exceptions.RequestException as req_err:
                        raise Exception(f"Network error connecting to camera {camera_number}: {str(req_err)}")
                    
                    # Check if the request was successful
                    if response.status_code != 200:
                        raise Exception(f"HTTP {response.status_code} from camera {camera_number}: {response.reason}. URL: {data_source}")
                    
                    # Check if response has content
                    if not response.text or len(response.text.strip()) == 0:
                        raise Exception(f"Empty response from camera {camera_number}. No data available for {start_date}.")

                    # Transfers all text from the webpage to a readable format.
                    soup = bs4.BeautifulSoup(response.text, "lxml")
                    data = soup.get_text()
                    
                    # Check if parsed data has enough content
                    if not data or len(data) < 20:
                        raise Exception(f"Invalid response format from camera {camera_number}. Response too short or malformed.")

                    # Does some pre-data cleaning cleaning.
                    data = data[16:]
                    data = data.split("},")
                    
                    # Check if we got valid data
                    if not data or len(data) == 0:
                        raise Exception(f"No data records found from camera {camera_number} for {start_date}.")

                    # Saves the data to a modifiable data frame.
                    df1 = pd.DataFrame(data)
                    df1.columns = ['Data']

                    # Splits the data by comma into 11 columns (produces 12 columns: 0-11).
                    df1_split = df1['Data'].str.split(',', n=11, expand=True)
                    
                    # Ensure we have exactly 12 columns - create new DataFrame with exactly 12 columns
                    expected_cols = 12
                    column_names = ["Zone ID", "Zone Name", "Time", "Average Speed", "Volume", "Occupancy", 
                                    "Through", "Right", "Left", "LefttoRight", "RighttoLeft"]
                    
                    # Get the number of columns from split
                    num_cols = df1_split.shape[1]
                    
                    # Create a dictionary to build the new DataFrame
                    data_dict = {}
                    num_rows = len(df1_split)
                    for i in range(expected_cols):
                        if i < num_cols:
                            data_dict[i] = df1_split[i]
                        else:
                            # Add None column if missing - create a Series with None values
                            data_dict[i] = pd.Series([None] * num_rows, index=df1_split.index)
                    
                    # Create new DataFrame with exactly expected_cols columns
                    df1_new = pd.DataFrame(data_dict, index=df1_split.index)
                    
                    # Assign column names
                    df1 = df1_new.copy()
                    df1.columns = column_names

                    # Data cleaning - removes titles from each data point.
                    df1["Zone ID"] = df1["Zone ID"].str.replace('{"zoneId":', "")
                    df1["Zone ID"] = df1["Zone ID"].str.replace('"zoneId":', "")
                    df1["Zone Name"] = df1["Zone Name"].str.replace('"zoneName":', "")
                    df1["Zone Name"] = df1["Zone Name"].str.replace('"', "")
                    df1["Time"] = df1["Time"].str.replace('"time":', "")
                    df1["Time"] = df1["Time"].str.replace('"', "")
                    df1["Average Speed"] = df1["Average Speed"].str.replace('"averageSpeed":', "")
                    df1["Volume"] = df1["Volume"].str.replace('"volume":', "")
                    df1["Occupancy"] = df1["Occupancy"].str.replace('"occupancy":', "")
                    df1["Through"] = df1["Through"].str.replace('"throughCount":', "")
                    df1["Right"] = df1["Right"].str.replace('"rightTurnCount":', "")
                    df1["Left"] = df1["Left"].str.replace('"leftTurnCount":', "")
                    df1["LefttoRight"] = df1["LefttoRight"].str.replace('"leftToRightCount":', "")
                    df1["RighttoLeft"] = df1["RighttoLeft"].str.replace('"rightToLeftCount":', "")
                    df1["RighttoLeft"] = df1["RighttoLeft"].str.replace('}]}', "")

                    # Map the zone names to standard directional format
                    # Apply the mapping function to each zone name in the dataframe
                    for idx in range(len(df1)):
                        original_zone = df1.loc[idx, "Zone Name"]
                        df1.loc[idx, "Zone Name"] = map_zone_to_direction(original_zone, camera_number, direction_list)

                    # Adds one to our placeholder, until we reach 4.
                    camera_number += 1

            except Exception as e:
                # If bypass has been checked, the program will skip a camera and continue the rest of the program.
                # use with caution, as it may skew data.
                error_msg = str(e)
                
                # Print error to terminal
                self.print_error(f"Error accessing camera {camera_number}: {error_msg}", e)
                
                if bypass == 1:
                    camera_number += 1
                    
                    while camera_number < 3 and nextstep == "yes":
                        try:
                            # Goes to the data source according to the user input.
                            data_source = ("http://" + ip_address_entry.get()  + "/api/v1/cameras/" + str(camera_number) + 
                            "/bin-statistics?start-time=" + start_of_day + "&end-time=" + end_of_day)

                            # Reads the webpage and retrieves all text on the page.
                            try:
                                response = rq.get(data_source, timeout=10)
                            except rq.exceptions.RequestException:
                                camera_number += 1
                                continue
                            
                            if response.status_code != 200 or not response.text:
                                camera_number += 1
                                continue

                            # Transfers all text from the webpage to a readable format.
                            soup = bs4.BeautifulSoup(response.text, "lxml")
                            data = soup.get_text()

                            if not data or len(data) < 20:
                                camera_number += 1
                                continue

                            # Does some pre-data cleaning cleaning.
                            data = data[16:]
                            data = data.split("},")
                            
                            if not data or len(data) == 0:
                                camera_number += 1
                                continue

                            # Saves the data to a modifiable data frame.
                            df1 = pd.DataFrame(data)
                            df1.columns = ['Data']

                            # Splits the data by comma into 11 columns (produces 12 columns: 0-11).
                            df1_split = df1['Data'].str.split(',', n=11, expand=True)
                            
                            # Ensure we have exactly 12 columns - create new DataFrame with exactly 12 columns
                            expected_cols = 12
                            column_names = ["Zone ID", "Zone Name", "Time", "Average Speed", "Volume", "Occupancy", 
                                    "Through", "Right", "Left", "LefttoRight", "RighttoLeft"]
                            
                            # Get the number of columns from split
                            num_cols = df1_split.shape[1]
                            
                            # Create a dictionary to build the new DataFrame
                            data_dict = {}
                            for i in range(expected_cols):
                                if i < num_cols:
                                    data_dict[i] = df1_split[i]
                                else:
                                    # Add None column if missing
                                    data_dict[i] = None
                            
                            # Create new DataFrame with exactly expected_cols columns
                            df1_new = pd.DataFrame(data_dict)
                            
                            # Assign column names
                            df1 = df1_new.copy()
                            df1.columns = column_names

                            # Data cleaning - removes titles from each data point.
                            df1["Zone ID"] = df1["Zone ID"].str.replace('{"zoneId":', "")
                            df1["Zone ID"] = df1["Zone ID"].str.replace('"zoneId":', "")
                            df1["Zone Name"] = df1["Zone Name"].str.replace('"zoneName":', "")
                            df1["Zone Name"] = df1["Zone Name"].str.replace('"', "")
                            df1["Time"] = df1["Time"].str.replace('"time":', "")
                            df1["Time"] = df1["Time"].str.replace('"', "")
                            df1["Average Speed"] = df1["Average Speed"].str.replace('"averageSpeed":', "")
                            df1["Volume"] = df1["Volume"].str.replace('"volume":', "")
                            df1["Occupancy"] = df1["Occupancy"].str.replace('"occupancy":', "")
                            df1["Through"] = df1["Through"].str.replace('"throughCount":', "")
                            df1["Right"] = df1["Right"].str.replace('"rightTurnCount":', "")
                            df1["Left"] = df1["Left"].str.replace('"leftTurnCount":', "")
                            df1["LefttoRight"] = df1["LefttoRight"].str.replace('"leftToRightCount":', "")
                            df1["RighttoLeft"] = df1["RighttoLeft"].str.replace('"rightToLeftCount":', "")
                            df1["RighttoLeft"] = df1["RighttoLeft"].str.replace('}]}', "")

                            # Map the zone names to standard directional format
                            # Apply the mapping function to each zone name in the dataframe
                            for idx in range(len(df1)):
                                original_zone = df1.loc[idx, "Zone Name"]
                                df1.loc[idx, "Zone Name"] = map_zone_to_direction(original_zone, camera_number, direction_list)

                            # Adds one to our placeholder, until we reach 4.
                            camera_number += 1
            
                        except (ValueError, IndexError, KeyError, AttributeError) as parse_err:
                                # If less than 2 cameras are found, then raises an error.
                                error_msg = f"Camera {camera_number} data parsing failed: {str(parse_err)}. Check data format."
                                self.print_error(error_msg, parse_err)
                                status_text.set(error_msg)
                                status_message["text"] = status_text.get()
                                nextstep = "no"
                                break

                else:
                    # Provide detailed error message instead of generic one
                    if "HTTP" in error_msg or "Network error" in error_msg:
                        status_text.set(f"{error_msg}. Check IP address ({ip_address_entry.get()}) and network connection.")
                    elif "Empty response" in error_msg or "No data records" in error_msg:
                        status_text.set(f"{error_msg}. Try a different date range or check if cameras have data for {start_date}.")
                    elif "Invalid response" in error_msg:
                        status_text.set(f"{error_msg}. API may have changed format.")
                    else:
                        status_text.set(f"Error accessing camera {camera_number}: {error_msg}")
                    status_message["text"] = status_text.get()
                    nextstep = "no"
            

            # Runs a loop that retrieves information from each directional camera.
            try:
                while camera_number < 5 and nextstep == "yes":
                    # Goes to the data source according to the user input.
                    data_source = ("http://" + ip_address_entry.get()  + "/api/v1/cameras/" + str(camera_number) + 
                    "/bin-statistics?start-time=" + start_of_day + "&end-time=" + end_of_day)

                    # Reads the webpage and retrieves all text on the page.
                    try:
                        response = rq.get(data_source, timeout=10)
                    except rq.exceptions.RequestException as req_err:
                        raise Exception(f"Network error connecting to camera {camera_number}: {str(req_err)}")
                    
                    # Check if the request was successful
                    if response.status_code != 200:
                        raise Exception(f"HTTP {response.status_code} from camera {camera_number}: {response.reason}. URL: {data_source}")
                    
                    # Check if response has content
                    if not response.text or len(response.text.strip()) == 0:
                        raise Exception(f"Empty response from camera {camera_number}. No data available for {start_date}.")

                    # Transfers all text from the webpage to a readable format.
                    soup = bs4.BeautifulSoup(response.text, "lxml")
                    data = soup.get_text()
                    
                    # Check if parsed data has enough content
                    if not data or len(data) < 20:
                        raise Exception(f"Invalid response format from camera {camera_number}. Response too short or malformed.")

                    # Does some pre-data cleaning cleaning.
                    data = data[16:]
                    data = data.split("},")
                    
                    # Check if we got valid data
                    if not data or len(data) == 0:
                        raise Exception(f"No data records found from camera {camera_number} for {start_date}.")

                    # Saves the data to a modifiable data frame.
                    df2 = pd.DataFrame(data)
                    df2.columns = ['Data']

                    # Splits the data by comma into 11 columns (produces 12 columns: 0-11).
                    df2_split = df2['Data'].str.split(',', n=11, expand=True)
                    
                    # Ensure we have exactly 12 columns - create new DataFrame with exactly 12 columns
                    expected_cols = 12
                    column_names = ["Zone ID", "Zone Name", "Time", "Average Speed", "Volume", "Occupancy", 
                                    "Through", "Right", "Left", "LefttoRight", "RighttoLeft"]
                    
                    # Get the number of columns from split
                    num_cols = df2_split.shape[1]
                    
                    # Create a dictionary to build the new DataFrame
                    data_dict = {}
                    for i in range(expected_cols):
                        if i < num_cols:
                            data_dict[i] = df2_split[i]
                        else:
                            # Add None column if missing
                            data_dict[i] = None
                    
                    # Create new DataFrame with exactly expected_cols columns
                    df2_new = pd.DataFrame(data_dict)
                    
                    # Assign column names
                    df2 = df2_new.copy()
                    df2.columns = column_names

                    # Data cleaning - removes titles from each data point.
                    df2["Zone ID"] = df2["Zone ID"].str.replace('{"zoneId":', "")
                    df2["Zone ID"] = df2["Zone ID"].str.replace('"zoneId":', "")
                    df2["Zone Name"] = df2["Zone Name"].str.replace('"zoneName":', "")
                    df2["Zone Name"] = df2["Zone Name"].str.replace('"', "")
                    df2["Time"] = df2["Time"].str.replace('"time":', "")
                    df2["Time"] = df2["Time"].str.replace('"', "")
                    df2["Average Speed"] = df2["Average Speed"].str.replace('"averageSpeed":', "")
                    df2["Volume"] = df2["Volume"].str.replace('"volume":', "")
                    df2["Occupancy"] = df2["Occupancy"].str.replace('"occupancy":', "")
                    df2["Through"] = df2["Through"].str.replace('"throughCount":', "")
                    df2["Right"] = df2["Right"].str.replace('"rightTurnCount":', "")
                    df2["Left"] = df2["Left"].str.replace('"leftTurnCount":', "")
                    df2["LefttoRight"] = df2["LefttoRight"].str.replace('"leftToRightCount":', "")
                    df2["RighttoLeft"] = df2["RighttoLeft"].str.replace('"rightToLeftCount":', "")
                    df2["RighttoLeft"] = df2["RighttoLeft"].str.replace('}]}', "")

                    # Map the zone names to standard directional format
                    # Apply the mapping function to each zone name in the dataframe
                    for idx in range(len(df2)):
                        original_zone = df2.loc[idx, "Zone Name"]
                        df2.loc[idx, "Zone Name"] = map_zone_to_direction(original_zone, camera_number, direction_list)

                    # Adds one to our placeholder, until we reach 4.
                    camera_number += 1

                    # Appends the other directions to a dataframe with the initial camera.
                    df1 = pd.concat([df1, df2])


            except Exception as e:

                # If bypass has been checked, the program will skip a camera and continue the rest of the program.
                # use with caution, as it may skew data.
                error_msg = str(e)
                
                if bypass == 1:
                    camera_number += 1
                    
                    while camera_number < 5 and nextstep == "yes":
                        try:
                            # Goes to the data source according to the user input.
                            data_source = ("http://" + ip_address_entry.get()  + "/api/v1/cameras/" + str(camera_number) + 
                            "/bin-statistics?start-time=" + start_of_day + "&end-time=" + end_of_day)

                            # Reads the webpage and retrieves all text on the page.
                            try:
                                response = rq.get(data_source, timeout=10)
                            except rq.exceptions.RequestException:
                                camera_number += 1
                                continue
                            
                            if response.status_code != 200 or not response.text:
                                camera_number += 1
                                continue

                            # Transfers all text from the webpage to a readable format.
                            soup = bs4.BeautifulSoup(response.text, "lxml")
                            data = soup.get_text()
                            
                            if not data or len(data) < 20:
                                camera_number += 1
                                continue

                            # Does some pre-data cleaning cleaning.
                            data = data[16:]
                            data = data.split("},")
                            
                            if not data or len(data) == 0:
                                camera_number += 1
                                continue

                            # Saves the data to a modifiable data frame.
                            df2 = pd.DataFrame(data)
                            df2.columns = ['Data']

                            # Splits the data by comma into 11 columns (produces 12 columns: 0-11).
                            df2_split = df2['Data'].str.split(',', n=11, expand=True)
                            
                            # Ensure we have exactly 12 columns - create new DataFrame with exactly 12 columns
                            expected_cols = 12
                            column_names = ["Zone ID", "Zone Name", "Time", "Average Speed", "Volume", "Occupancy", 
                                    "Through", "Right", "Left", "LefttoRight", "RighttoLeft"]
                            
                            # Get the number of columns from split
                            num_cols = df2_split.shape[1]
                            
                            # Create a dictionary to build the new DataFrame
                            data_dict = {}
                            for i in range(expected_cols):
                                if i < num_cols:
                                    data_dict[i] = df2_split[i]
                                else:
                                    # Add None column if missing
                                    data_dict[i] = None
                            
                            # Create new DataFrame with exactly expected_cols columns
                            df2_new = pd.DataFrame(data_dict)
                            
                            # Assign column names
                            df2 = df2_new.copy()
                            df2.columns = column_names

                            # Data cleaning - removes titles from each data point.
                            df2["Zone ID"] = df2["Zone ID"].str.replace('{"zoneId":', "")
                            df2["Zone ID"] = df2["Zone ID"].str.replace('"zoneId":', "")
                            df2["Zone Name"] = df2["Zone Name"].str.replace('"zoneName":', "")
                            df2["Zone Name"] = df2["Zone Name"].str.replace('"', "")
                            df2["Time"] = df2["Time"].str.replace('"time":', "")
                            df2["Time"] = df2["Time"].str.replace('"', "")
                            df2["Average Speed"] = df2["Average Speed"].str.replace('"averageSpeed":', "")
                            df2["Volume"] = df2["Volume"].str.replace('"volume":', "")
                            df2["Occupancy"] = df2["Occupancy"].str.replace('"occupancy":', "")
                            df2["Through"] = df2["Through"].str.replace('"throughCount":', "")
                            df2["Right"] = df2["Right"].str.replace('"rightTurnCount":', "")
                            df2["Left"] = df2["Left"].str.replace('"leftTurnCount":', "")
                            df2["LefttoRight"] = df2["LefttoRight"].str.replace('"leftToRightCount":', "")
                            df2["RighttoLeft"] = df2["RighttoLeft"].str.replace('"rightToLeftCount":', "")
                            df2["RighttoLeft"] = df2["RighttoLeft"].str.replace('}]}', "")

                            # Map the zone names to standard directional format
                            # Apply the mapping function to each zone name in the dataframe
                            for idx in range(len(df2)):
                                original_zone = df2.loc[idx, "Zone Name"]
                                df2.loc[idx, "Zone Name"] = map_zone_to_direction(original_zone, camera_number, direction_list)

                            # Adds one to our placeholder, until we reach 4.
                            camera_number += 1

                            # Appends the other directions to a dataframe with the initial camera.
                            df1 = pd.concat([df1, df2])
            
                        except (ValueError, IndexError, KeyError, AttributeError) as parse_err:
                                # If less than 2 cameras are found, then raises an error.
                                error_msg = f"Camera {camera_number} data parsing failed: {str(parse_err)}. Check data format."
                                self.print_error(error_msg, parse_err)
                                status_text.set(error_msg)
                                status_message["text"] = status_text.get()
                                nextstep = "no"
                                break

                # If there are only three cameras, then this will make sure an error is not raised
                # and instead we will continue with three directions.
                elif camera_number == 4:
                    pass

                else: 
                    # Provide detailed error message instead of generic one
                    if "HTTP" in error_msg or "Network error" in error_msg:
                        status_text.set(f"{error_msg}. Check IP address ({ip_address_entry.get()}) and network connection.")
                    elif "Empty response" in error_msg or "No data records" in error_msg:
                        status_text.set(f"{error_msg}. Try a different date range or check if cameras have data for {start_date}.")
                    elif "Invalid response" in error_msg:
                        status_text.set(f"{error_msg}. API may have changed format.")
                    else:
                        status_text.set(f"Error accessing camera {camera_number}: {error_msg}")
                    status_message["text"] = status_text.get()
                    nextstep = "no"

            # Code to use for testing - saves the raw data as a csv.
            #df1.to_csv(file_name + " raw_data.csv")
            #status_text.set("The raw data has been written to file.")
            #status_message["text"] = status_text.get()

            # Commands to remove advanced and bike detectors.
            # This can be switched on in the future, if desired.
            advanced_included = 0
            bike_included = 0

            # Reads the raw data. 
            exiting = df1

            if nextstep == 'yes':
                # Removes advanced detectors from being counted.
                if advanced_included == 0:
                    exiting = exiting[~exiting["Zone Name"].str.contains("ADV")]

                # Removes bike detectors from being counted.
                if bike_included == 0:
                    exiting = exiting[~exiting["Zone Name"].str.contains("BIKE")]

                    # Modify Time so that we can use a formula later.
                    exiting["Time"] = exiting["Time"].str.replace("T", " ")
                    exiting["Time"] = exiting["Time"].str.replace("Z", "")

                # Changes the time back to its appropriate format for CST.
                i = 0
                while i < len(exiting["Time"]):
                    exiting.iloc[i, 2] = convert_datetime_timezone(exiting.iloc[i, 2], "Etc/GMT+0", "US/Central")
                    i = i + 1

                # Changes time in a datetime format, so that we can slice it appropriately.
                exiting["Time"] = exiting["Time"].astype("datetime64[ns]")

                # Identifies the first date in the record, to calibrate the 15-minute bins we will create.
                first_date = str(exiting.iloc[0, 2])
                date_of_collection = first_date.split(" ")
                date_of_collection = date_of_collection[0]

                # Here, we create the 15 minute interval bins.
                fifteen_interval = pd.date_range(date_of_collection, freq = "15min", periods = 96)    
                exiting["Time of Day"] = pd.cut(exiting["Time"], bins = fifteen_interval, duplicates = "drop")

                # Next, we convert Time of Day to a string so we can manipulate it through slicing.
                exiting["Time of Day"] = exiting["Time of Day"].astype("string")

                # Specifically retrieves the data portion.
                exiting["Time"] = exiting["Time"].astype("string")
                exiting["Date"] = exiting["Time"].str[0:10]

                # Slices time, to remove the list and leave just the numbers.
                exiting["Consultant Time"] = exiting["Time of Day"].str[32:35] + exiting["Time of Day"].str[35:38]

                # Converts Consultant Time to a string so that we can manipulate it through slicing.
                exiting["Consultant Time"] = exiting["Consultant Time"].astype("string")

                # Adds in 12:00 AM - fix chained assignment warning by using direct assignment
                exiting["Consultant Time"] = exiting["Consultant Time"].fillna("0000")

                # Adds in 12:15 AM.
                exiting["Consultant Time"] = exiting["Consultant Time"].str.replace("]", "0015")

                # Removes ":".
                exiting["Consultant Time"] = exiting["Consultant Time"].str.replace(":", "")

                # Code to use for testing - saves the formatted data as a csv.
                #exiting.to_csv(file_name + " formatted_data.csv")
                #status_text.set("The formatted data has been written to file.")
                #status_message["text"] = status_text.get()

                

                # Changes zone name to a string, and time to a integer format.
                exiting["Zone Name"] = exiting["Zone Name"].astype("string")
                # Handle any remaining NA values before converting to int64
                exiting["Consultant Time"] = exiting["Consultant Time"].fillna("0000")
                exiting["Consultant Time"] = exiting["Consultant Time"].replace("", "0000")
                exiting["Consultant Time"] = exiting["Consultant Time"].replace("nan", "0000")
                exiting["Consultant Time"] = exiting["Consultant Time"].replace("<NA>", "0000")
                # Convert to numeric first, then to int64, handling any errors
                exiting["Consultant Time"] = pd.to_numeric(exiting["Consultant Time"], errors='coerce').fillna(0).astype("int64")

                # Changes volumes to integers to be added in the next step.
                exiting["Left"] = exiting["Left"].astype("int64")
                exiting["Through"] = exiting["Through"].astype("int64")
                exiting["Right"] = exiting["Right"].astype("int64")

                # Makes a new table with our summary statistics.
                summary = pd.DataFrame(columns = ["Time", "EBL", "EBT", "EBR", "WBL", "WBT", "WBR", 
                                                    "NBL", "NBT", "NBR", "SBL", "SBT", "SBR", "Veh Total"])

                # Creates empty lists, to put all of our times in at the end.
                summary_time = []
                ebl = []
                ebt = []
                ebr = []
                wbl = []
                wbt = []
                wbr = []
                nbl = []
                nbt = []
                nbr = []
                sbl = []
                sbt = []
                sbr = []
                vehicle_total = []
    
                # Generates all of our time values for our table.
                # Counts up from 0 to 23, to calculate which hour row our data should be entered into.
                for hour in range(0, 24):
    
                    # Counts up from 0 to 3, to calculate which minute bin our data should be entered into.
                    for minutes in range(0, 4):

                        # If minutes is 0, then we leave it as is.
                        if minutes == 0:
                            iter_minutes = "00"
            
                        # If minutes is 1, 2, or 3, we multiply it by 15 for our bins.
                        else:
                            iter_minutes = minutes * 15

                        # Finally, we add the hour to the minute, to get the same formatting as the consulting sheet.
                        # Then, we add it to our summary table.
                        a = ("".join([str(hour), str(iter_minutes)]))
                        a = int(a)
                        summary_time.append(a)

                        d = (exiting.loc[(exiting["Consultant Time"] == a), "Left"].sum() 
                                + exiting.loc[(exiting["Consultant Time"] == a), "Through"].sum()
                                + exiting.loc[(exiting["Consultant Time"] == a), "Right"].sum())
                        vehicle_total.append(d)

                        # Counts up from 0 to 3, to calculate which direction our data should be entered into.
                        for direction in range(0, 4):
                            if direction == 0:
                                iter_direction = "EB"
                            elif direction == 1:
                                iter_direction = "WB"
                            elif direction == 2:
                                iter_direction = "NB"
                            elif direction == 3:
                                iter_direction = "SB"
        
                        # Counts up from 0 to 2, to calculate which direction bin our data should be entered into.
                        # The variable "b" joins direction and maneuver.
                        # The variable "c" locates the time and maneuver combination in our exiting vehicle csv, and sums all values.
                        # Finally, we add the calculated value to its own list, to be inputted into our summary table.
                            for maneuver in range(0, 3):
                                if maneuver == 0:
                                    c = exiting.loc[(exiting["Consultant Time"] == a) & 
                                                    (exiting["Zone Name"].str[0:2] == iter_direction), "Left"].sum()

                                    if iter_direction == "EB":
                                        ebl.append(c)
                                    elif iter_direction == "WB":
                                        wbl.append(c)
                                    elif iter_direction == "NB":
                                        nbl.append(c)
                                    elif iter_direction == "SB":
                                        sbl.append(c)

                                elif maneuver == 1:
                                    c = exiting.loc[(exiting["Consultant Time"] == a) & 
                                                    (exiting["Zone Name"].str[0:2] == iter_direction), "Through"].sum()

                                    if iter_direction == "EB":
                                        ebt.append(c)
                                    elif iter_direction == "WB":
                                        wbt.append(c)
                                    elif iter_direction == "NB":
                                        nbt.append(c)
                                    elif iter_direction == "SB":
                                        sbt.append(c)

                                elif maneuver == 2:
                                    c = exiting.loc[(exiting["Consultant Time"] == a) & 
                                                    (exiting["Zone Name"].str[0:2] == iter_direction), "Right"].sum()

                                    if iter_direction == "EB":
                                        ebr.append(c)
                                    elif iter_direction == "WB":
                                        wbr.append(c)
                                    elif iter_direction == "NB":
                                        nbr.append(c)
                                    elif iter_direction == "SB":
                                        sbr.append(c)

                # Now, we add all of our time values to our summary table.
                summary["Time"] = summary_time
                summary["EBL"] = ebl
                summary["EBT"] = ebt
                summary["EBR"] = ebr
                summary["WBL"] = wbl
                summary["WBT"] = wbt
                summary["WBR"] = wbr
                summary["NBL"] = nbl
                summary["NBT"] = nbt
                summary["NBR"] = nbr
                summary["SBL"] = sbl
                summary["SBT"] = sbt
                summary["SBR"] = sbr
                summary["Veh Total"] = vehicle_total

                # Then, we get totals for each individual direction.
                directional_total = ["Total", sum(ebl), sum(ebt), sum(ebr), sum(wbl), sum(wbt), sum(wbr), 
                                    sum(nbl), sum(nbt), sum(nbr), sum(sbl), sum(sbt), sum(sbr), sum(vehicle_total)]
                summary.loc[len(summary)] = directional_total

                # We set our summary index as time to set it as our leftmost column.
                summary.set_index("Time", inplace = True)

                # Finally, we write all of these things to our final file as summary data.
                summary.to_csv(file_name + ".csv")

                # Prints that the program has finished running.
                status_text.set("The data transfer has been completed.")
                status_message["text"] = status_text.get()

                # Adds a day to the start_date, and runs the program until the end date is reached.
                start_date = pd.to_datetime(start_date) + pd.DateOffset(days=1)
                start_date = str(start_date)
                start_date = start_date[:10]



# This command is responsible for actually running the GUI.
if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()
    
