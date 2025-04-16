import json
import sys
import os
import base64
import yaml
from datetime import datetime, timedelta
import urllib3
import paramiko
import argparse

import utils
from utils import log_message, validate_yaml_file, get_input_parameters, decrypt_credentials, execute_ssh_command, \
    size_to_bytes, generate_report_rl

full_path = os.path.abspath(os.path.dirname(__file__))
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DATE = datetime.now().strftime("%d_%m_%Y")
LOGFILE = f"./script_{DATE}.log"
LOGSFOLDER = "./logs/"


class OpenSystem():
    # __init__ is a special method called whenever you try to make
    # an instance of a class. As you heard, it initializes the object.
    # Here, we'll initialize some of the data.
    def __init__(self):
        # Let's add some data to the [instance of the] class.
        self.input_parameters_file = None
        self.instance = None
        self.cred_file = None
        self.user = None
        self.password = None
        self.pool = None

        self.mechanism = None
        self.VTL_STATUS = False
        self.pool_retention_lock_enabled = False
        self.tape_list = []
        self.report = []

        self.retention_lock_period_for_tapes_in_days = 0
        self.retention_lock_period_for_tapes_for_monthly_in_years = 0

        self.minimum_retention_lock_period_daily_backup_in_days = 0
        self.maximum_retention_lock_period_daily_backup_in_days = 0

        self.minimum_retention_lock_period_monthly_backup_in_days = 0
        self.maximum_retention_lock_period_monthly_backup_in_years = 0

        self.minimum_tape_usage = None
        self.log_message = log_message
        self.execute_ssh_command = execute_ssh_command

    def validate_yaml_file(self, file):
        """
        Validates the existence of the specified YAML file.

        Method: validate_yaml_file
        Required: file (str) - The name of the YAML file to validate
        Returns: None

        - Checks if the file exists in the provided path.
        - If the file exists, sets the input parameters file to the full path.
        - If the file does not exist, logs an error message.
        """
        self.input_parameters_file = validate_yaml_file(file)

    def load_input_params(self):
        """
        Loads input parameters from the YAML configuration file.

        Method: load_input_params
        Required: None
        Returns: None

        - Reads the YAML file and assigns values to class attributes.
        - Logs and exits the program if there is an error while loading parameters.
        """

        try:
            data = get_input_parameters(self.input_parameters_file)
            if data:
                self.instance = data.get('open_system_instance')
                self.cred_file = data.get('open_system_credential_file_path')
                self.pool = data.get('pool_name')

                self.retention_lock_period_for_tapes_in_days = data.get(
                    'retention_lock_period_for_tapes_for_daily_in_days')
                self.retention_lock_period_for_tapes_for_monthly_in_years = data.get(
                    'retention_lock_period_for_tapes_for_monthly_in_years')
                self.minimum_retention_lock_period_daily_backup_in_days = data.get(
                    'minimum_retention_lock_period_for_mtree_daily_backup_in_days')
                self.maximum_retention_lock_period_daily_backup_in_days = data.get(
                    'maximum_retention_lock_period_for_mtree_daily_backup_in_days')
                self.minimum_retention_lock_period_monthly_backup_in_days = data.get(
                    'minimum_retention_lock_period_for_mtree_monthly_backup_in_days')
                self.maximum_retention_lock_period_monthly_backup_in_years = data.get(
                    'maximum_retention_lock_period_for_mtree_monthly_backup_in_years')

                self.mechanism = data.get('execution_logic_mechanism')
                self.minimum_tape_usage = data.get("minimum_tape_usage")

            else:
                print("Failed to load parameters.")
                sys.exit(1)
        except Exception as e:
            print(e)
            sys.exit(1)

    def validate_input_parameters(self):
        """
            This method checks whether all required parameters are provided in the input file.
            If any required parameter is missing, it logs an error message and stops the execution.
        """

        # Check if 'open_system_instance' parameter is provided
        if self.instance == None:
            # Log an error message specifying the missing parameter and the file name, then exit the program
            self.log_message(f"'open_system_instance' parameter is missing on {self.input_parameters_file} file")
            sys.exit(1)

        # Check if 'pool_name' parameter is provided
        if self.pool == None:
            # Log an error message specifying the missing parameter and the file name, then exit the program
            self.log_message(f"'pool_name' parameter is missing on {self.input_parameters_file} file")
            sys.exit(1)

        # Check if 'open_system_credential_file_path' parameter is provided
        if self.cred_file == None:
            # Log an error message specifying the missing parameter and the file name, then exit the program
            self.log_message(
                f"'open_system_credential_file_path' parameter missing on {self.input_parameters_file} file")
            sys.exit(1)

        # Check if 'minimum_tape_usage' parameter is provided
        if self.minimum_tape_usage == None:
            # Log an error message specifying the missing parameter and the file name, then exit the program
            self.log_message(f"'minimum_tape_usage' parameter missing on {self.input_parameters_file} file")
            sys.exit(1)

        # Check if 'execution_logic_mechanism' parameter is provided
        if self.mechanism == None:
            # Log an error message specifying the missing parameter and the file name, then exit the program
            self.log_message(f"'execution_logic_mechanism' parameter missing on {self.input_parameters_file} file")
            sys.exit(1)

        self.log_message("input parameters from YAML file has been loaded")

    def decrypt_credentials(self):
        """
           Decrypts credentials (username and password) from a base64-encoded file.

           Method: decrypt_credentials
           Required: None
           Returns: tuple or None

           - Reads the credentials file and decodes the username and password.
           - Returns a tuple (username, password).
           """
        self.user, self.password = decrypt_credentials(self.cred_file)

    def check_vtl_state(self):
        """
        Method: check_vtl_state
        Description: This method checks the state of the VTL (Virtual Tape Library) by executing the
                     "vtl status" command and verifying if the VTL is enabled, running, and licensed.
        Required: None
        Returns: None (Logs the status and updates the instance's VTL_STATUS attribute if VTL is enabled, running, and licensed)
        """
        self.log_message(f"OpenSystem: {self.instance} Checking VTL state...")
        command = "vtl status"
        self.log_message(f"[Executing Command]: {command}")
        vtl_status = self.execute_ssh_command(command, self.instance, self.user, self.password)

        if vtl_status and "enabled" in vtl_status and "running" in vtl_status and "licensed" in vtl_status:
            self.log_message(f"VTL is enabled, running, and licensed.")
            self.VTL_STATUS = True

    def get_tapes(self):
        """
        Method: get_tapes
        Description: This method fetches the list of tapes from the specified VTL pool, sorts by modification time,
                     and filters the tapes based on certain conditions like being in a slot, used status, state, and modification date.
        Required: vtl_pool_name (assigned to self.pool), mechanism (assigned to self.mechanism)
        Returns: None (Logs the filtered list of tapes and stores it in the instance's tape_list attribute)
        """
        vtl_pool_name = self.pool
        mechanism = self.mechanism
        self.log_message(f"Fetching Pool: {vtl_pool_name} Tape list...")

        command = f"vtl tape show pool {vtl_pool_name} sort-by modtime descending"
        self.log_message(f"[Executing Command]: {command}")

        pool_data = self.execute_ssh_command(command, self.instance, self.user, self.password)
        # print(pool_data)
        # exit(1)
        if not pool_data:
            self.log_message(f"Failed to retrieve Pool: {vtl_pool_name} details.")
            return []

        heading = ['Processing tapes....', 'Barcode', 'Pool', 'Location', 'State', 'Size', 'Used (%)', 'Comp',
                   'Modification Time',
                   'Total size of tapes:', 'Total pools:', 'Total number of tapes:', 'Average Compression:'
                                                                                     '--------',
                   ' --------------------', ' -----------------------', ' -----', ' --------', ' ----------------',
                   ' ----', ' -------------------'
                   ]
        tape_list = []
        try:
            for line in pool_data.split("\n"):

                line = line.split('  ')

                if line[0].strip() == '':
                    continue
                # print(line)
                # Check if any element in the array contains a dash '-'
                # remove_dash = any('-' in item for item in line)

                # if remove_dash:
                #    continue

                # print(line)
                # Check if any element in the array contains a dash 'Barcode'
                # remove_heading = True if line[0].strip() in heading else False
                remove_heading = set(line) & set(heading)

                if remove_heading:
                    continue

                if len(line) < 2:
                    continue
                # print(line)
                tapes = []

                for tape_data in line:
                    tape_data = tape_data.strip()
                    if tape_data:
                        tapes.append(tape_data)

                if len(tapes) == 8:

                    tape_info = {
                        "barcode": tapes[0],
                        "pool_name": tapes[1],
                        "location": tapes[2],
                        "state": tapes[3],
                        "size": tapes[4],
                        "used": tapes[5],
                        "modification_time": tapes[7]
                    }
                    # print(tape_info)
                    used = self.check_used(tape_info)
                    state = self.check_state(tape_info)
                    last_modified = self.check_modification_date(tape_info, mechanism)

                    if "slot" in tape_info["location"] and used and state and last_modified:
                        tape_list.append(tape_info)
        except Exception as e:
            self.log_message(str(e))

        self.log_message(
            "=======================================Begins RL for Below Barcodes===========================================")
        self.log_message(json.dumps(tape_list, indent=4))
        print("length::", len(tape_list))
        self.log_message(
            "========================================================================================================================")

        self.tape_list = tape_list

    def format_tape_data(self, pool_data):
        """
            Method: format_tape_data
            Description: This method formats the raw pool data received from a command into a structured list of dictionaries,
                         each representing tape information like barcode, pool name, state, size, used status, and modification time.
            Required: pool_data (raw data string representing tape information)
            Returns: List of dictionaries, each containing structured tape information.
        """
        tape_list = []
        i = 0
        try:
            for line in pool_data.split("\n"):
                # parts = line.split()
                line = line.split('  ')

                i += 1
                hyphen = line[0].count('-', 0, 2)

                if i > 3 and hyphen > 1:
                    break
                if i <= 3:
                    continue

                tapes = []

                for tape_data in line:
                    tape_data = tape_data.strip()
                    if tape_data:
                        tapes.append(tape_data)

                tape_list.append(
                    {
                        "barcode": tapes[0],
                        "pool_name": tapes[1],
                        "state": tapes[3],
                        "size": tapes[4],
                        "used": tapes[5],
                        "modification_time": tapes[7]
                    }
                )
        except Exception as e:
            self.log_message(e)

        return tape_list

    def check_pool_retention_lock_governance_mode(self):
        """
        Method: check_pool_retention_lock_governance_mode
        Description: This method checks if the Retention Lock feature is enabled for the VTL pool and if it is in Governance Mode.
        Required: None
        Returns: Boolean (True if Retention Lock is enabled in Governance Mode, False otherwise)
        """
        pool = self.pool
        self.log_message(f"Checking Retention Lock Governance Mode for MTREE /data/col1/{pool}")
        command = f"mtree retention-lock status mtree /data/col1/{pool}"
        self.log_message(f"[Executing Command]: {command}")
        data = self.execute_ssh_command(command, self.instance, self.user, self.password)

        i = 0
        retention_lock = False
        retention_lock_mode = None
        try:
            for line in data.split("\n"):
                # parts = line.split()
                line = line.split('  ')

                i += 1
                hyphen = line[0].count('-', 0, 2)

                if i > 2 and hyphen > 1:
                    break
                if i <= 2:
                    continue

                arr = []
                for mtree_data in line:
                    if mtree_data.strip() != "":
                        arr.append(mtree_data)
                option = arr[0].strip()
                value = arr[1].strip()
                if option == "Retention-lock" and value == "enabled":
                    retention_lock = True
                elif option == "Retention-lock mode" and value == "governance":
                    retention_lock_mode = value

        except Exception as e:
            self.log_message(e)

        if retention_lock == True and retention_lock_mode == "governance":
            self.log_message(f"Retention-lock feature is already enabled for MTREE /data/col1/{pool}")
            self.pool_retention_lock_enabled = True
        else:
            self.log_message(f"Retention-lock feature is not enabled for MTREE /data/col1/{pool}")
            self.pool_retention_lock_enabled = False
            return False

    def enable_retention_lock_pool(self):
        """
        Method: enable_retention_lock_pool
        Description: This method enables the Retention Lock Governance Mode for the VTL pool if it is not already enabled.
        Required: pool (assigned to self.pool)
        Returns: None (Logs the outcome of the operation)
        """
        if len(self.tape_list) > 0 and self.pool_retention_lock_enabled == False:
            pool = self.pool
            self.log_message(f"Enabling Retention Lock Governance Mode on MTREE /data/col1/{pool}")
            command = f"mtree retention-lock enable mode governance mtree /data/col1/{pool}"
            self.log_message(f"[Executing Command]: {command}")
            data = self.execute_ssh_command(command, self.instance, self.user, self.password)
            self.log_message(f"{data}")
            if data == False:
                self.pool_retention_lock_enabled = False
                self.log_message(f"Failed to enable Retention-Lock Governance Mode for pool {pool}")
            elif "Retention-lock feature is enabled" in data.strip():
                self.pool_retention_lock_enabled = True
            else:
                self.pool_retention_lock_enabled = False
                self.log_message(f"Failed to enable Retention-Lock Governance Mode for pool {pool}")

    def set_min_retention_lock_period_pool(self):
        """
        Method: set_min_retention_lock_period_pool
        Description: This method sets the minimum retention lock period for the VTL pool based on the defined backup period.
        Required: pool (assigned to self.pool), minimum_retention_lock_period_daily_backup_in_days (assigned to self.minimum_retention_lock_period_daily_backup_in_days)
        Returns: None (Logs the result of the operation)
        """
        if len(self.tape_list) > 0 and self.pool_retention_lock_enabled == True:
            pool = self.pool

            retention_lock = None

            if self.minimum_retention_lock_period_daily_backup_in_days is not None and isinstance(
                    self.minimum_retention_lock_period_daily_backup_in_days,
                    int) and self.minimum_retention_lock_period_daily_backup_in_days > 0:
                retention_lock = str(self.minimum_retention_lock_period_daily_backup_in_days) + "day"

            if self.minimum_retention_lock_period_monthly_backup_in_days is not None and isinstance(
                    self.minimum_retention_lock_period_monthly_backup_in_days,
                    int) and self.minimum_retention_lock_period_monthly_backup_in_days > 0:
                retention_lock = str(self.minimum_retention_lock_period_monthly_backup_in_days) + "day"

            if retention_lock:
                command = f"mtree retention-lock set min-retention-period {retention_lock} mtree  /data/col1/{pool}"
                self.log_message(f"[Executing Command]: {command}")
                result = self.execute_ssh_command(command, self.instance, self.user, self.password)
                self.log_message(result)
            else:
                self.log_message(f"unable to set minimun retention lock {retention_lock} mtree  /data/col1/{pool}")

    def set_max_retention_lock_period_pool(self):
        """
        Method: set_max_retention_lock_period_pool
        Description: This method sets the maximum retention lock period for the VTL pool based on the defined backup period.
        Required: pool (assigned to self.pool), maximum_retention_lock_period_daily_backup_in_days (assigned to self.maximum_retention_lock_period_daily_backup_in_days)
        Returns: None (Logs the result of the operation)
        """
        if len(self.tape_list) > 0 and self.pool_retention_lock_enabled == True:
            pool = self.pool
            retention_lock = None

            if self.maximum_retention_lock_period_daily_backup_in_days is not None and isinstance(
                    self.maximum_retention_lock_period_daily_backup_in_days,
                    int) and self.maximum_retention_lock_period_daily_backup_in_days > 0:
                retention_lock = str(self.maximum_retention_lock_period_daily_backup_in_days) + "day"

            if self.maximum_retention_lock_period_monthly_backup_in_years is not None and isinstance(
                    self.maximum_retention_lock_period_monthly_backup_in_years,
                    int) and self.maximum_retention_lock_period_monthly_backup_in_years > 0:
                retention_lock = str(self.maximum_retention_lock_period_monthly_backup_in_years) + "year"

            if retention_lock:
                command = f"mtree retention-lock set max-retention-period {retention_lock} mtree /data/col1/{pool}"
                self.log_message(f"[Executing Command]: {command}")
                result = self.execute_ssh_command(command, self.instance, self.user, self.password)
                self.log_message(result)
            else:
                self.log_message(f"unable to set maximum retention lock {retention_lock} mtree  /data/col1/{pool}")

    def check_used(self, tape_info):
        """
        Method: check_used
        Description: This method checks if the tape's usage percentage is greater than 0. If the usage is greater than 0 or if the tape is found to be used in the report, it returns True. Otherwise, it returns False.
        Required: tape_info (contains information about the tape including the usage)
        Returns: Boolean (True if the tape usage is > 0 or it is marked as used in the report, False otherwise)
        """
        # self.log_message(f"Checking USED%>0 for Bardcode: {tape_info['barcode']} used: {tape_info['used']}")
        used = tape_info["used"]
        used = used.split()
        used = float(used[0])
        check_usage_in_report = self.check_usage_in_report(tape_info)
        if used > 0 or check_usage_in_report == True:
            return True
        else:
            return False

    def generate_filesys_report(self):
        """
        Method: generate_filesys_report
        Description: This method generates a report for the VTL Pool's usage by executing the "filesys report generate" command
                     for the specified VTL pool and parsing the output to collect information on tapes in the pool.
        Required: vtl_pool_name (assigned to self.pool)
        Returns: None (Logs the generated report and stores the tape usage information in the self.report attribute)
        """
        vtl_pool_name = self.pool
        self.log_message(f"Generate filesys report for Pool: /data/col1/{vtl_pool_name}")
        command = f"filesys report generate file-location path /data/col1/{vtl_pool_name}"
        self.log_message(f"[Executing Command]: {command}")

        report = self.execute_ssh_command(command, self.instance, self.user, self.password)

        tape_list = []
        i = 0
        try:
            for line in report.split("\n"):
                # parts = line.split()
                line = line.split('\t')

                i += 1
                hyphen = line[0].count('-', 0, 2)

                if i > 3 and hyphen > 1:
                    break
                if i <= 3:
                    continue

                tape = []

                for tape_data in line:
                    tape_data = tape_data.strip()
                    if tape_data:
                        tape.append(tape_data)

                tape_list.append({
                    "file_name": tape[0],
                    "size": tape[2],
                    "placement_time": tape[3]
                })
        except Exception as e:
            self.log_message(e)

        self.report = tape_list

        self.log_message(
            f"=======================================Genearated Report MTREE /data/col1/{self.pool}===========================================")
        self.log_message(json.dumps(tape_list, indent=4))
        self.log_message(
            "========================================================================================================================")

    def check_usage_in_report(self, tape_info):
        """
        Method: check_usage_in_report
        Description: This method checks if a specific tape's size is larger than the defined reference size (minimum tape usage),
                     and compares the tape's modification date with the placement time recorded in the report.
                     Returns True if both the size condition and modification time match, otherwise returns False.
        Required: tape_info (a dictionary containing tape information, e.g., barcode and modification_time)
        Returns: Boolean value (True or False)
        """
        try:
            tape_report_data = None
            if len(self.report) > 0:
                for tape in self.report:
                    if tape_info["barcode"] in tape["file_name"]:
                        tape_report_data = tape
                        break

            if tape_report_data:
                self.log_message(f"Checking minimum usage for Barcode: {tape_info['barcode']}")
                # Reference size in GiB
                reference_size_gb = self.minimum_tape_usage
                reference_size_in_bytes = size_to_bytes(reference_size_gb)

                # Convert file size to bytes
                file_size_in_bytes = size_to_bytes(tape_report_data["size"])

                if file_size_in_bytes > reference_size_in_bytes:

                    modified_date = datetime.strptime(tape_info["modification_time"], '%Y/%m/%d %H:%M:%S')

                    # Get the date part only (2023/12/04) and set the time to 12:00 AM (midnight)
                    t_stamp = modified_date.replace(hour=0, minute=0, second=0, microsecond=0)

                    modified_date_timestamp = int(t_stamp.timestamp())

                    # Convert the string to a datetime object
                    report_placement_time = datetime.strptime(tape_report_data["placement_time"],
                                                              "%a %b %d %H:%M:%S %Y")

                    # Format the datetime object to dd-mm-yyyy
                    formatted_placement_time = report_placement_time.replace(hour=0, minute=0, second=0, microsecond=0)

                    # Convert the datetime object to a timestamp
                    midnight_dt_object = int(formatted_placement_time.timestamp())

                    if modified_date_timestamp == midnight_dt_object:
                        return True
                    else:
                        return True
                else:
                    return False
            else:
                return False
        except Exception as e:
            self.log_message(e)

    def check_state(self, tape_info):
        """
        Method: check_state
        Description: This method checks if the tape is in Retention Lock (RL) mode. If the tape is in RL mode, it returns False to exclude it from further processing. Otherwise, it returns True.
        Required: tape_info (contains information about the tape including its state)
        Returns: Boolean (True if the tape is not in Retention Lock mode, False otherwise)
        """
        # self.log_message(f"checking STATE for Bardcode: {tape_info['barcode']} state:{tape_info['state']}")
        state = tape_info["state"]
        state = state.strip()
        if "RL" in state:
            # self.log_message(f"Bardcode: {tape_info['barcode']} state:{tape_info['state']} Retention-Lock already set")
            return False
        else:
            return True

    def check_modification_date(self, tape_info, mechanism):
        """
        Method: check_modification_date
        Description: This method checks the modification date of the tape against the current date and yesterday's date. It validates the modification date based on the specified mechanism:
                     - Mechanism 1: The tape should have been modified today.
                     - Mechanism 2: The tape should have been modified yesterday.
        Required: tape_info (contains modification date of the tape), mechanism (integer value to determine the date comparison)
        Returns: Boolean (True if the modification date matches the expected date based on the mechanism, False otherwise)
        """

        modified_date = datetime.strptime(tape_info["modification_time"], '%Y/%m/%d %H:%M:%S')

        # self.log_message(f"checking LAST MODIFIED Barcode: {tape_info['barcode']} {tape_info['modification_time']} and Mechanism #{mechanism}")

        # Get the date part only (2023/12/04) and set the time to 12:00 AM (midnight)
        t_stamp = modified_date.replace(hour=0, minute=0, second=0, microsecond=0)

        modified_date_timestamp = int(t_stamp.timestamp())

        # todays date
        current_date = datetime.now()

        # Get the date part only (2023/12/04) and set the time to 12:00 AM (midnight)
        midnight_dt_object = current_date.replace(hour=0, minute=0, second=0, microsecond=0)

        current_date_epoch_timestamp = int(midnight_dt_object.timestamp())

        yesterday = (datetime.now() - timedelta(1)).strftime('%Y/%m/%d %H:%M:%S')

        dt_object = datetime.strptime(yesterday, '%Y/%m/%d %H:%M:%S')

        # Get the date part only (2023/12/04) and set the time to 12:00 AM (midnight)
        midnight_dt_object = dt_object.replace(hour=0, minute=0, second=0, microsecond=0)

        yesterday_epoch_timestamp = int(midnight_dt_object.timestamp())

        if current_date_epoch_timestamp == modified_date_timestamp and mechanism == 1:
            return True
        elif yesterday_epoch_timestamp == modified_date_timestamp and mechanism == 2:
            return True
        else:
            return False

    def set_retention_lock(self, tape_info):
        """
        Method: set_retention_lock
        Description: This method sets the retention lock on a tape in the specified pool. The retention lock period is determined based on daily or monthly backup settings.
        Required: tape_info (contains information about the tape including barcode and pool name), retention_lock_period (calculated retention lock period)
        Returns: Boolean (True if retention lock was successfully applied and the tape state is updated to "RO/RL*", False otherwise)
        """
        retention_lock_period = None

        if self.retention_lock_period_for_tapes_in_days is not None and isinstance(
                self.retention_lock_period_for_tapes_in_days, int) and self.retention_lock_period_for_tapes_in_days > 0:
            retention_lock_period = str(self.retention_lock_period_for_tapes_in_days) + "day"

        if self.retention_lock_period_for_tapes_for_monthly_in_years is not None and isinstance(
                self.retention_lock_period_for_tapes_for_monthly_in_years,
                int) and self.retention_lock_period_for_tapes_for_monthly_in_years > 0:
            retention_lock_period = str(self.retention_lock_period_for_tapes_for_monthly_in_years) + "year"

        if retention_lock_period:
            self.log_message(
                f"Setting Retention Lock on Barcode: {tape_info['barcode']} in Pool: {tape_info['pool_name']} to {retention_lock_period}.")
            command = f"vtl tape modify {tape_info['barcode']} pool {tape_info['pool_name']} retention-lock {retention_lock_period}"
            self.log_message(f"[Executing Command]: {command}")
            retention_set_result = self.execute_ssh_command(command, self.instance, self.user, self.password)

            self.log_message(
                f"Check Retention Lock Status: Pool Info {tape_info['pool_name']} barcode {tape_info['barcode']} ")
            command = f"vtl tape show pool {tape_info['pool_name']} barcode {tape_info['barcode']}"
            self.log_message(f"[Executing Command]: {command}")

            data = self.execute_ssh_command(command, self.instance, self.user, self.password)

            updated_info = self.format_tape_data(data)

            self.log_message(json.dumps(updated_info))

            if updated_info[0]["state"] == "RO/RL*":
                return True
            else:
                return False
        else:
            return False

    def get_pool_info(self):
        """
        Method: get_pool_info
        Description: This method checks if the VTL (Virtual Tape Library) status is enabled and logs details about the pool, mechanism, and retention lock period. It also verifies if the pool has retention lock governance mode enabled.
        Returns: None
        """
        if self.VTL_STATUS == True:
            self.log_message(
                "----------------------------------------------------------------------------------------------------------------------------")
            self.log_message(
                "----------------------------------------------------------------------------------------------------------------------------")

            self.log_message(
                f"[INFO] Pool:{self.pool}")

            # checking Pool has enabled with permission with Retention Lock Governance Mode
            self.check_pool_retention_lock_governance_mode()

    def apply_retention_lock_to_tapes(self):
        """
        Method: apply_retention_lock_to_tapes
        Description: This method applies retention lock to all tapes in the tape list if the retention lock governance mode is enabled for the pool. It iterates over each tape, applies the retention lock, and logs the results.
        Required: tape_list (list of tapes to apply retention lock), pool_retention_lock_enabled (boolean indicating if the pool is in retention lock governance mode)
        Returns: None
        """
        if len(self.tape_list) > 0 and self.pool_retention_lock_enabled == True:
            tape_list_result = []
            for tape in self.tape_list:
                self.log_message(
                    f"=======================================================================================")
                self.log_message(
                    f"Initiating Retention Lock: Barcode: {tape['barcode']}  used: {tape['used']} state: {tape['state']} ")
                result = self.set_retention_lock(tape)
                if result == True:
                    tape_list_result.append(tape['barcode'])
                    self.log_message(
                        f"retention-lock successfully set for {tape['barcode']} in Pool: {tape['pool_name']}")
                else:
                    self.log_message(
                        f"retention-lock failed to set for {tape['barcode']}")
                self.log_message(
                    f"=======================================================================================")

            self.log_message(
                "=======================================RL Result for Barcodes===========================================")
            self.log_message(json.dumps(self.get_result(tape_list_result), indent=4))
            self.log_message(
                "========================================================================================================================")
        else:
            self.log_message(f"Failed to enable Retention-Lock Governance Mode for pool {self.pool}")

    def get_result(self, tape_list_result):
        """
        Method: get_result
        Description: This method fetches the VTL pool tape data after applying retention lock, and returns a list of tape information for the barcodes passed in `tape_list_result`. It retrieves pool data, formats it, and filters the result by barcode.
        Required: tape_list_result (list of barcodes for which retention lock was applied)
        Returns: List of dictionaries containing tape information (e.g., barcode, state, size, modification time, etc.)
        """
        vtl_pool_name = self.pool
        mechanism = self.mechanism
        self.log_message(f"Fetching Pool: {vtl_pool_name} Tape list result ...")
        command = f"vtl tape show pool {vtl_pool_name} sort-by modtime descending"
        pool_data = self.execute_ssh_command(command, self.instance, self.user, self.password)
        # print(pool_data)
        if not pool_data:
            self.log_message(f"Failed to retrieve Pool: {vtl_pool_name} details.")
            return []

        heading = ['Processing tapes....', 'Barcode', 'Pool', 'Location', 'State', 'Size', 'Used (%)', 'Comp',
                   'Modification Time',
                   'Total size of tapes:', 'Total pools:', 'Total number of tapes:', 'Average Compression:'
                                                                                     '--------',
                   ' --------------------', ' -----------------------', ' -----', ' --------', ' ----------------',
                   ' ----', ' -------------------'
                   ]
        tape_list = []
        try:
            for line in pool_data.split("\n"):

                line = line.split('  ')

                if line[0].strip() == '':
                    continue
                # print(line)
                # Check if any element in the array contains a dash '-'
                # remove_dash = any('-' in item for item in line)

                # if remove_dash:
                #    continue

                # print(line)
                # Check if any element in the array contains a dash 'Barcode'
                # remove_heading = True if line[0].strip() in heading else False
                remove_heading = set(line) & set(heading)

                if remove_heading:
                    continue

                if len(line) < 2:
                    continue
                # print(line)
                tapes = []

                for tape_data in line:
                    tape_data = tape_data.strip()
                    if tape_data:
                        tapes.append(tape_data)

                if len(tapes) == 8:
                    if tapes[0].strip() in tape_list_result:
                        tape_info = {
                            "barcode": tapes[0],
                            "pool_name": tapes[1],
                            "location": tapes[2],
                            "state": tapes[3],
                            "size": tapes[4],
                            "used": tapes[5],
                            "modification_time": tapes[7]
                        }
                        tape_list.append(tape_info)
        except Exception as e:
            self.log_message(str(e))

        log_message("Generating report")
        generate_report_rl(vtl_pool_name, json.dumps(tape_list, indent=4))
        log_message("report generated successfully")
        return tape_list


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Load and validate YAML input parameters.")
    parser.add_argument("input_parameters", help="Path to the input YAML file.", type=str)

    args = parser.parse_args()
    file = args.input_parameters

    open_system_obj = OpenSystem()
    open_system_obj.__init__()

    # validate loaded yaml file
    open_system_obj.validate_yaml_file(file)

    # load yaml file
    open_system_obj.load_input_params()

    open_system_obj.log_message(
        "========================================================================================")
    open_system_obj.log_message(
        "------------[START] RetentionLock Tapes Immutable -----------------------")
    open_system_obj.log_message(
        "=========================================================================================")

    open_system_obj.validate_input_parameters()

    open_system_obj.decrypt_credentials()

    # check DD VTL Status
    open_system_obj.check_vtl_state()

    # Get Pool Information about RLGE
    open_system_obj.get_pool_info()

    # generate filesys report of all tapes
    open_system_obj.generate_filesys_report()

    # Get and list out all tapes
    open_system_obj.get_tapes()

    if len(open_system_obj.tape_list) > 0:
        #enable RLGE mode for Pool before applyint RL to tapes
        open_system_obj.enable_retention_lock_pool()

        #set minimum RL period for MTREE/POOL
        open_system_obj.set_min_retention_lock_period_pool()

        #set maximum RL period for MTREE/POOL
        open_system_obj.set_max_retention_lock_period_pool()

        #apply RL to tapes
        open_system_obj.apply_retention_lock_to_tapes()
    else:
        open_system_obj.log_message("No Tapes are available")

    open_system_obj.log_message(
        "========================================================================================")
    open_system_obj.log_message(
        "------------[END] RetentionLock Tapes Immutable -----------------------")
    open_system_obj.log_message(
        "=========================================================================================")
