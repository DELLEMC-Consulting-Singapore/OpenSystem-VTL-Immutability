import json
import sys
from datetime import datetime
import argparse
import re
import time

import utils
from utils import run_nsrmm_command, run_nsrjb_command, log_message, validate_yaml_file, get_input_parameters, decrypt_credentials, execute_ssh_command, generate_report_expired

class OpenSystemVTLReset():
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
        self.pool=None

        self.created_tapes = []
        self.VTL_STATUS=False
        self.pool_retention_lock_enabled=False
        self.retention_locked_tape_list =[]
        self.report = []
        self.imported_barcodes = []
        self.log_message = log_message
        self.doamin_specific_pools = []
        self.jukebox_name = []
    def set_pool(self, pool):
        self.pool = pool

    def set_instance(self, instance):
        self.instance = instance

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
                self.instances = data.get('open_system_instances')
                self.cred_file = data.get('open_system_credential_file_path')
                self.pools = data.get('pool_names')
                self.jukebox_name = data.get('jukebox_name')
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
        if self.instances == None:
            # Log an error message specifying the missing parameter and the file name, then exit the program
            self.log_message(f"'open_system_instance' parameter is missing on {self.input_parameters_file} file")
            sys.exit(1)

        # Check if 'pool_name' parameter is provided
        if self.pools == None:
            # Log an error message specifying the missing parameter and the file name, then exit the program
            self.log_message(f"'pool_name' parameter is missing on {self.input_parameters_file} file")
            sys.exit(1)

        # Check if 'open_system_credential_file_path' parameter is provided
        if self.cred_file == None:
            # Log an error message specifying the missing parameter and the file name, then exit the program
            self.log_message(
                f"'open_system_credential_file_path' parameter missing on {self.input_parameters_file} file")
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
        vtl_status = execute_ssh_command("vtl status", self.instance, self.user, self.password)

        if vtl_status and "enabled" in vtl_status and "running" in vtl_status and "licensed" in vtl_status:
            self.log_message(f"VTL is enabled, running, and licensed.")
            self.VTL_STATUS = True

    def get_tapes_by_pool(self):
        """
        Method: get_tapes
        Description: This method fetches the list of tapes from the specified VTL pool, sorts by modification time,
                     and filters the tapes based on certain conditions like being in a slot, used status, state, and modification date.
        Required: vtl_pool_name (assigned to self.pool), mechanism (assigned to self.mechanism)
        Returns: None (Logs the filtered list of tapes and stores it in the instance's tape_list attribute)
        """
        vtl_pool_name = self.pool
        self.log_message(f"Fetching Pool: {vtl_pool_name} Tape list...")
        command = f"vtl tape show pool {vtl_pool_name} time-display retention sort-by state ascending"
        self.log_message(f"[Executing Command]: {command}")
        pool_data = execute_ssh_command(command, self.instance, self.user, self.password)
        if not pool_data:
            self.log_message(f"Failed to retrieve Pool: {vtl_pool_name} details.")
            self.retention_locked_tape_list = []
            return

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

                tape_info = {
                        "barcode": tapes[0],
                        "pool_name": tapes[1],
                        "location": tapes[2],
                        "state": tapes[3],
                        "size": tapes[4],
                        "retention_time": tapes[7]
                    }
                if tape_info["barcode"] == "TEST01L5":
                    tape_list.append(tape_info)
                state = self.check_state(tape_info)
                check_retention_date_tape = self.check_retention_date(tape_info)
                if state and check_retention_date_tape:
                    tape_list.append(tape_info)
        except Exception as e:
            self.log_message(e)

#         if vtl_pool_name == "FEB_3":
#             tape_list = [    {
#         "barcode": "A55157LA",
#         "pool_name": "FEB_3",
#         "location": "Pool_TEST slot 667",
#         "state": "RO/RL*",
#         "size": "5 GiB",
#         "retention_time": "2025/02/16 19:45:48"
#     }
# ]
#         if vtl_pool_name == "POOL":
#             tape_list = [  {
#         "barcode": "A55004LA",
#         "pool_name": "POOL",
#         "location": "Pool_TEST slot 714",
#         "state": "RO/RL*",
#         "size": "5 GiB",
#         "retention_time": "2025/02/17 00:00:00"
#     }
#
#
#             ]
        self.log_message("Listing Retention Lock expired tapes")
        self.log_message(json.dumps(tape_list, indent=4))

        self.retention_locked_tape_list = tape_list

    def export_tape_from_library(self, tape):
        location = tape["location"]
        slot_number = re.search(r'(\d+)$', location)
        barcode = tape["barcode"]
        library = location.split(" ")
        library_name = library[0]
        vtl_pool_name = tape["pool_name"]
        #if slot number and library is other than vault
        if slot_number and library_name != "vault":
            slot = slot_number.group(1)
            self.log_message(
                f"Exporting Barcode: {barcode} from Library: {library_name} and Pool: {vtl_pool_name}.")
            command = f"vtl export {library_name} slot {slot}"
            self.log_message(f"[Executing Command]: {command}")

            execute_result = execute_ssh_command(command, self.instance, self.user, self.password)
            if execute_result == False:
                self.log_message(f"Error while exporitng Barcode: {barcode} from Pool: {vtl_pool_name}")
                return False
            else:
                self.log_message(f"Barcode: {barcode} from Pool: {vtl_pool_name} successfully exported")
                return True
    def import_tape_from_library(self, tape):
        vtl_pool_name = self.pool
        barcode = tape["barcode"]

        location = tape["location"]
        library = location.split(" ")
        library_name = library[0]

        if library_name != "vault":
            self.log_message(
                f"Importing Barcode: {barcode} into Pool: {vtl_pool_name}.")
            command = f"vtl import {library_name} barcode {barcode} count 1 pool {vtl_pool_name} element slot"
            self.log_message(f"[Executing Command]: {command}")

            execute_result = execute_ssh_command(command, self.instance, self.user, self.password)

            if execute_result == False:
                self.log_message(f"Error while importing Barcode: {barcode} to Pool: {vtl_pool_name}")
                return False
            else:
                self.log_message(f"Barcode: {barcode} on Pool: {vtl_pool_name} successfully imported")
                return True
        else:
            return False

    def remove_retention_locked_tapes(self):
        """
            Method: remove_retention_locked_tapes
            Description: This method removes the list of tapes from the specified VTL pool
            Required: vtl_pool_name (assigned to self.pool), mechanism (assigned to self.mechanism)
            Returns: None (Logs the filtered list of tapes and stores it in the instance's tape_list attribute)
            """

        if len(self.retention_locked_tape_list) > 0:
            removed_barcodes = []
            created_tapes = []

            failed_tape_while_export = []
            failed_tape_while_remove = []
            failed_tape_while_create = []
            failed_tape_while_import = []
            failed_tape_while_delete_on_networker = []
            for rl_tape in self.retention_locked_tape_list:
                self.log_message("===========================================================================================================")

                #delete from the networker
                command = ['nsrmm', '-d', rl_tape["barcode"]]
                delete_result = run_nsrmm_command(command)

                # sleep 60 seconds to refresh the tape on networker
                time.sleep(60)

                if delete_result:
                    self.log_message(f'barcode {rl_tape["barcode"]} has been deleted from the networker')
                    export_result = self.export_tape_from_library(rl_tape)
                    if export_result:
                        remove_result = self.execute_tape_remove_commmand(rl_tape)
                        if remove_result:
                            created_result = self.create_tape(rl_tape)
                            if created_result:
                                import_result = self.import_tape_from_library(rl_tape)
                                if import_result:

                                    #sleep 60 seconds to refresh the tape on networker
                                    time.sleep(60)

                                    # labelling the volume
                                    command = ['nsrjb', '-L', '-j', self.jukebox_name, f'-b{self.pool}', '-T', rl_tape["barcode"], '-Y']

                                    self.log_message(f'Labeling barcode {rl_tape["barcode"]} on networker')

                                    labeling = run_nsrjb_command(command)
                                    if labeling:
                                        self.log_message(f'Labeling barcode {rl_tape["barcode"]} on networker is completed')
                                    created_tapes.append(rl_tape["barcode"])
                                else:
                                    failed_tape_while_import.append(rl_tape)
                            else:
                                failed_tape_while_create.append(rl_tape)
                        else:
                            failed_tape_while_remove.append(rl_tape)
                    else:
                        failed_tape_while_export.append(rl_tape)
                else:
                    failed_tape_while_delete_on_networker.append(rl_tape)
                self.log_message(
                    "===========================================================================================================")

            if len(failed_tape_while_export) > 0:
                self.log_message("[START][FAILED] List of tapes failed while export from library")
                self.log_message(json.dumps(failed_tape_while_export, indent=4))
                self.log_message("[END][FAILED] Listing tapes failed while export from library")


            if len(failed_tape_while_remove) > 0:
                self.log_message("[START][FAILED] Listing tapes failed while remove from pool")
                self.log_message(json.dumps(failed_tape_while_remove, indent=4))
                self.log_message("[END][FAILED] Listing tapes failed while remove from pool")


            if len(failed_tape_while_create) > 0:
                self.log_message("[START][FAILED] List of tapes failed while create on pool")
                self.log_message(json.dumps(failed_tape_while_create, indent=4))
                self.log_message("[END][FAILED] Listing tapes failed while create on pool")


            if len(failed_tape_while_import) > 0:
                self.log_message("[START][FAILED] List of tapes failed while import into pool")
                self.log_message(json.dumps(failed_tape_while_import, indent=4))
                self.log_message("[END][FAILED] List of tapes failed while import into pool")

            self.created_tapes = created_tapes
        else:
            self.log_message("No RL tapes found")
    def execute_tape_remove_commmand(self, tape_info):
        """
       Method: execute_tape_remove_commmand
       Description: This method removes the list of tapes from the specified VTL pool
       Required: vtl_pool_name (assigned to self.pool), mechanism (assigned to self.mechanism)
       Returns: None (Logs the filtered list of tapes and stores it in the instance's tape_list attribute)
       """
        vtl_pool_name = self.pool
        barcode = tape_info['barcode']

        self.log_message(f"Removing Tape with Barcode: {barcode} from Pool: {vtl_pool_name} on OpenSystem: {self.instance}.")
        command = f"vtl tape del {barcode} pool {vtl_pool_name}"
        self.log_message(f"[Executing Command]: {command}")
        result = False
        execute_result = execute_ssh_command(command, self.instance, self.user, self.password)
        if execute_result == False:
            self.log_message(f"Error while removing Barcode: {barcode} on Pool: {vtl_pool_name}")
        else:
            self.log_message(f"Barcode: {barcode} on Pool: {vtl_pool_name} removed successfully")
            result = True
        return result
    def create_tape(self, rl_tape):
        """
      Method: create_tape
      Description: This method recreate the removed tapes for the specified VTL pool
      Required: rl_tape
      Returns: None (Logs the filtered list of tapes and stores it in the instance's tape_list attribute)
      """
        vtl_pool_name = self.pool
        barcode = rl_tape['barcode']
        size_str = rl_tape['size'].strip()
        size_value, size_unit = size_str.split()
        size_value = int(size_value)

        self.log_message(
            f"Creating Tape with Barcode: {barcode} on Pool: {vtl_pool_name} on OpenSystem: {self.instance}.")
        command = f"vtl tape add {barcode} capacity {size_value} pool {vtl_pool_name}"
        self.log_message(f"[Executing Command]: {command}")
        result = False
        execute_result = execute_ssh_command(command, self.instance, self.user, self.password)
        if execute_result == False:
            self.log_message(f"Error while creating Barcode: {barcode} on Pool: {vtl_pool_name}")
        else:
            self.log_message(f"Barcode: {barcode} on Pool: {vtl_pool_name} created successfully")
            result = True
        return result
    def check_state(self, tape_info):
        """
        Method: check_state
        Description: This method checks if the tape is in Retention Lock (RL) mode. If the tape is in RL mode, it returns True to exclude it from further processing. Otherwise, it returns False.
        Required: tape_info (contains information about the tape including its state)
        Returns: Boolean (True if the tape is in Retention Lock mode, False otherwise)
        """
        # self.log_message(f"checking STATE for Bardcode: {tape_info['barcode']} state:{tape_info['state']}")
        state = tape_info["state"]
        state = state.strip()
        if "RL" in state:
            # self.log_message(f"Bardcode: {tape_info['barcode']} state:{tape_info['state']} Retention-Lock already set")
            return True
        else:
            return False

    def check_retention_date(self, tape_info):
        """
        Method: check_modification_date
        Description: This method checks the modification date of the tape against the current date and yesterday's date. It validates the modification date based on the specified mechanism:
                     - Mechanism 1: The tape should have been modified today.
                     - Mechanism 2: The tape should have been modified yesterday.
        Required: tape_info (contains modification date of the tape), mechanism (integer value to determine the date comparison)
        Returns: Boolean (True if the modification date matches the expected date based on the mechanism, False otherwise)
        """

        if tape_info["retention_time"] != "n/a":
            modified_date = datetime.strptime(tape_info["retention_time"], '%Y/%m/%d %H:%M:%S')

            modified_date_timestamp = int(modified_date.timestamp())

            # todays date
            current_time_stamp = datetime.now().timestamp()

            if current_time_stamp > modified_date_timestamp:
                return True
            else:
                return False
        else:
            return False

    def check_result(self):
        if len(self.created_tapes):
            self.get_result(self.created_tapes)
        else:
            self.log_message("None of the Tapes are removed")
    def get_result(self, tape_list_result):
        """
        Method: get_result
        Description: This method fetches the VTL pool tape data after applying retention lock, and returns a list of tape information for the barcodes passed in `tape_list_result`. It retrieves pool data, formats it, and filters the result by barcode.
        Required: tape_list_result (list of barcodes for which retention lock was applied)
        Returns: List of dictionaries containing tape information (e.g., barcode, state, size, modification time, etc.)
        """
        vtl_pool_name = self.pool
        self.log_message(f"Fetching Pool: {vtl_pool_name} Tape list result ...")
        command = f"vtl tape show pool {vtl_pool_name} sort-by modtime descending"
        pool_data = execute_ssh_command(command, self.instance, self.user, self.password)
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

                # Check if any element in the array contains a dash '-'
                remove_dash = any('-' in item for item in line)

                if remove_dash:
                    continue

                # Check if any element in the array contains a dash 'Barcode'
                # remove_heading = True if line[0].strip() in heading else False
                remove_heading = set(line) & set(heading)

                if remove_heading:
                    continue

                if len(line) < 2:
                    continue

                tapes = []

                for tape_data in line:
                    tape_data = tape_data.strip()
                    if tape_data:
                        tapes.append(tape_data)

                if len(tapes) > 0:
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
        generate_report_expired(self.pool, json.dumps(tape_list, indent=4))
        log_message("report generated successfully")
        self.log_message(json.dumps(tape_list, indent=4))
    def get_pools_present_on_VTL(self):
        open_system_pool_names = self.pools
        result = [x.strip() for x in open_system_pool_names.split(',')]

        self.log_message(f"check all Pools exist on OpenSystem: {self.instance} provided on list:")
        command = f"vtl pool show all"
        self.log_message(f"[Executing Command]: {command}")

        pool_data = execute_ssh_command(command, self.instance, self.user, self.password)

        if not pool_data:
            self.log_message(f"No data found")
            return []

        exist_pool_list = []
        i = 0
        try:
            for line in pool_data.split("\n"):
                # parts = line.split()
                line = line.split('  ')
                if type(line) == list and line[0] in result:
                    exist_pool_list.append(line[0].strip())

        except Exception as e:
            self.log_message(e)

        self.doamin_specific_pools = exist_pool_list
        self.log_message(f"These pools are processing from OpenSystem: {self.instance} Pools:  {json.dumps(exist_pool_list)}")
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Load and validate YAML input parameters.")
    parser.add_argument("input_parameters", help="Path to the input YAML file.", type=str)

    args = parser.parse_args()
    file = args.input_parameters

    open_system_reset_obj = OpenSystemVTLReset()
    open_system_reset_obj.__init__()

    # validate loaded yaml file
    open_system_reset_obj.validate_yaml_file(file)

    # load yaml file
    open_system_reset_obj.load_input_params()

    open_system_reset_obj.log_message(
        "========================================================================================")
    open_system_reset_obj.log_message(
        "------------[START] RetentionLock Expired Tapes Auto delete and Create -----------------------")
    open_system_reset_obj.log_message(
        "=========================================================================================")

    open_system_reset_obj.validate_input_parameters()

    open_system_reset_obj.decrypt_credentials()

    open_system_instances = open_system_reset_obj.instances
    # open_system_pool_names = open_system_reset_obj.pools

    for open_system_instance in open_system_instances.split(","):
        #setting the each instance to the object
        open_system_reset_obj.set_instance(open_system_instance.strip())

        open_system_reset_obj.get_pools_present_on_VTL()

        open_system_pool_names = open_system_reset_obj.doamin_specific_pools

        # checking the VTL status of each instance
        open_system_reset_obj.check_vtl_state()
        if len(open_system_pool_names):
            for pool_name in open_system_pool_names:
                # setting the each instance to the object
                open_system_reset_obj.set_pool(pool_name.strip())

                #get all retention-lock expired tapes
                open_system_reset_obj.get_tapes_by_pool()

                # auto delete and create RL expired tapes
                open_system_reset_obj.remove_retention_locked_tapes()

                # check the result
                open_system_reset_obj.check_result()
        else:
            open_system_reset_obj.log_message(f"No pools exist on OpenSystem: {open_system_instance}")
    open_system_reset_obj.log_message(
        "========================================================================================")
    open_system_reset_obj.log_message(
        "------------[END] RetentionLock Expired Tapes Auto delete and Create -----------------------")
    open_system_reset_obj.log_message(
        "=========================================================================================")
