import time
import sys
import os
import base64
import yaml
from datetime import datetime, timedelta
import paramiko
import urllib3
import subprocess

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


full_path = os.path.abspath(os.path.dirname(__file__))
DATE = datetime.now().strftime("%d_%m_%Y")
LOGFILE = f"./script_{DATE}.log"
LOGSFOLDER = "./logs/"

REPORT_DATE_TIME = datetime.now().strftime("%d_%m_%Y_%H_%M_%S")

#Retention Lock report
RETENTION_LOCK_REPORT_FOLDER = full_path+'/reports_retention_lock/'

#Expired Retention Lock report
EXPIRED_RL_REPORT_FOLDER = full_path+'/reports_expired_RL/'

heading = ['Processing tapes....', 'Barcode', 'Pool', 'Location', 'State', 'Size', 'Used (%)', 'Comp',
               'Modification Time',
               'Total size of tapes:', 'Total pools:', 'Total number of tapes:', 'Average Compression:'
                                                                                 '--------',
               ' --------------------', ' -----------------------', ' -----', ' --------', ' ----------------',
               ' ----', ' -------------------'
               ]


def log_message(message):
    """
    Logs a message with the current timestamp.

    Method: log_message
    Required: message (str) - The message to log
    Returns: None

    - Prints the log message to the console with a timestamp.
    - Checks if the log folder exists, creates it if it does not.
    - Writes the log message into a log file.
    """
    current_time = datetime.now().strftime("%d-%m-%Y %I:%M:%S %p")
    log_entry = f"{current_time}  {message}"
    print(log_entry)

    # Check if the folder exists, if not, create it
    if not os.path.exists(LOGSFOLDER):
        os.makedirs(LOGSFOLDER)

    with open(LOGSFOLDER + LOGFILE, "a") as log_file:
        log_file.write(log_entry + "\n")

def generate_report_expired(pool, tapes):
    """
    Logs a message with the current timestamp.

    Method: log_message
    Required: message (str) - The message to log
    Returns: None

    - Prints the log message to the console with a timestamp.
    - Checks if the log folder exists, creates it if it does not.
    - Writes the log message into a log file.
    """

    # Check if the folder exists, if not, create it
    if not os.path.exists(EXPIRED_RL_REPORT_FOLDER):
        os.makedirs(EXPIRED_RL_REPORT_FOLDER)

    REPORT_FILE = f"{pool}_report_{REPORT_DATE_TIME}.txt"

    with open(EXPIRED_RL_REPORT_FOLDER + REPORT_FILE, "a") as log_file:
        log_message(f"Report generated on {EXPIRED_RL_REPORT_FOLDER + REPORT_FILE}")
        log_file.write(tapes + "\n")


def generate_report_rl(pool, tapes):
    """
    Logs a message with the current timestamp.

    Method: log_message
    Required: message (str) - The message to log
    Returns: None

    - Prints the log message to the console with a timestamp.
    - Checks if the log folder exists, creates it if it does not.
    - Writes the log message into a log file.
    """

    # Check if the folder exists, if not, create it
    if not os.path.exists(RETENTION_LOCK_REPORT_FOLDER):
        os.makedirs(RETENTION_LOCK_REPORT_FOLDER)

    REPORT_FILE = f"{pool}_report_{REPORT_DATE_TIME}.txt"

    with open(RETENTION_LOCK_REPORT_FOLDER + REPORT_FILE, "a") as log_file:
        log_message(f"Report generated on {RETENTION_LOCK_REPORT_FOLDER + REPORT_FILE}")
        log_file.write(tapes + "\n")

def validate_yaml_file(file):
    """
    Validates the existence of the specified YAML file.

    Method: validate_yaml_file
    Required: file (str) - The name of the YAML file to validate
    Returns: None

    - Checks if the file exists in the provided path.
    - If the file exists, sets the input parameters file to the full path.
    - If the file does not exist, logs an error message.
    """
    filepath = full_path + "/" + file
    input_parameters_file = None
    if os.path.exists(filepath):
        input_parameters_file = filepath
    else:
        log_message(f"The file '{file}' does not exist.")
    return input_parameters_file

def get_input_parameters(input_file_path):
    """
    Loads and reads the YAML file at the specified path.

    Method: get_input_parameters
    Required: input_file_path (str) - The full path to the YAML file
    Returns: dict or None

    - Reads the YAML file and returns the data as a dictionary.
    - If an error occurs, logs the exception and exits the program.
    """
    try:
        with open(input_file_path, 'r') as index_file:
            data = yaml.safe_load(index_file)
            return data
    except Exception as e:
        log_message(str(e))
        sys.exit(1)

def decrypt_credentials(cred_file):
    """
    Decrypts credentials (username and password) from a base64-encoded file.

    Method: decrypt_credentials
    Required: None
    Returns: tuple or None

    - Reads the credentials file and decodes the username and password.
    - Returns a tuple (username, password).
    """
    if cred_file:
        filepath = full_path + "/" + cred_file
        with open(filepath) as cred_file:
            lines = [line.strip() for line in cred_file.readlines()]
            user = base64.b64decode(lines[0]).decode('UTF-8')
            password = base64.b64decode(lines[1]).decode('UTF-8')
            return user, password
    return None, None

def execute_ssh_command(command, instance, user, password):
    """
    Executes an SSH command on a remote server.

    Method: execute_ssh_command
    Required: command (str) - The SSH command to execute
    Returns: str or bool

    - Connects to the remote server using SSH with the provided credentials.
    - Executes the given command and returns the output if successful.
    - Logs any errors encountered during the execution.
    """
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(instance, username=user, password=password)

        stdin, stdout, stderr = ssh.exec_command(command)
        output = stdout.read().decode().strip()
        error = stderr.read().decode().strip()

        ssh.close()

        if error:
            log_message(f"Error executing '{command}': {error}")
            return False

        return output

    except Exception as e:
        log_message(f"SSH Connection error: {str(e)}")
        return False



def size_to_bytes(size_str):
    """
    Method: size_to_bytes
    Description: This method converts a human-readable size string (e.g., "10 GB", "500 MB") into bytes.
    Required: size_str (string representing the size with a unit)
    Returns: Integer representing the size in bytes
    """
    size_str = size_str.strip()
    size_value, size_unit = size_str.split()
    size_value = float(size_value)

    size_unit = size_unit.upper()
    if size_unit == 'B':
        return size_value
    elif size_unit == 'KB':
        return size_value * 1024
    elif size_unit == 'MB' or size_unit == 'MIB':
        return size_value * 1024 * 1024
    elif size_unit == 'GB' or size_unit == 'GIB':
        return size_value * 1024 * 1024 * 1024
    elif size_unit == 'TB' or size_unit == 'TIB':
        return size_value * 1024 * 1024 * 1024 * 1024
    else:
        raise ValueError(f"Unsupported unit: {size_unit}")

def filter_result(pool_data, tape_list_result):
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
        log_message(str(e))

    return tape_list


def run_nsrjb_command(command):
    try:
        command_string = ' '.join(command)
        log_message(f"Executing labeling command : {command_string}")
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        # sleep 60 seconds to refresh the tape on networker
        time.sleep(60)
        log_message(f"Command output:\n {str(result.stdout)}")
        return True
    except subprocess.CalledProcessError as e:
        log_message(f"Error occurred: {str(e)}")
        return False


def run_nsrmm_command(command):
    try:
        command_string = ' '.join(command)
        log_message(f"Networker: Executing delete command : {command_string}")
        log_message("Deleting inprogress")
        # Start the process without input first
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   text=True)

        # Wait for 50 seconds before sending the input 'y'
        time.sleep(50)

        # Send 'y' to the process after the delay
        process.stdin.write('y\n')
        process.stdin.flush()

        # Capture the output after the input is sent
        stdout, stderr = process.communicate()

        # Log the command's output
        log_message(f"Command output:\n{stdout}")
        if stderr:
            log_message(f"Error occurred while deleting volume:\n{stderr}")

        return True
    except subprocess.CalledProcessError as e:
        log_message(f"Error occurred while deleting volume: {str(e)}")
        return False
