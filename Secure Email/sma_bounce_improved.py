import csv
import time
import os
import re
import logging
import getpass
import requests
import ipaddress
import datetime
import multiprocessing
import ahocorasick

# Global automaton and patterns list for multiprocessing Pool initializer
# These are set up once per worker process.
_automaton = None
_required_patterns_set = None  # Set of patterns that *must* be present in a line
_extraction_regexes = None 

def worker_init(required_patterns, raw_extraction_regexes):
    """
    Initializer function for multiprocessing pool.
    Each worker process will build its own Aho-Corasick automation and compile regexes.
    """
    global _automaton
    global _required_patterns_set
    global _extraction_regexes

    _required_patterns_set = set(required_patterns)

    # Build Aho-Corasick automaton for the required patterns
    A = ahocorasick.Automaton()
    for pattern in required_patterns:
        A.add_word(pattern, pattern)  # Store pattern itself as value
    A.make_automaton()
    _automaton = A

    # Compile extraction regexes
    _extraction_regexes = []
    for name, pattern_str in raw_extraction_regexes:
        try:
            compiled_re = re.compile(pattern_str)
            _extraction_regexes.append((name, compiled_re))
        except re.error as e:
            print(f"Warning: Invalid regex '{pattern_str}' for name '{name}' in worker process. Skipping. Error: {e}")
            continue
    current = multiprocessing.current_process()
    print(f"Automation built: {current.name} - {current._identity}")
    
def _search_file_worker(file_path, output_queue, data_attributes):
    """
    Worker function for multiprocessing pool.
    Searches for patterns in a single file, checks for all required patterns,
    and then extracts information using regex if the condition is met.
    """
    global _automaton
    global _required_patterns_set
    global _extraction_regexes

    if _automaton is None or _required_patterns_set is None or _extraction_regexes is None:
        raise RuntimeError("Worker globals not initialized. This indicates a problem with worker_init.")

    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line_num, line_content in enumerate(f, 1):
                line_content_stripped = line_content.strip()

                found_patterns_in_line = set()
                # Use Aho-Corasick to find all occurrences of the initial required patterns
                for end_index, matched_pattern in _automaton.iter(line_content_stripped):
                    found_patterns_in_line.add(matched_pattern)

                # Check if all required patterns are present in this line
                if _required_patterns_set.issubset(found_patterns_in_line):
                    result_row = {
                        'Message MID': data_attributes["attributes"]["mid"][0],
                        'Hostname': data_attributes["attributes"]["hostName"],
                        'Message Status': "Bounced",
                        'Sender IP': data_attributes["attributes"]["senderIp"],
                        'Recipient': _required_patterns_set,
                        'Subject': data_attributes["attributes"]["subject"],
                        'Timestamp': data_attributes["attributes"]["timestamp"],
                        'Sender': data_attributes["attributes"]["sender"]
                    }

                    # Apply extraction regexes
                    for regex_name, compiled_regex in _extraction_regexes:
                        match = compiled_regex.search(line_content_stripped)
                        if match:
                            # If regex has named groups, extract them
                            if compiled_regex.groupindex:
                                for group_name in compiled_regex.groupindex:
                                    result_row[group_name] = match.group(group_name)
                            else:
                                # If no named groups, use the regex name and take the whole match or first group
                                result_row[regex_name] = match.group(0) if match.groups() else match.group(0)
                        else:
                            # Ensure all expected extraction fields are present, even if empty
                            if compiled_regex.groupindex:
                                for group_name in compiled_regex.groupindex:
                                    result_row[group_name] = ''
                            else:
                                result_row[regex_name] = ''

                    output_queue.put(result_row)
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        
def find_files(directory):
    """
    Finds all files with in the given directory and its subdirectories.
    """
    file_list = []
    for root, _, files in os.walk(directory):
        for file in files:
                file_list.append(os.path.join(root, file))
    return file_list
        
def write_results_to_csv(output_queue, output_csv_path):
    """
    Consumes results from the queue and writes them to a CSV file.
    This function runs in a separate process to avoid blocking search workers.
    """
    columns = ['Message MID', 'Hostname' ,'Message Status', 'Sender IP', 'Recipient',
            'Subject', 'Timestamp', 'Sender', 'Error', 'Reason']
    with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()

        while True:
            item = output_queue.get()
            if item is None:  # Sentinel value to signal end of results
                break
            writer.writerow(item)
    print(f"CSV writing process finished. Results saved to {output_csv_path}")


def main():
    
    BOUNCE_FILE_PATH = r"D:\SMA_API_Scripts"
    BOUNCE_LOGS_DIR = r"D:\SMA_API_Scripts\ESABounceLogs"

    logging.basicConfig(filename=BOUNCE_FILE_PATH+"/esa_script_logs.log",
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filemode='w')

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    while True:
        SMA = input("Please enter SMA IP or FQDN: ")
        try:
            ipaddress.ip_address(SMA)
            break
        except ValueError:
            print("Invalid SMA IP address. Please enter correct IP")

    user_sma = input("Please enter the Username of SMA: ")
    pwd_sma = getpass.getpass(prompt="Enter SMA Password: ")

    while True:
        start_date = input("Please enter start date and time in the format YYYY-MM-DDTHH:MM:SS:")
        try:
            date_object_user = datetime.datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S")
            break
        except ValueError:
            print("Please enter the date and time in the correct format (YYYY-MM-DDTHH:MM:SS)")

    while True:
        end_date = input("Please enter end date and time in the format YYYY-MM-DDTHH:MM:SS:")
        try:
            date_object_user = datetime.datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S")
            break
        except ValueError:
            print("Please enter the date and time in the correct format (YYYY-MM-DDTHH:MM:SS)")

    email_regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'

    TOTAL_RECORDS  = input("Please enter total number of records to fetch:")

    while True:
        sender_email = input("Please enter sender email address. Press ENTER if you want to search for any sender email:")
        if (re.fullmatch(email_regex, sender_email) or sender_email==''):
            break
        print("Please enter valid email address or press just ENTER to leave it blank")

    while True:
        receiver_email = input("Please enter receiver email address. Press ENTER if you want to search for any receiver email:")
        if (re.fullmatch(email_regex, receiver_email) or receiver_email==''):
            break
        print("Please enter valid email address or press just ENTER to leave it blank")

    HEADERS_JSON = {"Content-Type":"application/json",
            "Accept": "application/json"}

    HEADERS_TEXT = {"Content-Type":"text/plain"}

    OUT_FILE = os.path.join(BOUNCE_FILE_PATH, 'consolidated_bounced_logs_output.csv')
    # Setup multiprocessing environment
    num_processes = multiprocessing.cpu_count()  # Use all available CPU cores
    print(f"Using {num_processes} worker processes.")

    multiprocessing.set_start_method("spawn", force=True)
    # Use a Manager for a shared queue that can be accessed by multiple processes
    manager = multiprocessing.Manager()
    results_queue = manager.Queue()

    # Start the CSV writer process
    csv_writer_process = multiprocessing.Process(
        target=write_results_to_csv,
        args=(results_queue, OUT_FILE)
        )
    csv_writer_process.start()

    start_time = time.time()

    all_files = find_files(BOUNCE_LOGS_DIR)

    raw_regex_patterns = [("Error", r"(?<= - )(.*?)(?= \()"), ("Reason", r"\((.*?)\)")]
        
    OFFSET = 0
    LIMIT = 100
    logger.info("Connecting to SMA: %s", SMA)

    while OFFSET <= int(TOTAL_RECORDS):
        SMA_URL = f"http://{SMA}:6080/sma/api/v2.0/message-tracking/messages?startDate={start_date}.000Z&endDate={end_date}.000Z&ciscoHost=All_Hosts&searchOption=messages&offset={OFFSET}&limit={LIMIT}&hardBounced=True"
        try:
            response = requests.get(SMA_URL, headers=HEADERS_JSON,
                                    timeout=100, auth=(user_sma, pwd_sma))
        except requests.exceptions.Timeout:
            logger.error("Time out to %s", SMA)
        if response.status_code == 200:
            data = response.json()
            if int(data["meta"]["totalCount"]) == 0:
                OFFSET += 1
            else:
                OFFSET += int(data["meta"]["totalCount"])
            print(f"Number of records received from SMA: {OFFSET}. Processing bounce log lookups...")
            for attributes in data["data"]:
                message_mid = attributes["attributes"]["mid"][0]
                for recipient in attributes["attributes"]["recipient"]:
                    required_patterns = ["Bounced", str(message_mid), recipient]
                    with multiprocessing.Pool(processes=num_processes, initializer=worker_init, initargs=(required_patterns, raw_regex_patterns)) as pool:
                        async_results = []
                        for file_path in all_files:
                            async_results.append(pool.apply_async(_search_file_worker, (file_path, results_queue, attributes)))

                        # Wait for all search tasks to complete. Calling .get() ensures any exceptions are propagated.
                        for res in async_results:
                            res.get()
                
            print(f"Total Number of records processed from SMA: {OFFSET}")
        else:
            logger.error("Unable to connect successfully with %s.", SMA)
                            
    results_queue.put(None)  # Send sentinel value
    csv_writer_process.join()
    end_time = time.time()
    print(f"Search completed in {end_time - start_time:.2f} seconds.")
    print(f"Results saved to {OUT_FILE}")

if __name__ == "__main__":
    main()
