import sys
import json


# To run the code runm on the terminal: python3 parser.py beeline_consent_query_stderr.txt
# This assumes that the txt file and the script are in the same folder, if not put the correct paths in the comand


# Function to load file once
def load_file(path):
    with open(path, "r") as file:
        return file.read()

# Function to find the duration for a given operation
def find_duration(data, operation):
    # Start serching after the name of the operation
    operations_position = data.find(operation) + len(operation)
    duration = ''
    for char in data[operations_position:]:
        # Skip empty spaces
        if char != ' ':
            duration += char
        # End when you find s (seconds)
        if char == 's':
            return duration

# Function to extract metrics from a section. 
# Takes as arguments the data spitted in lines, the index of the section we will work on
# and the list of metrcis we are searching for. It returns a dictionary
def extract_metrics(lines, start_index, metrics):
    dictionary = {}
    for line in lines[start_index:]:
        # End if we get to the line starting with "--", that ends the table
        if line.startswith("--"):
            break
        # Split each line and get rid of unecessary blank spaces
        new_line = [i.replace(" ", "") for i in line.split("  ") if i != ""]
        # Create the dictionary for each vertex and each metric
        dictionary[new_line[0]] = {metrics[j]: value for j, value in enumerate(new_line[1:])}
    return dictionary

# Function to process the file for the three objectives
def process_file(data):
    # 1st objective - Query Execution Summary
    # List of the operations we are searching for
    operations = ["Compile Query", "Prepare Plan", "Get Query Coordinator (AM)", "Submit Plan", "Start DAG", "Run DAG"]
    # Create dictionary for query execution summary, with the find_duration function
    query_execution_dictionary = {operation: find_duration(data, operation) for operation in operations}
    
    # Dump dictionary to a json file
    with open("Query_Execution_Summary.json", "w") as new_file:
        json.dump(query_execution_dictionary, new_file, indent=4)

    # 2nd objective - Task Execution Summary
    # Transform data into a list of lines to simplify the task. Get rid of the 'INFO' statement in the start
    lines = [line.replace('INFO  : ', '') for line in data.splitlines()]
    # Index of the section we want to work with. 
    # 4 lines under the title (This is fixed as we can see, with the metrics titles line and the "--" lines)
    sec_index_task = lines.index("Task Execution Summary") + 4
    # List of the metrcis we are searching for
    metrics = ["DURATION(ms)", "CPU_TIME(ms)", "GC_TIME(ms)", "INPUT_RECORDS", "OUTPUT_RECORDS"]
    # Create dictionary for task execution summary, with the extract_metrics function
    task_execution_dictionary = extract_metrics(lines, sec_index_task, metrics)
    
    # Dump dictionary to a json file
    with open("Task_Execution_Summary.json", "w") as new_file:
        json.dump(task_execution_dictionary, new_file, indent=4)

    # 3rd objective - Detailed Metrics per task
    # Reverse search to find last line starting with "--"
    metrics_start = data.rfind("--")
    # Transform data into a list of lines to simplify the task. Get rid of the 'INFO' statement in the start and any empty lines
    lines_detailed = list(filter(None, [line.replace('INFO  : ', '') for line in data[metrics_start:].splitlines() if line != "--"]))

    detailed_metrics_dictionary = {}
    for i, line in enumerate(lines_detailed):
        # End if we reach the line starting with "Completed..."
        if line.startswith("Completed "):
            break
        # Check if we have a line with a group tile or a metric
        elif not line.startswith(" "):
            # Add the name of the group to the dictionary as key
            group_title = line
            detailed_metrics_dictionary[group_title] = {}
        elif line.startswith("  "):
            # Add the name of the metric and the value of the metric to the dictionary as key-value for each group
            # Split the lines with metrics on ":", to get the name and value
            metric_title, value = line.replace(" ", "").split(":", 1)
            # Create the final nested dictionary
            detailed_metrics_dictionary[group_title][metric_title] = value
    
    # Dump dictionary to a json file
    with open("Detailed_Metrics_Per_Task.json", "w") as new_file:
        json.dump(detailed_metrics_dictionary, new_file, indent=4)

if __name__ == "__main__":
    # Load the data from the file specified in the command line argument. We have to give the file path on the terminal command
    data = load_file(sys.argv[1])
    # Run the process_file function to get the three json files needed
    process_file(data)
