# File Structure (Top Level)
The sources.yaml file has the following top level nodes:   
1. metadata - denotes how to recognise and parse the file
2. validation - contains data about how to determine if the file is complete and valid
3. padding - the size of any headers and footers in the file   

# Sources.yaml Nodes   
## Metadata
### file_type
The file format of the datafile - acceptable options are csv | txt | none
### encoding
The text encoding of the datafile - if in doubt stick with "utf-8-sig"
### filename_pattern
A regex expression denoting the filenaming convention.  
For example "sapar_.*\.csv" will match for anyfile starting with "sapar_", having any number of characters and then ending with .csv.   
Therefore for this example the following will all match:   
- sapar_20260101.csv
- sapar_test.csv
How this will *not* match:
- sap_20260101.csv
### delimiter
The file delimiter of the data file (more relvant for txt and none type files)
### timeout_seconds
How many seconds must elapse before the file is considered *stale*.   
This is specific to files that require a companion file for their validation/completion strategy. Should the data file be detected but the companion file is not how many seconds do we wait before treating the data file as invalid (because it can't be validated without it's companion file)
### stable_seconds
How many seconds does the data file have to remian stable for before it can be considered complete (this overrides the python filewatcher debounce setting)
## Validation
### strategy
#### type
The type of validation/completion strategy to be observed for this data file. Acceptable options are file | footer.   
When the type is "file" this denotes that a seperate completion file is expected - a failure of this file to be detected will result in the data file being quarantined after the number of seconds specified in "timeout_seconds".    
When the type is "footer" the data file will be moved for processing when it becomes stable (as defined in "stable_seconds").   
#### pattern
*ONLY RELEVANT WHEN TYPE = "FILE"*   
This describes a regex pattern that will match the naming convention of the companion file for this datafile
#### key_pattern
*ONLY RELEVANT WHEN TYPE = "FILE"* 
This is a regex pattern that should describe the elements of the filenames of data and companion files that should match in order for them to be a valid pair.   
### count_pattern
This is a regex pattern describing how to identify the count number in the footer or companion file.   
For example:
If the footer should contain "Count: 123" then the regex '#####' will inform the validation parser that the correct count number is 123.  
### amount_pattern
This is a regex pattern describing how to identify the amount number in the footer or companion file.   
For example:
If the footer should contain "Amount: 123" then the regex '#####' will inform the validation parser that the correct amount number is 123. 
### amount_column
This node desscribes how to find the amount column in the data file (so that it can be aggregated and compared to the footer or companion file). Only one of the below attributes is needed:
#### name
The full name of the Amount column to use
#### position
The position in the datafile of the amount column to use (to be used in teh case of datafiles with no column headers)
## Padding
### header_size
The number of rows in any header in the data file (will be excluded with counting rows for validation purposes)
### footer_size
The number of rows in any footer in the data file (will be excluded with counting rows for validation purposes)