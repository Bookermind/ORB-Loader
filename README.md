# ORB-Loader
This repository contains the code for running the ORB Python based data loader - both orchestrator and loader components.  
In addtion this repository contains a small DockerFile for locally running a Microsoft SQL database for local development purposes.   
**Please be aware that this MS-SQL database is not included in the project level docker-compose.yaml file**

# Functioning of the Loader  
## Orchestrator   
The orchestrator utilises a python implementation of the rust Notify library (see [here](https://pypi.org/project/watchfiles/)). 
Python watchfiles is configured to watch a specific fodler (in local development this is the data/landing folder in this repo, in a Docker container it will be /mnt/data/landing).   
The orchestrator received a configuration object from config/sources.yaml. This read in and presented to the rest of the code by a python class object based api.   
### File configuration   
The yaml configuration file has the following layout:
```yaml
sapar:                                                      <- Name of the file object
  metadata:                                                 <- Metadata node for file file object
    file_type: csv                                          <- File type or extension of the file
    encoding: utf-8-sig                                     <- txt encoding of the file
    filename_pattern: 'sapar_.*\.csv'                       <- Regex describing the file's naming convention
    delimiter: ","                                          <- Field delimited of the file [OPTIONAL]
    timeout_seconds: 300                                    <- Number of seconds before file is considered stale
    stable_seconds: 3                                       <- Number of seconds before file is considered stable
  validation:                                               <- Validation node for the file object
    strategy:                                               <- Validation strategy node for the file object
      type: file                                            <- Validation strategy (file | footer | none)
      pattern: 'sapar_.*\.trigger.csv'                      <- Regex describing the companion file's naming convention
      key_pattern: '(\d{8})'                                <- Key pattern telling the loader how to link data and companion files
    count_pattern: 'Count:\s*(\d+)'                         <- Regex for the object in the companion file denoting record count
    amount_pattern: 'Amount:\s*([\\$£€]?[\d,]+\.?\d*)'      <- Regex for the object in the companion file denoting amount
    amount_column:                                          <- Amount column node for the file object
      name: "Amount"                                        <- The name of the amount column in the data file [OPTIONAL]
      position: 3                                           <- The position of the amount column in the data file [OPTIONAL]
  padding:                                                  <- Padding node for the file object
    header_size: 1                                          <- Size of the data file's header [OPTIONAL]
    footer_size: 2                                          <- Size of the data file's footer [OPTIONAL]
```
See [here](./config/source-yaml.md) for a full explanation of the configuration
See [here](./regex_guide.md) for a primer on the regex language (used for example in the metadata.filename_pattern node).

## Orchestration Process Flow   
When a new file is detected in the watched folder:
1. It is identified by reference to the metadata.filename_pattern node in the above config.   
    a. If a file cannot be identified from this node then identification is attempted with reference to the validation.strategy.pattern node   
2. From this we can identify a couple of key pieces of information about the file:   
    a. What is it's validation strategy?   
    b. What is it's stable seconds value?
3. If the detected file has a validation strategy of file than it and it's companion are treated as a pair from this point forward.
4. The value of the file's stable seconds is fed into the debounce arguement of the file watcher - meaning that the file needs to remaining unchanged for that number of seconds before beifn considered "stable" - this allows for a per file configuration to allow for network transfers.
5. Once the data file or the identified file-pair are stable they are moved to the data/input folder and downstream ORB loading is commenced.
6. Should part of a file pair remain in the landing folder without becoming complete for the amount of time configured in metadata.timeout_seconds then the file is moved to data/quarantine/invalid and no longer watched.
7. Should a file be detected in the landing folder that does not match either a metadata.filename_pattern or a validation.strategy.pattern then it is moved to the data/quarantine/unknown folder.

All file processes (from detection right through to movement) are logged using python built in logging library.   

# Contribution   
To contribute to this repository the developer must have the following:  
1. Local Python installation (please see [here](./pip-config.md) for setup details for pip)
2. A python virtual environment setup in the developer's local root of this repository - it is recommended to install the virtual environment as ```.venv``` so that it is not committed to git.
3. All packages listed in requirements.txt installed into the virtual environment
4. Rancher desktop installed (if required for local SQL testing)
5. A configured .env file in the root of the project

# Local Development   
After any changes to the orchestrator python code a rebuild of the container will be required (to ensure that the orchestrator container has the up to date python files), this is best acheived by utilising the docker compose files:   
From the root directory of this project run   
```bash
docker compose -f docker-compose-sql.yaml build orchestrator
```   

For local testing an development it is recommended to use a throw-away Microsoft SQL container. The project contains a sud-directory called ```mssql``` for this purpose.   
Should you need to spin up *just* the SQL container (to test changes to the admin or loggin tables for example) please open a shell at the root of this project and execute the following:   
```bash
docker compose --env-file .env -f ./mssql/docker-compose/yaml up -d
```
The SQL container can then be accessed via SSMS etc on localhost port 1433.   

There is also a docker compose yaml file for local testing/development of the python based orchestrator. This can be run as follows from the project root:   
```bash
docker compose -f docker-compose-sql.yaml up -d
```
In this configuration both a SQL Server and a Python container will be spun up. The SQL container will only be accessible to the orchestrator and not via SSMS on your local machine.   
*This compose file should not be passed into any production/development workflow*