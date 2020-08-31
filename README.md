# DLG Python Test

This is a python test project! For the complete documentation, please download the XXXXXXXXX.

## Overview

### Main Goal

Convert some weather*.csv files to parquet files, and answer some questions.

### Proposed Solution

Use a **Cloud-based architecture**, with AWS, to store the CSV files, process, and finally store the outputed Parquet files in a new location. The processed file will be available to be queried as a table, using AWS Athena. The end-user will receive a message stating if the process was succeeded or not.

**Our goal is to make a generic process, so it can be independent of the inputted CSV file.**

## Architecture

![Project Architecture](/docs/images/architecture.jpg)

* **Infrastructure as Code** (IaC) -> Version Control the Infrastructure and easily recover from disasters.
* **Serverless** architecture -> Scalable and Fault tolerant.
* **Data Lake** structure -> Enables advanced analytics.
* **Glue Data Catalog** -> Centralizes the data catalog and, combined with others services, easily provides SQL access to the files.

## Using this project

### Requirements

These are the requirements for this project:

* Python 3.6;
* AWS SAM CLI;
* AWS CLI;
* AWS IAM User with permission to access AWS via CLI.

Install these programs and add them to the PATH, then run:
> cd PATH\TO\THE\PROJECT
> python -m pip install -r requirements.txt

### How to deploy the project to AWS

1. Meet the requirements specified on session above.
2. Change the parameter values for *.properties files that are inside the infra directory. You **must** change at least every bucket name.  
   *Remember: A S3 bucket name must be globally unique!*
3. Run the commands below, specifying the environment you want to deploy. The environment value must be the name of a .properties file.  
   > cd PATH\TO\THE\PROJECT
   > .\infra\deploy.ps1 *environment*

*Et Voilà!* Your AWS infrastructure was created!

### Quick Start

*Before beginning*: if you want to receive the messages published to AWS SNS, do not forget to subscribe your e-mail to the SNS Topic.

Open the "test-data" project directory. Each subdirectory represents a group of csv files that we can load into AWS. Let's name this subdirectory as "Data description directory". Choose one of these subdirectories and get into it.
Now, by running the commands below, every CSV file inside "incoming_data" directory is uploaded to Amazon S3 and later archived into the "archived_data" directory.
> cd PATH\TO\THE\PROJECT\INTO\DATA_DESC_DIR
> .\upload_data_to_s3.ps1 *environment*

**Note:** if you specify *dev* environment, then every single archived file is moved to the *incoming_data* directory, before the upload begins.

Each file uploaded to Amazon S3 triggers a sequence of events:

1. The S3 Raw Bucket generates a S3 Event Notification with information of the file that was uploaded.
2. A Lambda function is triggered by the S3 Event Notification and starts execution. The following is performed:
   * Columns are casted to the desired data type.
   * Is applied uppercase to every string value. *Except on ETL metadata columns*;
   * Metadata columns with info about the ETL process are added to the output;
   * Column's names are normalized to lowercase and words are split by underscore;
   * The output file is saved into an S3 Analytics Bucket as Parquet. The output file is compressed with Snappy compression and can be partitioned. *If the partition already exists, it is then overwritten*;
   * A table is defined into *analytics_db* database into AWS Glue Data Catalog with the name tbl_*data_description_directory*. For instance, the following tree:  

        ```bash
        data_title_example
        ├───archived_data
        └───incoming_data
        ```

        will generate the table *tbl_data_title_example*;
   * An AWS SNS notification is published and every subscriber of the SNS topic will receive an E-mail stating if the load was successful or if it failed.
3. Finally, the processed file is available to be queried by every AWS user via Amazon Athena!

And the best part is: **the whole process takes seconds to conclude!**

**Note:** You can change the default data types or specify a different partitioning schema by modifying the *metadata.json* file. Just be aware that this file is composed of key:value pairs and **every value must be a string!**

## Working with your own data

Now that you know how the project works, it is easy to use your own data!

Just copy the "test-data" directory into your preffered location and rename it to whatever best suits your needs. Now clean the directory:

* You must keep the *generic_upload_data_to_s3.ps1* script;
* For each dataset, you will need a *Data description directory* with:
  * *archived_data* directory
  * *incoming_data* directory
  * *upload_data_to_s3.ps1* script
  * Optionally, the *metadata.json* file

**By the way, the answer is:**

![Answer](/docs/images/answer.jpg)

**Made with love! I hope you like it!**
