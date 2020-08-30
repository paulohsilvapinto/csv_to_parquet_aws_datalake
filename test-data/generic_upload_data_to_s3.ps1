Param
(
    [parameter(Position = 0, Mandatory = $true)]
    [String]
    $data_folder,

    [parameter(Position = 1, Mandatory = $true)]
    [String]
    $environment
    
)

$ErrorActionPreference = "Stop"

Set-Location $(Split-Path $MyInvocation.MyCommand.Path)
Write-Output "`n`nLoading $data_folder data."

Write-Output "`Creating directories if does not exists."
New-Item -ItemType Directory -Force -Path ./$data_folder/incoming_data
New-Item -ItemType Directory -Force -Path ./$data_folder/archived_data

if ($environment -eq "dev") {
    Write-Output "`nDetected development environment. Moving archived_data files to incoming_data."
    Get-ChildItem -Path ./$data_folder/archived_data -Recurse | Move-Item -Destination ./$data_folder/incoming_data
}

$properties = ConvertFrom-StringData (Get-Content ../infra/$environment.properties -Raw)
$bucket = $properties.'S3RawBucketName'

$files = Get-ChildItem ./$data_folder/incoming_data -Filter *.csv | Select-Object -Expand FullName
if ([System.IO.File]::Exists("./$data_folder/metadata.json")){
    Write-Output "`nMetadata file was founded."
    foreach($file in $files){
        Write-Output "`nLoading file $file to $bucket."
        aws s3 cp $file s3://$bucket/csv_to_analytics/$data_folder/ --metadata file://$data_folder/metadata.json
    }
} 
else 
{
    Write-Output "`nMetadata file was not founded. Proceeding without metadata."
    foreach($file in $files){
        Write-Output "`nLoading file $file to $bucket."
        aws s3 cp $file s3://$bucket/csv_to_analytics/$data_folder/
    }  
}


Write-Output "`nArchiving uploaded data."
Get-ChildItem -Path ./$data_folder/incoming_data -Recurse | Move-Item -Destination ./$data_folder/archived_data

Write-Output "`n$data_folder data was loaded successfully."