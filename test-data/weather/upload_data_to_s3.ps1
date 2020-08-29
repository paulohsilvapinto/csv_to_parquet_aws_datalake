Param
(
    [parameter(Position = 0, Mandatory = $true)]
    [String]
    $environment
)

$ErrorActionPreference = "Stop"

Set-Location $(Split-Path $MyInvocation.MyCommand.Path)
Write-Output "Starting load of Weather data."

$properties = ConvertFrom-StringData (Get-Content ../../infra/$environment.properties -Raw)
$bucket = $properties.'S3RawBucketName'

$files = Get-ChildItem . -Filter *.csv | Select-Object -Expand FullName
foreach($file in $files){
    Write-Output "Loading file $file to $bucket."
    aws s3 cp $file s3://$bucket/csv_to_analytics/weather/ --metadata file://metadata.json
}

Write-Output "Load of Weather data is finished."