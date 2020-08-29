$bucket = "phsp-dlg-datalake-raw-dev"

$files = Get-ChildItem . -Filter *.csv | Select-Object -Expand FullName

foreach($file in $files){
    Write-Output $file
    aws s3 cp $file s3://$bucket/csv_to_analytics/weather/ --metadata partition-cols=ObservationDate
}