Param
(
    [parameter(Position = 0, Mandatory = $true)]
    [String]
    $environment
)

$ErrorActionPreference = "Stop"

Set-Location $(Split-Path $MyInvocation.MyCommand.Path)
Write-Output "`n`n#--- Initiating CloudFormation Deployment ---#"

Remove-Item -recurse -ErrorAction Ignore .aws-sam
Write-Output "`nReformatting Python code."
yapf --in-place --recursive ../lambdas lambda_layers/upload_lambda_layers.py
Write-Output "Checking python code."
flake8 ../lambdas lambda_layers/upload_lambda_layers.py

$properties = ConvertFrom-StringData (Get-Content ./$environment.properties -Raw)
$artifactsBucket = $properties.'S3ArtifactsBucketName'

Write-Output "`nVerifying if bucket $artifactsBucket exists."
aws s3 mb s3://$artifactsBucket

Write-Output "`nVerifying Lambda layers:"
python lambda_layers/upload_lambda_layers.py $artifactsBucket

Write-Output "`nBuilding CloudFormation Template"
sam build

sam deploy `
    --s3-bucket $properties.'S3ArtifactsBucketName' `
    --stack-name $properties.'StackName' `
    --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND `
    --parameter-overrides $(Get-Content "$environment.properties")

Remove-Item -recurse -ErrorAction Ignore .aws-sam 