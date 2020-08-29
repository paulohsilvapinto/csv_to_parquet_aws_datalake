Param
(
    [parameter(Position = 0, Mandatory = $true)]
    [String]
    $environment
)

$ErrorActionPreference = "Stop"

Set-Location $(Split-Path $MyInvocation.MyCommand.Path)
Write-Output "#--- Initiating CloudFormation Deployment ---#"

Remove-Item -recurse -ErrorAction Ignore .aws-sam
Write-Output "Reformatting Python code."
yapf --in-place --recursive ../lambdas
Write-Output "Checking python code."
flake8 ../lambdas

$properties = ConvertFrom-StringData (Get-Content ./$environment.properties -Raw)
$artifactsBucket = $properties.'S3ArtifactsBucketName'

Write-Output "Verifying if bucket $artifactsBucket exists."
aws s3 mb s3://$artifactsBucket

Write-Output "Deploying CloudFormation Template"
sam build

sam deploy `
    --s3-bucket $properties.'S3ArtifactsBucketName' `
    --stack-name $properties.'StackName' `
    --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND `
    --parameter-overrides $(Get-Content "$environment.properties")

Remove-Item -recurse -ErrorAction Ignore .aws-sam 