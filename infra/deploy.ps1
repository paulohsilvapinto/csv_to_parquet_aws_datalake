Param
(
    [parameter(Position = 0, Mandatory = $true)]
    [String]
    $environment
)

$ErrorActionPreference = "Stop"


Remove-Item -recurse -ErrorAction Ignore .aws-sam
yapf --in-place --recursive ../lambdas
flake8 ../lambdas

aws s3 mb s3://phsp-dlg-artifacts-$environment

sam build

sam deploy `
    --s3-bucket phsp-dlg-artifacts-$environment `
    --stack-name phsp-dlg-datalake-$environment `
    --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND `
    --parameter-overrides $(Get-Content "$environment.properties")

Remove-Item -recurse -ErrorAction Ignore .aws-sam 