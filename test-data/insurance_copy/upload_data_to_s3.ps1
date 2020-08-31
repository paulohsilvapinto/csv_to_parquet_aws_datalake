Param
(
    [parameter(Position = 0, Mandatory = $true)]
    [String]
    $environment
)

$ErrorActionPreference = "Stop"

$current_location = $(Split-Path $MyInvocation.MyCommand.Path)
Set-Location $current_location

$parentFolderName = Split-Path (Split-Path $MyInvocation.MyCommand.Path -Parent) -Leaf
../generic_upload_data_to_s3.ps1 $parentFolderName $environment

Set-Location $current_location