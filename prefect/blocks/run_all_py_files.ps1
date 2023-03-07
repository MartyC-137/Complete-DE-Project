#!/usr/bin/env pwsh
foreach ($file in Get-ChildItem -Path $PWD/prefect/blocks/*.py) {
    python $file.FullName
}