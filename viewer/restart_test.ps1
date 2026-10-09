# restart_test.ps1 — restart viewer against the test DB
$env:MART_DB = "E:\SEC_projects\sec-data-core\test_db\test_annual.db"
& "$PSScriptRoot\restart.ps1"
