$BasePath = "C:\Users\no1pr\Documents\000_Code\awss3_snowflake_powerbi\csv_export\AdventureWorks_CSV_SAFE"

$dimPath    = "$BasePath\dim"
$factPath   = "$BasePath\fact"
$lookupPath = "$BasePath\lookup"
$otherPath  = "$BasePath\other"

New-Item -ItemType Directory -Force -Path $dimPath, $factPath, $lookupPath, $otherPath | Out-Null

Get-ChildItem $BasePath -Filter "*.csv" | ForEach-Object {

    $name = $_.BaseName.ToLower()

    if ($name -match "detail|history|transaction|order|sales|purchase") {
        Move-Item $_.FullName $factPath
    }
    elseif ($name -match "type|status|category|reason|method|currency") {
        Move-Item $_.FullName $lookupPath
    }
    else {
        Move-Item $_.FullName $dimPath
    }
}
