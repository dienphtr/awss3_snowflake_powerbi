$Server   = "localhost"
$Database = "AdventureWorks2025"
$BasePath = "C:\Temp\AdventureWorks_CSV_SAFE"

New-Item -ItemType Directory -Force -Path $BasePath | Out-Null

$tables = Invoke-Sqlcmd -ServerInstance $Server -Database $Database -Query "
SELECT s.name AS SchemaName, t.name AS TableName
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
ORDER BY s.name, t.name
"

foreach ($t in $tables) {

    $schema = $t.SchemaName
    $table  = $t.TableName

    try {
        # Build safe SELECT
        $cols = Invoke-Sqlcmd -ServerInstance $Server -Database $Database -Query "
        SELECT
            c.name AS ColumnName,
            ty.name AS TypeName
        FROM sys.columns c
        JOIN sys.types ty ON c.user_type_id = ty.user_type_id
        WHERE c.object_id = OBJECT_ID('$schema.$table')
        ORDER BY c.column_id
        "

        $selectCols = @()

        foreach ($c in $cols) {
            if ($c.TypeName -in @(
                'hierarchyid','geometry','geography',
                'xml','sql_variant','varbinary','image','rowversion'
            )) {
                $selectCols += "CAST([$($c.ColumnName)] AS NVARCHAR(MAX)) AS [$($c.ColumnName)]"
            }
            else {
                $selectCols += "[$($c.ColumnName)]"
            }
        }

        $select = "SELECT " + ($selectCols -join ", ") + " FROM [$schema].[$table]"

        $file = "$BasePath\$schema`_$table.csv"

        Invoke-Sqlcmd -ServerInstance $Server -Database $Database `
            -Query $select -ErrorAction Stop |
            Export-Csv -Path $file -NoTypeInformation -Encoding UTF8

        Write-Host "Exported $schema.$table"
    }
    catch {
        Write-Warning "FAILED $schema.$table : $($_.Exception.Message)"
    }
}
