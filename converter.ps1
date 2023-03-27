Get-ChildItem 'C:\workspace\temp\data' | ForEach-Object{
    Get-ChildItem "C:\Workspace\temp\data\$_" | ForEach-Object{
    $content = get-content $_.FullName
    $content[-1] = $content[-1] -replace ','
    $content | set-content $_.FullName
}

Get-ChildItem "C:\Workspace\temp\data\$_" | ForEach-Object{
    $content = get-content $_.FullName
    "[" + $content + "]" | set-content $_.FullName
}
}