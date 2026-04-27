$files = Get-ChildItem -Path "d:\books\spark-book\md" -Filter "*.md" -Recurse
$count = 0
$totalRemoved = 0

foreach ($file in $files) {
    $lines = Get-Content $file.FullName -Encoding UTF8
    $newLines = @()
    $removed = 0
    
    foreach ($line in $lines) {
        # 检查是否包含 "命令 x.x" 格式的题注
        if ($line -match '命令\s+\d+\.\d+') {
            $removed++
            continue
        }
        $newLines += $line
    }
    
    if ($removed -gt 0) {
        $newContent = $newLines -join "`r`n"
        Set-Content -Path $file.FullName -Value $newContent -Encoding UTF8
        $count++
        $totalRemoved += $removed
        Write-Host "Modified: $($file.Name) - Removed $removed captions"
    }
}

Write-Host "Total modified files: $count"
Write-Host "Total removed captions: $totalRemoved"
