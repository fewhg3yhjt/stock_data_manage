$taskRoot = [System.IO.Path]::GetFullPath('D:\Project\PythonProgram\stock_data_manage')
$taskOwner = New-Object System.Security.Principal.NTAccount('书非の主机\sp181')
$taskFiles = @('config\datasets\adjustment_factor.yaml','config\normalization\adjustment_factor.yaml','provider_validation\docs\2026-10-04-adjustment-factor-input-collection.md')
$taskTargets = [System.Collections.Generic.List[string]]::new()
foreach ($taskFile in $taskFiles) { $taskTargets.Add((Join-Path $taskRoot $taskFile)) }
foreach ($taskDirectory in (Get-ChildItem -LiteralPath (Join-Path $taskRoot 'provider_validation\results') -Directory -Filter 'factor-*-20261004')) {
    foreach ($taskItem in (Get-ChildItem -LiteralPath $taskDirectory.FullName -Recurse -File)) { $taskTargets.Add($taskItem.FullName) }
}
foreach ($taskItem in (Get-ChildItem -LiteralPath (Join-Path $taskRoot 'provider_validation\results') -File -Filter 'factor-*20261004*.xml')) { $taskTargets.Add($taskItem.FullName) }
foreach ($taskDirectory in @('tmp\ff','tmp\factor-path')) {
    foreach ($taskItem in (Get-ChildItem -LiteralPath (Join-Path $taskRoot $taskDirectory) -Recurse -File)) { $taskTargets.Add($taskItem.FullName) }
}
$taskFixed = 0
foreach ($taskTarget in ($taskTargets | Sort-Object -Unique)) {
    $taskResolved = [System.IO.Path]::GetFullPath($taskTarget)
    if (-not $taskResolved.StartsWith($taskRoot + '\',[System.StringComparison]::OrdinalIgnoreCase)) { throw 'task ownership target escaped workspace' }
    $taskAcl = Get-Acl -LiteralPath $taskResolved
    if ($taskAcl.Owner -ne '书非の主机\sp181') {
        $taskFileInfo = [System.IO.FileInfo]::new($taskResolved)
        $taskOwnerAcl = [System.IO.FileSystemAclExtensions]::GetAccessControl($taskFileInfo,[System.Security.AccessControl.AccessControlSections]::Owner)
        $taskOwnerAcl.SetOwner($taskOwner)
        [System.IO.FileSystemAclExtensions]::SetAccessControl($taskFileInfo,$taskOwnerAcl)
        $taskFixed++
    }
    if ((Get-Acl -LiteralPath $taskResolved).Owner -ne '书非の主机\sp181') { throw ('owner verification failed: ' + $taskResolved) }
}
$taskEvidence = Join-Path $taskRoot 'provider_validation\results\factor-original-20261004\ownership-check.json'
@{ owner = '书非の主机\sp181'; verified = $taskTargets.Count; repaired = $taskFixed; scope = 'only new factor batch artifacts and this run test fixtures; no .git or unrelated paths' } | ConvertTo-Json | Set-Content -LiteralPath $taskEvidence -Encoding utf8
Write-Output ('Normal project owner verified for ' + $taskTargets.Count + ' task files')
