param([ValidateSet('GrantTestAccess','RepairOwner')][string]$Mode)
$ErrorActionPreference = 'Stop'
$taskRoot = [System.IO.Path]::GetFullPath('D:\Project\PythonProgram\stock_data_manage')
$taskOwnerName = '书非の主机\sp181'
$taskSandboxName = '书非の主机\CodexSandboxOffline'
$taskTestRoots = @('tmp\ad1','tmp\ad2','tmp\ad3','tmp\ad4','tmp\adf1')
function Check-TaskPath([string]$TaskPath) {
    $taskAbsolute = [System.IO.Path]::GetFullPath($TaskPath)
    if (-not $taskAbsolute.StartsWith($taskRoot+'\',[System.StringComparison]::OrdinalIgnoreCase)) { throw 'task target escaped workspace' }
    return $taskAbsolute
}
if ($Mode -eq 'GrantTestAccess') {
    foreach ($taskRelative in $taskTestRoots) {
        $taskResolved = Check-TaskPath (Join-Path $taskRoot $taskRelative)
        $taskDirectories = @($taskResolved) + @((Get-ChildItem -LiteralPath $taskResolved -Recurse -Directory | Where-Object { -not ($_.Attributes -band [System.IO.FileAttributes]::ReparsePoint) }).FullName)
        foreach ($taskChild in $taskDirectories) {
            $taskDirectory = [System.IO.DirectoryInfo]::new((Check-TaskPath $taskChild))
            $taskAcl = [System.IO.FileSystemAclExtensions]::GetAccessControl($taskDirectory,[System.Security.AccessControl.AccessControlSections]::Access)
            if ($taskAcl.AreAccessRulesProtected -and (Get-Acl -LiteralPath $taskChild).Owner -eq $taskSandboxName) {
                foreach ($taskPrincipal in @($taskOwnerName,$taskSandboxName)) {
                    $taskAcl.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($taskPrincipal,'FullControl','ContainerInherit,ObjectInherit','None','Allow')))
                }
                [System.IO.FileSystemAclExtensions]::SetAccessControl($taskDirectory,$taskAcl)
            }
        }
    }
    Write-Output 'Granted normal project user and sandbox access only to current actual-data-input private pytest directories'
    exit
}
$taskOwner = New-Object System.Security.Principal.NTAccount($taskOwnerName)
$taskTargets = [System.Collections.Generic.List[string]]::new()
$taskFiles = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'task-files.json') -Raw | ConvertFrom-Json
foreach ($taskFile in $taskFiles) { $taskTargets.Add((Join-Path $taskRoot $taskFile)) }
$taskRoots = @((Get-ChildItem -LiteralPath (Join-Path $taskRoot 'provider_validation\results') -Directory -Filter 'actual-data-*-20261004').FullName)
$taskRoots += @($taskTestRoots | ForEach-Object { Join-Path $taskRoot $_ })
foreach ($taskDirectory in $taskRoots) {
    $taskTargets.Add($taskDirectory)
    foreach ($taskItem in (Get-ChildItem -LiteralPath $taskDirectory -Recurse | Where-Object { -not ($_.Attributes -band [System.IO.FileAttributes]::ReparsePoint) })) { $taskTargets.Add($taskItem.FullName) }
}
foreach ($taskItem in (Get-ChildItem -LiteralPath (Join-Path $taskRoot 'provider_validation\results') -File | Where-Object { $_.Name -like 'actual-data-*20261004*.xml' -or $_.Name -like 'actual-data-*20261004*.log' })) { $taskTargets.Add($taskItem.FullName) }
$taskFixed = 0
foreach ($taskTarget in ($taskTargets | Sort-Object -Unique)) {
    $taskResolved = Check-TaskPath $taskTarget
    if ((Get-Acl -LiteralPath $taskResolved).Owner -ne $taskOwnerName) {
        $taskInfo = if ([System.IO.Directory]::Exists($taskResolved)) { [System.IO.DirectoryInfo]::new($taskResolved) } else { [System.IO.FileInfo]::new($taskResolved) }
        $taskAcl = [System.IO.FileSystemAclExtensions]::GetAccessControl($taskInfo,[System.Security.AccessControl.AccessControlSections]::Owner)
        $taskAcl.SetOwner($taskOwner)
        [System.IO.FileSystemAclExtensions]::SetAccessControl($taskInfo,$taskAcl)
        $taskFixed++
    }
    if ((Get-Acl -LiteralPath $taskResolved).Owner -ne $taskOwnerName) { throw ('owner verification failed: '+$taskResolved) }
}
$taskEvidence = Join-Path $PSScriptRoot 'ownership-check.json'
@{ owner=$taskOwnerName; verified=$taskTargets.Count; repaired=$taskFixed; scope='only current actual data inputs source/artifacts and exact test roots; no unrelated paths or .git' } | ConvertTo-Json | Set-Content -LiteralPath $taskEvidence -Encoding utf8
Write-Output ('Normal project owner verified for '+$taskTargets.Count+' task paths')
