param([ValidateSet('GrantTestAccess','RepairOwner')][string]$Mode)
$ErrorActionPreference = 'Stop'
$taskRoot = [System.IO.Path]::GetFullPath('D:\Project\PythonProgram\stock_data_manage')
$taskOwnerName = '书非の主机\sp181'
$taskSandboxName = '书非の主机\CodexSandboxOffline'
$taskTestRoots = @('tmp\rs1','tmp\rs2','tmp\rs3','tmp\rsf')
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
    Write-Output 'Granted normal project user and sandbox access only to this batch private pytest directories'
    exit
}
$taskOwner = New-Object System.Security.Principal.NTAccount($taskOwnerName)
$taskTargets = [System.Collections.Generic.List[string]]::new()
$taskFiles = @('.gitattributes','CODE_STRUCTURE.md','PROJECT_PROGRESS.md','config\providers.yaml','tests\test_input_collection.py',
    'src\stock_data_manage\providers\eastmoney\financial.py','src\stock_data_manage\pipeline\inputs.py','src\stock_data_manage\routing\factory.py','src\stock_data_manage\providers\transport.py','src\stock_data_manage\storage\raw.py',
    'provider_validation\tests\replay_input_capabilities.py','provider_validation\docs\2026-10-04-reports-seats-input-collection.md',
    'config\datasets\eastmoney_reports.yaml','config\datasets\dragon_tiger_seats.yaml','config\normalization\eastmoney_reports.yaml','config\normalization\dragon_tiger_seats.yaml')
foreach ($taskFile in $taskFiles) { $taskTargets.Add((Join-Path $taskRoot $taskFile)) }
$taskRoots = @((Get-ChildItem -LiteralPath (Join-Path $taskRoot 'provider_validation\results') -Directory -Filter 'reports-seats-*-20261004').FullName)
$taskRoots += @($taskTestRoots | ForEach-Object { Join-Path $taskRoot $_ })
foreach ($taskDirectory in $taskRoots) {
    $taskTargets.Add($taskDirectory)
    foreach ($taskItem in (Get-ChildItem -LiteralPath $taskDirectory -Recurse | Where-Object { -not ($_.Attributes -band [System.IO.FileAttributes]::ReparsePoint) })) { $taskTargets.Add($taskItem.FullName) }
}
foreach ($taskItem in (Get-ChildItem -LiteralPath (Join-Path $taskRoot 'provider_validation\results') -File -Filter 'reports-seats-*20261004*.xml')) { $taskTargets.Add($taskItem.FullName) }
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
@{ owner=$taskOwnerName; verified=$taskTargets.Count; repaired=$taskFixed; scope='only current reports and seats source/artifacts and exact test roots; no unrelated paths or .git' } | ConvertTo-Json | Set-Content -LiteralPath $taskEvidence -Encoding utf8
Write-Output ('Normal project owner verified for '+$taskTargets.Count+' task paths')
