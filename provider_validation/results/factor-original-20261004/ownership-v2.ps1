$ErrorActionPreference = 'Stop'
$taskRoot = [System.IO.Path]::GetFullPath('D:\Project\PythonProgram\stock_data_manage')
$taskOwner = New-Object System.Security.Principal.NTAccount('书非の主机\sp181')
# Python 3.13 creates pytest base directories with an owner-only ACL.
# Only these newly generated test roots need normal project-user access.
foreach ($taskRelative in @('provider_validation\results\factor-fulltests-20261004','provider_validation\results\factor-tests-dev-20261004','provider_validation\results\factor-tests-dev2-20261004','tmp\ff','tmp\factor-path')) {
    $taskResolved = [System.IO.Path]::GetFullPath((Join-Path $taskRoot $taskRelative))
    if (-not $taskResolved.StartsWith($taskRoot + '\',[System.StringComparison]::OrdinalIgnoreCase)) { throw 'ownership target escaped workspace' }
    $taskDirectory = [System.IO.DirectoryInfo]::new($taskResolved)
    $taskAcl = [System.IO.FileSystemAclExtensions]::GetAccessControl($taskDirectory,[System.Security.AccessControl.AccessControlSections]::Owner)
    $taskAcl.SetOwner($taskOwner)
    [System.IO.FileSystemAclExtensions]::SetAccessControl($taskDirectory,$taskAcl)
    if ((Get-Acl -LiteralPath $taskResolved).Owner -ne '书非の主机\sp181') { throw 'pytest root owner check failed' }
}
& (Join-Path $taskRoot 'provider_validation\results\factor-original-20261004\ownership.ps1')
if ($LASTEXITCODE) { throw 'ownership verification did not finish' }
