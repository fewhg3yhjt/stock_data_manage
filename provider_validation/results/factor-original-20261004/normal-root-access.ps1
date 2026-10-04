$ErrorActionPreference = 'Stop'
$taskRoot = [System.IO.Path]::GetFullPath('D:\Project\PythonProgram\stock_data_manage')
foreach ($taskRelative in @('provider_validation\results\factor-fulltests-20261004','provider_validation\results\factor-tests-dev-20261004','provider_validation\results\factor-tests-dev2-20261004','tmp\ff','tmp\factor-path')) {
    $taskResolved = [System.IO.Path]::GetFullPath((Join-Path $taskRoot $taskRelative))
    if (-not $taskResolved.StartsWith($taskRoot + '\',[System.StringComparison]::OrdinalIgnoreCase)) { throw 'target escaped workspace' }
    if ((Get-Acl -LiteralPath $taskResolved).Owner -ne '书非の主机\sp181') { throw 'root ownership repair required first' }
    $taskDirectory = [System.IO.DirectoryInfo]::new($taskResolved)
    $taskAcl = [System.IO.FileSystemAclExtensions]::GetAccessControl($taskDirectory,[System.Security.AccessControl.AccessControlSections]::Access)
    $taskAccess = New-Object System.Security.AccessControl.FileSystemAccessRule('书非の主机\CodexSandboxOffline','FullControl','ContainerInherit,ObjectInherit','None','Allow')
    $taskAcl.AddAccessRule($taskAccess)
    [System.IO.FileSystemAclExtensions]::SetAccessControl($taskDirectory,$taskAcl)
}
