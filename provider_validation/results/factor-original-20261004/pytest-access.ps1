$ErrorActionPreference = 'Stop'
$taskRoot = [System.IO.Path]::GetFullPath('D:\Project\PythonProgram\stock_data_manage')
foreach ($taskRelative in @('provider_validation\results\factor-fulltests-20261004','provider_validation\results\factor-tests-dev-20261004','provider_validation\results\factor-tests-dev2-20261004','tmp\ff','tmp\factor-path')) {
    $taskResolved = [System.IO.Path]::GetFullPath((Join-Path $taskRoot $taskRelative))
    if (-not $taskResolved.StartsWith($taskRoot + '\',[System.StringComparison]::OrdinalIgnoreCase)) { throw 'target escaped workspace' }
    $taskDirectories = @($taskResolved) + @((Get-ChildItem -LiteralPath $taskResolved -Recurse -Directory).FullName)
    foreach ($taskChild in $taskDirectories) {
        if (-not $taskChild.StartsWith($taskRoot + '\',[System.StringComparison]::OrdinalIgnoreCase)) { throw 'test directory escaped workspace' }
        $taskDirectory = [System.IO.DirectoryInfo]::new($taskChild)
        $taskAcl = [System.IO.FileSystemAclExtensions]::GetAccessControl($taskDirectory,[System.Security.AccessControl.AccessControlSections]::Access)
        # Each pytest test root can also have a private, non-inherited ACL.
        if ($taskAcl.AreAccessRulesProtected -and (Get-Acl -LiteralPath $taskChild).Owner -eq '书非の主机\CodexSandboxOffline') {
            $taskAccess = New-Object System.Security.AccessControl.FileSystemAccessRule('书非の主机\sp181','FullControl','ContainerInherit,ObjectInherit','None','Allow')
            $taskAcl.AddAccessRule($taskAccess)
            $taskSandboxAccess = New-Object System.Security.AccessControl.FileSystemAccessRule('书非の主机\CodexSandboxOffline','FullControl','ContainerInherit,ObjectInherit','None','Allow')
            $taskAcl.AddAccessRule($taskSandboxAccess)
            [System.IO.FileSystemAclExtensions]::SetAccessControl($taskDirectory,$taskAcl)
        }
    }
}
Write-Output 'Granted normal project-user access only to five test roots created by this task'
