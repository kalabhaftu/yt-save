$ErrorActionPreference = "Stop"

function Assert-GitHubReleaseRunner {
    $requiredEnvironment = @{
        GITHUB_ACTIONS = "true"
        RUNNER_OS      = "Windows"
        GITHUB_WORKFLOW = "Windows release"
        GITHUB_JOB     = "package"
    }
    foreach ($entry in $requiredEnvironment.GetEnumerator()) {
        $actual = [Environment]::GetEnvironmentVariable($entry.Key)
        if ($actual -ne $entry.Value) {
            throw "Installer smoke tests may run only in the Windows release workflow on GitHub-hosted runners. Expected $($entry.Key)='$($entry.Value)', got '$actual'."
        }
    }
    if ($env:GITHUB_REF -notmatch '^refs/tags/v\d') {
        throw "Installer smoke tests require a version tag push; GITHUB_REF is '$env:GITHUB_REF'."
    }
    if ([string]::IsNullOrWhiteSpace($env:RUNNER_TEMP) -or
        [string]::IsNullOrWhiteSpace($env:GITHUB_WORKSPACE) -or
        [string]::IsNullOrWhiteSpace($env:ImageOS) -or
        [string]::IsNullOrWhiteSpace($env:GITHUB_RUN_ID) -or
        [string]::IsNullOrWhiteSpace($env:GITHUB_RUN_ATTEMPT)) {
        throw "Required GitHub-hosted runner environment is missing (RUNNER_TEMP, GITHUB_WORKSPACE, ImageOS, GITHUB_RUN_ID, or GITHUB_RUN_ATTEMPT)."
    }

    $workspace = [System.IO.Path]::GetFullPath($env:GITHUB_WORKSPACE).TrimEnd('\')
    $scriptRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot "..")).TrimEnd('\')
    if (-not [string]::Equals($workspace, $scriptRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "The script root '$scriptRoot' does not match GITHUB_WORKSPACE '$workspace'; refusing to install packages from an unexpected checkout."
    }
}

function Invoke-Msi {
    param(
        [Parameter(Mandatory)][string[]]$Arguments,
        [Parameter(Mandatory)][string]$Description,
        [Parameter(Mandatory)][string]$LogPath
    )

    $allArguments = @($Arguments + @('/norestart', '/l*v', "`"$LogPath`""))
    Write-Host "$Description (msiexec $($allArguments -join ' '))"
    $process = Start-Process -FilePath "$env:WINDIR\System32\msiexec.exe" `
        -ArgumentList $allArguments -PassThru -Wait -WindowStyle Hidden
    if ($process.ExitCode -notin @(0, 3010)) {
        throw "$Description failed with Windows Installer exit code $($process.ExitCode). Diagnostic log: $LogPath"
    }
    if ($process.ExitCode -eq 3010) {
        Write-Warning "$Description completed and requested a reboot (3010); /norestart prevented an automatic restart."
    }
}

function Assert-BundledTools {
    param([Parameter(Mandatory)][string]$InstallDirectory)

    $binDirectory = Join-Path $InstallDirectory 'bin'
    foreach ($tool in @(
        @{ Name = 'yt-dlp.exe'; Arguments = @('--version') },
        @{ Name = 'deno.exe'; Arguments = @('--version') },
        @{ Name = 'ffmpeg.exe'; Arguments = @('-version') },
        @{ Name = 'ffprobe.exe'; Arguments = @('-version') }
    )) {
        $toolPath = Join-Path $binDirectory $tool.Name
        if (-not (Test-Path -LiteralPath $toolPath -PathType Leaf)) {
            throw "Installer did not place bundled tool '$($tool.Name)' at '$toolPath'."
        }
        $toolArguments = $tool.Arguments
        $toolOutput = (& $toolPath @toolArguments 2>&1 | Out-String).Trim()
        $toolExitCode = $LASTEXITCODE
        if ($toolExitCode -ne 0 -or [string]::IsNullOrWhiteSpace($toolOutput)) {
            throw "Bundled tool '$toolPath' failed its version check (exit code $toolExitCode). Output: $toolOutput"
        }
        Write-Host "Verified $($tool.Name): $($toolOutput -split "`r?`n" | Select-Object -First 1)"
    }

    $ytDlpPath = Join-Path $binDirectory 'yt-dlp.exe'
    $denoPath = Join-Path $binDirectory 'deno.exe'
    $runtimeArguments = @('--js-runtimes', "deno:$denoPath", '--version')
    $runtimeOutput = (& $ytDlpPath @runtimeArguments 2>&1 | Out-String).Trim()
    $runtimeExitCode = $LASTEXITCODE
    if ($runtimeExitCode -ne 0 -or [string]::IsNullOrWhiteSpace($runtimeOutput)) {
        throw "yt-dlp did not accept the installed Deno runtime specification 'deno:$denoPath' (exit code $runtimeExitCode). Output: $runtimeOutput"
    }
    Write-Host "Verified yt-dlp accepted --js-runtimes deno:<installed path> (CLI/path validation only; no YouTube download attempted)."
}

if (-not ('YtSaveInstallerSmokeWindowProbe' -as [type])) {
    Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class YtSaveInstallerSmokeWindowProbe
{
    [DllImport("user32.dll")]
    public static extern bool IsWindowVisible(IntPtr hWnd);
}
'@
}

function Start-AndVerifyApplication {
    param(
        [Parameter(Mandatory)][string]$InstallDirectory,
        [Parameter(Mandatory)][string]$ExecutableName,
        [Parameter(Mandatory)][string]$ProductName
    )

    $executable = Join-Path $InstallDirectory $ExecutableName
    if (-not (Test-Path -LiteralPath $executable -PathType Leaf)) {
        throw "Installed application executable is missing: $executable"
    }

    $application = Start-Process -FilePath $executable -WorkingDirectory $InstallDirectory -PassThru
    try {
        $deadline = [DateTime]::UtcNow.AddSeconds(30)
        $windowFound = $false
        while ([DateTime]::UtcNow -lt $deadline) {
            $application.Refresh()
            if ($application.HasExited) {
                throw "Installed application exited before presenting its main window (exit code $($application.ExitCode)): $executable"
            }
            $windowHandle = $application.MainWindowHandle
            $windowTitle = $application.MainWindowTitle
            if ($windowHandle -ne [IntPtr]::Zero -and
                [YtSaveInstallerSmokeWindowProbe]::IsWindowVisible($windowHandle) -and
                $windowTitle -like "*$ProductName*") {
                $windowFound = $true
                Write-Host "Confirmed visible application window '$windowTitle' (PID $($application.Id))."
                break
            }
            Start-Sleep -Milliseconds 500
        }
        if (-not $windowFound) {
            $application.Refresh()
            throw "Application process $($application.Id) did not show a visible '$ProductName' window within 30 seconds. MainWindowTitle='$($application.MainWindowTitle)'."
        }
        return $application
    } catch {
        try { Close-Application $application } catch { Write-Warning "Could not close failed smoke-test process: $($_.Exception.Message)" }
        throw
    }
}

function Close-Application {
    param([System.Diagnostics.Process]$Application)
    if (-not $Application) { return }
    try {
        $Application.Refresh()
        if (-not $Application.HasExited) {
            if ($Application.MainWindowHandle -ne [IntPtr]::Zero) {
                [void]$Application.CloseMainWindow()
                if (-not $Application.WaitForExit(15000)) {
                    Write-Warning "Application PID $($Application.Id) did not close normally; stopping this smoke-test process."
                    $Application.Kill()
                    [void]$Application.WaitForExit(10000)
                }
            } else {
                $Application.Kill()
                [void]$Application.WaitForExit(10000)
            }
        }
    } finally {
        $Application.Dispose()
    }
}

function Assert-InstallRemoved {
    param([Parameter(Mandatory)][string]$InstallDirectory)

    if (Test-Path -LiteralPath $InstallDirectory) {
        $remaining = Get-ChildItem -LiteralPath $InstallDirectory -Force -ErrorAction SilentlyContinue |
            Select-Object -ExpandProperty Name -First 10
        throw "Uninstallation left its isolated install directory behind: '$InstallDirectory'. Remaining entries: $($remaining -join ', ')."
    }
    Write-Host "Confirmed installation removed: $InstallDirectory"
}

function Test-MsiInstaller {
    param(
        [Parameter(Mandatory)][string]$MsiPath,
        [Parameter(Mandatory)][string]$InstallDirectory,
        [Parameter(Mandatory)][string]$ExecutableName,
        [Parameter(Mandatory)][string]$ProductName,
        [Parameter(Mandatory)][string]$LogDirectory
    )

    $application = $null
    $installationAttempted = $false
    $installationRemoved = $false
    try {
        $installationAttempted = $true
        Invoke-Msi -Arguments @('/i', "`"$MsiPath`"", '/qn', "INSTALLFOLDER=`"$InstallDirectory`"") `
            -Description "Install MSI to isolated runner path '$InstallDirectory'" `
            -LogPath (Join-Path $LogDirectory 'msi-install.log')
        Assert-BundledTools -InstallDirectory $InstallDirectory
        $application = Start-AndVerifyApplication -InstallDirectory $InstallDirectory `
            -ExecutableName $ExecutableName -ProductName $ProductName
        Close-Application $application
        $application = $null

        Invoke-Msi -Arguments @('/x', "`"$MsiPath`"", '/qn') `
            -Description "Uninstall MSI from '$InstallDirectory'" `
            -LogPath (Join-Path $LogDirectory 'msi-uninstall.log')
        $installationRemoved = $true
        Assert-InstallRemoved -InstallDirectory $InstallDirectory
    } finally {
        Close-Application $application
        if ($installationAttempted -and -not $installationRemoved -and (Test-Path -LiteralPath $MsiPath -PathType Leaf)) {
            try {
                Invoke-Msi -Arguments @('/x', "`"$MsiPath`"", '/qn') `
                    -Description "Best-effort MSI cleanup after smoke-test failure" `
                    -LogPath (Join-Path $LogDirectory 'msi-cleanup.log')
            } catch {
                Write-Warning "MSI cleanup failed: $($_.Exception.Message)"
            }
        }
    }
}

function Test-InnoInstaller {
    param(
        [Parameter(Mandatory)][string]$SetupPath,
        [Parameter(Mandatory)][string]$InstallDirectory,
        [Parameter(Mandatory)][string]$ExecutableName,
        [Parameter(Mandatory)][string]$ProductName,
        [Parameter(Mandatory)][string]$LogDirectory
    )

    $application = $null
    $installationAttempted = $false
    $installationRemoved = $false
    try {
        $installationAttempted = $true
        $setupLog = Join-Path $LogDirectory 'inno-install.log'
        $installer = Start-Process -FilePath $SetupPath -ArgumentList @(
            '/VERYSILENT', '/SUPPRESSMSGBOXES', '/NORESTART', '/SP-', "/DIR=`"$InstallDirectory`"", "/LOG=`"$setupLog`""
        ) -PassThru -Wait -WindowStyle Hidden
        if ($installer.ExitCode -ne 0) {
            throw "Inno Setup failed with exit code $($installer.ExitCode). Diagnostic log: $setupLog"
        }

        Assert-BundledTools -InstallDirectory $InstallDirectory
        $application = Start-AndVerifyApplication -InstallDirectory $InstallDirectory `
            -ExecutableName $ExecutableName -ProductName $ProductName
        Close-Application $application
        $application = $null

        $uninstaller = Join-Path $InstallDirectory 'unins000.exe'
        if (-not (Test-Path -LiteralPath $uninstaller -PathType Leaf)) {
            throw "Inno Setup uninstaller is missing: $uninstaller"
        }
        $uninstallLog = Join-Path $LogDirectory 'inno-uninstall.log'
        $uninstallProcess = Start-Process -FilePath $uninstaller -ArgumentList @(
            '/VERYSILENT', '/SUPPRESSMSGBOXES', '/NORESTART', '/SP-', "/LOG=`"$uninstallLog`""
        ) -PassThru -Wait -WindowStyle Hidden
        if ($uninstallProcess.ExitCode -ne 0) {
            throw "Inno Setup uninstaller failed with exit code $($uninstallProcess.ExitCode). Diagnostic log: $uninstallLog"
        }
        $installationRemoved = $true
        Assert-InstallRemoved -InstallDirectory $InstallDirectory
    } finally {
        Close-Application $application
        if ($installationAttempted -and -not $installationRemoved) {
            $uninstaller = Join-Path $InstallDirectory 'unins000.exe'
            if (Test-Path -LiteralPath $uninstaller -PathType Leaf) {
                try {
                    $cleanupLog = Join-Path $LogDirectory 'inno-cleanup.log'
                    $cleanup = Start-Process -FilePath $uninstaller -ArgumentList @(
                        '/VERYSILENT', '/SUPPRESSMSGBOXES', '/NORESTART', '/SP-', "/LOG=`"$cleanupLog`""
                    ) -PassThru -Wait -WindowStyle Hidden
                    if ($cleanup.ExitCode -ne 0) { Write-Warning "Inno cleanup returned exit code $($cleanup.ExitCode). Log: $cleanupLog" }
                } catch {
                    Write-Warning "Inno cleanup failed: $($_.Exception.Message)"
                }
            }
        }
    }
}

Assert-GitHubReleaseRunner
$root = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$branding = Get-Content -Raw -LiteralPath (Join-Path $root 'branding.json') | ConvertFrom-Json
$installerPath = [System.IO.Path]::GetFullPath((Join-Path $root "dist\$($branding.version)"))
$msiPath = Join-Path $installerPath "$($branding.package_name)-$($branding.version)-x64.msi"
$setupPath = Join-Path $installerPath "$($branding.package_name)-$($branding.version)-x64-setup.exe"
foreach ($path in @($msiPath, $setupPath)) {
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        throw "Expected release installer is missing: $path. Run scripts/build_windows_package.ps1 first."
    }
}

$runDirectory = Join-Path $env:RUNNER_TEMP "yt-save-installer-smoke-$($env:GITHUB_RUN_ID)-$($env:GITHUB_RUN_ATTEMPT)"
$logDirectory = Join-Path $runDirectory 'logs'
$msiInstallDirectory = Join-Path $runDirectory 'msi-install'
$innoInstallDirectory = Join-Path $runDirectory 'inno-install'
if (Test-Path -LiteralPath $runDirectory) {
    throw "Smoke-test install path already exists under RUNNER_TEMP; refusing to overwrite: $runDirectory"
}
New-Item -ItemType Directory -Force -Path $logDirectory | Out-Null

Write-Host "Testing MSI: $msiPath"
Test-MsiInstaller -MsiPath $msiPath -InstallDirectory $msiInstallDirectory `
    -ExecutableName $branding.executable_name -ProductName $branding.product_name -LogDirectory $logDirectory

Write-Host "Testing Inno Setup EXE: $setupPath"
Test-InnoInstaller -SetupPath $setupPath -InstallDirectory $innoInstallDirectory `
    -ExecutableName $branding.executable_name -ProductName $branding.product_name -LogDirectory $logDirectory

Write-Host "Both installer formats installed, launched with a visible application window, and uninstalled successfully. Logs: $logDirectory"
