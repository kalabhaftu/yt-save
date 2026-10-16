[CmdletBinding()]
param(
    [ValidateSet("debug", "release")]
    [string]$Configuration = "release",
    [string]$OutputDirectory
)

$ErrorActionPreference = "Stop"
$root = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$branding = Get-Content -Raw -LiteralPath (Join-Path $root "branding.json") | ConvertFrom-Json
$targetDirectory = [System.IO.Path]::GetFullPath((Join-Path $root "target"))
$stageDirectory = [System.IO.Path]::GetFullPath((Join-Path $targetDirectory "package"))
$stageMarkerPath = Join-Path $targetDirectory ".yt-save-package-stage"
$stageMarkerContents = "Yt Save package staging directory v1"
$binaryDirectory = Join-Path $stageDirectory "bin"
if ([string]::IsNullOrWhiteSpace($OutputDirectory)) {
    $OutputDirectory = Join-Path $root "dist\$($branding.version)"
}
$outputPath = [System.IO.Path]::GetFullPath($OutputDirectory)
$targetPrefix = $targetDirectory.TrimEnd([System.IO.Path]::DirectorySeparatorChar) + [System.IO.Path]::DirectorySeparatorChar
$rootPrefix = $root.TrimEnd([System.IO.Path]::DirectorySeparatorChar) + [System.IO.Path]::DirectorySeparatorChar

if (-not $stageDirectory.StartsWith($targetPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Package staging path must remain below target: $stageDirectory"
}
if (-not $outputPath.StartsWith($rootPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Installer output must remain inside the repository: $outputPath"
}

if (Test-Path -LiteralPath $stageDirectory) {
    if (Test-Path -LiteralPath $stageMarkerPath -PathType Leaf) {
        $marker = Get-Content -Raw -LiteralPath $stageMarkerPath
        if ($marker.Trim() -ne $stageMarkerContents) {
            throw "Package staging marker is invalid; refusing to clear $stageDirectory."
        }
    } elseif (Get-ChildItem -LiteralPath $stageDirectory -Force | Select-Object -First 1) {
        throw "Package staging directory has content but no Yt Save marker; refusing to clear $stageDirectory."
    }
} else {
    New-Item -ItemType Directory -Force -Path $stageDirectory | Out-Null
}

$localWix = Join-Path $targetDirectory "tooling\wix.exe"
$wix = if (Test-Path -LiteralPath $localWix -PathType Leaf) {
    Get-Command -Name $localWix
} else {
    Get-Command wix.exe -ErrorAction SilentlyContinue
}
if (-not $wix) { $wix = Get-Command wix -ErrorAction SilentlyContinue }
if (-not $wix) { throw "WiX Toolset 5.0.2 is required. Install it with 'dotnet tool install --tool-path target\tooling wix --version 5.0.2'." }

$isccCandidates = @(
    (Get-Command ISCC.exe -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source -ErrorAction SilentlyContinue),
    "${env:ProgramFiles(x86)}\Inno Setup 6\ISCC.exe",
    "$env:ProgramFiles\Inno Setup 6\ISCC.exe",
    "$env:LOCALAPPDATA\Programs\Inno Setup 6\ISCC.exe"
) | Where-Object { $_ }
$isccPath = $isccCandidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
if (-not $isccPath) { throw "Inno Setup 6 is required. Install it from https://jrsoftware.org/isinfo.php." }

$cargoArgs = @("build", "--package", $branding.package_name)
$binaryDirectoryName = if ($Configuration -eq "release") { "release" } else { "debug" }
if ($Configuration -eq "release") { $cargoArgs += "--release" }
$exePath = Join-Path $targetDirectory "$binaryDirectoryName\$($branding.executable_name)"

New-Item -ItemType Directory -Force -Path $stageDirectory, $binaryDirectory, $outputPath | Out-Null
Get-ChildItem -LiteralPath $stageDirectory -Force |
    Remove-Item -Recurse -Force
Set-Content -LiteralPath $stageMarkerPath -Value $stageMarkerContents -Encoding ascii
$msiPath = Join-Path $outputPath "$($branding.package_name)-$($branding.version)-x64.msi"
$setupPath = Join-Path $outputPath "$($branding.package_name)-$($branding.version)-x64-setup.exe"
$checksumPath = Join-Path $outputPath "SHA256SUMS.txt"
Remove-Item -LiteralPath $msiPath, $setupPath, $checksumPath -Force -ErrorAction SilentlyContinue

Push-Location $root
try {
    & cargo @cargoArgs
    if ($LASTEXITCODE -ne 0) { throw "Cargo build failed with exit code $LASTEXITCODE." }

    & python (Join-Path $root "scripts\fetch_dependencies.py") --output $binaryDirectory
    if ($LASTEXITCODE -ne 0) { throw "Verified dependency acquisition failed with exit code $LASTEXITCODE." }

    Copy-Item -LiteralPath $exePath -Destination (Join-Path $stageDirectory $branding.executable_name)
    Copy-Item -LiteralPath (Join-Path $root "LICENSE") -Destination $stageDirectory
    Copy-Item -LiteralPath (Join-Path $root "THIRD_PARTY_NOTICES.md") -Destination $stageDirectory
    foreach ($required in @("yt-dlp.exe", "deno.exe", "ffmpeg.exe", "ffprobe.exe")) {
        if (-not (Test-Path -LiteralPath (Join-Path $binaryDirectory $required) -PathType Leaf)) {
            throw "The staged runtime is missing $required."
        }
    }

    $versionOutput = & (Join-Path $binaryDirectory "yt-dlp.exe") --version
    if ($LASTEXITCODE -ne 0 -or -not $versionOutput) { throw "The staged yt-dlp executable did not pass its version check." }
    $denoOutput = & (Join-Path $binaryDirectory "deno.exe") --version
    if ($LASTEXITCODE -ne 0 -or -not $denoOutput) { throw "The staged Deno executable did not pass its version check." }
    $ffmpegOutput = & (Join-Path $binaryDirectory "ffmpeg.exe") -version
    if ($LASTEXITCODE -ne 0 -or -not $ffmpegOutput) { throw "The staged FFmpeg executable did not pass its version check." }
    $ffprobeOutput = & (Join-Path $binaryDirectory "ffprobe.exe") -version
    if ($LASTEXITCODE -ne 0 -or -not $ffprobeOutput) { throw "The staged FFprobe executable did not pass its version check." }

    & $wix.Source build (Join-Path $root "packaging\wix\YtSave.wxs") -arch x64 `
        -d "ProductName=$($branding.product_name)" `
        -d "ProductVersion=$($branding.version)" `
        -d "Publisher=$($branding.publisher)" `
        -d "BundleIdentifier=$($branding.bundle_identifier)" `
        -d "ExecutableName=$($branding.executable_name)" `
        -d "UpgradeCode=$($branding.upgrade_code)" `
        -d "PackageName=$($branding.package_name)" `
        -d "StageDir=$stageDirectory" `
        -d "ProjectDir=$root" `
        -o $msiPath
    if ($LASTEXITCODE -ne 0) { throw "WiX failed with exit code $LASTEXITCODE." }

    & $isccPath "/DProductName=$($branding.product_name)" `
        "/DProductVersion=$($branding.version)" `
        "/DPublisher=$($branding.publisher)" `
        "/DBundleIdentifier=$($branding.bundle_identifier)" `
        "/DExecutableName=$($branding.executable_name)" `
        "/DPackageName=$($branding.package_name)" `
        "/DUpgradeCode=$($branding.upgrade_code)" `
        "/DStageDir=$stageDirectory" `
        "/DOutputDir=$outputPath" `
        (Join-Path $root "packaging\inno\YtSave.iss")
    if ($LASTEXITCODE -ne 0) { throw "Inno Setup failed with exit code $LASTEXITCODE." }

    if (-not (Test-Path -LiteralPath $msiPath) -or -not (Test-Path -LiteralPath $setupPath)) {
        throw "The packaging tools exited successfully but one or more installer files are missing."
    }
    Get-FileHash -Algorithm SHA256 -LiteralPath $msiPath, $setupPath |
        ForEach-Object { "{0}  {1}" -f $_.Hash.ToLowerInvariant(), [System.IO.Path]::GetFileName($_.Path) } |
        Set-Content -Encoding ascii -LiteralPath $checksumPath
    Get-Item -LiteralPath $msiPath, $setupPath, $checksumPath | Select-Object FullName, Length
}
finally {
    Pop-Location
}
