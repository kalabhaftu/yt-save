param(
    [string]$Source = (Join-Path $PSScriptRoot "..\assets\yt-save.png"),
    [string]$Output = (Join-Path $PSScriptRoot "..\assets\yt-save.ico")
)

$ErrorActionPreference = "Stop"
Add-Type -AssemblyName System.Drawing

$sourcePath = (Resolve-Path -LiteralPath $Source).Path
$outputPath = [System.IO.Path]::GetFullPath($Output)
$repoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
if (-not $outputPath.StartsWith($repoRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Icon output must remain inside the workspace: $outputPath"
}

$intermediate = Join-Path $repoRoot "target\icon-pngs"
[System.IO.Directory]::CreateDirectory($intermediate) | Out-Null
$sizes = @(16, 24, 32, 48, 64, 128, 256)
$pngFiles = @()
$sourceImage = [System.Drawing.Image]::FromFile($sourcePath)
try {
    foreach ($size in $sizes) {
        $bitmap = [System.Drawing.Bitmap]::new($size, $size, [System.Drawing.Imaging.PixelFormat]::Format32bppArgb)
        try {
            $graphics = [System.Drawing.Graphics]::FromImage($bitmap)
            try {
                $graphics.Clear([System.Drawing.Color]::Transparent)
                $graphics.CompositingQuality = [System.Drawing.Drawing2D.CompositingQuality]::HighQuality
                $graphics.InterpolationMode = [System.Drawing.Drawing2D.InterpolationMode]::HighQualityBicubic
                $graphics.PixelOffsetMode = [System.Drawing.Drawing2D.PixelOffsetMode]::HighQuality
                $graphics.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::HighQuality
                $graphics.DrawImage($sourceImage, [System.Drawing.Rectangle]::new(0, 0, $size, $size))
            }
            finally {
                $graphics.Dispose()
            }
            $pngPath = Join-Path $intermediate "$size.png"
            $bitmap.Save($pngPath, [System.Drawing.Imaging.ImageFormat]::Png)
            $pngFiles += $pngPath
        }
        finally {
            $bitmap.Dispose()
        }
    }
}
finally {
    $sourceImage.Dispose()
}

$entries = [System.Collections.Generic.List[byte[]]]::new()
foreach ($index in 0..($sizes.Count - 1)) {
    $entries.Add([System.IO.File]::ReadAllBytes($pngFiles[$index]))
}
$directoryBytes = 6 + 16 * $sizes.Count
$offset = $directoryBytes
$outputDirectory = [System.IO.Path]::GetDirectoryName($outputPath)
[System.IO.Directory]::CreateDirectory($outputDirectory) | Out-Null
$stream = [System.IO.File]::Create($outputPath)
$writer = [System.IO.BinaryWriter]::new($stream)
try {
    $writer.Write([UInt16]0)
    $writer.Write([UInt16]1)
    $writer.Write([UInt16]$sizes.Count)
    for ($index = 0; $index -lt $sizes.Count; $index++) {
        $size = $sizes[$index]
        $width = if ($size -eq 256) { [byte]0 } else { [byte]$size }
        $data = $entries[$index]
        $writer.Write($width)
        $writer.Write($width)
        $writer.Write([byte]0)
        $writer.Write([byte]0)
        $writer.Write([UInt16]1)
        $writer.Write([UInt16]32)
        $writer.Write([UInt32]$data.Length)
        $writer.Write([UInt32]$offset)
        $offset += $data.Length
    }
    foreach ($data in $entries) {
        $writer.Write($data)
    }
}
finally {
    $writer.Dispose()
}

Write-Output "Wrote transparent multi-resolution icon to $outputPath"
