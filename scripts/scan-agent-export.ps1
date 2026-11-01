[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
& (Join-Path $PSScriptRoot 'verify-boundaries.ps1')
if (-not $?) { exit 1 }

$patterns = @(
  '-----BEGIN (RSA|OPENSSH|EC|PRIVATE) KEY-----',
  'AKIA[0-9A-Z]{16}',
  'ghp_[A-Za-z0-9]{36}',
  'github_pat_[A-Za-z0-9_]{20,}',
  'xox[baprs]-[A-Za-z0-9-]{10,}'
)

$hits = [System.Collections.Generic.List[string]]::new()
# Scan repository content only. Vendored dependencies and emitted build output are
# gitignored and never exported, and third-party bundles produce false positives.
$files = Get-ChildItem -LiteralPath $root -Recurse -Force -File |
  Where-Object {
    $_.FullName -notmatch '[\\/]\.git[\\/]' -and
    $_.FullName -notmatch '[\\/]node_modules[\\/]' -and
    $_.FullName -notmatch '[\\/]dist[\\/]'
  }
foreach ($pattern in $patterns) {
  $matches = $files | Select-String -Pattern $pattern -AllMatches -ErrorAction SilentlyContinue
  foreach ($match in $matches) {
    $hits.Add("Potential secret at $($match.Path):$($match.LineNumber)")
  }
}

if ($hits.Count -gt 0) {
  $hits | Sort-Object -Unique | ForEach-Object { Write-Error $_ }
  exit 1
}

Write-Output 'Export scan passed: no generic secret signatures found.'
