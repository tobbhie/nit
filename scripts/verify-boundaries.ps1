[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot

$forbiddenPaths = @(
  'private',
  'EDS',
  'TIS',
  'config/production',
  'docs/private'
)

$violations = [System.Collections.Generic.List[string]]::new()
foreach ($relativePath in $forbiddenPaths) {
  if (Test-Path -LiteralPath (Join-Path $root $relativePath)) {
    $violations.Add("Forbidden path present: $relativePath")
  }
}

# Scan repository content only, matching scan-agent-export.ps1. Vendored
# dependencies and emitted build output are gitignored and never exported, and a
# dependency shipping a test .key fixture is not a boundary violation.
#
# A root .env and .env.example are allowed. Both are gitignored (see .gitignore),
# and the root .env is the intended home for the NIT-specific burner development
# credentials this workspace needs — NIT_TEST_DATABASE_URL among them. What stays
# forbidden is an .env anywhere other than the root, and any other .env.<name> at
# the root: a file called .env.production names a credential set that does not
# belong in this workspace at all.
$allowedEnvAtRoot = @('.env', '.env.example')
$forbiddenFiles = Get-ChildItem -LiteralPath $root -Recurse -Force -File |
  Where-Object {
    $_.FullName -notmatch '[\\/]\.git[\\/]' -and
    $_.FullName -notmatch '[\\/]node_modules[\\/]' -and
    $_.FullName -notmatch '[\\/]dist[\\/]'
  } |
  Where-Object {
    ($_.Name -match '^\.env($|\.)' -and
      -not ($_.DirectoryName -eq $root -and $allowedEnvAtRoot -contains $_.Name)) -or
    $_.Extension -in @('.pem', '.key', '.p12')
  }
foreach ($file in $forbiddenFiles) {
  $violations.Add("Forbidden file present: $($file.FullName)")
}

if ($violations.Count -gt 0) {
  # -ErrorAction Continue is required: $ErrorActionPreference is 'Stop', so a bare
  # Write-Error terminates on the first violation and the operator has to re-run
  # the check once per problem to discover them all.
  foreach ($violation in $violations) {
    Write-Error $violation -ErrorAction Continue
  }
  exit 1
}

Write-Output 'Boundary check passed: no forbidden paths or files found.'
