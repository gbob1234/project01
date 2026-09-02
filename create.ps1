$ErrorActionPreference = 'Stop'

$baseDirectory = 'C:\File_Collector'
$startBatch = Join-Path $baseDirectory 'startProducer.bat'
$stopBatch  = Join-Path $baseDirectory 'stopProducer.bat'

# 공용 시작프로그램 폴더 사용을 위한 관리자 권한 확인
$currentIdentity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = [Security.Principal.WindowsPrincipal]::new($currentIdentity)

$isAdministrator = $principal.IsInRole(
    [Security.Principal.WindowsBuiltInRole]::Administrator
)

# 관리자가 아니면 관리자 권한으로 현재 PowerShell 스크립트 재실행
if (-not $isAdministrator) {
    $arguments = @(
        '-NoProfile'
        '-ExecutionPolicy', 'Bypass'
        '-File', "`"$PSCommandPath`""
    )

    Start-Process `
        -FilePath 'powershell.exe' `
        -ArgumentList $arguments `
        -Verb RunAs

    exit
}

# 대상 파일 확인
if (-not (Test-Path -LiteralPath $startBatch -PathType Leaf)) {
    throw "파일을 찾을 수 없습니다: $startBatch"
}

if (-not (Test-Path -LiteralPath $stopBatch -PathType Leaf)) {
    throw "파일을 찾을 수 없습니다: $stopBatch"
}

$desktopDirectory = [Environment]::GetFolderPath('Desktop')
$startupDirectory = [Environment]::GetFolderPath('CommonStartup')

$startShortcutName = 'DP_수집기_기동.lnk'
$stopShortcutName  = 'DP_수집기_정지.lnk'

$wshShell = New-Object -ComObject WScript.Shell

function New-BatchShortcut {
    param(
        [Parameter(Mandatory)]
        [string] $ShortcutPath,

        [Parameter(Mandatory)]
        [string] $TargetPath
    )

    $shortcut = $wshShell.CreateShortcut($ShortcutPath)

    $shortcut.TargetPath = $TargetPath
    $shortcut.WorkingDirectory = $baseDirectory
    $shortcut.WindowStyle = 1

    $shortcut.Save()

    Write-Host "생성 완료: $ShortcutPath" -ForegroundColor Green
}

# 바탕화면 바로가기
New-BatchShortcut `
    -ShortcutPath (Join-Path $desktopDirectory $startShortcutName) `
    -TargetPath $startBatch

New-BatchShortcut `
    -ShortcutPath (Join-Path $desktopDirectory $stopShortcutName) `
    -TargetPath $stopBatch

# 모든 사용자 공용 시작프로그램 바로가기
New-BatchShortcut `
    -ShortcutPath (Join-Path $startupDirectory $startShortcutName) `
    -TargetPath $startBatch

Write-Host ''
Write-Host '모든 바로가기가 생성되었습니다.' -ForegroundColor Cyan
Write-Host "바탕화면: $desktopDirectory"
Write-Host "시작프로그램: $startupDirectory"

Read-Host 'Enter 키를 누르면 종료합니다'
