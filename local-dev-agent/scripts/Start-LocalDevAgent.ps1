param(
    [string]$Workspace = "..",
    [string]$HostName = "127.0.0.1",
    [int]$Port = 8765,
    [string]$Python = "python"
)

$ErrorActionPreference = "Stop"
$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Resolve-Path (Join-Path $ScriptRoot "..")
$Server = Join-Path $ProjectRoot "server\agent_server.py"

& $Python $Server --workspace $Workspace --host $HostName --port $Port

