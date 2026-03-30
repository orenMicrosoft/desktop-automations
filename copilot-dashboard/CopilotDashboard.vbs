' Copilot CLI Dashboard - Hidden Launcher
' Starts the dashboard server without showing a console window
Set WshShell = CreateObject("WScript.Shell")
Dim PythonPath, ScriptDir
ScriptDir = CreateObject("Scripting.FileSystemObject").GetParentFolderName(WScript.ScriptFullName)

' Find pythonw via where command
Dim oExec
Set oExec = WshShell.Exec("cmd /c where pythonw.exe 2>nul")
PythonPath = Trim(oExec.StdOut.ReadLine())

If PythonPath = "" Then
    ' Fallback: try python.exe (will show console briefly)
    Set oExec = WshShell.Exec("cmd /c where python.exe 2>nul")
    PythonPath = Trim(oExec.StdOut.ReadLine())
End If

WshShell.Run """" & PythonPath & """ """ & ScriptDir & "\launch.py""", 0, False