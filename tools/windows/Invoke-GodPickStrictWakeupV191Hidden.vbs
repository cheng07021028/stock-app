Option Explicit

Dim args, shell, psScript, repository, workflow, branch, tokenFile, wakeupSource, commandLine
Set args = WScript.Arguments

If args.Count < 6 Then
    WScript.Quit 2
End If

psScript = args.Item(0)
repository = args.Item(1)
workflow = args.Item(2)
branch = args.Item(3)
tokenFile = args.Item(4)
wakeupSource = args.Item(5)

Function QuoteArg(ByVal value)
    QuoteArg = Chr(34) & Replace(CStr(value), Chr(34), Chr(34) & Chr(34)) & Chr(34)
End Function

commandLine = "powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File " & QuoteArg(psScript) _
    & " -Repository " & QuoteArg(repository) _
    & " -Workflow " & QuoteArg(workflow) _
    & " -Branch " & QuoteArg(branch) _
    & " -TokenFile " & QuoteArg(tokenFile) _
    & " -WakeupSource " & QuoteArg(wakeupSource)

Set shell = CreateObject("WScript.Shell")
WScript.Quit shell.Run(commandLine, 0, True)
