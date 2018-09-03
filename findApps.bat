@echo searching for Office Paths : 
setlocal
CD c:\
for /f "delims=" %%F in ('dir "soffice.exe" /s /p') do set p=%%F
if defined p (
echo %p%
) else (
echo File not found
)

endlocal