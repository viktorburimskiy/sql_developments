echo off
cd C:\ProgramData\Microsoft\Windows\Start Menu\Programs\Teradata Client 16.20\
bteq .RUN FILE = C:\Temp\bteq_for_shell.txt
@echo off goto end
:end @echo exit
