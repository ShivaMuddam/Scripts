SELECT @@VERSION

SELECT SERVERPROPERTY('productversion'), SERVERPROPERTY ('productlevel'), SERVERPROPERTY ('edition')


EXEC xp_cmdshell 'powershell.exe -c "gwmi -Class Win32_PageFileSetting"'
--If the result is NULL it meants Windows managing the page size

EXEC xp_cmdshell 'powershell.exe -c "Get-WmiObject Win32_PageFileusage | format-list"'


--This will enable paging by windows

--EXEC xp_cmdshell 'powershell.exe -c "wmic computersystem -EnableAllPrivileges set AutomaticManagedPagefile=true"'

