$serviceName = $OctopusParameters["Octopus.Action.WindowsService.ServiceName"]
& "sc.exe" failure $serviceName reset= 0 actions= restart/120000/restart/120000/restart/120000