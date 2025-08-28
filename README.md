# Oddjob

[![pipeline](https://github.com/nrkno/distribution-oddjob/actions/workflows/pipeline.yml/badge.svg)](https://github.com/nrkno/distribution-oddjob/actions/workflows/pipeline.yml)
[![system-tests](https://github.com/nrkno/distribution-oddjob/actions/workflows/system-tests.yml/badge.svg)](https://github.com/nrkno/distribution-oddjob/actions/workflows/system-tests.yml)

<p align="center">
  <img src="./assets/oddjob.png" alt="Picture of Oddjob">
</p>

Oddjob is the distribution engine used for publishing on-demand media files and new clips at NRK origins.

## Project documentation

Extensive Oddjob documentation can be found [here](documentation/Index%20📇.md).

## Building the project

You need to add two custom nuget feeds.

- Phobos
  - Find URL from sdkbin. 
  - Create an account with your @nrk.no e-mail. Use your personal username and password to access the nuget feed

```shell
dotnet tool restore
dotnet build
dotnet test
```

_Note, Jetbrains Rider IDE uses its own restore engine for nuget packages, this might result in an update of all lock files on build and restore.
To resolve this, go to `settings > Build, Execution, Deployment > NuGet > .NET Core restore engine`, and set to Console.
This will result in same behaviour across Rider and Terminal restoration of nuget packages._

### Environment variables
Using your IDE you can login and access the different nuget feed. However, we recommend to add the nuget feed tokens, usernames and passwords to environment variables. The nuget.config file expects variables `PHOBOS_NUGET_PASSWORD` and `PHOBOS_NUGET_USERNAME`. If you choose to use environment varables, you need to add these three variables as environment variables   

## Running services locally
To get secrets locally, you need to add `appsettings.Development.json` to Bootstrapper/config/appsettings.
We use the NRK vault-configuration-provider package, and you can check out its documentation to see how you can prefer local variables over vault variables, etc: https://github.com/nrkno/vault-configuration-provider/

If you want to monitor OpenTelemetry metrics, you can either use the Grafana dashboard configured in the localservices, or spawn a .NET Aspire dashboard by executing the following command:

```shell
docker run --rm -it -p 18888:18888 -p 4317:18889 --name aspire-dashboard mcr.microsoft.com/dotnet/aspire-dashboard:latest
```

### CLI (Preferred)

```json
{
  "Vault": {
    "Enabled": true,
    "Authentication": {
      "Type": "VaultCli"
    }
  }
}
```
Make sure to have the Vault CLI installed; a website will automatically open on project start and use AD authentication to fetch the token from Vault.

### Token

```json
{
  "Vault": {
    "Enabled": true,
    "Authentication": {
      "Type": "Token",
      "Token": "<your-vault-token-here>"
    }
  }
}
```
The value for the token must be your own, extracted from vault (click the profile symbol at https://vault.nrk.cloud:8200/ui/vault/dashboard and copy token).

## Running services in a Docker container
You may need to login to Azure first:
```shell
az login
az acr login -n plattform.azurecr.io
az acr login -n nrkpsregistry.azurecr.io
```

To mount Oddjob file archive volume, you need to define environment variables FILE_SHARE_USERNAME and FILE_SHARE_PASSWORD.
These are typicall your n-user credentials. To avoid having them in a command history, you can enter them using 'read' command followed by 'export' command:
```shell
read -s FILE_SHARE_PASSWORD
export FILE_SHARE_PASSWORD
```

You also need to provide secrets for PHOBOS_NUGET_USERNAME and PHOBOS_NUGET_PASSWORD.

Build the images:
```shell
docker build . -t oddjob:latest --secret id=PHOBOS_NUGET_USERNAME --secret id=PHOBOS_NUGET_PASSWORD
```

Then run docker compose to start all services:
```shell
docker compose up
```
or
```shell
docker compose up -d
```
To show only logs for a specific service (oddjob in this example), use the following command:
```shell
docker compose logs --follow oddjob
```

Services connection strings are specified in localservices/docker-compose.yaml file but can be overwritten in appsettings.Development.json.

Queues for local developement can be configured as follows:

```json
{
  "QueuesConnections": {
    "QueueServers": [
      {
        "Name": "amqp-dev",
        "Hosts": {
          "Amqp": "localhost",
          "Http": "http://localhost:15672"
        },
        "VirtualHost": "/",
        "Port": 5672,
        "ManagementPort": 443,
        "Username": "guest",
        "Password": "guest"
      }
    ]
  }
}
```

Use the following addresses and ports to monitor services in containers:

| Service        | Address         |
|----------------|-----------------|
| Oddjob Web API | localhost:1953  |
| Event Store    | localhost:8080  |
| Grafana        | localhost:3000  |
| RabbitMQ UI    | localhost:15672 |
| SQL Service    | localhost:1433  |


Run the following command to list all tables in Azurite service:
```shell
az storage table list \
  --connection-string "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
```
And to list all rows in a given table (in this case akkadiscovery):
```shell
az storage entity query --table-name akkadiscovery \
  --connection-string "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
```

## CI/CD
Oddjob build pipelines and system tests are run in github actions. The pipelines are defined in .github/workflows.

## Documentation

* https://confluence.nrk.no/display/DM/Distribusjonsmotor
