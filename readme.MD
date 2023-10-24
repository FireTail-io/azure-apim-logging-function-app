create local.settings.json

```json
{
  "IsEncrypted": false,
  "Values": {
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "AzureWebJobsFeatureFlags": "EnableWorkerIndexing",
    "AzureWebJobsStorage": "DefaultEndpointsProtocol=https;AccountName=example;AccountKey=example;EndpointSuffix=core.windows.net",
    "eventhub_connection_str": "Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=example;SharedAccessKey=example"
  }
}
```

