using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Health;

namespace FhirImporter
{
    public static class FhirImportTelemetry
    {
        static string connectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
        static string containerName = "fhirimportmanual";

        [FunctionName("FhirImportTelemetry")]
        public static async Task RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            var startTime = context.CurrentUtcDateTime;

            var tasks = new List<Task>();

            // Get the blobs, orchestrator must call out to activity function for this task
            var blobs = await context.CallActivityAsync<IList<string>>("FhirImportTelemetry_GetBlobs", "Fetch list of bundles");            

            // Import to FHIR API 
            for (int i = 0; i < blobs.Count; i++)
            {              
                Task task = context.CallActivityAsync("FhirImportTelemetry_ProcessBundle", blobs[i]);
                tasks.Add(task);
            }

            await Task.WhenAll(tasks);

            var endTime = context.CurrentUtcDateTime;

            var duration = endTime - startTime;
            log.LogInformation($"Total import duration: {duration}");

        }

        [FunctionName("FhirImportTelemetry_GetBlobs")]
        public static async Task<IList<string>> GetBlobs([ActivityTrigger] string msg, ILogger log)
        {
            log.LogInformation(msg);

            var result = new List<string>();

            try
            {
                BlobContainerClient container = new BlobContainerClient(connectionString, containerName);
                var blobs = container.GetBlobsAsync().AsPages();

                await foreach(var page in blobs)
                {
                    foreach (BlobItem blobItem in page.Values)
                    {
                        result.Add(blobItem.Name);
                    }
                }

                return result;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Error fetching list of bundles");
                return result;
            }
        }

        [FunctionName("FhirImportTelemetry_ProcessBundle")]
        public static async Task ProcessBundle([ActivityTrigger] string blobName, ILogger log)
        {
            log.LogInformation("Received bundle from orchestrator.");

            try
            {
                BlockBlobClient block = new BlockBlobClient(connectionString, containerName, blobName);
                string blobJson = await new StreamReader(block.OpenRead()).ReadToEndAsync();
                await FhirImport.ImportBundle(blobJson, log);

                await block.DeleteIfExistsAsync();

            }
            catch (Exception ex)
            {
                log.LogError(ex, "Error processing bundle");           
            }
        }

        [FunctionName("FhirImportTelemetry_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post")]HttpRequestMessage req,
            [DurableClient]IDurableOrchestrationClient starter,
            ILogger log)
        {
            // Function input comes from the request content.
            string instanceId = await starter.StartNewAsync("FhirImportTelemetry", null);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}
