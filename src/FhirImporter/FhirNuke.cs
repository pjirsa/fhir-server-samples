using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Flurl.Http;
using System;
using Microsoft.Health;
using System.Linq;
using FhirImporter.Extensions;

namespace FhirImporter
{
    public static class FhirNuke
    {
        [FunctionName("FhirNuke")]
        public static async Task RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            var startTime = context.CurrentUtcDateTime;
            var endpoints = await context.CallActivityAsync<IList<string>>("FhirNuke_GetEndpoints", "Get Endpoints");

            log.LogInformation($"Found {endpoints.Count} endpoints.");

            var tasks = new List<Task>();

            for (int i = 0; i < endpoints.Count; i++)
            {
                tasks.Add(context.CallActivityAsync("FhirNuke_DeleteAllEntries", endpoints[i]));
            }
            //tasks.Add(context.CallActivityAsync("FhirNuke_DeleteAllEntries", "Organization"));

            await Task.WhenAll(tasks);

            var endTime = context.CurrentUtcDateTime;
            log.LogInformation($"Total time to nuke resources: {endTime - startTime}");

            return;
        }

        [FunctionName("FhirNuke_GetEndpoints")]
        public static async Task<IList<string>> GetEndpoints(
            [ActivityTrigger] string name, ILogger log)
        {
            try
            {
                return await FhirImport.GetEndpointsAsync(log);
            }
            catch
            {
                log.LogError("Could not fetch fhir endpoints.");
                return null;
            }
        }

        [FunctionName("FhirNuke_DeleteAllEntries")]
        public static async Task DeleteAllEntries([ActivityTrigger] string resourceType, ILogger log)
        {
            log.LogInformation($"Removing all {resourceType}");

            try
            {
                List<string> bundles = (await FhirImport.GetEntriesJsonAsync(resourceType, log)).ToList();
                await bundles.AsyncParallelForEach(async bundle =>
                {
                    await FhirImport.DeleteBundle(bundle, log);
                }, 2);
            }
            catch
            {
                log.LogError($"Could not delete {resourceType}.");
            }
        }

        [FunctionName("FhirNuke_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            // Function input comes from the request content.
            string instanceId = await starter.StartNewAsync("FhirNuke", null);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}
