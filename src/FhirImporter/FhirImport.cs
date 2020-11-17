using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Net.Http;
using System.Net.Http.Headers;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Polly;

namespace Microsoft.Health
{
    public static class FhirImport
    {
        private static readonly HttpClient httpClient = new HttpClient();
        
        public static async Task ImportBundle(string fhirString, ILogger log)
        {
            JObject bundle;
            JArray entries;
            try
            {
                bundle = JObject.Parse(fhirString);
            }
            catch (JsonReaderException ex)
            {
                var msg = "Input file is not a valid JSON document";
                log.LogError(msg);
                throw new Exception(msg, ex);
            }

            log.LogInformation("Bundle read");

            try
            {
                FhirImportReferenceConverter.ConvertUUIDs(bundle);
            }
            catch (Exception ex)
            {
                var msg = "Failed to resolve references in doc";
                log.LogError(msg);
                throw new Exception(msg, ex);
            }

            entries = (JArray)bundle["entry"];
            if (entries == null)
            {
                log.LogError("No entries found in bundle");
                throw new FhirImportException("No entries found in bundle");
            }


            AuthenticationContext authContext;
            ClientCredential clientCredential;
            AuthenticationResult authResult;

            string authority = System.Environment.GetEnvironmentVariable("Authority");
            string audience = System.Environment.GetEnvironmentVariable("Audience");
            string clientId = System.Environment.GetEnvironmentVariable("ClientId");
            string clientSecret = System.Environment.GetEnvironmentVariable("ClientSecret");
            Uri fhirServerUrl = new Uri(System.Environment.GetEnvironmentVariable("FhirServerUrl"));

            int maxDegreeOfParallelism;
            if (!int.TryParse(System.Environment.GetEnvironmentVariable("MaxDegreeOfParallelism"), out maxDegreeOfParallelism))
            {
                maxDegreeOfParallelism = 16;
            }

            try
            {
                authContext = new AuthenticationContext(authority);
                clientCredential = new ClientCredential(clientId, clientSecret);
                authResult = authContext.AcquireTokenAsync(audience, clientCredential).Result;
            }
            catch (Exception ee)
            {
                log.LogCritical(string.Format("Unable to obtain token to access FHIR server in FhirImportService {0}", ee.ToString()));
                throw;
            }

            //var entriesNum = Enumerable.Range(0,entries.Count-1);
            var actionBlock = new ActionBlock<int>(async i =>
            {
                var entry_json = ((JObject)entries[i])["resource"].ToString();
                string resource_type = (string)((JObject)entries[i])["resource"]["resourceType"];
                string id = (string)((JObject)entries[i])["resource"]["id"];
                var randomGenerator = new Random();

                Thread.Sleep(TimeSpan.FromMilliseconds(randomGenerator.Next(50)));

                if (string.IsNullOrEmpty(entry_json))
                {
                    log.LogError("No 'resource' section found in JSON document");
                    throw new FhirImportException("'resource' not found or empty");
                }

                if (string.IsNullOrEmpty(resource_type))
                {
                    log.LogError("No resource_type found.");
                    throw new FhirImportException("No resource_type in resource.");
                }

                StringContent content = new StringContent(entry_json, Encoding.UTF8, "application/json");
                var pollyDelays =
                        new[]
                        {
                                TimeSpan.FromMilliseconds(2000 + randomGenerator.Next(50)),
                                TimeSpan.FromMilliseconds(3000 + randomGenerator.Next(50)),
                                TimeSpan.FromMilliseconds(5000 + randomGenerator.Next(50)),
                                TimeSpan.FromMilliseconds(8000 + randomGenerator.Next(50))
                        };


                HttpResponseMessage uploadResult = await Policy
                    .HandleResult<HttpResponseMessage>(response => !response.IsSuccessStatusCode)
                    .WaitAndRetryAsync(pollyDelays, (result, timeSpan, retryCount, context) =>
                    {
                        log.LogWarning($"Request failed with {result.Result.StatusCode}. Waiting {timeSpan} before next retry. Retry attempt {retryCount}");
                    })
                    .ExecuteAsync(() =>
                    {
                        var message = string.IsNullOrEmpty(id)
                            ? new HttpRequestMessage(HttpMethod.Post, new Uri(fhirServerUrl, $"/{resource_type}"))
                            : new HttpRequestMessage(HttpMethod.Put, new Uri(fhirServerUrl, $"/{resource_type}/{id}"));

                        message.Content = content;
                        message.Headers.Authorization = new AuthenticationHeaderValue("Bearer", authResult.AccessToken);
                        return httpClient.SendAsync(message);
                    });

                if (!uploadResult.IsSuccessStatusCode)
                {
                    string resultContent = await uploadResult.Content.ReadAsStringAsync();
                    log.LogError(resultContent);

                        // Throwing a generic exception here. This will leave the blob in storage and retry.
                        throw new Exception($"Unable to upload to server. Error code {uploadResult.StatusCode}");
                }
                else
                {
                    log.LogInformation($"Uploaded /{resource_type}/{id}");
                }
            },
                new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = maxDegreeOfParallelism
                }
            );

            for (var i = 0; i < entries.Count; i++)
            {
                actionBlock.Post(i);
            }
            actionBlock.Complete();
            actionBlock.Completion.Wait();

            return;
        }
    }
}