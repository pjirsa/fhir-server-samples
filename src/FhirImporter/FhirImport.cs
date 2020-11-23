using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Http;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Polly;
using FhirImporter.Extensions;
using Flurl.Http;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using FhirImporter.Models;

namespace Microsoft.Health
{
    public static class FhirImport
    {
        public static async Task ImportBundle(string fhirString, ILogger log)
        {
                await ProcessItems(fhirString, log, "upsert");
        }

        public static async Task DeleteBundle(string fhirString, ILogger log)
        {
            await ProcessItems(fhirString, log, "delete");
        }

        public static async Task<IList<string>> GetEndpointsAsync(ILogger log)
        {

            string fhirServerUrl = Environment.GetEnvironmentVariable("FhirServerUrl");

            string fhirResult = await $"{fhirServerUrl}/metadata".GetStringAsync();

            try
            {
                JObject capabilityStatement = JObject.Parse(fhirResult);
                JArray resources = (JArray)((JArray)capabilityStatement["rest"].FirstOrDefault()["resource"]);
                return resources.Select(i => (string)i["type"]).ToList();
            }
            catch (Exception ex)
            {
                log.LogError("Error fetching metadata", ex);
                throw ex;
            }
        }

        public static async Task<IList<string>> GetEntriesJsonAsync(string resource, ILogger log)
        {
            var result = new List<string>();
            try
            {
                string fhirServerUrl = Environment.GetEnvironmentVariable("FhirServerUrl");
                string authToken = (await GetAuthTokenAsync()).AccessToken;

                string nextLink = resource;

                do
                {
                    var bundleJson = await $"{fhirServerUrl}/{nextLink}"
                       .WithOAuthBearerToken(authToken)
                       .GetStringAsync();
                    result.Add(bundleJson);

                    var response = JsonConvert.DeserializeObject<FhirResponse>(bundleJson);
                    nextLink = response.Link.FirstOrDefault(l => l.Relation == "next")?.Url.PathAndQuery;
                }
                while (!string.IsNullOrEmpty(nextLink));

                return result;
            }
            catch (Exception ex)
            {
                log.LogError($"Error fetching {resource}");
                throw ex;
            }
        }

        private static async Task<AuthenticationResult> GetAuthTokenAsync()
        {
            string authority = Environment.GetEnvironmentVariable("Authority");
            string audience = Environment.GetEnvironmentVariable("Audience");
            string clientId = Environment.GetEnvironmentVariable("ClientId");
            string clientSecret = Environment.GetEnvironmentVariable("ClientSecret");

            AuthenticationContext authContext;
            ClientCredential clientCredential;
            AuthenticationResult authResult;
            try
            {
                authContext = new AuthenticationContext(authority);
                clientCredential = new ClientCredential(clientId, clientSecret);
                authResult = await authContext.AcquireTokenAsync(audience, clientCredential);
                return authResult;
            }
            catch (Exception ee)
            {
                throw new Exception($"Unable to obtain token to access FHIR server in FhirImportService {ee}", ee);
            }
        }

        private static async Task ProcessItems(string fhirString, ILogger log, string operation = "upsert")
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
                throw new Exception(msg, ex);
            }

            entries = (JArray)bundle["entry"];
            if (entries == null)
            {
                log.LogWarning("No entries found in bundle");
                return;
            }

            try
            {
                FhirImportReferenceConverter.ConvertUUIDs(bundle);
            }
            catch (Exception ex)
            {
                var msg = "Failed to resolve references in doc";
                throw new Exception(msg, ex);
            }
            
            int maxDegreeOfParallelism;
            if (!int.TryParse(Environment.GetEnvironmentVariable("MaxDegreeOfParallelism"), out maxDegreeOfParallelism))
            {
                maxDegreeOfParallelism = 16;
            }

            string fhirServerUrl = Environment.GetEnvironmentVariable("FhirServerUrl");
            var authToken = (await GetAuthTokenAsync()).AccessToken;

            await entries.AsyncParallelForEach(async entry => {

                var entry_json = ((JObject)entry)["resource"].ToString();
                string resource_type = (string)((JObject)entry)["resource"]["resourceType"];
                string id = (string)((JObject)entry)["resource"]["id"];
                var randomGenerator = new Random();

                Thread.Sleep(TimeSpan.FromMilliseconds(randomGenerator.Next(50)));

                if (string.IsNullOrEmpty(entry_json))
                {
                    log.LogError("No 'resource' section found in JSON document");
                    throw new FhirImportException("'resource' not found or empty");
                }

                if (string.IsNullOrEmpty(resource_type))
                {
                    throw new FhirImportException("No resource_type in resource.");
                }

                var pollyDelays =
                        new[]
                        {
                                TimeSpan.FromMilliseconds(2000 + randomGenerator.Next(50)),
                                TimeSpan.FromMilliseconds(3000 + randomGenerator.Next(50)),
                                TimeSpan.FromMilliseconds(5000 + randomGenerator.Next(50)),
                                TimeSpan.FromMilliseconds(8000 + randomGenerator.Next(50)),
                        };


                IFlurlResponse uploadResult = await Policy
                    .Handle<FlurlHttpException>()
                    //.Handle<FlurlHttpException>(flurlEx => null == flurlEx.Call || !flurlEx.Call.Succeeded)
                    .WaitAndRetryAsync(pollyDelays, (result, timeSpan, retryCount, context) =>
                    {
                        log.LogWarning($"Request failed with {result.Message}. Waiting {timeSpan} before next retry. Retry attempt {retryCount}");
                    })
                    .ExecuteAsync(() =>
                    {
                        switch (operation)
                        {
                            case "delete":
                                return $"{fhirServerUrl}/{resource_type}/{id}"
                                    .WithOAuthBearerToken(authToken)
                                    .DeleteAsync();
                            case "upsert":
                            default:
                                StringContent content = new StringContent(entry_json, Encoding.UTF8, "application/json");
                                return string.IsNullOrEmpty(id)
                                    ? $"{fhirServerUrl}/{resource_type}".WithOAuthBearerToken(authToken).PostAsync(content)
                                    : $"{fhirServerUrl}/{resource_type}/{id}".WithOAuthBearerToken(authToken).PutAsync(content);
                        }
                    });

                if (!uploadResult.ResponseMessage.IsSuccessStatusCode)
                {
                    string resultContent = await uploadResult.GetStringAsync();
                    log.LogError(resultContent);

                    // Throwing a generic exception here. This will leave the blob in storage and retry.
                    throw new Exception($"Unable to {operation}. Error code {uploadResult.StatusCode}");
                }
                else
                {
                    log.LogInformation($"{operation}ed /{resource_type}/{id}");
                }
            }, maxDegreeOfParallelism);
        }
    }
}
