using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;


namespace Microsoft.Health
{
    public static class FhirBundleHttpTrigger
    {

        [FunctionName("FhirBundleHttpTrigger")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("Received bundle from http request.");

            string requestBody = new StreamReader(req.Body).ReadToEnd();
            
            try
            {
                await FhirImport.ImportBundle(requestBody, log);
                return new AcceptedResult();
            }
            catch (Exception ex)
            {
                return new BadRequestObjectResult(new { message = ex.Message, trace = ex.StackTrace});
            }
        }
    }
}