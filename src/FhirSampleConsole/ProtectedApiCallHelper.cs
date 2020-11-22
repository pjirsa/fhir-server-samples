// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Threading.Tasks;
using FhirSampleConsole.Models;
using Flurl.Http;

namespace FhirSampleConsole
{
    /// <summary>
    /// Helper class to call a protected API and process its result
    /// </summary>
    public class ProtectedApiCallHelper
    {

        /// <summary>
        /// Calls the protected web API and processes the result
        /// </summary>
        /// <param name="webApiUrl">URL of the web API to call (supposed to return Json)</param>
        /// <param name="accessToken">Access token used as a bearer security token to call the web API</param>
        /// <param name="processResult">Callback used to process the result of the call to the web API</param>
        public async Task CallWebApiAndProcessResultASync(string webApiUrl, string accessToken, Action<FhirResponse> processResult, string httpMethod = "GET")
        {
            if (!string.IsNullOrEmpty(accessToken))
            {
                switch (httpMethod)
                {
                    case "GET":
                        var result = await webApiUrl
                            .WithOAuthBearerToken(accessToken)
                            .GetJsonAsync<FhirResponse>();

                        processResult(result);

                        var nextLink = result.Link.FirstOrDefault(l => "next".Equals(l.Relation))?.Url;

                        if (nextLink != null)
                            await CallWebApiAndProcessResultASync(nextLink.AbsoluteUri, accessToken, processResult, "GET");
                        break;
                    case "DELETE":
                        var response = await webApiUrl
                            .WithOAuthBearerToken(accessToken)
                            .DeleteAsync();
                        break;                       
                    default:
                        break;
                }


            }
        }

    }
}
