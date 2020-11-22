// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Newtonsoft.Json.Linq;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FhirSampleConsole.Models;
using Flurl;
using Flurl.Http;

namespace FhirSampleConsole
{
    /// <summary>
    /// This sample shows how to query the Microsoft Graph from a daemon application
    /// which uses application permissions.
    /// For more information see https://aka.ms/msal-net-client-credentials
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                RunAsync("Patient").GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.Message);
                Console.ResetColor();
            }

            Console.WriteLine("Press any key to exit");
            Console.ReadKey();
        }

        private static async Task RunAsync(string resource)
        {
            AuthenticationConfig config = AuthenticationConfig.ReadFromJsonFile("appsettings.json");

            AuthenticationContext authContext;
            ClientCredential clientCredential;
            AuthenticationResult authResult;

            try
            {
                authContext = new AuthenticationContext(config.Authority);
                clientCredential = new ClientCredential(config.ClientId, config.ClientSecret);
                authResult = await authContext.AcquireTokenAsync(config.Audience, clientCredential);
            }
            catch (Exception ee)
            {
                Console.WriteLine(string.Format("Unable to obtain token to access FHIR server in FhirImportService {0}", ee.ToString()));
                throw;
            }

            // TODO: Custom FHIR API calls go here
            if (authResult == null)
                return;

            List<FhirResource> results = new List<FhirResource>();
            Uri nextLink = new Uri($"{config.ApiUrl}{resource}");
            while (nextLink != null)
            {
                var entries = await nextLink.AbsoluteUri
                    .WithOAuthBearerToken(authResult.AccessToken)
                    .GetJsonAsync<FhirResponse>();

                results.AddRange(entries.Entry);

                nextLink = entries.Link.FirstOrDefault(l => "next".Equals(l.Relation))?.Url;
            }

            await Delete(results, authResult.AccessToken);
            
        }

        /// <summary>
        /// Display the result of the Web API call
        /// </summary>
        /// <param name="result">Object to display</param>
        private static void Display(IList<FhirResource> items)
        {
            foreach (var item in items)
            {
                Console.WriteLine(item.FullUrl);
            }
            /*
            foreach (JProperty child in result.Properties().Where(p => !p.Name.StartsWith("@")))
            {
                Console.WriteLine($"{child.Name} = {child.Value}");
            }
            */
        }

        private static async Task Delete(IList<FhirResource> items, string accessToken)
        {
            
            foreach (var item in items)
            {
                Console.WriteLine($"Deleting: {item.FullUrl}");

                await item.FullUrl
                    .WithOAuthBearerToken(accessToken)
                    .DeleteAsync();
            }
        }

    }
}
