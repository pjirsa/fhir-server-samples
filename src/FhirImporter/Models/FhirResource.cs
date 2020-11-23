using System;
using System.Collections;
using System.Collections.Generic;

namespace FhirImporter.Models 
{
    public class FhirResponse
    {
        public string ResourceType { get; set; }
        public IList<FhirResource> Entry { get; set; }
        public IList<ResponseLink> Link { get; set; }
    }

    public class ResponseLink
    {
        public string Relation { get; set; }
        public Uri Url { get; set; }
    }

    public class FhirResource 
    {
        public string FullUrl { get; set; }

        public Resource Resource { get; set; }
    }

    public class Resource
    {
        public string ResourceType { get; set; }
        public Guid Id { get; set; }
    }
}
