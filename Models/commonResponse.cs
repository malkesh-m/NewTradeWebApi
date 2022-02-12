using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace TradeWeb.API.Entity
{
    public class commonResponse
    {
        public bool status { get; set; }
        public int status_code { get; set; }
        public string message { get; set; }
        public string error_message { get; set; }
        public dynamic data { get; set; }
    }

    public class tokenResponse : commonResponse
    {
        public string token { get; set; }
    }

    public class DynamicResponse
    {
        public dynamic response { get; set; }
    }

    public class commonResponse1
    {
        public bool status { get; set; }
        public int statusCode { get; set; }
        public string message { get; set; }
        public string errorMessage { get; set; }
        public dynamic data { get; set; }
    }

    public class downloadResponse
    {
        public bool status { get; set; }
        public int statusCode { get; set; }
        public string message { get; set; }
        public string fileName { get; set; }
        public string fileData { get; set; }
    }
}
