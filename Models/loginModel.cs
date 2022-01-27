using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TradeWeb.API.Models
{
    public class loginModel
    {
        public string username { get; set; }
        public string password { get; set; }
        public string companycode { get; set; } = String.Empty;
    }

    public class TradeWebLoginModel
    {
        public string username { get; set; }
        public string password { get; set; }
    }
}
