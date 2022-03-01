using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TradeWeb.API.Models
{
    public class UnPledgeRequestModel
    {
        public string Securities_Code { get; set; }
        public string Request_Qty { get; set; }
    }
}
