using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TradeWeb.API.Models
{
    public class RmsRequest
    {
        public string CompanyExchangeSegment { get; set; }
        public string dpId { get; set; }
        public double PayOut { get; set; }
    }

    public class PayOutRequest
    {
        public double BranchRequestAmount { get; set; }
        public double RmsAmount { get; set; }
        public List<RmsRequest> Data { get; set; }
    }
}
