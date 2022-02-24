using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TradeWeb.API.Models
{
    public class RmsRequest
    {
        public string Exch { get; set; }
        public string ld_dpid { get; set; }
        public double amt { get; set; }
    }

    public class PayOutRequest
    {
        public double BranchRequestAmount { get; set; }
        public double RmsAmount { get; set; }
        public List<RmsRequest> Data { get; set; }
    }
}
