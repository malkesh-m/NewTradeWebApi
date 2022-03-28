using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TradeWeb.API.Models
{
    public class ProfitLossCombinedModel
    {
        public dynamic CashSummary { get; set; }
        public dynamic FoSummary { get; set; }
        public dynamic CommoditySummary { get; set; }
    }

    public class ProfitLossCombinedInputModel
    {
        public string FromDate { get; set; }
        public string ToDate { get; set; }
    }
}
