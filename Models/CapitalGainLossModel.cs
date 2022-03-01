using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TradeWeb.API.Models
{
    public class GainLossDividendModel
    {
        public string FromDate { get; set; }
        public string ToDate { get; set; }
    }

    public class GainLossActualPLSummaryModel
    {
        public string FromDate { get; set; }
        public string ToDate { get; set; }
        public bool ChkJobing { get; set; }
        public bool ChkDelivery { get; set; }
        public bool ChkIgnore112A { get; set; }
        public string Type { get; set; }
    }

    public class GainLossActualPLDetailModel
    {
        public string FromDate { get; set; }
        public string ToDate { get; set; }
        public string Type { get; set; }
        public string Ignore112A { get; set; }
        public string ScripCode { get; set; }
    }

    public class GainLossTradeListingSummary
    {
        public string FromDate { get; set; }
        public string ToDate { get; set; }
    }

    public class GainLossTradeListingDetailModel
    {
        public string FromDate { get; set; }
        public string ToDate { get; set; }
        public string ScripCode { get; set; }
    }

    public class GainLossTradeInsertModel
    {
        public string Date { get; set; }
        public string Settelment { get; set; }

        public string ScripCode { get; set; }
    }
}
