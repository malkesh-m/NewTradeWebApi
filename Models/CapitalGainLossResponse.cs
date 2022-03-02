using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TradeWeb.API.Models
{
    public class GainLossDivedendResponse
    {
        public string DivDate { get; set; }
        public string ScripCode { get; set; }
        public string ScripName { get; set; }
        public double Qty { get; set; }
        public double Rate { get; set; }
        public double Amount { get; set; }
    }

    public class GainLossActualPLSummaryResponse
    {
        public string ScripCode { get; set; }
        public string ScripName { get; set; }
        public string ISIN { get; set; }
        public double BQty { get; set; }
        public double BAmount { get; set; }
        public double SQty { get; set; }
        public double SAmount { get; set; }
        public double NetQty { get; set; }
        public double StockAtCost { get; set; }
        public double Trading { get; set; }
        public double ShortTerm { get; set; }
        public double LongTerm { get; set; }
        public double MarketRate { get; set; }
        public double StockAtMkt { get; set; }
        public double UnRealGain { get; set; }
        public double STT { get; set; }
    }

    public class GainLossActualPLDetailResponse
    {
        public string ScripCode { get; set; }
        public string ScripName { get; set; }
        public string ISIN { get; set; }
        public string SellDate { get; set; }
        public double SellRate { get; set; }
        public double Qty { get; set; }
        public string BuyDate { get; set; }
        public double BuyRate { get; set; }
        public double StockAtCost { get; set; }
        public double StockAtMkt { get; set; }
        public double Trading { get; set; }
        public double LongTerm { get; set; }
        public double ShortTerm { get; set; }
        public double UnRealGain { get; set; }
        public string Type { get; set; }
        public double Days { get; set; }
        public double Rate { get; set; }
        public string QtrSlab { get; set; }
        public double STT { get; set; }
        public string LTCG { get; set; }
        public string Rate112A { get; set; }
    }

    public class GainLossTradeListingSummaryResponse
    {
        public string ScripCode { get; set; }
        public string ScripName { get; set; }
        public double BuyQuantity { get; set; }
        public double BuyRate { get; set; }
        public double BuyAmount { get; set; }
        public double SellQuantity { get; set; }
        public double SellRate { get; set; }
        public double SellAmount { get; set; }
        public double NetQuantity { get; set; }
        public double NetAmount { get; set; }
    }

    public class GainLossTradeListingDetailResponse
    {
        public string SrNo { get; set; }
        public string Settelment { get; set; }
        public string TrxFlag { get; set; }
        public string TrdType { get; set; }
        public string Date { get; set; }
        public string BsFlag { get; set; }
        public double Quantity { get; set; }
        public double Rate { get; set; }
        public double Value { get; set; }
        public double ServiceTax { get; set; }
        public double STT { get; set; }
        public double OtherCharge1 { get; set; }
        public double OtherCharge2 { get; set; }
    }
}
