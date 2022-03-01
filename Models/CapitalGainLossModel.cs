using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
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
        [Required(ErrorMessage ="Please enter date")]
        public string Date { get; set; }
        [Required(ErrorMessage ="Please enter settelment")]
        public string Settelment { get; set; }
        [Required]
        public string Flag { get; set; }
        [Required]
        public string Type { get; set; }
        [Required]
        public double Quantity { get; set; }
        [Required]
        public double NetRate { get; set; }
        [Required]
        public double ServiceTax { get; set; }
        [Required]
        public double STT { get; set; }
        [Required]
        public double OtherCharge1 { get; set; }
        [Required]
        public double OtherCharge2 { get; set; }
        [Required]
        public string ScripCode { get; set; }
    }
}
