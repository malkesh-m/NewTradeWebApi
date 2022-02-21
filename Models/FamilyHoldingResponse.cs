using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TradeWeb.API.Models
{
    public class FamilyHoldingResponse
    {
        public string FamilyCode { get; set; }
        public string FamilyName { get; set; }
        public List<HoldingDetails> HoldingDetails { get; set; }
    }

    public class HoldingDetails
    {
        public string ISIN { get; set; }
        public string ISINName { get; set; }
        public string Quantity { get; set; }
        public string Valuation { get; set; }
    }
} 
