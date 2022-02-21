using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TradeWeb.API.Models
{
    public class FamilyHoldingResponse
    {
        public string Code { get; set; }
        public string Name { get; set; }
        public List<HoldingDetails> HoldingDetails { get; set; }
    }

    public class HoldingDetails
    {
        public string ISIN { get; set; }
        public string ss_Name { get; set; }
        public string Quantity { get; set; }
        public string Valuation { get; set; }
    }
} 
