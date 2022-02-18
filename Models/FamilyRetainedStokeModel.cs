using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TradeWeb.API.Models
{
    public class FamilyRetainedStokeResponse
    {
        public string Code { get; set; }
        public string Name { get; set; }
        public List<StokeDetails> StokeDetails { get; set; }
    }

    public class StokeDetails
    {
        public string ISIN { get; set; }
        public string ss_Name { get; set; }
        public string Quantity { get; set; }
        public string Valuation { get; set; }
    }
}
