using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TradeWeb.API.Models
{
    public class FamilyRetainedStokeResponse
    {
        public string FamilyCode { get; set; }
        public string FamilyName { get; set; }
        public List<StokeDetails> StokeDetails { get; set; }
    }

    public class StokeDetails
    {
        public string ISIN { get; set; }
        public string ISINName { get; set; }
        public string Quantity { get; set; }
        public string Valuation { get; set; }
    }
}
