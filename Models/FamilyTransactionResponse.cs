using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TradeWeb.API.Models
{
    public class FamilyTransactionModel
    {
        public string SelectedValue { get; set; }
        public string FromDate { get; set; }
        public string ToDate { get; set; }
        public List<string> UCC_Codes { get;set; }
    }
}
