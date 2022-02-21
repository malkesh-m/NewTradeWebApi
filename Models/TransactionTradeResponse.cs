using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TradeWeb.API.Models
{
    public class TransactionTradeResponse
    {
        public string FamilyCode { get; set; }
        public string FamilyName { get; set; }
        public List<TransactionTradeDetails> TransactionTradeDetails { get; set; }
    }

    public class TransactionTradeDetails
    {
        public string Scrip { get; set; }
        public string Name { get; set; }
        public string Quantity { get; set; }
        public string Rate { get; set; }
    }

    public class TransactionRecieptResponse
    {
        public string FamilyCode { get; set; }
        public string FamilyName { get; set; }
        public string Receipt { get; set; }
        public string Payment { get; set; }
    }

    public class TransactionJournalResponse
    {
        public string FamilyCode { get; set; }
        public string FamilyName { get; set; }
        public string Debit { get; set; }
        public string Credit { get; set; }
    }

}
