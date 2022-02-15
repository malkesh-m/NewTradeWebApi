using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TradeWeb.API.Models
{
    public class FamilyBalanceResponse
    {
        public string Code { get; set; }
        public string Name { get; set; }
        public List<ExchSegBalance> ExchSegBalances { get; set; }
    }

    public class ExchSegBalance
    {
        public string ExchSeg { get; set; }
        public string Balance { get; set; }
    }
}
