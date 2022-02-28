using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TradeWeb.API.Models
{
    public class PledgeForMarginModel
    {
        public string DematActNo { get; set; }
        public string Securities_Code { get; set; }
        public string Request_Qty { get; set; }
    }
}
