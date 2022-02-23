using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TradeWeb.API.Models
{
    public class MarginResponse
    {
        public string ExchSeg { get; set; }
        public decimal EOD_Margin_Required { get; set; }
        public decimal EOD_Margin_Available { get; set; }
        public decimal EOD_ShortFall_Amount { get; set; }
        public decimal EOD_ShortFall_Per { get; set; }
        public decimal Peak_Margin_Required { get; set; }
        public decimal Peak_Margin_To_Be_Collected { get; set; }
        public decimal Peak_Margin_Available { get; set; }
        public decimal Peak_Margin_Shortfall { get; set; }
        public decimal Peak_Margin_Highest_Shortfall { get; set; }
    }
}
