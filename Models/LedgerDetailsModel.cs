using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TradeWeb.API.Models
{
    public class LedgerDetailsModel
    {
        public string FromDate { get; set; }
        public string ToDate { get; set; }

        public List<SegmentModel> type_ExchSeg { get; set; }
    }

    public class SegmentModel
    {
        public string type { get; set; }

        public List<string> exchSeg { get; set; }
    }
}
