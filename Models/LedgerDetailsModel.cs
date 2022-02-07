using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TradeWeb.API.Models
{
    public class LedgerDetailsModel
    {
        public string fromDate { get; set; }
        public string toDate { get; set; }

        public List<SegmentModel> segmentModel { get; set; }
    }

    public class SegmentModel
    {
        public string type { get; set; }

        public List<string> exchangeSegment { get; set; }
    }
}
