using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Net.Mail;
using System.Net;
using Microsoft.VisualBasic;
using System.IO;
using System.Text.RegularExpressions;
using System.Xml;
using Microsoft.Extensions.Configuration;
using Microsoft.Data.SqlClient;
using TradeWeb.Repo.Interface;
using Microsoft.EntityFrameworkCore;
using TradeWeb.API.Data;

namespace TradeWeb.API.Repository
{
    public class UtilityCommon : IUtilityCommon
    {
        public static string strTempRMSSummary;
        public static string strTempRMSDetail;
        public readonly IConfiguration ConfigurationManager;
        public IConfiguration session;
        public string StrConn = ""; //ConfigurationManager.ConnectionStrings["TradeWebConnectionString"].ConnectionString;

        #region Constructor
        /// <summary>
        /// //TODO : Add constructor for declare initialize values.
        /// </summary>
        public UtilityCommon(IConfiguration config)
        {
            ConfigurationManager = config;
        }

        //public UtilityCommon()
        //{
        //}
        #endregion


        public enum MessageType { Success, Error, Info, Warning };


        public bool InvalidDateCheck(dynamic txtobj)
        {
            if (Regex.IsMatch(txtobj.Text, (@"(((0[1-9]|[12][0-9]|3[01])([/])(0[13578]|10|12)([/])(\d{4}))|(([0][1-9]|[12][0-9]|30)([/])(0[469]|11)([/])(\d{4}))|((0[1-9]|1[0-9]|2[0-8])([/])(02)([/])(\d{4}))|((29)(\.|-|\/)(02)([/])([02468][048]00))|((29)([/])(02)([/])([13579][26]00))|((29)([/])(02)([/])([0-9][0-9][0][48]))|((29)([/])(02)([/])([0-9][0-9][2468][048]))|((29)([/])(02)([/])([0-9][0-9][13579][26])))")))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public string EsignConnectionString(bool IsCommexES, string StrDt)
        {
            string ConnectionString = string.Empty;
            string strproduct = string.Empty;
            char[] ArrSeparators = new char[1];
            string strsql = string.Empty;
            ArrSeparators[0] = '/';
            if ((ConfigurationManager["IsTradeweb"] == "T") || (ConfigurationManager["IsTradeweb"] == "O") || (Convert.ToInt16(fnFireQueryTradeWeb("sysobjects", "count(0)", "name", "Other_Products", true)) > 0))
            {
                if (IsCommexES == true)
                    strproduct = "CommexES";
                else
                {
                    if (ConfigurationManager["IsTradeweb"] == "O" || (Convert.ToInt16(fnFireQueryTradeWeb("sysobjects", "count(0)", "name", "Other_Products", true)) > 0))
                    {
                        strsql = "select * from Other_Products Where OP_Product Like 'TPLUSESIGN%'";
                        strsql += " and '" + StrDt.Trim() + "'  between OP_DSN and OP_URL ";
                        DataSet ds = new DataSet();
                        ds = OpenDataSet(strsql);
                        if (ds.Tables[0].Rows.Count > 0)
                        {
                            ConnectionString = "server=" + ds.Tables[0].Rows[0]["OP_Server"].ToString().Trim() + ";Database=" + ds.Tables[0].Rows[0]["OP_DataBase"].ToString().Trim() + ";Uid=" + ds.Tables[0].Rows[0]["OP_User"].ToString().Trim() + ";Pwd=" + ds.Tables[0].Rows[0]["OP_PWD"].ToString().Trim() + ";Max Pool Size=200;Connect Timeout=20000;pooling='true'";
                            return ConnectionString;
                        }
                        else
                        {
                            strproduct = "TPlusES";
                        }
                    }
                    else
                    {
                        strproduct = "TPlusES";
                    }
                }
            }
            else if (ConfigurationManager["IsTradeweb"] == "C")
            {
                strproduct = "CrossES";
            }
            else if (ConfigurationManager["IsTradeweb"] == "E")
            {
                strproduct = "EstroES";
            }
            string[] TPlusES = GetWebParameter(strproduct).Split(ArrSeparators);
            if (TPlusES.Length > 1 || (TPlusES.Length == 1 && TPlusES[0] != string.Empty))
            {
                ConnectionString = "server=" + TPlusES[0].ToString().Trim() + ";Database=" + TPlusES[1].ToString().Trim() + ";Uid=" + TPlusES[3].ToString().Trim() + ";Pwd=" + TPlusES[4].ToString().Trim() + ";Max Pool Size=200;Connect Timeout=20000;pooling='true'";
            }
            return ConnectionString;
        }

        public DataView MergeDataTablesOnKey(DataView ObjView1, DataView ObjView2, string Sort)
        {

            DataTable ObjTable1 = ObjView1.ToTable();
            DataTable ObjTable2 = ObjView2.ToTable();
            string CommonKeyColumnName = ObjTable2.Columns[0].ColumnName;
            //add all table belonging to second view to the first table except the common column
            foreach (DataColumn ObjTable2Column in ObjTable2.Columns)
            {
                if (ObjTable2Column.ColumnName != CommonKeyColumnName && ObjTable2Column.ColumnName != "Status" && ObjTable1.Columns.Contains(ObjTable2Column.ColumnName) == false)
                {
                    DataColumn ObjNewTable1Column = new DataColumn(ObjTable2Column.ColumnName);
                    ObjNewTable1Column.DataType = ObjTable2Column.DataType;
                    ObjNewTable1Column.MaxLength = ObjTable2Column.MaxLength;
                    ObjNewTable1Column.AllowDBNull = ObjTable2Column.AllowDBNull;
                    ObjNewTable1Column.Unique = ObjTable2Column.Unique;
                    ObjNewTable1Column.AutoIncrement = ObjTable2Column.AutoIncrement;
                    ObjNewTable1Column.AutoIncrementSeed = ObjTable2Column.AutoIncrementSeed;
                    ObjNewTable1Column.AutoIncrementStep = ObjTable2Column.AutoIncrementStep;
                    ObjTable1.Columns.Add(ObjNewTable1Column);

                }
            }
            //move through all rows table 1
            foreach (DataRow ObjTable1Row in ObjTable1.Rows)
            {
                //move through all rows of table 2
                foreach (DataRow ObjTable2Row in ObjTable2.Rows)
                {
                    //if the value of the common column field for both table1 row and table 2 row are equal
                    // throw new Exception(ObjTable1Row[CommonKeyColumnName].ToString() +" "+ ObjTable2Row[CommonKeyColumnName].ToString());
                    if (ObjTable1Row[CommonKeyColumnName].Equals(ObjTable2Row[CommonKeyColumnName]))
                    {
                        //copy values of all fields of table2row  to table1 row
                        foreach (DataColumn ObjTable2Column in ObjTable2.Columns)
                        {
                            if (ObjTable2Column.ColumnName != "Status")
                                ObjTable1Row[ObjTable2Column.ColumnName] = ObjTable2Row[ObjTable2Column.ColumnName];
                        }
                    }
                }

            }
            DataView ObjView = ObjTable1.DefaultView;
            ObjView.Sort = Sort;
            return ObjView;
        }
        public void SendEmail(MailMessage ObjMessage)
        {
            if (ConfigurationManager["EnableEmail"] == "False")
                return;
            ObjMessage.IsBodyHtml = true;
            SmtpClient ObjClient = new SmtpClient();
            ObjClient.EnableSsl = true;
            ObjClient.Host = ConfigurationManager["EmailServer"];
            ObjClient.Port = int.Parse(ConfigurationManager["EmailServerPort"]);
            ObjClient.Credentials = new NetworkCredential(ConfigurationManager["EmailServerUserID"], ConfigurationManager["EmailServerPassword"]);
            ObjClient.Send(ObjMessage);
        }

        public string GetApplicationMode()
        {
            return "MultipleOrganizationsMultipleBranch";
        }

        public DateTime GetDateTimeFromTimeString(string TimeString)
        {
            DateTime ObjDateTime = DateTime.Today;
            string StrHoursMinutes = TimeString.Substring(0, TimeString.Length - 3);
            string StrAMPM = TimeString.Substring(TimeString.Length - 2);
            int Hours = int.Parse(StrHoursMinutes.Substring(0, StrHoursMinutes.IndexOf(':')).Trim());
            int Minutes = int.Parse(StrHoursMinutes.Substring(StrHoursMinutes.IndexOf(':') + 1, 2).Trim());
            if (StrAMPM == "AM")
                ObjDateTime = ObjDateTime.AddHours(Hours);
            else
                ObjDateTime = ObjDateTime.AddHours(Hours + 12);
            ObjDateTime = ObjDateTime.AddMinutes(Minutes);
            return ObjDateTime;
        }

        public DataView MergeViews(dynamic ObjDataViewCollection, string Sort)
        {
            DataView ObjMergedView = ObjDataViewCollection[0];
            int i = 0;
            while (i < ObjDataViewCollection.Count - 1)
            {
                ObjMergedView = MergeDataTablesOnKey(ObjMergedView, ObjDataViewCollection[i + 1], string.Empty);
                i++;
            }
            ObjMergedView.Sort = Sort;
            return ObjMergedView;
        }
        public string ParseDate(string Date)
        {
            char[] ArrSeparators = new char[1];
            ArrSeparators[0] = ' ';
            string[] ArrDateParts = Date.Split(ArrSeparators);
            string DatePart2 = string.Empty;
            switch (ArrDateParts[1])
            {
                case "Jan":
                    DatePart2 = "01";
                    break;
                case "Feb":
                    DatePart2 = "02";
                    break;
                case "Mar":
                    DatePart2 = "03";
                    break;

                case "Apr":
                    DatePart2 = "04";
                    break;

                case "May":
                    DatePart2 = "05";
                    break;

                case "Jun":
                    DatePart2 = "06";
                    break;

                case "Jul":
                    DatePart2 = "07";
                    break;

                case "Aug":
                    DatePart2 = "08";
                    break;

                case "Sep":
                    DatePart2 = "09";
                    break;

                case "Oct":
                    DatePart2 = "10";
                    break;


                case "Nov":
                    DatePart2 = "11";
                    break;

                case "Dec":
                    DatePart2 = "12";
                    break;

            }
            return ArrDateParts[2] + DatePart2 + ArrDateParts[0];
        }
        public void prTempFOBill(SqlConnection ObjConnectionTmp)
        {
            string strCreate = string.Empty;
            try
            {
                strCreate = "drop table  #tmpfobill ";
                ExecuteSQLTmp(strCreate, ObjConnectionTmp);
            }
            catch (Exception EX)
            {

                strCreate = "CREATE TABLE  #tmpfobill ( ";
                strCreate = strCreate + "[tx_controlflag] numeric(18,3) NOT NULL ,";
                strCreate = strCreate + "[tx_dt] [char] (8) NOT NULL ,";
                strCreate = strCreate + "[tx_clientcd] [char] (8) NOT NULL ,";
                strCreate = strCreate + "[tx_mainbrcd] [char] (8) NOT NULL ,";
                strCreate = strCreate + "[tx_seriesid] [numeric]  NOT NULL ,";
                strCreate = strCreate + "[tx_desc] char(45) NOT NULL,";
                strCreate = strCreate + "[tx_bqty] numeric (18,3)  NOT NULL ,";
                strCreate = strCreate + "[tx_sqty] numeric(18,3)   NOT NULL ,";
                strCreate = strCreate + "[tx_rate] [money]  NOT NULL ,";
                strCreate = strCreate + "[tx_mainbrrate] [money]  NOT NULL ,";
                strCreate = strCreate + "[tx_marketrate] [money]  NOT NULL ,";
                strCreate = strCreate + "[tx_servicetax] [money]  NOT NULL ,";
                strCreate = strCreate + "[tx_closerate] [money]  NOT NULL ,";
                strCreate = strCreate + "[tx_sortlist] [numeric] NOT NULL,";
                strCreate = strCreate + "[tx_prodtype] [char] (2) NOT NULL,";
                strCreate = strCreate + "[tx_value] [money] NOT NULL, ";
                strCreate = strCreate + "[tx_exchange] [char] (1) NOT NULL, ";
                strCreate = strCreate + "[tx_marketrate1] [money]  NOT NULL ,";
                strCreate = strCreate + "[tx_Brokerage] [money]  NOT NULL ";
                strCreate = strCreate + " )";
                ExecuteSQLTmp(strCreate, ObjConnectionTmp);
            }

            try
            {
                strCreate = "Drop table [#tmpbillcharges]";
                ExecuteSQLTmp(strCreate, ObjConnectionTmp);
            }
            catch (Exception ex)
            {
                strCreate = "CREATE TABLE [#tmpbillcharges] ([bc_dt] [char] (8) NOT NULL,[bc_clientcd] [char] (8) NOT NULL,[bc_desc] [char] (40) NOT NULL,[bc_amount] [money] NOT NULL,[bc_billno] [numeric] NOT NULL,[bc_exchange] [char] (1) NOT NULL) ";
                ExecuteSQLTmp(strCreate, ObjConnectionTmp);
            }

            try
            {
                strCreate = " drop table [#tmpmosesdates] ";
                ExecuteSQLTmp(strCreate, ObjConnectionTmp);
            }
            catch (Exception ex)
            {
                strCreate = "CREATE TABLE [#tmpmosesdates] ([bd_dt] [char] (8) NOT NULL )";
                ExecuteSQLTmp(strCreate, ObjConnectionTmp);
            }
        }
        public void prTempFOBill1(SqlConnection ObjConnectionTmp)
        {
            string strsql = string.Empty;
            try
            {
                strsql = " Drop Table #tmpfobill ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp);

            }
            catch (Exception ex)
            {
                strsql = "CREATE TABLE [#tmpfobill] ([tx_controlflag] numeric(18,3) NOT NULL , ";
                strsql = strsql + " [tx_dt] [char] (8) NOT NULL ,[tx_clientcd] [char] (8) NOT NULL , ";
                strsql = strsql + " [tx_mainbrcd] [char] (8) NOT NULL ,[tx_seriesid] [numeric]  NOT NULL , ";
                strsql = strsql + " [tx_desc] char(45) NOT NULL,[tx_bqty] numeric (18,3)  NOT NULL , ";
                strsql = strsql + " [tx_sqty] numeric(18,3)   NOT NULL ,[tx_rate] [money]  NOT NULL ,";
                strsql = strsql + " [tx_mainbrrate] [money]  NOT NULL ,[tx_marketrate] [money]  NOT NULL ,";
                strsql = strsql + " [tx_servicetax] [money]  NOT NULL ,[tx_closerate] [money]  NOT NULL ,";
                strsql = strsql + " [tx_sortlist] [numeric] NOT NULL,[tx_prodtype] [char] (2) NOT NULL,";
                strsql = strsql + " [tx_value] [money] NOT NULL,[tx_multiplier] [money] NOT Null, ";
                strsql = strsql + " tx_tradeid varchar(20) NOT NULL,tx_time char (8) NOT NULL,";
                strsql = strsql + " tx_orderid varchar(20) NOT NULL) ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp);
            }
            try
            {
                strsql = " Drop Table #tmpbillcharges ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp);
            }

            catch (Exception ex)
            {
                strsql = "CREATE TABLE [#tmpbillcharges] ([bc_dt] [char] (8) NOT NULL, ";
                strsql = strsql + "[bc_clientcd] [char] (8) NOT NULL,[bc_desc] [char] (40) NOT NULL, ";
                strsql = strsql + "[bc_amount] [money] NOT NULL,[bc_billno] [numeric] NOT NULL ) ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp);
            }

            try
            {
                strsql = " Drop Table #tmpmosesdates";
                ExecuteSQLTmp(strsql, ObjConnectionTmp);
            }

            catch (Exception ex)
            {
                strsql = "CREATE TABLE [#tmpmosesdates] ([bd_dt] [char] (8) NOT NULL )";
                ExecuteSQLTmp(strsql, ObjConnectionTmp);
            }
        }
        //public DataSet fnForBill(string strclientid, string strExcode, string StrFromDt, string StrToDt, string Exchange, string Segment, SqlConnection ObjConnectionTmp)
        //{
        //    if ((Exchange != "MCX-COMM") && (Exchange != "NCDEX-COMM") && (Exchange != "ICEX-COMM") && (Exchange != "NCME-COMM") && (Exchange != "MCX") && (Exchange != "NCDEX") && (Exchange != "ICEX") && (Exchange != "NCME"))
        //    {
        //        prTempFOBill(ObjConnectionTmp);

        //        string strinsert = string.Empty;
        //        strinsert = "insert into #tmpmosesdates values('" + StrFromDt + "') ";
        //        ExecuteSQLTmp(strinsert, ObjConnectionTmp);

        //        string StrExchWhere = "";
        //        string strIndexName = string.Empty;
        //        string strsql = string.Empty;

        //        strIndexName = "idx_trades_dt_clientcd";
        //        strsql = "Select Name from sysindexes where Name= 'idx_trades_clientcd'";
        //        DataSet ObjIndexds = OpenDataSet(strsql);
        //        if (ObjIndexds.Tables[0].Rows.Count > 0)
        //        {
        //            strIndexName = "idx_trades_clientcd";
        //        }

        //        if (strExcode.Contains("IOP"))
        //        {
        //            StrExchWhere = "";
        //            strExcode = Strings.Right(strExcode, 1);
        //        }
        //        else
        //        { StrExchWhere = "and td_exchange = '" + strExcode + "'"; }

        //        strinsert = " insert into  #tmpfobill  select 1 td_controlflag,'" + StrFromDt + "',td_clientcd, ";
        //        strinsert = strinsert + " td_mainbrcd, td_seriesid,'',case sign(sum(td_bqty - td_sqty)) when 1 then abs(sum(td_bqty - td_sqty)) else 0 end  td_bqty, case sign(sum(td_bqty - td_sqty)) when 1 then 0 else abs(sum(td_bqty - td_sqty)) end td_sqty, 0.0000 td_rate,0.0000 td_mainbrrate,0.0000 td_mainbrrate, 0.0000 td_servicetax,0.0000 td_closeprice, case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'EF' then 2 when 'IO' then 5 else 6 end td_sortlist, sm_prodtype,0 ,td_exchange ";
        //        strinsert = strinsert + " ,0,0 From Trades with(nolock,index(" + strIndexName + ")) , Series_master with(nolock),Client_master with(nolock)";
        //        strinsert = strinsert + " Where td_clientcd = cm_cd and td_exchange = sm_exchange and td_Segment = sm_Segment And td_seriesid = sm_seriesid and sm_expirydt >= '" + StrFromDt + "' and  td_dt < '" + StrFromDt + "' " + StrExchWhere + " and td_Segment = '" + Segment + "' and sm_prodtype in('IF','EF','CF')  and ltrim(rtrim(td_groupid)) <> 'B'  and td_clientcd = '" + strclientid + "' group by td_clientcd,td_mainbrcd,td_seriesid,sm_prodtype,td_exchange having sum(td_bqty - td_sqty) <> 0 ";
        //        ExecuteSQLTmp(strinsert, ObjConnectionTmp);

        //        strinsert = " insert into #tmpfobill select 2 td_controlflag,td_dt,td_clientcd,td_mainbrcd, td_seriesid,'',td_bqty,td_sqty, td_rate, td_mainbrrate, td_mainbrrate, td_servicetax,0.0000 td_closeprice, case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'EF' then 2 when 'IO' then 5 else 6 end td_sortlist, sm_prodtype,0,td_exchange ";
        //        strinsert = strinsert + " ,convert(decimal (15,2),td_marketrate) as tx_marketrate1,convert(decimal (15,4),td_brokerage) tx_Brokerage From Trades with(nolock,index(" + strIndexName + ")) , Series_master with(nolock),Client_master with(nolock)";
        //        strinsert = strinsert + " Where td_clientcd = cm_cd and td_exchange = sm_exchange and td_Segment = sm_Segment and td_seriesid = sm_seriesid and sm_expirydt >= '" + StrFromDt + "' and  td_dt between '" + StrFromDt + "' and '" + StrFromDt + "' " + StrExchWhere + " and td_Segment = '" + Segment + "' and cm_cd = '" + strclientid + "' Order By td_tradeid , td_subtradeid ";
        //        ExecuteSQLTmp(strinsert, ObjConnectionTmp);

        //        strinsert = " insert into #tmpfobill select 2 td_controlflag,td_dt,td_clientcd,td_mainbrcd, td_seriesid,'',td_sqty,td_bqty, convert(decimal (15,2),td_marketrate) as tx_marketrate1, td_mainbrrate, td_mainbrrate, 0,0.0000 td_closeprice, case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'EF' then 2 when 'IO' then 5 else 6 end td_sortlist, sm_prodtype,0,td_exchange ";
        //        strinsert = strinsert + " ,convert(decimal (15,2),td_marketrate) as tx_marketrate1,convert(decimal (15,4),td_brokerage) tx_Brokerage From Trades with(nolock,index(" + strIndexName + ")) , Series_master,Client_master ";
        //        strinsert = strinsert + " Where td_clientcd = cm_cd and td_exchange = sm_exchange and td_Segment = sm_Segment and td_seriesid = sm_seriesid and sm_expirydt >= '" + StrFromDt + "' and  td_dt between '" + StrFromDt + "' and '" + StrFromDt + "' " + StrExchWhere + " and td_Segment = '" + Segment + "' and ltrim(rtrim(td_groupid)) = 'B'  and td_clientcd = '" + strclientid + "' Order By td_tradeid , td_subtradeid ";
        //        ExecuteSQLTmp(strinsert, ObjConnectionTmp);

        //        strinsert = " insert into #tmpfobill  select 99 tx_controlflag,bd_dt,tx_clientcd,tx_mainbrcd, tx_seriesid,'',  case sign(sum(tx_bqty - tx_sqty)) when 1 then abs(sum(tx_bqty - tx_sqty)) else 0 end  tx_bqty,  case sign(sum(tx_bqty - tx_sqty)) when 1 then 0 else abs(sum(tx_bqty - tx_sqty)) end tx_sqty,  0.0000 tx_rate,0.0000 tx_mainbrrate,0.0000 tx_mainbrrate, 0.0000 tx_servicetax,0.0000 tx_closeprice,  case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'EF' then 2 when 'IO' then 5 else 6 end tx_sortlist, sm_prodtype,0 ,sm_exchange";
        //        strinsert = strinsert + " ,0,0 From #tmpfobill  , #tmpmosesdates , Series_master with (nolock),Client_master with (nolock)";
        //        strinsert = strinsert + " Where tx_clientcd = cm_cd and sm_exchange = '" + strExcode + "' and sm_Segment = '" + Segment + "' And tx_seriesid = sm_seriesid  and sm_expirydt >= bd_dt and  tx_dt < bd_dt  and sm_prodtype in('IF','EF','CF')  and tx_controlflag not in ( '99','3')  group by bd_dt,tx_clientcd,tx_mainbrcd,tx_seriesid,sm_prodtype,sm_exchange  Having Sum(tx_bqty - tx_sqty) <> 0 ";
        //        strinsert = strinsert + " Union All ";
        //        strinsert = strinsert + " select 3 tx_controlflag,bd_dt,tx_clientcd,tx_mainbrcd, tx_seriesid,'', ";
        //        strinsert = strinsert + " case sign(sum(tx_bqty - tx_sqty)) when 1 then 0 else abs(sum(tx_bqty - tx_sqty)) end tx_bqty, ";
        //        strinsert = strinsert + " case sign(sum(tx_bqty - tx_sqty)) when 1 then abs(sum(tx_bqty - tx_sqty)) else 0 end  tx_sqty, ";
        //        strinsert = strinsert + " 0.0000 tx_rate,0.0000 tx_mainbrrate,0.0000 tx_mainbrrate, 0.0000 tx_servicetax,0.0000 tx_closeprice, ";
        //        strinsert = strinsert + " case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'EF' then 2 when 'IO' then 5 else 6 end tx_sortlist, sm_prodtype,0,sm_exchange ";
        //        strinsert = strinsert + " ,0,0 From #tmpfobill  , #tmpmosesdates , Series_master with (nolock),Client_master with (nolock) ";
        //        strinsert = strinsert + " Where tx_clientcd = cm_cd and sm_exchange = '" + strExcode + "' and sm_Segment = '" + Segment + "' And tx_seriesid = sm_seriesid ";
        //        strinsert = strinsert + " and sm_expirydt >= bd_dt and  tx_dt <= bd_dt ";
        //        strinsert = strinsert + " and sm_prodtype in('IF','EF','CF') ";
        //        strinsert = strinsert + " and tx_controlflag not in ( '99','3') ";
        //        strinsert = strinsert + " group by bd_dt,tx_clientcd,tx_mainbrcd,tx_seriesid,sm_prodtype,sm_exchange ";
        //        strinsert = strinsert + " Having Sum(tx_bqty - tx_sqty) <> 0 ";
        //        ExecuteSQLTmp(strinsert, ObjConnectionTmp);

        //        strinsert = "Update #tmpfobill set tx_controlflag = '1' where tx_controlflag = '99' ";
        //        ExecuteSQLTmp(strinsert, ObjConnectionTmp);

        //        strinsert = "insert into #tmpfobill select case ex_eaflag when 'E' then 5 else 6 end td_controlflag,ex_dt,ex_clientcd,ex_mainbrcd, ex_seriesid,'',ex_eqty,ex_aqty, ex_diffbrokrate,ex_mainbrdiffrate,ex_mainbrdiffrate, ex_servicetax,ex_settlerate, case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'EF' then 2 when 'IO' then 5 else 6 end + 3 td_sortlist, sm_prodtype,0 ,ex_exchange";
        //        strinsert = strinsert + " ,convert(decimal (15,2),ex_diffrate) as tx_marketrate1 ,convert(decimal(15,2),ex_brokerage) tx_Brokerage From Exercise with (nolock), Series_master with (nolock) ,Client_master with (nolock) ";
        //        strinsert = strinsert + "Where ex_clientcd = cm_cd and ex_exchange = sm_exchange and ex_Segment = sm_Segment And ex_seriesid = sm_seriesid and sm_expirydt >= '" + StrFromDt + "' and ex_dt between '" + StrFromDt + "' and '" + StrFromDt + "' " + StrExchWhere.Replace("td_", "ex_") + " and ex_Segment = '" + Segment + "' and cm_cd = '" + strclientid + "' ";
        //        ExecuteSQLTmp(strinsert, ObjConnectionTmp);

        //        strinsert = "insert into #tmpbillcharges select tx_dt,tx_clientcd,'SERVICE TAX',round(sum(tx_servicetax),2),0,tx_exchange ";
        //        strinsert = strinsert + " from #tmpfobill,#tmpmosesdates,Client_master with (nolock) ";
        //        strinsert = strinsert + " where tx_clientcd = cm_cd and tx_dt = bd_dt and cm_cd = '" + strclientid + "' group by tx_dt,tx_clientcd,tx_exchange having sum(tx_servicetax) > 0 ";
        //        ExecuteSQLTmp(strinsert, ObjConnectionTmp);

        //        strinsert = "update #tmpfobill set tx_closerate = ms_lastprice ";
        //        strinsert = strinsert + " from #tmpfobill, Market_summary with (nolock) ";
        //        strinsert = strinsert + " where ms_seriesid = tx_seriesid and tx_controlflag in ('1','2') and ms_exchange=tx_exchange and ms_Segment = '" + Segment + "' and ms_dt = tx_dt ";
        //        ExecuteSQLTmp(strinsert, ObjConnectionTmp);

        //        strinsert = "update #tmpfobill set tx_rate = ms_prcloseprice ";
        //        strinsert = strinsert + " from #tmpfobill,Market_summary with (nolock) ";
        //        strinsert = strinsert + " where ms_seriesid = tx_seriesid and tx_controlflag = 1 and ms_exchange = tx_Exchange  and ms_Segment = '" + Segment + "' and ms_dt = tx_dt ";
        //        ExecuteSQLTmp(strinsert, ObjConnectionTmp);

        //        strinsert = "update #tmpfobill set tx_rate = ms_lastprice from #tmpfobill,Market_summary with (nolock) ";
        //        strinsert = strinsert + " where ms_seriesid = tx_seriesid and tx_controlflag = 3";
        //        strinsert = strinsert + " and ms_exchange = tx_Exchange  and ms_Segment = '" + Segment + "'";
        //        strinsert = strinsert + " and ms_dt = tx_dt";
        //        ExecuteSQLTmp(strinsert, ObjConnectionTmp);

        //        strinsert = "insert into #tmpbillcharges select fc_dt,fc_clientcd,fc_desc,round(sum(fc_amount),2),0,fc_exchange ";
        //        strinsert = strinsert + " from Fspecialcharges with (nolock) ,#tmpmosesdates,Client_master with (nolock) ";
        //        strinsert = strinsert + " where fc_clientcd = cm_cd and fc_dt = bd_dt and cm_cd = '" + strclientid + "' " + StrExchWhere.Replace("td_", "fc_") + " and fc_Segment = '" + Segment + "' group by fc_dt,fc_clientcd,fc_desc,fc_exchange having round(sum(fc_amount),2) <> 0 ";
        //        ExecuteSQLTmp(strinsert, ObjConnectionTmp);

        //        strinsert = "insert into #tmpbillcharges select fc_dt,fc_clientcd,'SERVICE TAX',round(sum(fc_servicetax),2),0,fc_exchange ";
        //        strinsert = strinsert + " from Fspecialcharges with (nolock) ,#tmpmosesdates,Client_master with (nolock) ";
        //        strinsert = strinsert + " where fc_clientcd = cm_cd and fc_dt = bd_dt and cm_cd = '" + strclientid + "' " + StrExchWhere.Replace("td_", "fc_") + " and fc_Segment = '" + Segment + "' group by fc_dt,fc_clientcd,fc_desc,fc_exchange having round(sum(fc_servicetax),2) <> 0 ";
        //        ExecuteSQLTmp(strinsert, ObjConnectionTmp);

        //        strinsert = "update #tmpfobill set tx_value = round(((case tx_controlflag when 5  then (tx_bqty + tx_sqty)*-1 when 6 then (tx_bqty + tx_sqty)*-1 else (tx_bqty - tx_sqty) end) *tx_rate)*sm_multiplier,4)";
        //        strinsert = strinsert + " From series_master with (nolock) ";
        //        strinsert = strinsert + " Where sm_Segment = '" + Segment + "' and sm_exchange = tx_Exchange and tx_seriesid = sm_seriesid ";
        //        ExecuteSQLTmp(strinsert, ObjConnectionTmp);

        //        strinsert = "insert into #tmpfobill select 10 ,bc_dt,bc_clientcd,bc_clientcd, 1,upper(bc_desc),0 td_bqty,0 td_sqty, 0,0 td_mainbrrate,0 td_mainbrrate, 0 td_servicetax,0.0000 td_closeprice, 10 td_sortlist, 'XX',round(sum(bc_amount),2),bc_exchange,0,0 From #tmpbillcharges group by bc_dt,bc_clientcd,bc_desc,bc_exchange ";
        //        ExecuteSQLTmp(strinsert, ObjConnectionTmp);

        //        strinsert = "insert into #tmpfobill select 90 ,fb_billdt,fb_clientcd,fb_clientcd, 1,'[PREV. DAY MRGN.]',0 td_bqty,0 td_sqty, 0,0 td_mainbrrate,0 td_mainbrrate, 0 td_servicetax,0.0000 td_closeprice, 90 td_sortlist, 'XX',round(Case when fb_postmrgyn = 'Y' then fb_margin1 else 0 end + CAse When fb_postExpmrgyn = 'Y' then fb_Expmargin1 else 0 end ,2),fb_exchange ";
        //        strinsert = strinsert + " ,0,0 From #tmpmosesdates,Fbills with (nolock) ,Client_master with (nolock) ";
        //        strinsert = strinsert + " where fb_clientcd = cm_cd and fb_billdt = bd_dt and fb_exchange = '" + strExcode + "' and fb_Segment = '" + Segment + "' and round(Case when fb_postmrgyn = 'Y' then fb_margin1 else 0 end + CAse When fb_postExpmrgyn = 'Y' then fb_Expmargin1 else 0 end ,2) <> 0  and cm_cd = '" + strclientid + "' ";
        //        ExecuteSQLTmp(strinsert, ObjConnectionTmp);

        //        strinsert = "insert into #tmpfobill select 91 ,fb_billdt,fb_clientcd,fb_clientcd, 1,'[CURR. DAY MRGN.]',0 td_bqty,0 td_sqty, 0,0 td_mainbrrate,0 td_mainbrrate, 0 td_servicetax,0.0000 td_closeprice, 91 td_sortlist, 'XX',round(Case When fb_postmrgyn = 'Y' then fb_margin2 else 0 end + Case When fb_postExpmrgyn = 'Y' then fb_Expmargin2 else 0 end,2),fb_exchange ";
        //        strinsert = strinsert + " ,0,0 From #tmpmosesdates,Fbills with (nolock) ,Client_master with (nolock) ";
        //        strinsert = strinsert + " where fb_clientcd = cm_cd and fb_billdt = bd_dt and fb_exchange = '" + strExcode + "' and fb_Segment = '" + Segment + "' and round(Case When fb_postmrgyn = 'Y' then fb_margin2 else 0 end + Case When fb_postExpmrgyn = 'Y' then fb_Expmargin2 else 0 end,2) <> 0  and cm_cd = '" + strclientid + "' ";
        //        ExecuteSQLTmp(strinsert, ObjConnectionTmp);

        //        strinsert = "alter table #tmpfobill add tx_billno numeric default(0) NOT NULL ";
        //        ExecuteSQLTmp(strinsert, ObjConnectionTmp);

        //        strinsert = "update #tmpfobill set tx_billno = fb_billno from #tmpfobill,Fbills with (nolock) where fb_clientcd = tx_clientcd  and fb_exchange = tx_exchange and fb_Segment = '" + Segment + "' and fb_billdt = tx_dt  ";
        //        ExecuteSQLTmp(strinsert, ObjConnectionTmp);

        //        strinsert = "select tx_sortlist,tx_dt as dt,tx_billno,case tx_controlflag when '1' then 'b/f' when '3' then 'c/f' else convert(char,convert(datetime,tx_dt),103) end tx_dt,tx_clientcd, tx_seriesid,sm_sname as tx_desc, cast((tx_bqty)as decimal(15,0))as tx_bqty,cast((tx_sqty)as decimal(15,0))as tx_sqty,cast((tx_rate)as decimal(15,4))as tx_rate,cast((tx_closerate)as decimal(15,4))as tx_closerate,cast((tx_value)as decimal(15,2))as drcr,cast((((tx_bqty-tx_sqty) * tx_rate)) as decimal(15,2)) value,sm_sname,sm_desc,sm_productcd,sm_symbol, ";
        //        strinsert = strinsert + " sm_expirydt,sm_strikeprice,sm_callput,sm_optionstyle,cm_name, ";
        //        strinsert = strinsert + " cm_add1,cm_add2,cm_add3,cm_tele1,cm_tele2,cm_email,cm_sebino, cm_panno, cm_add4, cm_pincode,sm_prodtype,cm_groupcd,cm_familycd,cm_brboffcode,cm_subbroker,sm_multiplier, ";
        //        strinsert = strinsert + " cm_introducer ,replicate(' ',8-len(ltrim(rtrim(tx_clientcd)))) + ltrim(rtrim(tx_clientcd)) ,case tx_controlflag when '1' then 0 when '3' then 2 else 1 end ord, '' as NetValue ";
        //        strinsert = strinsert + " ,tx_marketrate1,tx_Brokerage from #tmpfobill,Series_master with (nolock) ,Client_master with (nolock) ";
        //        strinsert = strinsert + " where tx_clientcd = cm_cd and tx_seriesid = sm_seriesid and tx_exchange = sm_exchange and sm_Segment = '" + Segment + "' and tx_controlflag < 10 ";
        //        strinsert = strinsert + " union all ";
        //        strinsert = strinsert + " select tx_sortlist,tx_dt as dt,tx_billno,case tx_controlflag when '1' then 'b/f' when '3' then 'c/f' else convert(char,convert(datetime,tx_dt),103) end tx_dt,tx_clientcd, tx_seriesid,tx_desc, cast((tx_bqty)as decimal(15,0))as tx_bqty,cast((tx_sqty)as decimal(15,0))as tx_sqty,cast((tx_rate)as decimal(15,4))as tx_rate,cast((tx_closerate)as decimal(15,4))as tx_closerate,cast((tx_value)as decimal(15,2)) as drcr,cast((((tx_bqty-tx_sqty) * tx_rate))as decimal(15,2)) value, tx_desc as sm_sname,'' sm_desc,'' sm_productcd,'' sm_symbol,'' sm_expirydt, 0 sm_strikeprice, 'X' sm_callput,'X' sm_optionstyle, cm_name,cm_add1,cm_add2,cm_add3,cm_tele1,cm_tele2,cm_email, cm_sebino, cm_panno,cm_add4, ";
        //        strinsert = strinsert + " cm_pincode,'EF' as sm_prodtype,cm_groupcd,cm_familycd,cm_brboffcode, ";
        //        strinsert = strinsert + " cm_subbroker,0 sm_multiplier,cm_introducer ,replicate('',8-len(ltrim(rtrim(tx_clientcd)))) + ltrim(rtrim(tx_clientcd)),case tx_controlflag when '1' then 0 when '3' then 2 else 1 end ord, '' as NetValue  ";
        //        strinsert = strinsert + " ,tx_marketrate1,tx_Brokerage from #tmpfobill,Client_master with (nolock) ";
        //        strinsert = strinsert + " where tx_clientcd = cm_cd and tx_controlflag >= 10 ";
        //        strinsert = strinsert + " order by  replicate(' ',8-len(ltrim(rtrim(tx_clientcd)))) + ltrim(rtrim(tx_clientcd))  ,dt ,tx_sortlist,sm_symbol,tx_seriesid,tx_desc  , ord ";

        //        DataSet objDataset = new DataSet();
        //        objDataset = OpenDataSetTmp(strinsert, ObjConnectionTmp);

        //        string StrDrop = string.Empty;
        //        prTempFOBill(ObjConnectionTmp);
        //        return objDataset;

        //    }

        //    else //MCX And NCDEX
        //    {
        //        prTempFOBill1(ObjConnectionTmp);
        //        string strsql = string.Empty;

        //        strsql = "insert into #tmpmosesdates values('" + StrFromDt + "') ";
        //        ExecuteSQLTmp(strsql, ObjConnectionTmp);

        //        string strDelivery = string.Empty;
        //        string strSeries_master = string.Empty;
        //        string strClient_master = string.Empty;
        //        string strTrades = string.Empty;
        //        string strMarket_summary = string.Empty;
        //        string strFspecialcharges = string.Empty;
        //        string strproduct_master = string.Empty;
        //        string strFbills = string.Empty;


        //        if (ConfigurationManager["IsTradeWeb"] == "O")
        //        {
        //            if (ConfigurationManager["Commex"] != null || ConfigurationManager["Commex"] != string.Empty)
        //            {
        //                string StrCommexConn = "";
        //                StrCommexConn = GetCommexConnection();

        //                strSeries_master = StrCommexConn + ".Series_master";
        //                strClient_master = StrCommexConn + ".Client_master";
        //                strTrades = StrCommexConn + ".Trades";
        //                strDelivery = StrCommexConn + ".Delivery";
        //                strMarket_summary = StrCommexConn + ".Market_summary";
        //                strFspecialcharges = StrCommexConn + ".Fspecialcharges";
        //                strproduct_master = StrCommexConn + ".product_master";
        //                strFbills = StrCommexConn + ".Fbills";
        //            }
        //        }
        //        strsql = "insert into #tmpfobill select 1 td_controlflag,bd_dt,td_clientcd,td_mainbrcd, td_seriesid,'',case sign(sum(td_bqty - td_sqty)) when 1 then abs(sum(td_bqty - td_sqty)) else 0 end  td_bqty, case sign(sum(td_bqty - td_sqty)) when 1 then 0 else abs(sum(td_bqty - td_sqty)) end td_sqty, 0.0000 td_rate,0.0000 td_mainbrrate,0.0000 td_mainbrrate, 0.0000 td_servicetax,0.0000 td_closeprice, case sm_prodtype when 'CF' then 1 when 'CO' then 2 else 6 end td_sortlist, sm_prodtype,0, sm_multiplier , 0 td_tradeid, '' td_time, 0 td_orderid";
        //        strsql = strsql + " From " + strTrades + ", #tmpmosesdates, " + strSeries_master + "," + strClient_master + " ";
        //        strsql = strsql + "Where td_clientcd = cm_cd and td_exchange = sm_exchange And td_seriesid = sm_seriesid and sm_expirydt >= bd_dt and  td_dt < bd_dt and td_exchange = '" + strExcode + "' and sm_prodtype in('CF') and cm_cd = '" + strclientid + "' group by bd_dt,td_clientcd,td_mainbrcd,td_seriesid,sm_prodtype, sm_multiplier   having sum(td_bqty - td_sqty) <> 0 ";
        //        ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

        //        strsql = "insert into #tmpfobill select 2 td_controlflag,bd_dt,td_clientcd,td_mainbrcd, td_seriesid,'',td_bqty,td_sqty, td_rate,td_mainbrrate,td_mainbrrate, td_servicetax,0.0000 td_closeprice, case sm_prodtype when 'CF' then 1 when 'CO' then 2 else 6 end td_sortlist, sm_prodtype, 0, sm_multiplier, td_tradeid, td_time, td_orderid";
        //        strsql = strsql + " From " + strTrades + ", #tmpmosesdates, " + strSeries_master + "," + strClient_master + " ";
        //        strsql = strsql + "Where td_clientcd = cm_cd and td_exchange = sm_exchange and td_seriesid = sm_seriesid and sm_expirydt >= bd_dt and  td_dt = bd_dt and td_exchange = '" + strExcode + "' and cm_cd = '" + strclientid + "' ";
        //        ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

        //        strsql = " insert into #tmpfobill  select Case dl_type When 'DL' Then 7 When 'PD' Then 8 When 'SL' Then 9 When 'DS' Then 9.5 Else '' End td_controlflag, dl_BillDate,dl_clientcd,dl_mainbrcd, dl_seriesid,'',  dl_Bqty , dl_SQty ,  dl_rate , dl_mainbrrate, dl_marketrate, dl_servicetax,0,  case sm_prodtype when 'CF' then 1 when 'CO' then 2 else 6 end + 3 td_sortlist, sm_prodtype, (dl_bqty - dl_sQty) * dl_Rate * sm_multiplier  , sm_multiplier , 0 tx_tradeid, '' tx_time, 0 tx_orderid ";
        //        strsql = strsql + "  From " + strDelivery + ", #tmpmosesdates, " + strSeries_master + "," + strClient_master + " ";
        //        strsql = strsql + "  Where dl_clientcd = cm_cd And dl_exchange = sm_exchange And dl_seriesid = sm_seriesid  and  dl_BillDate = bd_dt  and dl_exchange = '" + strExcode + "' and dl_type In ('DL','SL','PD','DS')  and cm_cd = '" + strclientid + "' ";
        //        ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

        //        strsql = "insert into #tmpfobill select 3 td_controlflag,bd_dt,td_clientcd,td_mainbrcd, td_seriesid,'',case sign(sum(td_bqty - td_sqty)) when 1 then 0 else abs(sum(td_bqty - td_sqty)) end  td_bqty, case sign(sum(td_bqty - td_sqty)) when 1 then abs(sum(td_bqty - td_sqty)) else 0 end td_sqty, 0.0000 td_rate,0.0000 td_mainbrrate,0.0000 td_mainbrrate, 0.0000 td_servicetax,0.0000 td_closeprice, case sm_prodtype when 'CF' then 1 when 'CO' then 2 else 6 end td_sortlist, sm_prodtype,0, sm_multiplier, 0,'',0       ";
        //        strsql = strsql + " From " + strTrades + ", #tmpmosesdates, " + strSeries_master + "," + strClient_master + " ";
        //        strsql = strsql + " Where td_clientcd = cm_cd and td_exchange = sm_exchange And td_seriesid = sm_seriesid and sm_expirydt >= bd_dt and  td_dt <= bd_dt and td_exchange = '" + strExcode + "' and sm_prodtype in('CF') and cm_cd = '" + strclientid + "' group by bd_dt,td_clientcd,td_mainbrcd,td_seriesid,sm_prodtype, sm_multiplier having sum(td_bqty - td_sqty) <> 0";
        //        ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

        //        strsql = "insert into #tmpbillcharges select tx_dt,tx_clientcd,'SERVICE TAX',round(sum(tx_servicetax),2),0 ";
        //        strsql = strsql + " from #tmpfobill,#tmpmosesdates," + strClient_master + " ";
        //        strsql = strsql + "where tx_clientcd = cm_cd and tx_dt = bd_dt and cm_cd = '" + strclientid + "' group by tx_dt,tx_clientcd having sum(tx_servicetax) > 0 ";
        //        ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

        //        strsql = "update #tmpfobill set tx_closerate = ms_lastprice ";
        //        strsql = strsql + " from #tmpfobill, " + strMarket_summary + " ";
        //        strsql = strsql + " where ms_seriesid = tx_seriesid and tx_controlflag in ('1','2') and ms_exchange = '" + strExcode + "' and ms_dt = tx_dt ";
        //        ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

        //        strsql = "update #tmpfobill set tx_rate = ms_prcloseprice ";
        //        strsql = strsql + " from #tmpfobill," + strMarket_summary + " ";
        //        strsql = strsql + " where ms_seriesid = tx_seriesid and tx_controlflag = 1 and ms_exchange = '" + strExcode + "' and ms_dt = tx_dt ";
        //        ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

        //        strsql = "update #tmpfobill set tx_rate = ms_lastprice ";
        //        strsql = strsql + " from #tmpfobill, " + strMarket_summary + " ";
        //        strsql = strsql + " where ms_seriesid = tx_seriesid and tx_controlflag = 3 and ms_exchange = '" + strExcode + "' and ms_dt = tx_dt ";
        //        ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

        //        strsql = "insert into #tmpbillcharges select fc_dt,fc_clientcd,fc_desc,round(sum(fc_amount),2),0";
        //        strsql = strsql + " from " + strFspecialcharges + ",#tmpmosesdates," + strClient_master + " ";
        //        strsql = strsql + " where fc_clientcd = cm_cd and fc_dt = bd_dt and cm_cd = '" + strclientid + "' and fc_exchange = '" + strExcode + "' group by fc_dt,fc_clientcd,fc_desc having round(sum(fc_amount),2) <> 0 ";
        //        ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

        //        strsql = "insert into #tmpbillcharges select fc_dt,fc_clientcd,'SERVICE TAX',round(sum(fc_servicetax),2),0 ";
        //        strsql = strsql + " from " + strFspecialcharges + ",#tmpmosesdates," + strClient_master + " ";
        //        strsql = strsql + " where fc_clientcd = cm_cd and fc_dt = bd_dt and cm_cd = '" + strclientid + "' and fc_exchange = '" + strExcode + "' group by fc_dt,fc_clientcd,fc_desc having round(sum(fc_servicetax),2) <> 0 ";
        //        ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

        //        strsql = "update #tmpfobill set tx_value = round(((case tx_controlflag when 5  then (tx_bqty + tx_sqty)*0 when 6 then (tx_bqty + tx_sqty)*0 else (tx_bqty - tx_sqty) end) *tx_rate)*tx_multiplier,4) ";
        //        ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

        //        strsql = "insert into #tmpfobill select 10 ,bc_dt,bc_clientcd,bc_clientcd, 1,upper(bc_desc),0 td_bqty,0 td_sqty, 0,0 td_mainbrrate,0 td_mainbrrate, 0 td_servicetax,0.0000 td_closeprice, 10 td_sortlist, 'XX',round(sum(bc_amount),2),0,0,'',0 ";
        //        strsql = strsql + " From #tmpbillcharges group by bc_dt,bc_clientcd,bc_desc ";
        //        ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

        //        strsql = "insert into #tmpfobill select 90 ,fb_billdt,fb_clientcd,fb_clientcd, 1,'[PREV. DAY MRGN.]',0 td_bqty,0 td_sqty, 0,0 td_mainbrrate,0 td_mainbrrate, 0 td_servicetax,0.0000 td_closeprice, 90 td_sortlist, 'XX',round(fb_margin1,2),0, 0,'',0 ";
        //        strsql = strsql + " From #tmpmosesdates," + strFbills + "," + strClient_master + " ";
        //        strsql = strsql + " where fb_clientcd = cm_cd and fb_billdt = bd_dt and fb_exchange = '" + strExcode + "' and fb_margin1 <> 0 and fb_postmrgyn = 'Y' and cm_cd = '" + strclientid + "' ";
        //        ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

        //        strsql = "insert into #tmpfobill select 91 ,fb_billdt,fb_clientcd,fb_clientcd, 1,'[CURR. DAY MRGN.]',0 td_bqty,0 td_sqty, 0,0 td_mainbrrate,0 td_mainbrrate, 0 td_servicetax,0.0000 td_closeprice, 91 td_sortlist, 'XX',round(fb_margin2,2),0, 0, '', 0 ";
        //        strsql = strsql + " From #tmpmosesdates," + strFbills + "," + strClient_master + " ";
        //        strsql = strsql + " where fb_clientcd = cm_cd and fb_billdt = bd_dt and fb_exchange = '" + strExcode + "' and fb_margin2 <> 0 and fb_postmrgyn = 'Y' and cm_cd = '" + strclientid + "' ";
        //        ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

        //        strsql = "alter table #tmpfobill add tx_billno numeric default(0) NOT NULL ";
        //        ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

        //        strsql = "update #tmpfobill set tx_billno = fb_billno ";
        //        strsql = strsql + " from #tmpfobill," + strFbills + " ";
        //        strsql = strsql + " where fb_clientcd = tx_clientcd  and fb_exchange = '" + strExcode + "' and fb_billdt = tx_dt ";
        //        ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

        //        strsql = "alter table #tmpfobill add tx_unit char (15) ";
        //        ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

        //        strsql = "update a Set tx_unit=left(rtrim(convert(char,convert(numeric,floor(pm_divisor))))+' '+pm_unitper,15) ";
        //        strsql = strsql + " from #tmpfobill a, " + strSeries_master + ", " + strproduct_master + " ";
        //        strsql = strsql + " where tx_seriesid = sm_seriesid and sm_exchange='" + strExcode + "' and sm_prodtype=pm_type and sm_exchange=pm_exchange and sm_symbol=pm_assetcd ";
        //        ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

        //        strsql = "select tx_sortlist,tx_dt as dt,tx_billno,case tx_controlflag when '1' then 'b/f' when '3' then 'c/f' else convert(char,convert(datetime,tx_dt),103) end tx_dt,tx_clientcd, tx_seriesid,sm_sname as tx_desc,cast((tx_bqty) as decimal(15,2)) as tx_bqty,cast((tx_sqty) as decimal(15,2)) as tx_sqty,cast((tx_rate) as decimal(15,4)) as tx_rate,cast((tx_closerate) as decimal(15,4)) as tx_closerate,cast((tx_value) as decimal(15,2)) as drcr,cast((((tx_bqty-tx_sqty) * tx_rate)) as decimal(15,2))  value,tx_multiplier, sm_sname,sm_desc,sm_productcd,sm_symbol, sm_expirydt,sm_strikeprice, sm_callput,sm_optionstyle,cm_name,cm_add1, cm_add2,cm_add3,cm_tele1, cm_tele2,cm_email, cm_sebino, cm_panno, cm_add4, cm_pincode, sm_prodtype,cm_groupcd,cm_familycd, cm_brboffcode, tx_tradeid,";
        //        strsql = strsql + " case tx_controlflag when '1' then 'b/f' when '3' then 'c/f' else tx_time end tx_time, tx_orderid, tx_marketrate, tx_unit,case tx_controlflag when '1' then 0 when '3' then 2 else 1 end ord , '' as NetValue";
        //        strsql = strsql + " from #tmpfobill," + strSeries_master + "," + strClient_master + " ";
        //        strsql = strsql + " where tx_clientcd = cm_cd and tx_seriesid = sm_seriesid and sm_exchange = '" + strExcode + "' and tx_controlflag < 10 and cm_brboffcode <> '' ";
        //        strsql = strsql + "union all ";
        //        strsql = strsql + "select tx_sortlist,tx_dt as dt,tx_billno,case tx_controlflag when '1' then 'b/f' when '3' then 'c/f' else convert(char,convert(datetime,tx_dt),103) end tx_dt,tx_clientcd, tx_seriesid,tx_desc, cast((tx_bqty) as decimal(15,2)) as tx_bqty, cast((tx_sqty) as decimal(15,4)) as tx_sqty,tx_rate,cast((tx_closerate) as decimal(15,4)) as tx_closerate ,cast((tx_value) as decimal(15,2)) as drcr,cast((((tx_bqty-tx_sqty) * tx_rate)) as decimal(15,2))  value,tx_multiplier, tx_desc as  sm_sname,'' sm_desc,'' sm_productcd,'' sm_symbol,'' sm_expirydt, 0 sm_strikeprice, 'X' sm_callput,'X' sm_optionstyle, cm_name,cm_add1,cm_add2,cm_add3,cm_tele1,cm_tele2,cm_email, cm_sebino, cm_panno,cm_add4, cm_pincode,'EF' as sm_prodtype,cm_groupcd,cm_familycd, cm_brboffcode, tx_tradeid,";
        //        strsql = strsql + "case tx_controlflag when '1' then 'b/f' when '3' then 'c/f' else tx_time end tx_time, tx_orderid, tx_marketrate, tx_unit,case tx_controlflag when '1' then 0 when '3' then 2 else 1 end ord, '' as NetValue  ";
        //        strsql = strsql + " from #tmpfobill," + strClient_master + " ";
        //        strsql = strsql + " where tx_clientcd = cm_cd  and tx_controlflag >= 10 and cm_brboffcode <> ''  order by tx_clientcd,dt ,tx_sortlist,sm_symbol,tx_seriesid,tx_desc  , ord  ";
        //        DataSet objDataset = new DataSet();
        //        objDataset = OpenDataSetTmp(strsql, ObjConnectionTmp);

        //        string StrDrop = string.Empty;
        //        prTempFOBill1(ObjConnectionTmp);
        //        return objDataset;
        //    }
        //}

        public DataSet fnForBill(string strclientid, string strExcode, string StrFromDt, string StrToDt, string Exchange, string Segment, SqlConnection ObjConnectionTmp)
        {
            if ((Exchange != "MCX-COMM") && (Exchange != "NCDEX-COMM") && (Exchange != "ICEX-COMM") && (Exchange != "NCME-COMM") && (Exchange != "MCX") && (Exchange != "NCDEX") && (Exchange != "ICEX") && (Exchange != "NCME"))
            {
                prTempFOBill(ObjConnectionTmp);

                string strinsert = string.Empty;
                strinsert = "insert into #tmpmosesdates values('" + StrFromDt + "') ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                string StrExchWhere = "";
                string strIndexName = string.Empty;
                string strsql = string.Empty;

                strIndexName = "idx_trades_dt_clientcd";
                strsql = "Select Name from sysindexes where Name= 'idx_trades_clientcd'";
                DataSet ObjIndexds = OpenDataSet(strsql);
                if (ObjIndexds.Tables[0].Rows.Count > 0)
                {
                    strIndexName = "idx_trades_clientcd";
                }

                if (strExcode.Contains("IOP"))
                {
                    StrExchWhere = "";
                    strExcode = Strings.Right(strExcode, 1);
                }
                else
                { StrExchWhere = "and td_exchange = '" + strExcode + "'"; }

                strinsert = " insert into  #tmpfobill  select 1 td_controlflag,'" + StrFromDt + "',td_clientcd, ";
                strinsert = strinsert + " td_mainbrcd, td_seriesid,'',case sign(sum(td_bqty - td_sqty)) when 1 then abs(sum(td_bqty - td_sqty)) else 0 end  td_bqty, case sign(sum(td_bqty - td_sqty)) when 1 then 0 else abs(sum(td_bqty - td_sqty)) end td_sqty, 0.0000 td_rate,0.0000 td_mainbrrate,0.0000 td_mainbrrate, 0.0000 td_servicetax,0.0000 td_closeprice, case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'EF' then 2 when 'IO' then 5 else 6 end td_sortlist, sm_prodtype,0 ,td_exchange ";
                strinsert = strinsert + " ,0,0 From Trades with(nolock,index(" + strIndexName + ")) , Series_master with(nolock),Client_master with(nolock)";
                strinsert = strinsert + " Where td_clientcd = cm_cd and td_exchange = sm_exchange and td_Segment = sm_Segment And td_seriesid = sm_seriesid and sm_expirydt >= '" + StrFromDt + "' and  td_dt < '" + StrFromDt + "' " + StrExchWhere + " and td_Segment = '" + Segment + "' and sm_prodtype in('IF','EF','CF')  and ltrim(rtrim(td_groupid)) <> 'B'  and td_clientcd = '" + strclientid + "' group by td_clientcd,td_mainbrcd,td_seriesid,sm_prodtype,td_exchange having sum(td_bqty - td_sqty) <> 0 ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = " insert into #tmpfobill select 2 td_controlflag,td_dt,td_clientcd,td_mainbrcd, td_seriesid,'',td_bqty,td_sqty, td_rate, td_mainbrrate, td_mainbrrate, td_servicetax,0.0000 td_closeprice, case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'EF' then 2 when 'IO' then 5 else 6 end td_sortlist, sm_prodtype,0,td_exchange ";
                strinsert = strinsert + " ,convert(decimal (15,2),td_marketrate) as tx_marketrate1,convert(decimal (15,4),td_brokerage) tx_Brokerage From Trades with(nolock,index(" + strIndexName + ")) , Series_master with(nolock),Client_master with(nolock)";
                strinsert = strinsert + " Where td_clientcd = cm_cd and td_exchange = sm_exchange and td_Segment = sm_Segment and td_seriesid = sm_seriesid and sm_expirydt >= '" + StrFromDt + "' and  td_dt between '" + StrFromDt + "' and '" + StrFromDt + "' " + StrExchWhere + " and td_Segment = '" + Segment + "' and cm_cd = '" + strclientid + "' Order By td_tradeid , td_subtradeid ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = " insert into #tmpfobill select 2 td_controlflag,td_dt,td_clientcd,td_mainbrcd, td_seriesid,'',td_sqty,td_bqty, convert(decimal (15,2),td_marketrate) as tx_marketrate1, td_mainbrrate, td_mainbrrate, 0,0.0000 td_closeprice, case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'EF' then 2 when 'IO' then 5 else 6 end td_sortlist, sm_prodtype,0,td_exchange ";
                strinsert = strinsert + " ,convert(decimal (15,2),td_marketrate) as tx_marketrate1,convert(decimal (15,4),td_brokerage) tx_Brokerage From Trades with(nolock,index(" + strIndexName + ")) , Series_master,Client_master ";
                strinsert = strinsert + " Where td_clientcd = cm_cd and td_exchange = sm_exchange and td_Segment = sm_Segment and td_seriesid = sm_seriesid and sm_expirydt >= '" + StrFromDt + "' and  td_dt between '" + StrFromDt + "' and '" + StrFromDt + "' " + StrExchWhere + " and td_Segment = '" + Segment + "' and ltrim(rtrim(td_groupid)) = 'B'  and td_clientcd = '" + strclientid + "' Order By td_tradeid , td_subtradeid ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = " insert into #tmpfobill  select 99 tx_controlflag,bd_dt,tx_clientcd,tx_mainbrcd, tx_seriesid,'',  case sign(sum(tx_bqty - tx_sqty)) when 1 then abs(sum(tx_bqty - tx_sqty)) else 0 end  tx_bqty,  case sign(sum(tx_bqty - tx_sqty)) when 1 then 0 else abs(sum(tx_bqty - tx_sqty)) end tx_sqty,  0.0000 tx_rate,0.0000 tx_mainbrrate,0.0000 tx_mainbrrate, 0.0000 tx_servicetax,0.0000 tx_closeprice,  case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'EF' then 2 when 'IO' then 5 else 6 end tx_sortlist, sm_prodtype,0 ,sm_exchange";
                strinsert = strinsert + " ,0,0 From #tmpfobill  , #tmpmosesdates , Series_master with (nolock),Client_master with (nolock)";
                strinsert = strinsert + " Where tx_clientcd = cm_cd and sm_exchange = '" + strExcode + "' and sm_Segment = '" + Segment + "' And tx_seriesid = sm_seriesid  and sm_expirydt >= bd_dt and  tx_dt < bd_dt  and sm_prodtype in('IF','EF','CF')  and tx_controlflag not in ( '99','3')  group by bd_dt,tx_clientcd,tx_mainbrcd,tx_seriesid,sm_prodtype,sm_exchange  Having Sum(tx_bqty - tx_sqty) <> 0 ";
                strinsert = strinsert + " Union All ";
                strinsert = strinsert + " select 3 tx_controlflag,bd_dt,tx_clientcd,tx_mainbrcd, tx_seriesid,'', ";
                strinsert = strinsert + " case sign(sum(tx_bqty - tx_sqty)) when 1 then 0 else abs(sum(tx_bqty - tx_sqty)) end tx_bqty, ";
                strinsert = strinsert + " case sign(sum(tx_bqty - tx_sqty)) when 1 then abs(sum(tx_bqty - tx_sqty)) else 0 end  tx_sqty, ";
                strinsert = strinsert + " 0.0000 tx_rate,0.0000 tx_mainbrrate,0.0000 tx_mainbrrate, 0.0000 tx_servicetax,0.0000 tx_closeprice, ";
                strinsert = strinsert + " case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'EF' then 2 when 'IO' then 5 else 6 end tx_sortlist, sm_prodtype,0,sm_exchange ";
                strinsert = strinsert + " ,0,0 From #tmpfobill  , #tmpmosesdates , Series_master with (nolock),Client_master with (nolock) ";
                strinsert = strinsert + " Where tx_clientcd = cm_cd and sm_exchange = '" + strExcode + "' and sm_Segment = '" + Segment + "' And tx_seriesid = sm_seriesid ";
                strinsert = strinsert + " and sm_expirydt >= bd_dt and  tx_dt <= bd_dt ";
                strinsert = strinsert + " and sm_prodtype in('IF','EF','CF') ";
                strinsert = strinsert + " and tx_controlflag not in ( '99','3') ";
                strinsert = strinsert + " group by bd_dt,tx_clientcd,tx_mainbrcd,tx_seriesid,sm_prodtype,sm_exchange ";
                strinsert = strinsert + " Having Sum(tx_bqty - tx_sqty) <> 0 ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "Update #tmpfobill set tx_controlflag = '1' where tx_controlflag = '99' ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "insert into #tmpfobill select case ex_eaflag when 'E' then 5 else 6 end td_controlflag,ex_dt,ex_clientcd,ex_mainbrcd, ex_seriesid,'',ex_eqty,ex_aqty, ex_diffbrokrate,ex_mainbrdiffrate,ex_mainbrdiffrate, ex_servicetax,ex_settlerate, case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'EF' then 2 when 'IO' then 5 else 6 end + 3 td_sortlist, sm_prodtype,0 ,ex_exchange";
                strinsert = strinsert + " ,convert(decimal (15,2),ex_diffrate) as tx_marketrate1 ,convert(decimal(15,2),ex_brokerage) tx_Brokerage From Exercise with (nolock), Series_master with (nolock) ,Client_master with (nolock) ";
                strinsert = strinsert + "Where ex_clientcd = cm_cd and ex_exchange = sm_exchange and ex_Segment = sm_Segment And ex_seriesid = sm_seriesid and sm_expirydt >= '" + StrFromDt + "' and ex_dt between '" + StrFromDt + "' and '" + StrFromDt + "' " + StrExchWhere.Replace("td_", "ex_") + " and ex_Segment = '" + Segment + "' and cm_cd = '" + strclientid + "' ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "insert into #tmpbillcharges select tx_dt,tx_clientcd,'SERVICE TAX',round(sum(tx_servicetax),2),0,tx_exchange ";
                strinsert = strinsert + " from #tmpfobill,#tmpmosesdates,Client_master with (nolock) ";
                strinsert = strinsert + " where tx_clientcd = cm_cd and tx_dt = bd_dt and cm_cd = '" + strclientid + "' group by tx_dt,tx_clientcd,tx_exchange having sum(tx_servicetax) > 0 ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "update #tmpfobill set tx_closerate = ms_lastprice ";
                strinsert = strinsert + " from #tmpfobill, Market_summary with (nolock) ";
                strinsert = strinsert + " where ms_seriesid = tx_seriesid and tx_controlflag in ('1','2') and ms_exchange=tx_exchange and ms_Segment = '" + Segment + "' and ms_dt = tx_dt ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "update #tmpfobill set tx_rate = ms_prcloseprice ";
                strinsert = strinsert + " from #tmpfobill,Market_summary with (nolock) ";
                strinsert = strinsert + " where ms_seriesid = tx_seriesid and tx_controlflag = 1 and ms_exchange = tx_Exchange  and ms_Segment = '" + Segment + "' and ms_dt = tx_dt ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "update #tmpfobill set tx_rate = ms_lastprice from #tmpfobill,Market_summary with (nolock) ";
                strinsert = strinsert + " where ms_seriesid = tx_seriesid and tx_controlflag = 3";
                strinsert = strinsert + " and ms_exchange = tx_Exchange  and ms_Segment = '" + Segment + "'";
                strinsert = strinsert + " and ms_dt = tx_dt";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "insert into #tmpbillcharges select fc_dt,fc_clientcd,fc_desc,round(sum(fc_amount),2),0,fc_exchange ";
                strinsert = strinsert + " from Fspecialcharges with (nolock) ,#tmpmosesdates,Client_master with (nolock) ";
                strinsert = strinsert + " where fc_clientcd = cm_cd and fc_dt = bd_dt and cm_cd = '" + strclientid + "' " + StrExchWhere.Replace("td_", "fc_") + " and fc_Segment = '" + Segment + "' group by fc_dt,fc_clientcd,fc_desc,fc_exchange having round(sum(fc_amount),2) <> 0 ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "insert into #tmpbillcharges select fc_dt,fc_clientcd,'SERVICE TAX',round(sum(fc_servicetax),2),0,fc_exchange ";
                strinsert = strinsert + " from Fspecialcharges with (nolock) ,#tmpmosesdates,Client_master with (nolock) ";
                strinsert = strinsert + " where fc_clientcd = cm_cd and fc_dt = bd_dt and cm_cd = '" + strclientid + "' " + StrExchWhere.Replace("td_", "fc_") + " and fc_Segment = '" + Segment + "' group by fc_dt,fc_clientcd,fc_desc,fc_exchange having round(sum(fc_servicetax),2) <> 0 ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "update #tmpfobill set tx_value = round(((case tx_controlflag when 5  then (tx_bqty + tx_sqty)*-1 when 6 then (tx_bqty + tx_sqty)*-1 else (tx_bqty - tx_sqty) end) *tx_rate)*sm_multiplier,4)";
                strinsert = strinsert + " From series_master with (nolock) ";
                strinsert = strinsert + " Where sm_Segment = '" + Segment + "' and sm_exchange = tx_Exchange and tx_seriesid = sm_seriesid ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "insert into #tmpfobill select 10 ,bc_dt,bc_clientcd,bc_clientcd, 1,upper(bc_desc),0 td_bqty,0 td_sqty, 0,0 td_mainbrrate,0 td_mainbrrate, 0 td_servicetax,0.0000 td_closeprice, 10 td_sortlist, 'XX',round(sum(bc_amount),2),bc_exchange,0,0 From #tmpbillcharges group by bc_dt,bc_clientcd,bc_desc,bc_exchange ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "insert into #tmpfobill select 90 ,fb_billdt,fb_clientcd,fb_clientcd, 1,'[PREV. DAY MRGN.]',0 td_bqty,0 td_sqty, 0,0 td_mainbrrate,0 td_mainbrrate, 0 td_servicetax,0.0000 td_closeprice, 90 td_sortlist, 'XX',round(Case when fb_postmrgyn = 'Y' then fb_margin1 else 0 end + CAse When fb_postExpmrgyn = 'Y' then fb_Expmargin1 else 0 end ,2),fb_exchange ";
                strinsert = strinsert + " ,0,0 From #tmpmosesdates,Fbills with (nolock) ,Client_master with (nolock) ";
                strinsert = strinsert + " where fb_clientcd = cm_cd and fb_billdt = bd_dt and fb_exchange = '" + strExcode + "' and fb_Segment = '" + Segment + "' and round(Case when fb_postmrgyn = 'Y' then fb_margin1 else 0 end + CAse When fb_postExpmrgyn = 'Y' then fb_Expmargin1 else 0 end ,2) <> 0  and cm_cd = '" + strclientid + "' ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "insert into #tmpfobill select 91 ,fb_billdt,fb_clientcd,fb_clientcd, 1,'[CURR. DAY MRGN.]',0 td_bqty,0 td_sqty, 0,0 td_mainbrrate,0 td_mainbrrate, 0 td_servicetax,0.0000 td_closeprice, 91 td_sortlist, 'XX',round(Case When fb_postmrgyn = 'Y' then fb_margin2 else 0 end + Case When fb_postExpmrgyn = 'Y' then fb_Expmargin2 else 0 end,2),fb_exchange ";
                strinsert = strinsert + " ,0,0 From #tmpmosesdates,Fbills with (nolock) ,Client_master with (nolock) ";
                strinsert = strinsert + " where fb_clientcd = cm_cd and fb_billdt = bd_dt and fb_exchange = '" + strExcode + "' and fb_Segment = '" + Segment + "' and round(Case When fb_postmrgyn = 'Y' then fb_margin2 else 0 end + Case When fb_postExpmrgyn = 'Y' then fb_Expmargin2 else 0 end,2) <> 0  and cm_cd = '" + strclientid + "' ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "alter table #tmpfobill add tx_billno numeric default(0) NOT NULL ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "update #tmpfobill set tx_billno = fb_billno from #tmpfobill,Fbills with (nolock) where fb_clientcd = tx_clientcd  and fb_exchange = tx_exchange and fb_Segment = '" + Segment + "' and fb_billdt = tx_dt  ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "select tx_sortlist,tx_dt as dt,tx_billno,case tx_controlflag when '1' then 'b/f' when '3' then 'c/f' else convert(char,convert(datetime,tx_dt),103) end tx_dt,tx_clientcd, tx_seriesid,sm_sname as tx_desc, cast((tx_bqty)as decimal(15,0))as tx_bqty,cast((tx_sqty)as decimal(15,0))as tx_sqty,cast((tx_rate)as decimal(15,4))as tx_rate,cast((tx_closerate)as decimal(15,4))as tx_closerate,cast((tx_value)as decimal(15,2))as drcr,cast((((tx_bqty-tx_sqty) * tx_rate)) as decimal(15,2)) value,sm_sname,sm_desc,sm_productcd,sm_symbol, ";
                strinsert = strinsert + " sm_expirydt,sm_strikeprice,sm_callput,sm_optionstyle,cm_name, ";
                strinsert = strinsert + " cm_add1,cm_add2,cm_add3,cm_tele1,cm_tele2,cm_email,cm_sebino, cm_panno, cm_add4, cm_pincode,sm_prodtype,cm_groupcd,cm_familycd,cm_brboffcode,cm_subbroker,sm_multiplier, ";
                strinsert = strinsert + " cm_introducer ,replicate(' ',8-len(ltrim(rtrim(tx_clientcd)))) + ltrim(rtrim(tx_clientcd)) ,case tx_controlflag when '1' then 0 when '3' then 2 else 1 end ord, '' as NetValue ";
                strinsert = strinsert + " ,tx_marketrate1,tx_Brokerage from #tmpfobill,Series_master with (nolock) ,Client_master with (nolock) ";
                strinsert = strinsert + " where tx_clientcd = cm_cd and tx_seriesid = sm_seriesid and tx_exchange = sm_exchange and sm_Segment = '" + Segment + "' and tx_controlflag < 10 ";
                strinsert = strinsert + " union all ";
                strinsert = strinsert + " select tx_sortlist,tx_dt as dt,tx_billno,case tx_controlflag when '1' then 'b/f' when '3' then 'c/f' else convert(char,convert(datetime,tx_dt),103) end tx_dt,tx_clientcd, tx_seriesid,tx_desc, cast((tx_bqty)as decimal(15,0))as tx_bqty,cast((tx_sqty)as decimal(15,0))as tx_sqty,cast((tx_rate)as decimal(15,4))as tx_rate,cast((tx_closerate)as decimal(15,4))as tx_closerate,cast((tx_value)as decimal(15,2)) as drcr,cast((((tx_bqty-tx_sqty) * tx_rate))as decimal(15,2)) value, tx_desc as sm_sname,'' sm_desc,'' sm_productcd,'' sm_symbol,'' sm_expirydt, 0 sm_strikeprice, 'X' sm_callput,'X' sm_optionstyle, cm_name,cm_add1,cm_add2,cm_add3,cm_tele1,cm_tele2,cm_email, cm_sebino, cm_panno,cm_add4, ";
                strinsert = strinsert + " cm_pincode,'EF' as sm_prodtype,cm_groupcd,cm_familycd,cm_brboffcode, ";
                strinsert = strinsert + " cm_subbroker,0 sm_multiplier,cm_introducer ,replicate('',8-len(ltrim(rtrim(tx_clientcd)))) + ltrim(rtrim(tx_clientcd)),case tx_controlflag when '1' then 0 when '3' then 2 else 1 end ord, '' as NetValue  ";
                strinsert = strinsert + " ,tx_marketrate1,tx_Brokerage from #tmpfobill,Client_master with (nolock) ";
                strinsert = strinsert + " where tx_clientcd = cm_cd and tx_controlflag >= 10 ";
                strinsert = strinsert + " order by  replicate(' ',8-len(ltrim(rtrim(tx_clientcd)))) + ltrim(rtrim(tx_clientcd))  ,dt ,tx_sortlist,sm_symbol,tx_seriesid,tx_desc  , ord ";

                DataSet objDataset = new DataSet();
                objDataset = OpenDataSetTmp(strinsert, ObjConnectionTmp);

                string StrDrop = string.Empty;
                prTempFOBill(ObjConnectionTmp);
                return objDataset;

            }

            else //MCX And NCDEX
            {
                prTempFOBill1(ObjConnectionTmp);
                string strsql = string.Empty;

                strsql = "insert into #tmpmosesdates values('" + StrFromDt + "') ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp);

                string strDelivery = string.Empty;
                string strSeries_master = string.Empty;
                string strClient_master = string.Empty;
                string strTrades = string.Empty;
                string strMarket_summary = string.Empty;
                string strFspecialcharges = string.Empty;
                string strproduct_master = string.Empty;
                string strFbills = string.Empty;


                if (ConfigurationManager["IsTradeWeb"] == "O")
                {
                    if (ConfigurationManager["Commex"] != null || ConfigurationManager["Commex"] != string.Empty)
                    {
                        string StrCommexConn = "";
                        StrCommexConn = GetCommexConnection();

                        strSeries_master = StrCommexConn + ".Series_master";
                        strClient_master = StrCommexConn + ".Client_master";
                        strTrades = StrCommexConn + ".Trades";
                        strDelivery = StrCommexConn + ".Delivery";
                        strMarket_summary = StrCommexConn + ".Market_summary";
                        strFspecialcharges = StrCommexConn + ".Fspecialcharges";
                        strproduct_master = StrCommexConn + ".product_master";
                        strFbills = StrCommexConn + ".Fbills";
                    }
                }
                strsql = "insert into #tmpfobill select 1 td_controlflag,bd_dt,td_clientcd,td_mainbrcd, td_seriesid,'',case sign(sum(td_bqty - td_sqty)) when 1 then abs(sum(td_bqty - td_sqty)) else 0 end  td_bqty, case sign(sum(td_bqty - td_sqty)) when 1 then 0 else abs(sum(td_bqty - td_sqty)) end td_sqty, 0.0000 td_rate,0.0000 td_mainbrrate,0.0000 td_mainbrrate, 0.0000 td_servicetax,0.0000 td_closeprice, case sm_prodtype when 'CF' then 1 when 'CO' then 2 else 6 end td_sortlist, sm_prodtype,0, sm_multiplier , 0 td_tradeid, '' td_time, 0 td_orderid";
                strsql = strsql + " From " + strTrades + ", #tmpmosesdates, " + strSeries_master + "," + strClient_master + " ";
                strsql = strsql + "Where td_clientcd = cm_cd and td_exchange = sm_exchange And td_seriesid = sm_seriesid and sm_expirydt >= bd_dt and  td_dt < bd_dt and td_exchange = '" + strExcode + "' and sm_prodtype in('CF') and cm_cd = '" + strclientid + "' group by bd_dt,td_clientcd,td_mainbrcd,td_seriesid,sm_prodtype, sm_multiplier   having sum(td_bqty - td_sqty) <> 0 ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "insert into #tmpfobill select 2 td_controlflag,bd_dt,td_clientcd,td_mainbrcd, td_seriesid,'',td_bqty,td_sqty, td_rate,td_mainbrrate,td_mainbrrate, td_servicetax,0.0000 td_closeprice, case sm_prodtype when 'CF' then 1 when 'CO' then 2 else 6 end td_sortlist, sm_prodtype, 0, sm_multiplier, td_tradeid, td_time, td_orderid";
                strsql = strsql + " From " + strTrades + ", #tmpmosesdates, " + strSeries_master + "," + strClient_master + " ";
                strsql = strsql + "Where td_clientcd = cm_cd and td_exchange = sm_exchange and td_seriesid = sm_seriesid and sm_expirydt >= bd_dt and  td_dt = bd_dt and td_exchange = '" + strExcode + "' and cm_cd = '" + strclientid + "' ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = " insert into #tmpfobill  select Case dl_type When 'DL' Then 7 When 'PD' Then 8 When 'SL' Then 9 When 'DS' Then 9.5 Else '' End td_controlflag, dl_BillDate,dl_clientcd,dl_mainbrcd, dl_seriesid,'',  dl_Bqty , dl_SQty ,  dl_rate , dl_mainbrrate, dl_marketrate, dl_servicetax,0,  case sm_prodtype when 'CF' then 1 when 'CO' then 2 else 6 end + 3 td_sortlist, sm_prodtype, (dl_bqty - dl_sQty) * dl_Rate * sm_multiplier  , sm_multiplier , 0 tx_tradeid, '' tx_time, 0 tx_orderid ";
                strsql = strsql + "  From " + strDelivery + ", #tmpmosesdates, " + strSeries_master + "," + strClient_master + " ";
                strsql = strsql + "  Where dl_clientcd = cm_cd And dl_exchange = sm_exchange And dl_seriesid = sm_seriesid  and  dl_BillDate = bd_dt  and dl_exchange = '" + strExcode + "' and dl_type In ('DL','SL','PD','DS')  and cm_cd = '" + strclientid + "' ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "insert into #tmpfobill select 3 td_controlflag,bd_dt,td_clientcd,td_mainbrcd, td_seriesid,'',case sign(sum(td_bqty - td_sqty)) when 1 then 0 else abs(sum(td_bqty - td_sqty)) end  td_bqty, case sign(sum(td_bqty - td_sqty)) when 1 then abs(sum(td_bqty - td_sqty)) else 0 end td_sqty, 0.0000 td_rate,0.0000 td_mainbrrate,0.0000 td_mainbrrate, 0.0000 td_servicetax,0.0000 td_closeprice, case sm_prodtype when 'CF' then 1 when 'CO' then 2 else 6 end td_sortlist, sm_prodtype,0, sm_multiplier, 0,'',0       ";
                strsql = strsql + " From " + strTrades + ", #tmpmosesdates, " + strSeries_master + "," + strClient_master + " ";
                strsql = strsql + " Where td_clientcd = cm_cd and td_exchange = sm_exchange And td_seriesid = sm_seriesid and sm_expirydt >= bd_dt and  td_dt <= bd_dt and td_exchange = '" + strExcode + "' and sm_prodtype in('CF') and cm_cd = '" + strclientid + "' group by bd_dt,td_clientcd,td_mainbrcd,td_seriesid,sm_prodtype, sm_multiplier having sum(td_bqty - td_sqty) <> 0";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "insert into #tmpbillcharges select tx_dt,tx_clientcd,'SERVICE TAX',round(sum(tx_servicetax),2),0 ";
                strsql = strsql + " from #tmpfobill,#tmpmosesdates," + strClient_master + " ";
                strsql = strsql + "where tx_clientcd = cm_cd and tx_dt = bd_dt and cm_cd = '" + strclientid + "' group by tx_dt,tx_clientcd having sum(tx_servicetax) > 0 ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "update #tmpfobill set tx_closerate = ms_lastprice ";
                strsql = strsql + " from #tmpfobill, " + strMarket_summary + " ";
                strsql = strsql + " where ms_seriesid = tx_seriesid and tx_controlflag in ('1','2') and ms_exchange = '" + strExcode + "' and ms_dt = tx_dt ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "update #tmpfobill set tx_rate = ms_prcloseprice ";
                strsql = strsql + " from #tmpfobill," + strMarket_summary + " ";
                strsql = strsql + " where ms_seriesid = tx_seriesid and tx_controlflag = 1 and ms_exchange = '" + strExcode + "' and ms_dt = tx_dt ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "update #tmpfobill set tx_rate = ms_lastprice ";
                strsql = strsql + " from #tmpfobill, " + strMarket_summary + " ";
                strsql = strsql + " where ms_seriesid = tx_seriesid and tx_controlflag = 3 and ms_exchange = '" + strExcode + "' and ms_dt = tx_dt ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "insert into #tmpbillcharges select fc_dt,fc_clientcd,fc_desc,round(sum(fc_amount),2),0";
                strsql = strsql + " from " + strFspecialcharges + ",#tmpmosesdates," + strClient_master + " ";
                strsql = strsql + " where fc_clientcd = cm_cd and fc_dt = bd_dt and cm_cd = '" + strclientid + "' and fc_exchange = '" + strExcode + "' group by fc_dt,fc_clientcd,fc_desc having round(sum(fc_amount),2) <> 0 ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "insert into #tmpbillcharges select fc_dt,fc_clientcd,'SERVICE TAX',round(sum(fc_servicetax),2),0 ";
                strsql = strsql + " from " + strFspecialcharges + ",#tmpmosesdates," + strClient_master + " ";
                strsql = strsql + " where fc_clientcd = cm_cd and fc_dt = bd_dt and cm_cd = '" + strclientid + "' and fc_exchange = '" + strExcode + "' group by fc_dt,fc_clientcd,fc_desc having round(sum(fc_servicetax),2) <> 0 ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "update #tmpfobill set tx_value = round(((case tx_controlflag when 5  then (tx_bqty + tx_sqty)*0 when 6 then (tx_bqty + tx_sqty)*0 else (tx_bqty - tx_sqty) end) *tx_rate)*tx_multiplier,4) ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "insert into #tmpfobill select 10 ,bc_dt,bc_clientcd,bc_clientcd, 1,upper(bc_desc),0 td_bqty,0 td_sqty, 0,0 td_mainbrrate,0 td_mainbrrate, 0 td_servicetax,0.0000 td_closeprice, 10 td_sortlist, 'XX',round(sum(bc_amount),2),0,0,'',0 ";
                strsql = strsql + " From #tmpbillcharges group by bc_dt,bc_clientcd,bc_desc ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "insert into #tmpfobill select 90 ,fb_billdt,fb_clientcd,fb_clientcd, 1,'[PREV. DAY MRGN.]',0 td_bqty,0 td_sqty, 0,0 td_mainbrrate,0 td_mainbrrate, 0 td_servicetax,0.0000 td_closeprice, 90 td_sortlist, 'XX',round(fb_margin1,2),0, 0,'',0 ";
                strsql = strsql + " From #tmpmosesdates," + strFbills + "," + strClient_master + " ";
                strsql = strsql + " where fb_clientcd = cm_cd and fb_billdt = bd_dt and fb_exchange = '" + strExcode + "' and fb_margin1 <> 0 and fb_postmrgyn = 'Y' and cm_cd = '" + strclientid + "' ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "insert into #tmpfobill select 91 ,fb_billdt,fb_clientcd,fb_clientcd, 1,'[CURR. DAY MRGN.]',0 td_bqty,0 td_sqty, 0,0 td_mainbrrate,0 td_mainbrrate, 0 td_servicetax,0.0000 td_closeprice, 91 td_sortlist, 'XX',round(fb_margin2,2),0, 0, '', 0 ";
                strsql = strsql + " From #tmpmosesdates," + strFbills + "," + strClient_master + " ";
                strsql = strsql + " where fb_clientcd = cm_cd and fb_billdt = bd_dt and fb_exchange = '" + strExcode + "' and fb_margin2 <> 0 and fb_postmrgyn = 'Y' and cm_cd = '" + strclientid + "' ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "alter table #tmpfobill add tx_billno numeric default(0) NOT NULL ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "update #tmpfobill set tx_billno = fb_billno ";
                strsql = strsql + " from #tmpfobill," + strFbills + " ";
                strsql = strsql + " where fb_clientcd = tx_clientcd  and fb_exchange = '" + strExcode + "' and fb_billdt = tx_dt ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "alter table #tmpfobill add tx_unit char (15) ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "update a Set tx_unit=left(rtrim(convert(char,convert(numeric,floor(pm_divisor))))+' '+pm_unitper,15) ";
                strsql = strsql + " from #tmpfobill a, " + strSeries_master + ", " + strproduct_master + " ";
                strsql = strsql + " where tx_seriesid = sm_seriesid and sm_exchange='" + strExcode + "' and sm_prodtype=pm_type and sm_exchange=pm_exchange and sm_symbol=pm_assetcd ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "select tx_sortlist,tx_dt as dt,tx_billno,case tx_controlflag when '1' then 'b/f' when '3' then 'c/f' else convert(char,convert(datetime,tx_dt),103) end tx_dt,tx_clientcd, tx_seriesid,sm_sname as tx_desc,cast((tx_bqty) as decimal(15,2)) as tx_bqty,cast((tx_sqty) as decimal(15,2)) as tx_sqty,cast((tx_rate) as decimal(15,4)) as tx_rate,cast((tx_closerate) as decimal(15,4)) as tx_closerate,cast((tx_value) as decimal(15,2)) as drcr,cast((((tx_bqty-tx_sqty) * tx_rate)) as decimal(15,2))  value,tx_multiplier, sm_sname,sm_desc,sm_productcd,sm_symbol, sm_expirydt,sm_strikeprice, sm_callput,sm_optionstyle,cm_name,cm_add1, cm_add2,cm_add3,cm_tele1, cm_tele2,cm_email, cm_sebino, cm_panno, cm_add4, cm_pincode, sm_prodtype,cm_groupcd,cm_familycd, cm_brboffcode, tx_tradeid,";
                strsql = strsql + " case tx_controlflag when '1' then 'b/f' when '3' then 'c/f' else tx_time end tx_time, tx_orderid, tx_marketrate, tx_unit,case tx_controlflag when '1' then 0 when '3' then 2 else 1 end ord , '' as NetValue";
                strsql = strsql + " from #tmpfobill," + strSeries_master + "," + strClient_master + " ";
                strsql = strsql + " where tx_clientcd = cm_cd and tx_seriesid = sm_seriesid and sm_exchange = '" + strExcode + "' and tx_controlflag < 10 and cm_brboffcode <> '' ";
                strsql = strsql + "union all ";
                strsql = strsql + "select tx_sortlist,tx_dt as dt,tx_billno,case tx_controlflag when '1' then 'b/f' when '3' then 'c/f' else convert(char,convert(datetime,tx_dt),103) end tx_dt,tx_clientcd, tx_seriesid,tx_desc, cast((tx_bqty) as decimal(15,2)) as tx_bqty, cast((tx_sqty) as decimal(15,4)) as tx_sqty,tx_rate,cast((tx_closerate) as decimal(15,4)) as tx_closerate ,cast((tx_value) as decimal(15,2)) as drcr,cast((((tx_bqty-tx_sqty) * tx_rate)) as decimal(15,2))  value,tx_multiplier, tx_desc as  sm_sname,'' sm_desc,'' sm_productcd,'' sm_symbol,'' sm_expirydt, 0 sm_strikeprice, 'X' sm_callput,'X' sm_optionstyle, cm_name,cm_add1,cm_add2,cm_add3,cm_tele1,cm_tele2,cm_email, cm_sebino, cm_panno,cm_add4, cm_pincode,'EF' as sm_prodtype,cm_groupcd,cm_familycd, cm_brboffcode, tx_tradeid,";
                strsql = strsql + "case tx_controlflag when '1' then 'b/f' when '3' then 'c/f' else tx_time end tx_time, tx_orderid, tx_marketrate, tx_unit,case tx_controlflag when '1' then 0 when '3' then 2 else 1 end ord, '' as NetValue  ";
                strsql = strsql + " from #tmpfobill," + strClient_master + " ";
                strsql = strsql + " where tx_clientcd = cm_cd  and tx_controlflag >= 10 and cm_brboffcode <> ''  order by tx_clientcd,dt ,tx_sortlist,sm_symbol,tx_seriesid,tx_desc  , ord  ";
                DataSet objDataset = new DataSet();
                objDataset = OpenDataSetTmp(strsql, ObjConnectionTmp);

                string StrDrop = string.Empty;
                prTempFOBill1(ObjConnectionTmp);
                return objDataset;
            }
        }
        //// TODO : Added for return string value;
        public string fnForBill_String(string strclientid, string strExcode, string StrFromDt, string StrToDt, string Exchange, string Segment)
        {
            SqlConnection ObjConnectionTmp;
            using (var db = new DataContext())
            {
                ObjConnectionTmp = new SqlConnection((db.Database.GetDbConnection()).ConnectionString);
            }
            if (ObjConnectionTmp.State == ConnectionState.Closed)
            {
                ObjConnectionTmp.Open();
            }
            if ((Exchange != "MCX-COMM") && (Exchange != "NCDEX-COMM") && (Exchange != "ICEX-COMM") && (Exchange != "NCME-COMM") && (Exchange != "MCX") && (Exchange != "NCDEX") && (Exchange != "ICEX") && (Exchange != "NCME"))
            {
                prTempFOBill(ObjConnectionTmp);

                string strinsert = string.Empty;
                strinsert = "insert into #tmpmosesdates values('" + StrFromDt + "') ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                string StrExchWhere = "";
                string strIndexName = string.Empty;
                string strsql = string.Empty;

                strIndexName = "idx_trades_dt_clientcd";
                strsql = "Select Name from sysindexes where Name= 'idx_trades_clientcd'";
                DataSet ObjIndexds = OpenDataSet(strsql);
                if (ObjIndexds.Tables[0].Rows.Count > 0)
                {
                    strIndexName = "idx_trades_clientcd";
                }

                if (strExcode.Contains("IOP"))
                {
                    StrExchWhere = "";
                    strExcode = Strings.Right(strExcode, 1);
                }
                else
                { StrExchWhere = "and td_exchange = '" + strExcode + "'"; }

                strinsert = " insert into  #tmpfobill  select 1 td_controlflag,'" + StrFromDt + "',td_clientcd, ";
                strinsert = strinsert + " td_mainbrcd, td_seriesid,'',case sign(sum(td_bqty - td_sqty)) when 1 then abs(sum(td_bqty - td_sqty)) else 0 end  td_bqty, case sign(sum(td_bqty - td_sqty)) when 1 then 0 else abs(sum(td_bqty - td_sqty)) end td_sqty, 0.0000 td_rate,0.0000 td_mainbrrate,0.0000 td_mainbrrate, 0.0000 td_servicetax,0.0000 td_closeprice, case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'EF' then 2 when 'IO' then 5 else 6 end td_sortlist, sm_prodtype,0 ,td_exchange ";
                strinsert = strinsert + " ,0,0 From Trades with(nolock,index(" + strIndexName + ")) , Series_master with(nolock),Client_master with(nolock)";
                strinsert = strinsert + " Where td_clientcd = cm_cd and td_exchange = sm_exchange and td_Segment = sm_Segment And td_seriesid = sm_seriesid and sm_expirydt >= '" + StrFromDt + "' and  td_dt < '" + StrFromDt + "' " + StrExchWhere + " and td_Segment = '" + Segment + "' and sm_prodtype in('IF','EF','CF')  and ltrim(rtrim(td_groupid)) <> 'B'  and td_clientcd = '" + strclientid + "' group by td_clientcd,td_mainbrcd,td_seriesid,sm_prodtype,td_exchange having sum(td_bqty - td_sqty) <> 0 ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = " insert into #tmpfobill select 2 td_controlflag,td_dt,td_clientcd,td_mainbrcd, td_seriesid,'',td_bqty,td_sqty, td_rate, td_mainbrrate, td_mainbrrate, td_servicetax,0.0000 td_closeprice, case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'EF' then 2 when 'IO' then 5 else 6 end td_sortlist, sm_prodtype,0,td_exchange ";
                strinsert = strinsert + " ,convert(decimal (15,2),td_marketrate) as tx_marketrate1,convert(decimal (15,4),td_brokerage) tx_Brokerage From Trades with(nolock,index(" + strIndexName + ")) , Series_master with(nolock),Client_master with(nolock)";
                strinsert = strinsert + " Where td_clientcd = cm_cd and td_exchange = sm_exchange and td_Segment = sm_Segment and td_seriesid = sm_seriesid and sm_expirydt >= '" + StrFromDt + "' and  td_dt between '" + StrFromDt + "' and '" + StrFromDt + "' " + StrExchWhere + " and td_Segment = '" + Segment + "' and cm_cd = '" + strclientid + "' Order By td_tradeid , td_subtradeid ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = " insert into #tmpfobill select 2 td_controlflag,td_dt,td_clientcd,td_mainbrcd, td_seriesid,'',td_sqty,td_bqty, convert(decimal (15,2),td_marketrate) as tx_marketrate1, td_mainbrrate, td_mainbrrate, 0,0.0000 td_closeprice, case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'EF' then 2 when 'IO' then 5 else 6 end td_sortlist, sm_prodtype,0,td_exchange ";
                strinsert = strinsert + " ,convert(decimal (15,2),td_marketrate) as tx_marketrate1,convert(decimal (15,4),td_brokerage) tx_Brokerage From Trades with(nolock,index(" + strIndexName + ")) , Series_master,Client_master ";
                strinsert = strinsert + " Where td_clientcd = cm_cd and td_exchange = sm_exchange and td_Segment = sm_Segment and td_seriesid = sm_seriesid and sm_expirydt >= '" + StrFromDt + "' and  td_dt between '" + StrFromDt + "' and '" + StrFromDt + "' " + StrExchWhere + " and td_Segment = '" + Segment + "' and ltrim(rtrim(td_groupid)) = 'B'  and td_clientcd = '" + strclientid + "' Order By td_tradeid , td_subtradeid ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = " insert into #tmpfobill  select 99 tx_controlflag,bd_dt,tx_clientcd,tx_mainbrcd, tx_seriesid,'',  case sign(sum(tx_bqty - tx_sqty)) when 1 then abs(sum(tx_bqty - tx_sqty)) else 0 end  tx_bqty,  case sign(sum(tx_bqty - tx_sqty)) when 1 then 0 else abs(sum(tx_bqty - tx_sqty)) end tx_sqty,  0.0000 tx_rate,0.0000 tx_mainbrrate,0.0000 tx_mainbrrate, 0.0000 tx_servicetax,0.0000 tx_closeprice,  case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'EF' then 2 when 'IO' then 5 else 6 end tx_sortlist, sm_prodtype,0 ,sm_exchange";
                strinsert = strinsert + " ,0,0 From #tmpfobill  , #tmpmosesdates , Series_master with (nolock),Client_master with (nolock)";
                strinsert = strinsert + " Where tx_clientcd = cm_cd and sm_exchange = '" + strExcode + "' and sm_Segment = '" + Segment + "' And tx_seriesid = sm_seriesid  and sm_expirydt >= bd_dt and  tx_dt < bd_dt  and sm_prodtype in('IF','EF','CF')  and tx_controlflag not in ( '99','3')  group by bd_dt,tx_clientcd,tx_mainbrcd,tx_seriesid,sm_prodtype,sm_exchange  Having Sum(tx_bqty - tx_sqty) <> 0 ";
                strinsert = strinsert + " Union All ";
                strinsert = strinsert + " select 3 tx_controlflag,bd_dt,tx_clientcd,tx_mainbrcd, tx_seriesid,'', ";
                strinsert = strinsert + " case sign(sum(tx_bqty - tx_sqty)) when 1 then 0 else abs(sum(tx_bqty - tx_sqty)) end tx_bqty, ";
                strinsert = strinsert + " case sign(sum(tx_bqty - tx_sqty)) when 1 then abs(sum(tx_bqty - tx_sqty)) else 0 end  tx_sqty, ";
                strinsert = strinsert + " 0.0000 tx_rate,0.0000 tx_mainbrrate,0.0000 tx_mainbrrate, 0.0000 tx_servicetax,0.0000 tx_closeprice, ";
                strinsert = strinsert + " case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'EF' then 2 when 'IO' then 5 else 6 end tx_sortlist, sm_prodtype,0,sm_exchange ";
                strinsert = strinsert + " ,0,0 From #tmpfobill  , #tmpmosesdates , Series_master with (nolock),Client_master with (nolock) ";
                strinsert = strinsert + " Where tx_clientcd = cm_cd and sm_exchange = '" + strExcode + "' and sm_Segment = '" + Segment + "' And tx_seriesid = sm_seriesid ";
                strinsert = strinsert + " and sm_expirydt >= bd_dt and  tx_dt <= bd_dt ";
                strinsert = strinsert + " and sm_prodtype in('IF','EF','CF') ";
                strinsert = strinsert + " and tx_controlflag not in ( '99','3') ";
                strinsert = strinsert + " group by bd_dt,tx_clientcd,tx_mainbrcd,tx_seriesid,sm_prodtype,sm_exchange ";
                strinsert = strinsert + " Having Sum(tx_bqty - tx_sqty) <> 0 ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "Update #tmpfobill set tx_controlflag = '1' where tx_controlflag = '99' ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "insert into #tmpfobill select case ex_eaflag when 'E' then 5 else 6 end td_controlflag,ex_dt,ex_clientcd,ex_mainbrcd, ex_seriesid,'',ex_eqty,ex_aqty, ex_diffbrokrate,ex_mainbrdiffrate,ex_mainbrdiffrate, ex_servicetax,ex_settlerate, case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'EF' then 2 when 'IO' then 5 else 6 end + 3 td_sortlist, sm_prodtype,0 ,ex_exchange";
                strinsert = strinsert + " ,convert(decimal (15,2),ex_diffrate) as tx_marketrate1 ,convert(decimal(15,2),ex_brokerage) tx_Brokerage From Exercise with (nolock), Series_master with (nolock) ,Client_master with (nolock) ";
                strinsert = strinsert + "Where ex_clientcd = cm_cd and ex_exchange = sm_exchange and ex_Segment = sm_Segment And ex_seriesid = sm_seriesid and sm_expirydt >= '" + StrFromDt + "' and ex_dt between '" + StrFromDt + "' and '" + StrFromDt + "' " + StrExchWhere.Replace("td_", "ex_") + " and ex_Segment = '" + Segment + "' and cm_cd = '" + strclientid + "' ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "insert into #tmpbillcharges select tx_dt,tx_clientcd,'SERVICE TAX',round(sum(tx_servicetax),2),0,tx_exchange ";
                strinsert = strinsert + " from #tmpfobill,#tmpmosesdates,Client_master with (nolock) ";
                strinsert = strinsert + " where tx_clientcd = cm_cd and tx_dt = bd_dt and cm_cd = '" + strclientid + "' group by tx_dt,tx_clientcd,tx_exchange having sum(tx_servicetax) > 0 ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "update #tmpfobill set tx_closerate = ms_lastprice ";
                strinsert = strinsert + " from #tmpfobill, Market_summary with (nolock) ";
                strinsert = strinsert + " where ms_seriesid = tx_seriesid and tx_controlflag in ('1','2') and ms_exchange=tx_exchange and ms_Segment = '" + Segment + "' and ms_dt = tx_dt ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "update #tmpfobill set tx_rate = ms_prcloseprice ";
                strinsert = strinsert + " from #tmpfobill,Market_summary with (nolock) ";
                strinsert = strinsert + " where ms_seriesid = tx_seriesid and tx_controlflag = 1 and ms_exchange = tx_Exchange  and ms_Segment = '" + Segment + "' and ms_dt = tx_dt ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "update #tmpfobill set tx_rate = ms_lastprice from #tmpfobill,Market_summary with (nolock) ";
                strinsert = strinsert + " where ms_seriesid = tx_seriesid and tx_controlflag = 3";
                strinsert = strinsert + " and ms_exchange = tx_Exchange  and ms_Segment = '" + Segment + "'";
                strinsert = strinsert + " and ms_dt = tx_dt";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "insert into #tmpbillcharges select fc_dt,fc_clientcd,fc_desc,round(sum(fc_amount),2),0,fc_exchange ";
                strinsert = strinsert + " from Fspecialcharges with (nolock) ,#tmpmosesdates,Client_master with (nolock) ";
                strinsert = strinsert + " where fc_clientcd = cm_cd and fc_dt = bd_dt and cm_cd = '" + strclientid + "' " + StrExchWhere.Replace("td_", "fc_") + " and fc_Segment = '" + Segment + "' group by fc_dt,fc_clientcd,fc_desc,fc_exchange having round(sum(fc_amount),2) <> 0 ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "insert into #tmpbillcharges select fc_dt,fc_clientcd,'SERVICE TAX',round(sum(fc_servicetax),2),0,fc_exchange ";
                strinsert = strinsert + " from Fspecialcharges with (nolock) ,#tmpmosesdates,Client_master with (nolock) ";
                strinsert = strinsert + " where fc_clientcd = cm_cd and fc_dt = bd_dt and cm_cd = '" + strclientid + "' " + StrExchWhere.Replace("td_", "fc_") + " and fc_Segment = '" + Segment + "' group by fc_dt,fc_clientcd,fc_desc,fc_exchange having round(sum(fc_servicetax),2) <> 0 ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "update #tmpfobill set tx_value = round(((case tx_controlflag when 5  then (tx_bqty + tx_sqty)*-1 when 6 then (tx_bqty + tx_sqty)*-1 else (tx_bqty - tx_sqty) end) *tx_rate)*sm_multiplier,4)";
                strinsert = strinsert + " From series_master with (nolock) ";
                strinsert = strinsert + " Where sm_Segment = '" + Segment + "' and sm_exchange = tx_Exchange and tx_seriesid = sm_seriesid ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "insert into #tmpfobill select 10 ,bc_dt,bc_clientcd,bc_clientcd, 1,upper(bc_desc),0 td_bqty,0 td_sqty, 0,0 td_mainbrrate,0 td_mainbrrate, 0 td_servicetax,0.0000 td_closeprice, 10 td_sortlist, 'XX',round(sum(bc_amount),2),bc_exchange,0,0 From #tmpbillcharges group by bc_dt,bc_clientcd,bc_desc,bc_exchange ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "insert into #tmpfobill select 90 ,fb_billdt,fb_clientcd,fb_clientcd, 1,'[PREV. DAY MRGN.]',0 td_bqty,0 td_sqty, 0,0 td_mainbrrate,0 td_mainbrrate, 0 td_servicetax,0.0000 td_closeprice, 90 td_sortlist, 'XX',round(Case when fb_postmrgyn = 'Y' then fb_margin1 else 0 end + CAse When fb_postExpmrgyn = 'Y' then fb_Expmargin1 else 0 end ,2),fb_exchange ";
                strinsert = strinsert + " ,0,0 From #tmpmosesdates,Fbills with (nolock) ,Client_master with (nolock) ";
                strinsert = strinsert + " where fb_clientcd = cm_cd and fb_billdt = bd_dt and fb_exchange = '" + strExcode + "' and fb_Segment = '" + Segment + "' and round(Case when fb_postmrgyn = 'Y' then fb_margin1 else 0 end + CAse When fb_postExpmrgyn = 'Y' then fb_Expmargin1 else 0 end ,2) <> 0  and cm_cd = '" + strclientid + "' ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "insert into #tmpfobill select 91 ,fb_billdt,fb_clientcd,fb_clientcd, 1,'[CURR. DAY MRGN.]',0 td_bqty,0 td_sqty, 0,0 td_mainbrrate,0 td_mainbrrate, 0 td_servicetax,0.0000 td_closeprice, 91 td_sortlist, 'XX',round(Case When fb_postmrgyn = 'Y' then fb_margin2 else 0 end + Case When fb_postExpmrgyn = 'Y' then fb_Expmargin2 else 0 end,2),fb_exchange ";
                strinsert = strinsert + " ,0,0 From #tmpmosesdates,Fbills with (nolock) ,Client_master with (nolock) ";
                strinsert = strinsert + " where fb_clientcd = cm_cd and fb_billdt = bd_dt and fb_exchange = '" + strExcode + "' and fb_Segment = '" + Segment + "' and round(Case When fb_postmrgyn = 'Y' then fb_margin2 else 0 end + Case When fb_postExpmrgyn = 'Y' then fb_Expmargin2 else 0 end,2) <> 0  and cm_cd = '" + strclientid + "' ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "alter table #tmpfobill add tx_billno numeric default(0) NOT NULL ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "update #tmpfobill set tx_billno = fb_billno from #tmpfobill,Fbills with (nolock) where fb_clientcd = tx_clientcd  and fb_exchange = tx_exchange and fb_Segment = '" + Segment + "' and fb_billdt = tx_dt  ";
                ExecuteSQLTmp(strinsert, ObjConnectionTmp);

                strinsert = "select tx_sortlist,tx_dt as dt,tx_billno,case tx_controlflag when '1' then 'b/f' when '3' then 'c/f' else convert(char,convert(datetime,tx_dt),103) end tx_dt,tx_clientcd, tx_seriesid,sm_sname as tx_desc, cast((tx_bqty)as decimal(15,0))as tx_bqty,cast((tx_sqty)as decimal(15,0))as tx_sqty,cast((tx_rate)as decimal(15,4))as tx_rate,cast((tx_closerate)as decimal(15,4))as tx_closerate,cast((tx_value)as decimal(15,2))as drcr,cast((((tx_bqty-tx_sqty) * tx_rate)) as decimal(15,2)) value,sm_sname,sm_desc,sm_productcd,sm_symbol, ";
                strinsert = strinsert + " sm_expirydt,sm_strikeprice,sm_callput,sm_optionstyle,cm_name, ";
                strinsert = strinsert + " cm_add1,cm_add2,cm_add3,cm_tele1,cm_tele2,cm_email,cm_sebino, cm_panno, cm_add4, cm_pincode,sm_prodtype,cm_groupcd,cm_familycd,cm_brboffcode,cm_subbroker,sm_multiplier, ";
                strinsert = strinsert + " cm_introducer ,replicate(' ',8-len(ltrim(rtrim(tx_clientcd)))) + ltrim(rtrim(tx_clientcd)) ,case tx_controlflag when '1' then 0 when '3' then 2 else 1 end ord, '' as NetValue ";
                strinsert = strinsert + " ,tx_marketrate1,tx_Brokerage from #tmpfobill,Series_master with (nolock) ,Client_master with (nolock) ";
                strinsert = strinsert + " where tx_clientcd = cm_cd and tx_seriesid = sm_seriesid and tx_exchange = sm_exchange and sm_Segment = '" + Segment + "' and tx_controlflag < 10 ";
                strinsert = strinsert + " union all ";
                strinsert = strinsert + " select tx_sortlist,tx_dt as dt,tx_billno,case tx_controlflag when '1' then 'b/f' when '3' then 'c/f' else convert(char,convert(datetime,tx_dt),103) end tx_dt,tx_clientcd, tx_seriesid,tx_desc, cast((tx_bqty)as decimal(15,0))as tx_bqty,cast((tx_sqty)as decimal(15,0))as tx_sqty,cast((tx_rate)as decimal(15,4))as tx_rate,cast((tx_closerate)as decimal(15,4))as tx_closerate,cast((tx_value)as decimal(15,2)) as drcr,cast((((tx_bqty-tx_sqty) * tx_rate))as decimal(15,2)) value, tx_desc as sm_sname,'' sm_desc,'' sm_productcd,'' sm_symbol,'' sm_expirydt, 0 sm_strikeprice, 'X' sm_callput,'X' sm_optionstyle, cm_name,cm_add1,cm_add2,cm_add3,cm_tele1,cm_tele2,cm_email, cm_sebino, cm_panno,cm_add4, ";
                strinsert = strinsert + " cm_pincode,'EF' as sm_prodtype,cm_groupcd,cm_familycd,cm_brboffcode, ";
                strinsert = strinsert + " cm_subbroker,0 sm_multiplier,cm_introducer ,replicate('',8-len(ltrim(rtrim(tx_clientcd)))) + ltrim(rtrim(tx_clientcd)),case tx_controlflag when '1' then 0 when '3' then 2 else 1 end ord, '' as NetValue  ";
                strinsert = strinsert + " ,tx_marketrate1,tx_Brokerage from #tmpfobill,Client_master with (nolock) ";
                strinsert = strinsert + " where tx_clientcd = cm_cd and tx_controlflag >= 10 ";
                strinsert = strinsert + " order by  replicate(' ',8-len(ltrim(rtrim(tx_clientcd)))) + ltrim(rtrim(tx_clientcd))  ,dt ,tx_sortlist,sm_symbol,tx_seriesid,tx_desc  , ord ";


                DataSet ds = new DataSet();
                ds = OpenDataSetTmp(strinsert, ObjConnectionTmp);

                prTempFOBill(ObjConnectionTmp);
                // ds = OpenDataSet(strinsert);
                return strinsert;
            }

            else //MCX And NCDEX
            {
                prTempFOBill1(ObjConnectionTmp);
                string strsql = string.Empty;

                strsql = "insert into #tmpmosesdates values('" + StrFromDt + "') ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp);

                string strDelivery = string.Empty;
                string strSeries_master = string.Empty;
                string strClient_master = string.Empty;
                string strTrades = string.Empty;
                string strMarket_summary = string.Empty;
                string strFspecialcharges = string.Empty;
                string strproduct_master = string.Empty;
                string strFbills = string.Empty;

                if (ConfigurationManager["IsTradeWeb"] == "O")
                {
                    if (ConfigurationManager["Commex"] != null || ConfigurationManager["Commex"] != string.Empty)
                    {
                        string StrCommexConn = "";
                        StrCommexConn = GetCommexConnection();

                        strSeries_master = StrCommexConn + ".Series_master";
                        strClient_master = StrCommexConn + ".Client_master";
                        strTrades = StrCommexConn + ".Trades";
                        strDelivery = StrCommexConn + ".Delivery";
                        strMarket_summary = StrCommexConn + ".Market_summary";
                        strFspecialcharges = StrCommexConn + ".Fspecialcharges";
                        strproduct_master = StrCommexConn + ".product_master";
                        strFbills = StrCommexConn + ".Fbills";
                    }
                }
                strsql = "insert into #tmpfobill select 1 td_controlflag,bd_dt,td_clientcd,td_mainbrcd, td_seriesid,'',case sign(sum(td_bqty - td_sqty)) when 1 then abs(sum(td_bqty - td_sqty)) else 0 end  td_bqty, case sign(sum(td_bqty - td_sqty)) when 1 then 0 else abs(sum(td_bqty - td_sqty)) end td_sqty, 0.0000 td_rate,0.0000 td_mainbrrate,0.0000 td_mainbrrate, 0.0000 td_servicetax,0.0000 td_closeprice, case sm_prodtype when 'CF' then 1 when 'CO' then 2 else 6 end td_sortlist, sm_prodtype,0, sm_multiplier , 0 td_tradeid, '' td_time, 0 td_orderid";
                strsql = strsql + " From " + strTrades + ", #tmpmosesdates, " + strSeries_master + "," + strClient_master + " ";
                strsql = strsql + "Where td_clientcd = cm_cd and td_exchange = sm_exchange And td_seriesid = sm_seriesid and sm_expirydt >= bd_dt and  td_dt < bd_dt and td_exchange = '" + strExcode + "' and sm_prodtype in('CF') and cm_cd = '" + strclientid + "' group by bd_dt,td_clientcd,td_mainbrcd,td_seriesid,sm_prodtype, sm_multiplier   having sum(td_bqty - td_sqty) <> 0 ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "insert into #tmpfobill select 2 td_controlflag,bd_dt,td_clientcd,td_mainbrcd, td_seriesid,'',td_bqty,td_sqty, td_rate,td_mainbrrate,td_mainbrrate, td_servicetax,0.0000 td_closeprice, case sm_prodtype when 'CF' then 1 when 'CO' then 2 else 6 end td_sortlist, sm_prodtype, 0, sm_multiplier, td_tradeid, td_time, td_orderid";
                strsql = strsql + " From " + strTrades + ", #tmpmosesdates, " + strSeries_master + "," + strClient_master + " ";
                strsql = strsql + "Where td_clientcd = cm_cd and td_exchange = sm_exchange and td_seriesid = sm_seriesid and sm_expirydt >= bd_dt and  td_dt = bd_dt and td_exchange = '" + strExcode + "' and cm_cd = '" + strclientid + "' ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = " insert into #tmpfobill  select Case dl_type When 'DL' Then 7 When 'PD' Then 8 When 'SL' Then 9 When 'DS' Then 9.5 Else '' End td_controlflag, dl_BillDate,dl_clientcd,dl_mainbrcd, dl_seriesid,'',  dl_Bqty , dl_SQty ,  dl_rate , dl_mainbrrate, dl_marketrate, dl_servicetax,0,  case sm_prodtype when 'CF' then 1 when 'CO' then 2 else 6 end + 3 td_sortlist, sm_prodtype, (dl_bqty - dl_sQty) * dl_Rate * sm_multiplier  , sm_multiplier , 0 tx_tradeid, '' tx_time, 0 tx_orderid ";
                strsql = strsql + "  From " + strDelivery + ", #tmpmosesdates, " + strSeries_master + "," + strClient_master + " ";
                strsql = strsql + "  Where dl_clientcd = cm_cd And dl_exchange = sm_exchange And dl_seriesid = sm_seriesid  and  dl_BillDate = bd_dt  and dl_exchange = '" + strExcode + "' and dl_type In ('DL','SL','PD','DS')  and cm_cd = '" + strclientid + "' ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "insert into #tmpfobill select 3 td_controlflag,bd_dt,td_clientcd,td_mainbrcd, td_seriesid,'',case sign(sum(td_bqty - td_sqty)) when 1 then 0 else abs(sum(td_bqty - td_sqty)) end  td_bqty, case sign(sum(td_bqty - td_sqty)) when 1 then abs(sum(td_bqty - td_sqty)) else 0 end td_sqty, 0.0000 td_rate,0.0000 td_mainbrrate,0.0000 td_mainbrrate, 0.0000 td_servicetax,0.0000 td_closeprice, case sm_prodtype when 'CF' then 1 when 'CO' then 2 else 6 end td_sortlist, sm_prodtype,0, sm_multiplier, 0,'',0       ";
                strsql = strsql + " From " + strTrades + ", #tmpmosesdates, " + strSeries_master + "," + strClient_master + " ";
                strsql = strsql + " Where td_clientcd = cm_cd and td_exchange = sm_exchange And td_seriesid = sm_seriesid and sm_expirydt >= bd_dt and  td_dt <= bd_dt and td_exchange = '" + strExcode + "' and sm_prodtype in('CF') and cm_cd = '" + strclientid + "' group by bd_dt,td_clientcd,td_mainbrcd,td_seriesid,sm_prodtype, sm_multiplier having sum(td_bqty - td_sqty) <> 0";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "insert into #tmpbillcharges select tx_dt,tx_clientcd,'SERVICE TAX',round(sum(tx_servicetax),2),0 ";
                strsql = strsql + " from #tmpfobill,#tmpmosesdates," + strClient_master + " ";
                strsql = strsql + "where tx_clientcd = cm_cd and tx_dt = bd_dt and cm_cd = '" + strclientid + "' group by tx_dt,tx_clientcd having sum(tx_servicetax) > 0 ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "update #tmpfobill set tx_closerate = ms_lastprice ";
                strsql = strsql + " from #tmpfobill, " + strMarket_summary + " ";
                strsql = strsql + " where ms_seriesid = tx_seriesid and tx_controlflag in ('1','2') and ms_exchange = '" + strExcode + "' and ms_dt = tx_dt ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "update #tmpfobill set tx_rate = ms_prcloseprice ";
                strsql = strsql + " from #tmpfobill," + strMarket_summary + " ";
                strsql = strsql + " where ms_seriesid = tx_seriesid and tx_controlflag = 1 and ms_exchange = '" + strExcode + "' and ms_dt = tx_dt ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "update #tmpfobill set tx_rate = ms_lastprice ";
                strsql = strsql + " from #tmpfobill, " + strMarket_summary + " ";
                strsql = strsql + " where ms_seriesid = tx_seriesid and tx_controlflag = 3 and ms_exchange = '" + strExcode + "' and ms_dt = tx_dt ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "insert into #tmpbillcharges select fc_dt,fc_clientcd,fc_desc,round(sum(fc_amount),2),0";
                strsql = strsql + " from " + strFspecialcharges + ",#tmpmosesdates," + strClient_master + " ";
                strsql = strsql + " where fc_clientcd = cm_cd and fc_dt = bd_dt and cm_cd = '" + strclientid + "' and fc_exchange = '" + strExcode + "' group by fc_dt,fc_clientcd,fc_desc having round(sum(fc_amount),2) <> 0 ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "insert into #tmpbillcharges select fc_dt,fc_clientcd,'SERVICE TAX',round(sum(fc_servicetax),2),0 ";
                strsql = strsql + " from " + strFspecialcharges + ",#tmpmosesdates," + strClient_master + " ";
                strsql = strsql + " where fc_clientcd = cm_cd and fc_dt = bd_dt and cm_cd = '" + strclientid + "' and fc_exchange = '" + strExcode + "' group by fc_dt,fc_clientcd,fc_desc having round(sum(fc_servicetax),2) <> 0 ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "update #tmpfobill set tx_value = round(((case tx_controlflag when 5  then (tx_bqty + tx_sqty)*0 when 6 then (tx_bqty + tx_sqty)*0 else (tx_bqty - tx_sqty) end) *tx_rate)*tx_multiplier,4) ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "insert into #tmpfobill select 10 ,bc_dt,bc_clientcd,bc_clientcd, 1,upper(bc_desc),0 td_bqty,0 td_sqty, 0,0 td_mainbrrate,0 td_mainbrrate, 0 td_servicetax,0.0000 td_closeprice, 10 td_sortlist, 'XX',round(sum(bc_amount),2),0,0,'',0 ";
                strsql = strsql + " From #tmpbillcharges group by bc_dt,bc_clientcd,bc_desc ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "insert into #tmpfobill select 90 ,fb_billdt,fb_clientcd,fb_clientcd, 1,'[PREV. DAY MRGN.]',0 td_bqty,0 td_sqty, 0,0 td_mainbrrate,0 td_mainbrrate, 0 td_servicetax,0.0000 td_closeprice, 90 td_sortlist, 'XX',round(fb_margin1,2),0, 0,'',0 ";
                strsql = strsql + " From #tmpmosesdates," + strFbills + "," + strClient_master + " ";
                strsql = strsql + " where fb_clientcd = cm_cd and fb_billdt = bd_dt and fb_exchange = '" + strExcode + "' and fb_margin1 <> 0 and fb_postmrgyn = 'Y' and cm_cd = '" + strclientid + "' ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "insert into #tmpfobill select 91 ,fb_billdt,fb_clientcd,fb_clientcd, 1,'[CURR. DAY MRGN.]',0 td_bqty,0 td_sqty, 0,0 td_mainbrrate,0 td_mainbrrate, 0 td_servicetax,0.0000 td_closeprice, 91 td_sortlist, 'XX',round(fb_margin2,2),0, 0, '', 0 ";
                strsql = strsql + " From #tmpmosesdates," + strFbills + "," + strClient_master + " ";
                strsql = strsql + " where fb_clientcd = cm_cd and fb_billdt = bd_dt and fb_exchange = '" + strExcode + "' and fb_margin2 <> 0 and fb_postmrgyn = 'Y' and cm_cd = '" + strclientid + "' ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "alter table #tmpfobill add tx_billno numeric default(0) NOT NULL ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "update #tmpfobill set tx_billno = fb_billno ";
                strsql = strsql + " from #tmpfobill," + strFbills + " ";
                strsql = strsql + " where fb_clientcd = tx_clientcd  and fb_exchange = '" + strExcode + "' and fb_billdt = tx_dt ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "alter table #tmpfobill add tx_unit char (15) ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "update a Set tx_unit=left(rtrim(convert(char,convert(numeric,floor(pm_divisor))))+' '+pm_unitper,15) ";
                strsql = strsql + " from #tmpfobill a, " + strSeries_master + ", " + strproduct_master + " ";
                strsql = strsql + " where tx_seriesid = sm_seriesid and sm_exchange='" + strExcode + "' and sm_prodtype=pm_type and sm_exchange=pm_exchange and sm_symbol=pm_assetcd ";
                ExecuteSQLTmp(strsql, ObjConnectionTmp); ;

                strsql = "select tx_sortlist,tx_dt as dt,tx_billno,case tx_controlflag when '1' then 'b/f' when '3' then 'c/f' else convert(char,convert(datetime,tx_dt),103) end tx_dt,tx_clientcd, tx_seriesid,sm_sname as tx_desc,cast((tx_bqty) as decimal(15,2)) as tx_bqty,cast((tx_sqty) as decimal(15,2)) as tx_sqty,cast((tx_rate) as decimal(15,4)) as tx_rate,cast((tx_closerate) as decimal(15,4)) as tx_closerate,cast((tx_value) as decimal(15,2)) as drcr,cast((((tx_bqty-tx_sqty) * tx_rate)) as decimal(15,2))  value,tx_multiplier, sm_sname,sm_desc,sm_productcd,sm_symbol, sm_expirydt,sm_strikeprice, sm_callput,sm_optionstyle,cm_name,cm_add1, cm_add2,cm_add3,cm_tele1, cm_tele2,cm_email, cm_sebino, cm_panno, cm_add4, cm_pincode, sm_prodtype,cm_groupcd,cm_familycd, cm_brboffcode, tx_tradeid,";
                strsql = strsql + " case tx_controlflag when '1' then 'b/f' when '3' then 'c/f' else tx_time end tx_time, tx_orderid, tx_marketrate, tx_unit,case tx_controlflag when '1' then 0 when '3' then 2 else 1 end ord , '' as NetValue";
                strsql = strsql + " from #tmpfobill," + strSeries_master + "," + strClient_master + " ";
                strsql = strsql + " where tx_clientcd = cm_cd and tx_seriesid = sm_seriesid and sm_exchange = '" + strExcode + "' and tx_controlflag < 10 and cm_brboffcode <> '' ";
                strsql = strsql + "union all ";
                strsql = strsql + "select tx_sortlist,tx_dt as dt,tx_billno,case tx_controlflag when '1' then 'b/f' when '3' then 'c/f' else convert(char,convert(datetime,tx_dt),103) end tx_dt,tx_clientcd, tx_seriesid,tx_desc, cast((tx_bqty) as decimal(15,2)) as tx_bqty, cast((tx_sqty) as decimal(15,4)) as tx_sqty,tx_rate,cast((tx_closerate) as decimal(15,4)) as tx_closerate ,cast((tx_value) as decimal(15,2)) as drcr,cast((((tx_bqty-tx_sqty) * tx_rate)) as decimal(15,2))  value,tx_multiplier, tx_desc as  sm_sname,'' sm_desc,'' sm_productcd,'' sm_symbol,'' sm_expirydt, 0 sm_strikeprice, 'X' sm_callput,'X' sm_optionstyle, cm_name,cm_add1,cm_add2,cm_add3,cm_tele1,cm_tele2,cm_email, cm_sebino, cm_panno,cm_add4, cm_pincode,'EF' as sm_prodtype,cm_groupcd,cm_familycd, cm_brboffcode, tx_tradeid,";
                strsql = strsql + "case tx_controlflag when '1' then 'b/f' when '3' then 'c/f' else tx_time end tx_time, tx_orderid, tx_marketrate, tx_unit,case tx_controlflag when '1' then 0 when '3' then 2 else 1 end ord, '' as NetValue  ";
                strsql = strsql + " from #tmpfobill," + strClient_master + " ";
                strsql = strsql + " where tx_clientcd = cm_cd  and tx_controlflag >= 10 and cm_brboffcode <> ''  order by tx_clientcd,dt ,tx_sortlist,sm_symbol,tx_seriesid,tx_desc  , ord  ";

                prTempFOBill1(ObjConnectionTmp);

                DataSet ds = new DataSet();
                ds = OpenDataSet(strsql);

                return strsql;


            }
        }

        public string GetPaddedString(string StringToPad, int TotalExpectedLength)
        {

            int i = StringToPad.Length;
            while (i < TotalExpectedLength)
            {
                StringToPad = StringToPad + " ";
                i++;
            }
            return StringToPad;
        }
        public string GetLeftPaddedString(string StringToPad, int TotalExpectedLength)
        {

            int i = StringToPad.Length;
            while (i < TotalExpectedLength)
            {
                StringToPad = " " + StringToPad;
                i++;
            }
            return StringToPad;

        }
        public string GetFormattedDate(string Date)
        {
            int Year = int.Parse(Date.Substring(0, 4));
            int Month = int.Parse(Date.Substring(4, 2));
            int Day = int.Parse(Date.Substring(6, 2));

            return new DateTime(Year, Month, Day).ToString("dd mm yyyy");
        }
        public string dtos(string dDate)
        {    //for converting date to string format
            string dtos = dDate.Substring(6, 4) + dDate.Substring(3, 2) + dDate.Substring(0, 2);
            return dtos;
        }
        public DateTime stod(string Date)
        {
            int Year = int.Parse(Date.Substring(0, 4));
            int Month = int.Parse(Date.Substring(4, 2));
            int Day = int.Parse(Date.Substring(6, 2));
            return new DateTime(Year, Month, Day);
        }
        public string mfnFormatCurrency(Object objAmount, int intDecimal)
        {
            return Convert.ToDecimal(objAmount).ToString("N" + intDecimal).Trim().Replace(",", "");
        }
        public string GetHtmlFromUrl(string url)
        {
            if (string.IsNullOrEmpty(url))
                throw new ArgumentNullException("url", "Parameter is null or empty");

            string html = "";
            HttpWebRequest request = GenerateHttpWebRequest(url);
            using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
            {

                // Get the response stream.
                Stream responseStream = response.GetResponseStream();
                // Use a stream reader that understands UTF8.
                using (StreamReader reader =
                new StreamReader(responseStream, Encoding.UTF8))
                {
                    html = reader.ReadToEnd();
                }
            }
            return html;
        }

        public HttpWebRequest GenerateHttpWebRequest(string UriString)
        {
            // Get a Uri object.
            Uri Uri = new Uri(UriString);
            // Create the initial request.
            HttpWebRequest httpRequest = (HttpWebRequest)WebRequest.Create(Uri);
            // Return the request.
            return httpRequest;
        }

        public decimal mfnRoundoffCashbill(string strClient, string strRefDt, decimal dblBillamount, string strExchange, string strCompanyCode)
        {
            string strGroup;
            string strFamily;
            string strsql = "";
            decimal mfnRoundoffCashbill;

            dblBillamount = Math.Round(dblBillamount, 4);

            strsql = "select cm_groupcd,cm_familycd from Client_master where cm_cd = '" + strClient + "'";
            DataSet objDataset = new DataSet();
            objDataset = OpenDataSet(strsql);


            if (objDataset.Tables[0].Rows.Count <= 0)
            {
                mfnRoundoffCashbill = Math.Round(dblBillamount, 2);
                return mfnRoundoffCashbill;
            }
            else
            {
                strGroup = objDataset.Tables[0].Rows[0]["cm_groupcd"].ToString();
                strFamily = objDataset.Tables[0].Rows[0]["cm_familycd"].ToString();
            }

            if (dblBillamount == 0 || strClient.Trim() == "")
            {
                mfnRoundoffCashbill = Math.Round(dblBillamount, 2);
                return mfnRoundoffCashbill;
            }

            strsql = "select cg_ledgercd,(case cd_ratep when 0 then 1 else cd_ratep end) cd_ratep from Client_charges,Charges_master";
            strsql = strsql + " where cg_companycode = cd_companycode and cg_exchange = cd_exchange and cd_cd = cg_cd and cd_companycode = '" + strCompanyCode + "'";
            strsql = strsql + " and cd_exchange = '" + strExchange + "' and cg_cd = '00'";
            strsql = strsql + " and (cd_clientcd + cd_groupcd + cd_familycd + cd_fromdt)";
            strsql = strsql + " = (select max(cd_clientcd + cd_groupcd + cd_familycd + cd_fromdt)";
            strsql = strsql + " from Client_charges where cd_cd = '00' and cd_companycode = cg_companycode";
            strsql = strsql + " and cd_exchange = cg_exchange and (cd_clientcd ='" + strClient + "'";
            strsql = strsql + " or cd_groupcd = '" + strGroup + "' or cd_familycd = '" + strFamily + "'";
            strsql = strsql + " or (cd_clientcd + cd_groupcd + cd_familycd) ='')";
            strsql = strsql + " and cd_fromdt <= '" + (strRefDt) + "')";

            objDataset = new DataSet();
            objDataset = OpenDataSet(strsql);

            if (objDataset.Tables[0].Rows.Count == 0)
            {
                mfnRoundoffCashbill = Math.Round(dblBillamount, 2);
            }
            else
            {
                if (Convert.ToInt16(objDataset.Tables[0].Rows[0]["cd_ratep"]) == 1)
                    mfnRoundoffCashbill = Math.Round(dblBillamount, 0);
                else
                    mfnRoundoffCashbill = Math.Round(dblBillamount, 2);
            }
            return mfnRoundoffCashbill;
        }

        public string Encrypt(string strenc)
        {
            int m = 0;
            string strEncKey = null;
            string gsEcDc = null;
            string gsFinal = null;
            string gsCompare = null;
            int glNumber = 0;
            strEncKey = "ASHOKKHE";
            gsFinal = "";
            glNumber = Strings.Len(Strings.Trim(strenc));
            for (m = 1; m <= Math.Round((decimal)glNumber / 8) + 1; m++)
            {
                strEncKey = strEncKey + strEncKey;
            }
            m = 0;
            for (m = 1; m <= glNumber; m++)
            {
                gsEcDc = Strings.Mid(strenc, m, 1);
                gsCompare = Strings.Mid(strEncKey, m, 1);
                gsFinal = gsFinal + Strings.Chr(Strings.Asc(gsEcDc) + Strings.Asc(gsCompare) + 13);
            }
            return gsFinal;
        }
        public string Decrypt(string strenc)
        {
            int m = 0;
            string strEncKey = null;
            string gsEcDc = null;
            string gsFinal = null;
            string gsCompare = null;
            int glNumber = 0;
            strEncKey = "ASHOKKHE";
            gsFinal = "";
            glNumber = Strings.Len(Strings.Trim(strenc));
            for (m = 1; m <= Math.Round((decimal)glNumber / 8) + 1; m++)
            {
                strEncKey = strEncKey + strEncKey;
            }
            m = 0;
            for (m = 1; m <= glNumber; m++)
            {
                try
                {
                    gsEcDc = Strings.Mid(strenc, m, 1);
                    gsCompare = Strings.Mid(strEncKey, m, 1);
                    gsFinal = gsFinal + Strings.Chr(Strings.Asc(gsEcDc) - Strings.Asc(gsCompare) - 13);
                }
                catch (Exception e) { }
            }
            return gsFinal;
        }

        public bool mfnGetSysSplFeature(string strKeyCode)
        {
            string strsql = "";
            string strcomname = "";
            DataSet dsfind = new DataSet();
            DataSet dscomp = new DataSet();
            bool result = false;
            if (Convert.ToInt32(fnFireQueryTradeWeb("sysobjects", "count(*)", "name", "sysTable", true)) == 0)
            { return false; }
            strsql = "select st_KeyCode ,st_KeyVal From sysTable Where st_KeyCode  = '" + strKeyCode + "'";
            dsfind = OpenDataSet(strsql);
            if (dsfind.Tables[0].Rows.Count > 0)
            {
                strsql = "select em_Name from Entity_master where em_cd =(select min(em_cd) from Entity_master)";
                dscomp = OpenDataSet(strsql);
                if (dscomp.Tables[0].Rows.Count > 0)
                {
                    strcomname = Strings.Left(dscomp.Tables[0].Rows[0]["em_Name"].ToString().ToUpper(), 20).Trim();
                    string chk = Decrypt(dsfind.Tables[0].Rows[0]["st_KeyVal"].ToString());
                    if (chk == (strKeyCode + strcomname).ToUpper())
                        result = true;
                }

            }
            return result;
        }
        public bool mfnGetSysSplFeatureCommodity(string strKeyCode)
        {
            string strsql = "";
            string strcomname = "";
            DataSet dsfind = new DataSet();
            DataSet dscomp = new DataSet();
            bool result = false;
            if (Convert.ToInt32(fnFireQueryTradeWeb("sysobjects", "count(*)", "name", "sysTable", true)) == 0)
            { return false; }
            strsql = "select st_KeyCode ,st_KeyVal From " + GetCommexConnection() + ".sysTable Where st_KeyCode  = '" + strKeyCode + "'";
            dsfind = OpenDataSet(strsql);
            if (dsfind.Tables[0].Rows.Count > 0)
            {
                strsql = "select em_Name from " + GetCommexConnection() + ".Entity_master where em_cd =(select min(em_cd) from Entity_master)";
                dscomp = OpenDataSet(strsql);
                if (dscomp.Tables[0].Rows.Count > 0)
                {
                    strcomname = Strings.Left(dscomp.Tables[0].Rows[0]["em_Name"].ToString().ToUpper(), 20).Trim();
                    if (Decrypt(dsfind.Tables[0].Rows[0]["st_KeyVal"].ToString()) == (strKeyCode + strcomname).ToUpper())
                        result = true;
                }

            }
            return result;
        }


        public string GetSysParmSt(string strParmcd, string strTableName)//GetSysParmSt("ACREPORT", "Stationary");
        {
            string strsql = string.Empty;
            string strReturn = string.Empty;
            if (strTableName == "" || strTableName == null)
            {
                strsql = "Select sp_sysvalue from Sysparameter where sp_parmcd= '" + strParmcd + "'";
                DataSet ObjDataset = OpenDataSet(strsql);
                if (ObjDataset.Tables[0].Rows.Count > 0)
                {
                    strReturn = ObjDataset.Tables[0].Rows[0]["sp_sysvalue"].ToString().Trim();
                    return strReturn;
                }
                else
                {
                    return strReturn;
                }
            }
            else if (Strings.UCase(strTableName) == "STATIONARY")
            {
                strsql = "Select st_sysvalue from Stationary where st_parmcd= '" + strParmcd + "'";
                //strsql = strsql + " and st_companycode = '" + Convert.ToString(sessionCompanyCode) + "' and st_exchange = 'N'";
                strsql = strsql + " and st_companycode = '" + 'A' + "' and st_exchange = 'N'";//"'A'";
                DataSet ObjDataset1 = OpenDataSet(strsql);
                if (ObjDataset1.Tables[0].Rows.Count > 0)
                {
                    strReturn = ObjDataset1.Tables[0].Rows[0]["st_sysvalue"].ToString().Trim();
                    return strReturn;
                }
                else
                { return strReturn; }
            }
            return strReturn;
        }
        public string GetWebParameter(string strParmcd)
        {
            string strsql = string.Empty;
            string strReturn = string.Empty;
            strsql = "select sp_sysvalue  from WebParameter where sp_parmcd ='" + strParmcd + "'";
            DataSet ds = new DataSet();
            ds = OpenDataSet(strsql);
            if (ds.Tables[0].Rows.Count > 0)
            {
                strReturn = Convert.ToString(ds.Tables[0].Rows[0]["sp_sysvalue"]);
                return strReturn;
            }
            else
            {
                return "";
            }

        }
        public string prAcWhere_DPID()
        {
            string strTemp = string.Empty;
            string gsBankName = "TRADEWEB";
            string gstrLedger_DPID = string.Empty;
            string gStrProduct = "F";
            if (gsBankName.Trim() == "TRADEWEB")
                strTemp = GetSysParmSt("ACREPORT", "Stationary");
            else
                strTemp = "CES";
            switch (strTemp.Trim())
            {
                case "CES":
                    gstrLedger_DPID = " ld_dpid= 'ANF' ";
                    //if (gStrProduct == "C")
                    //    gstrLedger_DPID = gstrLedger_DPID + " and Substring(ld_dpid,2,1) in ('F','D','M')";
                    //else
                    //    gstrLedger_DPID = gstrLedger_DPID + " and Substring(ld_dpid,2,1) not in ('F','D','M')";
                    break;
                case "CE":
                    gstrLedger_DPID = "  left(ld_dpid,2)='" + Strings.Left("ANF", 2) + "'";
                    //if (gStrProduct == "C")
                    //    gstrLedger_DPID = gstrLedger_DPID + " and Substring(ld_dpid,2,1) in ('F','D','M')";
                    //else
                    //    gstrLedger_DPID = gstrLedger_DPID + " and Substring(ld_dpid,2,1) not in ('F','D','M')";
                    break;
                case "ES":
                    gstrLedger_DPID = "  right(ld_dpid,2)='" + Strings.Right("ANF", 2) + "'";
                    //if (gStrProduct == "C")
                    //    gstrLedger_DPID = gstrLedger_DPID + " and Substring(ld_dpid,2,1) in ('F','D','M')";
                    //else
                    //    gstrLedger_DPID = gstrLedger_DPID + " and Substring(ld_dpid,2,1) not in ('F','D','M')";
                    break;
                case "CS":
                    gstrLedger_DPID = "  ld_dpid like '" + Strings.Left("ANF", 1) + "_" + Strings.Right("ANF", 1) + "'";
                    //if (gStrProduct == "C")
                    //    gstrLedger_DPID = gstrLedger_DPID + " and Substring(ld_dpid,2,1) in ('F','D','M')";
                    //else
                    //    gstrLedger_DPID = gstrLedger_DPID + " and Substring(ld_dpid,2,1) not in ('F','D','M')";
                    break;
                case "C":
                    gstrLedger_DPID = "  left(ld_dpid,1)='" + Strings.Left("ANF", 1) + "'";
                    //if (gStrProduct == "C")
                    //    gstrLedger_DPID = gstrLedger_DPID + " and Substring(ld_dpid,2,1) in ('F','D','M')";
                    //else
                    //    gstrLedger_DPID = gstrLedger_DPID + " and Substring(ld_dpid,2,1) not in ('F','D','M')";
                    break;

                case "E":
                    gstrLedger_DPID = "  substring(ld_dpid,2,1)='" + Strings.Mid("ANF", 2, 1) + "'";
                    //if (gStrProduct == "C")
                    //    gstrLedger_DPID = gstrLedger_DPID + " and Substring(ld_dpid,2,1) in ('F','D','M')";
                    //else
                    //    gstrLedger_DPID = gstrLedger_DPID + " and Substring(ld_dpid,2,1) not in ('F','D','M')";
                    break;
                case "S":
                    gstrLedger_DPID = "  right(ld_dpid,1)='" + Strings.Right("ANF", 1) + "'";
                    //if (gStrProduct == "C")
                    //    gstrLedger_DPID = gstrLedger_DPID + " and Substring(ld_dpid,2,1) in ('F','D','M')";
                    //else
                    //    gstrLedger_DPID = gstrLedger_DPID + " and Substring(ld_dpid,2,1) not in ('F','D','M')";
                    break;
                case "N":
                    gstrLedger_DPID = "  ld_dpid<>'X'";
                    //if (gStrProduct == "C")
                    //    gstrLedger_DPID = gstrLedger_DPID + " and Substring(ld_dpid,2,1) in ('F','D','M')";
                    //else
                    //    gstrLedger_DPID = gstrLedger_DPID + " and Substring(ld_dpid,2,1) not in ('F','D','M')";
                    break;
            }
            return gstrLedger_DPID.Trim();
        }

        public string fnFireQueryTradeWeb(string strTable, string strSelect, string strParam1, string strParam2, bool strInt)
        {
            string strReturn = string.Empty;
            string strsql = string.Empty;
            if ((strParam1.Length == 0) || (strParam2.Length == 0))
            { strReturn = ""; }

            if (strInt == true)
                strsql = "select " + strSelect + " from " + strTable + " where " + strParam1 + " = '" + strParam2.Trim() + "'";
            else
                strsql = "select " + strSelect + " from " + strTable + " where " + strParam1 + " = " + strParam2.Trim();

            DataSet ObjDataSet = new DataSet();
            ObjDataSet = OpenDataSet(strsql);

            if (ObjDataSet.Tables[0].Rows.Count < 1)
            {
                if (strInt == true)
                { strReturn = ""; }
                else
                { strReturn = "0"; }
            }
            else
            {
                strReturn = ObjDataSet.Tables[0].Rows[0][0].ToString().Trim();
            }
            return strReturn;
        }

        public DataSet fnFireQueryTradeWebDs(string strTable, string strSelect, string strParam1, string strParam2, bool strInt)
        {
            string strReturn = string.Empty;
            string strsql = string.Empty;
            if ((strParam1.Length == 0) || (strParam2.Length == 0))
            { strReturn = ""; }

            if (strInt == true)
                strsql = "select " + strSelect + " from " + strTable + " where " + strParam1 + " = '" + strParam2.Trim() + "'";
            else
                strsql = "select " + strSelect + " from " + strTable + " where " + strParam1 + " = " + strParam2.Trim();

            DataSet ObjDataSet = new DataSet();
            ObjDataSet = OpenDataSet(strsql);

            return ObjDataSet;
        }

        //public DataSet OpenDataSet(string strFireQry)
        //{
        //    using (SqlConnection ObjConnection = new SqlConnection(StrConn))
        //    {
        //        ObjConnection.Open();
        //        using (SqlCommand objSqlCommand = new SqlCommand(strFireQry, ObjConnection))
        //        {
        //            objSqlCommand.Connection = ObjConnection;
        //            objSqlCommand.CommandText = strFireQry;
        //            objSqlCommand.CommandTimeout = 5000;
        //            DataSet MyDataSet = new DataSet();
        //            using (SqlDataAdapter MyAdapter = new SqlDataAdapter(objSqlCommand))
        //            {
        //                MyAdapter.Fill(MyDataSet);
        //                MyAdapter.Dispose();
        //                return MyDataSet;
        //            }
        //        }
        //    }
        //}
        public DataSet OpenDataSet(string strFireQry)
        {
            DataSet ds = new DataSet();
            try
            {
                Connection con = new Connection();
                try
                {
                    con.OpenConection();
                    ds = con.ReturnDataSet(strFireQry);
                    con.CloseConnection();
                    return ds;
                }
                catch (Exception e)
                {
                    con.DisposeConnection();
                    throw e;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //public DataSet OpenDataSet(string strFireQry, bool blncon, int commandTimeout)
        //{
        //    if (myConnection.State == ConnectionState.Closed)
        //    { myConnection.Open(); }

        //    using (SqlCommand objSqlCommand = new SqlCommand(strFireQry, myConnection))
        //    {
        //        objSqlCommand.CommandTimeout = commandTimeout;
        //        using (SqlDataAdapter MyAdapter = new SqlDataAdapter(objSqlCommand))
        //        {
        //            DataSet MyDataSet = new DataSet();
        //            MyAdapter.Fill(MyDataSet);
        //            MyAdapter.Dispose();
        //            return MyDataSet;
        //        }
        //    }
        //}



        //public void ExecuteSQL(string Str_Qry, SqlConnection con)
        //{

        //    using (SqlCommand MyCmd = new SqlCommand())
        //    {
        //        MyCmd.Connection = con;
        //        MyCmd.CommandTimeout = 5000;
        //        MyCmd.CommandText = Str_Qry;
        //        MyCmd.ExecuteNonQuery();
        //        MyCmd.Dispose();
        //    }
        //}


        public void ExecuteSQL(string Str_Qry)
        {
            SqlConnection con;
            using (var db = new DataContext())
            {
                con = new SqlConnection((db.Database.GetDbConnection()).ConnectionString);
            }
            con.Open();
            using (SqlCommand MyCmd = new SqlCommand())
            {
                MyCmd.Connection = con;
                MyCmd.CommandTimeout = 5000;
                MyCmd.CommandText = Str_Qry;
                MyCmd.ExecuteNonQuery();
                MyCmd.Dispose();
            }

        }

        //public void ExecuteSQL(string Str_Qry, bool blnConnClose, int commandTimeout)
        //{
        //    if (myConnection.State == ConnectionState.Closed)
        //    { myConnection.Open(); }
        //    SqlCommand MyCmd = new SqlCommand();
        //    MyCmd.Connection = myConnection;
        //    MyCmd.CommandText = Str_Qry;
        //    MyCmd.CommandTimeout = commandTimeout;
        //    MyCmd.ExecuteNonQuery();
        //    MyCmd.Dispose();
        //    if (blnConnClose)
        //    {
        //        myConnection.Close();
        //    }
        //}

        public string ColourScheme(string ColourFor, string ColourBack)
        {
            char[] ArrSeparters = new char[1];
            ArrSeparters[0] = '/';
            string[] ColourTitle = ConfigurationManager["ColourTitle"].Split(ArrSeparters);
            string[] ColourMenu = ConfigurationManager["ColourMenu"].Split(ArrSeparters);
            string[] ColourHeader = ConfigurationManager["ColourHeader"].Split(ArrSeparters);
            string[] ColourTotals = ConfigurationManager["ColourTotals"].Split(ArrSeparters);
            string ColourCode = string.Empty;
            if ("ColourTitle" == ColourFor)
            {
                ColourCode = "BackColor".ToUpper() == ColourBack.ToUpper() ? ColourTitle[1].ToString() : ColourTitle[0].ToString();
                return ColourCode;
            }
            else if ("ColourMenu" == ColourFor)
            {
                ColourCode = "BackColor".ToUpper() == ColourBack.ToUpper() ? ColourMenu[1].ToString() : ColourMenu[0].ToString();
                return ColourCode;
            }
            else if ("ColourHeader" == ColourFor)
            {
                ColourCode = "BackColor".ToUpper() == ColourBack.ToUpper() ? ColourHeader[1].ToString() : ColourHeader[0].ToString();
                return ColourCode;
            }
            else if ("ColourTotals" == ColourFor)
            {
                ColourCode = "BackColor".ToUpper() == ColourBack.ToUpper() ? ColourTotals[1].ToString() : ColourTotals[0].ToString();
                return ColourCode;
            }
            else
            {
                return string.Empty;
            }
        }
        public string mfngetDemopass()
        {
            string strDate = null;
            string strDemoPass = null;
            int iloop = 0;
            int intTot = 0;
            string strHeader = null;
            string strTrailor = null;
            strDemoPass = (int.Parse(DateTime.Today.ToString().Substring(0, 2)) - DateAndTime.Month(System.DateTime.Today)).ToString() + "/" + DateTime.Today.Month.ToString() + "/" + DateTime.Today.Year.ToString();
            strDemoPass = DateAndTime.Day(Convert.ToDateTime(strDemoPass)).ToString() + DateAndTime.Month(Convert.ToDateTime(strDemoPass)).ToString();
            strDate = System.DateTime.Today.DayOfWeek.ToString();
            for (iloop = 1; iloop <= 3; iloop++)
            {
                intTot = intTot + Strings.Asc(Strings.Mid(strDate, iloop, 1));
            }
            intTot = int.Parse((int.Parse(intTot.ToString()) + Conversion.Val(strDemoPass)).ToString());
            for (iloop = 1; iloop <= 2; iloop++)
            {
                strHeader = strHeader + Strings.Chr(Strings.Asc(Strings.Mid(Strings.Mid(strDate, 3, 2), iloop, 1)) - 1);
            }
            for (iloop = 1; iloop <= 2; iloop++)
            {
                strTrailor = strTrailor + Strings.Chr(Strings.Asc(Strings.Mid(Strings.Mid(strDate, 2, 2), iloop, 1)) + 1);
            }
            strDemoPass = strHeader + intTot + strTrailor;
            return Strings.LCase(strDemoPass);
        }
        public string mfnGetDemoMonthpwd()
        {
            string strHeader = null;
            string sDate = null;
            string sNewDate = null;
            string Date3 = null;
            string Date4 = null;
            string strsMonth = null;
            string strsday = null;
            string strsYear = null;
            string strMonth = null;
            string strYear = null;
            int i = 0;
            System.DateTime strdt = default(System.DateTime);
            strsMonth = System.DateTime.Today.Month.ToString();
            strsYear = System.DateTime.Today.Year.ToString();
            strsday = System.DateTime.Today.Day.ToString();

            strHeader = "";
            sDate = strsday + "/" + strsMonth + "/" + strsYear;
            //dd/mm/yyyy
            strdt = DateTime.Parse(sDate);
            sNewDate = DateAndTime.DateAdd(DateInterval.Month, 0, strdt).ToString();
            strMonth = Strings.UCase(Strings.Left(DateAndTime.DateAdd(DateInterval.Month, DateAndTime.Month(DateTime.Parse(sNewDate)) % 4, strdt).ToString("MMMM", new System.Globalization.CultureInfo("en-US")), 3));
            Date3 = Strings.Right(Strings.FormatDateTime(DateTime.Parse(sDate), 0), 2);

            Date4 = Strings.Left(Strings.FormatDateTime(DateTime.Parse(sDate), 0), 2);
            if (Strings.InStr(1, Date4, "/", CompareMethod.Text) > 0)
            {
                Date4 = 0 + Strings.Mid(Strings.Replace(Strings.FormatDateTime(strdt, DateFormat.ShortDate), "-", "/", 0, Strings.FormatDateTime(strdt, DateFormat.ShortDate).ToString().Length, CompareMethod.Text), 4, 1);
            }
            else
            {
                Date4 = Strings.Mid(Strings.Replace(Strings.FormatDateTime(strdt, DateFormat.ShortDate), "-", "/", 1, Strings.FormatDateTime(strdt, DateFormat.ShortDate).ToString().Length, CompareMethod.Text), 4, 2);
            }
            strYear = Strings.Right(DateAndTime.DateAdd("m", (int.Parse(Date3) * int.Parse(Date4)), sDate).ToString("dd-MM-yyyy"), 2).ToString();
            for (i = 1; i <= Strings.Len(strMonth); i++)
            {
                strHeader = strHeader + Strings.Chr(65 + (Strings.Asc(Strings.Mid(strMonth, i, 1)) % 26));
                if (i < 3)
                {
                    strHeader = strHeader + ((Strings.Asc(Strings.Mid(strYear, i, 1)) * 77 * i) + 7) % 10;
                }
                else
                {
                    strHeader = strHeader + Strings.Chr(65 + ((Strings.Asc(Strings.Mid(strMonth, i, 1)) + 7) % 26));
                }
            }
            return Strings.LCase(strHeader);
        }
        public void PrVisit(string datafield, string ClientId)
        {
            string strsql = string.Empty;
            string strReturn = string.Empty;
            int cnt = 0;
            strsql = "Select " + datafield + " From User_Visit Where uv_cd='" + ClientId + "'";// and uv_date='20090227'";
            DataSet ObjDataset = new DataSet();
            ObjDataset = OpenDataSet(strsql);

            if (ObjDataset.Tables[0].Rows.Count > 0)
            {
                strReturn = ObjDataset.Tables[0].Rows[0][0].ToString();
                if (strReturn.Trim() == "")
                {
                    cnt++;
                }
                else
                {
                    cnt = Convert.ToInt32(strReturn.Trim()) + 1;
                }
                strsql = "Update User_visit set " + datafield + "='" + cnt + "' where uv_cd='" + ClientId + "'";
                ExecuteSQL(strsql);//,uv_date='" + objUtilitydtos(System.DateTime.Today.Date.ToString()) +"'
            }
        }

        public DataSet ConvertJsonToDatatable(string jsonString, string strdeccol)
        {
            DataTable dt = new DataTable();
            DataSet ds = new DataSet();
            jsonString = jsonString.Replace("{\"Data\" : ", "");
            string[] jsonParts = Regex.Split(jsonString.Replace("[", "").Replace("]", ""), "},{");
            List<string> dtColumns = new List<string>();
            //get columns
            foreach (string jp in jsonParts)
            {
                string[] propData = Regex.Split(jp.Replace("{", "").Replace("}", ""), ",");
                foreach (string rowData in propData)
                {
                    try
                    {
                        int idx = rowData.IndexOf(":");
                        var n = rowData.Substring(0, idx - 1);
                        var v = rowData.Substring(idx + 1);
                        if (!dtColumns.Contains(n))
                        {
                            dtColumns.Add(n.Replace("\"", ""));
                        }
                    }
                    catch (Exception ex)
                    {
                        throw new Exception(string.Format("Error Parsing Column Name : {0}", rowData));
                    }

                }
                break;
            }

            foreach (var c in dtColumns)
            {
                if (strdeccol.Contains(c))
                { dt.Columns.Add(c, typeof(decimal)); }
                else
                { dt.Columns.Add(c, typeof(System.String)); }

            }
            //get table data
            foreach (string jp in jsonParts)
            {
                string[] propData = Regex.Split(jp.Replace("{", "").Replace("}", ""), ",");
                DataRow nr = dt.NewRow();
                foreach (string rowData in propData)
                {
                    try
                    {
                        int idx = rowData.IndexOf(":");
                        string n = rowData.Substring(0, idx - 1).Replace("\"", "");
                        string v = rowData.Substring(idx + 1).Replace("\"", "");
                        nr[n] = v;
                    }
                    catch (Exception ex)
                    {
                        continue;
                    }
                }
                dt.Rows.Add(nr);
            }
            ds.Tables.Add(dt);
            return ds;
        }

        public bool WebRequestTest(string strurl)
        {
            string url = strurl;

            if (string.IsNullOrEmpty(strurl))
                return false;
            try
            {
                System.Net.WebRequest myRequest = System.Net.WebRequest.Create(url);
                System.Net.WebResponse myResponse = myRequest.GetResponse();
            }
            catch (System.Net.WebException)
            {
                return false;
            }
            return true;
        }
        public string mfnReplaceForSQLInjection(String strParms)
        {
            if (!string.IsNullOrEmpty(strParms))
            {
                return strParms.Replace("'", "");
            }
            return "";
        }

        public void Changetheme(string struserid, string strcolor)
        {
            DataSet Dschktable = new DataSet();
            string strsql = string.Empty;
            Dschktable = OpenDataSet("select * from SysObjects where name= 'Custom_themes'");
            if (Dschktable.Tables[0].Rows.Count == 0)
            {
                strsql = " create table  Custom_themes ( ";
                strsql += " ct_cd char(16) not null  constraint pk_Customtheme primary key ,";
                strsql += "  ct_color varchar(50) not null,ct_filler1 char(10) not null, ct_filler2 char(10) not null,ct_filler3 char(10)not null )";
                ExecuteSQL(strsql);
            }

            strsql = " IF EXISTS(select * from Custom_themes where ct_cd ='" + struserid + "') ";
            strsql += " update Custom_themes set ct_color='" + strcolor + "' where ct_cd ='" + struserid + "'";
            strsql += "  ELSE";
            strsql += "  insert into Custom_themes values('" + struserid + "','" + strcolor + "','','','');";
            ExecuteSQL(strsql);


        }

        public DataTable OpendatatableXml(string strDataXml, string strtblname)
        {
            DataTable ObjDataTable = new DataTable();
            DataSet dsData = new DataSet();
            XmlDocument doc = new XmlDocument();
            doc.LoadXml(strDataXml);
            using (XmlNodeReader nodereader = new XmlNodeReader(doc))
            {
                dsData.ReadXml(nodereader);
                ObjDataTable = dsData.Tables[strtblname];
            }
            return ObjDataTable;
        }
        public DateTime ConvertDT(string Date)
        {
            int Year = int.Parse(Date.Substring(0, 4));
            int Month = int.Parse(Date.Substring(4, 2));
            int Day = int.Parse(Date.Substring(6, 2));
            return new DateTime(Year, Month, Day);
        }
        public void prCreateTempTable(bool blnCollExcessOppExch, SqlConnection objConnectionTmp)
        {
            string strsql;
            strTempRMSSummary = "#TmpRMSSummaryReport";
            strTempRMSDetail = "#TmpRMSDetailReport";

            try
            {
                strsql = "Drop Table " + strTempRMSSummary;
                ExecuteSQLTmp(strsql, objConnectionTmp);

                strsql = "Create Table " + strTempRMSSummary + " (";
                strsql += " Tmp_Clientcd VarChar(8),";
                strsql += " Tmp_Limit Money,";
                strsql += " Tmp_TplusBal Money,";
                strsql += " Tmp_LoanBal Money,";
                strsql += " Tmp_FundedAmount Money,";
                strsql += " Tmp_FundedMrgReq Money,";
                strsql += " Tmp_CollateralFund Money,";
                strsql += " Tmp_CollateralValue Money,";
                strsql += " Tmp_ShortFallExcess Money,";
                strsql += " Tmp_TradeValue Money,";
                strsql += " Tmp_M2MLoss Money)";
                ExecuteSQLTmp(strsql, objConnectionTmp);
            }
            catch
            {
                strsql = "Create Table " + strTempRMSSummary + " (";
                strsql += " Tmp_Clientcd VarChar(8),";
                strsql += " Tmp_Limit Money,";
                strsql += " Tmp_TplusBal Money,";
                strsql += " Tmp_LoanBal Money,";
                strsql += " Tmp_FundedAmount Money,";
                strsql += " Tmp_FundedMrgReq Money,";
                strsql += " Tmp_CollateralFund Money,";
                strsql += " Tmp_CollateralValue Money,";
                strsql += " Tmp_ShortFallExcess Money,";
                strsql += " Tmp_TradeValue Money,";
                strsql += " Tmp_M2MLoss Money)";
                ExecuteSQLTmp(strsql, objConnectionTmp);
            }
            try
            {
                strsql = "Drop Table " + strTempRMSDetail;
                ExecuteSQLTmp(strsql, objConnectionTmp);

                strsql = "Create Table " + strTempRMSDetail + " (";
                strsql += " Tmp_Type Char(1),";
                strsql += " Tmp_Exchange Char(1),";
                strsql += " Tmp_Clientcd VarChar(8),";
                strsql += " Tmp_Scripcd VarChar(6),";
                strsql += " Tmp_RegForFO VarChar(1),";
                strsql += " Tmp_Qty Numeric,";
                strsql += " Tmp_Rate Money,";
                strsql += " Tmp_MarketRate Money,";
                strsql += " Tmp_Value Money,";
                strsql += " Tmp_MrgHairCut Money,";
                strsql += " Tmp_NetValue Money)";
                ExecuteSQLTmp(strsql, objConnectionTmp);
            }
            catch
            {
                strsql = "Create Table " + strTempRMSDetail + " (";
                strsql += " Tmp_Type Char(1),";
                strsql += " Tmp_Exchange Char(1),";
                strsql += " Tmp_Clientcd VarChar(8),";
                strsql += " Tmp_Scripcd VarChar(6),";
                strsql += " Tmp_RegForFO VarChar(1),";
                strsql += " Tmp_Qty Numeric,";
                strsql += " Tmp_Rate Money,";
                strsql += " Tmp_MarketRate Money,";
                strsql += " Tmp_Value Money,";
                strsql += " Tmp_MrgHairCut Money,";
                strsql += " Tmp_NetValue Money)";
                ExecuteSQLTmp(strsql, objConnectionTmp);
            }
        }
        public Boolean fnchkTable(string strTable)
        {
            DataTable dtChk = OpenDataSet("select count(*) from sysobjects where name ='" + strTable + "'").Tables[0];
            return Conversion.Val(dtChk.Rows[0][0].ToString()) > 0;
        }
        public void prCreateTableHolding(SqlConnection objConnectionTmp)
        {
            string strSql = "";
            try
            {
                strSql = "Drop table #tmpHoldingrepM";
                ExecuteSQLTmp(strSql, objConnectionTmp);

            }
            catch
            {
                strSql = "Create table  #tmpHoldingrepM (";
                strSql += " dm_Type Varchar(9),";
                strSql += " dm_clientcd char(8),";
                strSql += " dm_ActNo char(16),";
                strSql += " dm_scripcd char(6),";
                strSql += " dm_ISIN char(12),";
                strSql += " dm_bcqty Numeric,";
                strSql += " dm_lastprice money,";
                strSql += " dm_approved Varchar(20),";
                strSql += " dm_grossvalue money,";
                strSql += " dm_haircut money,";
                strSql += " dm_haircutvalue money,";
                strSql += " dm_netvalue money) ";
                ExecuteSQLTmp(strSql, objConnectionTmp);
            }



        }
        public void prPledgeProcess(string strDT, string strExchange, string strWhere, bool blnCollExcessOppExch, bool blnisFundTransfer, bool blnIncludeUNDEL, SqlConnection objConnectionTmp, string sessionCompanyCode)
        {

            Boolean blnVarHairCut = true;
            double dblHairCut = Conversion.Val(GetSysParmSt("MTFP_HAIRCUT", ""));
            string strT2Date = "";
            string strsql;
            if (!fnchkTable("FMargins_MTF") && GetSysParmSt("MTFPLDCOLLAT", "") == "N")
            {
                strsql = "Create Table FMargins_MTF (";
                strsql += "Fmt_Companycode char(1) Not Null,  ";
                strsql += "Fmt_Dt varchar(8) Not Null,  ";
                strsql += "Fmt_Exchange char(1) Not Null,  ";
                strsql += "Fmt_Clientcd varchar(8) Not Null,  ";
                strsql += "Fmt_scripcd varchar(6) Not Null,  ";
                strsql += "Fmt_qty Numeric Not Null,  ";
                strsql += "Fmt_Rate Money Not Null,  ";
                strsql += "Fmt_Value Money Not Null,  ";
                strsql += "Fmt_HairCut Money Not Null,  ";
                strsql += "Fmt_HairCutValue Money Not Null,  ";
                strsql += "Fmt_NetValue Money Not Null,  ";
                strsql += "Fmt_Type Char(1) Not Null,  ";
                strsql += "Fmt_Filler1 varchar(1) Not Null,  ";
                strsql += "Fmt_Filler2 varchar(1) Not Null,  ";
                strsql += "Fmt_Filler3 varchar(1) Not Null,  ";
                strsql += "Fmt_NFiller1 Numeric Not Null,  ";
                strsql += "Fmt_NFiller2 Numeric Not Null,  ";
                strsql += "Fmt_NFiller3 Numeric Not Null,  ";
                strsql += "CONSTRAINT [PK_FMargins_MTF] PRIMARY KEY CLUSTERED (";
                strsql += "[Fmt_Companycode] ASC,";
                strsql += "[Fmt_Dt] ASC,";
                strsql += "[Fmt_Exchange] ASC,";
                strsql += "[Fmt_Clientcd] ASC,";
                strsql += "[Fmt_scripcd] ASC,";
                strsql += "[Fmt_Type] ASC)";
                strsql += ")";
                ExecuteSQLTmp(strsql, objConnectionTmp);
            }

            dblHairCut = 0;
            Boolean blnBSE = false;
            Boolean blnNSE = false;
            string strDMTBSE = "";
            string strDMTNSE = "";
            string strDMTColBSE = "";
            string strDMTColNSE = "";
            blnBSE = (GetSysParmSt("MTFP_LICBSE", "") == "Y");
            blnNSE = (GetSysParmSt("MTFP_LICNSE", "") == "Y");
            strDMTBSE = GetSysParmSt("MTFP_DMTBSEP", "");
            strDMTNSE = GetSysParmSt("MTFP_DMTNSEP", "");
            strDMTColBSE = "";
            strDMTColNSE = "";
            strDMTColBSE = GetSysParmSt("MTFP_DMBSECP", "");
            strDMTColNSE = GetSysParmSt("MTFP_DMNSECP", "");

            prCreateTableHolding(objConnectionTmp);
            prCreateTempTable(blnCollExcessOppExch, objConnectionTmp);

            strsql = "Insert into " + strTempRMSSummary;
            strsql += " select MTFC_CMcd,MTFC_AllowLimit,0,0,0,0,0,0,0,0,0";
            strsql += " from MrgTdgFin_Clients,Client_master ";
            strsql += " Where MTFC_CMcd = cm_cd and (MTFC_Status='A' or (MTFC_Status='I' and MTFC_FillerA >'" + strDT + "'))  and MTFC_RegDt<='" + strDT + "'" + strWhere;
            ExecuteSQLTmp(strsql, objConnectionTmp);

            strsql = "Update " + strTempRMSSummary;
            strsql += " set Tmp_TplusBAL = A.ld_amount ";
            strsql += " from (select MTFC_CMcd,Sum(ld_amount) ld_amount ";
            strsql += " from Ledger,MrgTdgFin_Clients,Client_master";
            strsql += " Where cm_cd = MTFC_CMcd and (ld_clientcd = cm_cd " + ((GetSysParmSt("MTFP_MRGNBAL", "").Trim()) == "Y" ? " or ld_clientcd= cm_brkggroup" : "") + ")";
            strsql += " and ld_dt <='" + strDT + "'";
            strsql += " Group By MTFC_CMcd ) a ";
            strsql += " Where MTFC_CMcd = Tmp_Clientcd ";
            ExecuteSQLTmp(strsql, objConnectionTmp);

            strT2Date = mfnGetT2Dt(strExchange, strDT);
            strsql = "Update " + strTempRMSSummary;
            strsql += " set Tmp_LoanBal = A.ld_amount ";
            strsql += " from (select MTFC_CMcd,Sum(ld_amount) ld_amount ";
            strsql += " from Ledger,MrgTdgFin_Clients,Client_master ";
            strsql += " Where MTFC_CMcd = cm_Cd and ld_clientcd = Rtrim(MTFC_FillerB) ";
            strsql += " and ld_dt <= '" + strT2Date + "' ";

            if (strExchange != "")
                strsql += " and left(ld_DPID,2) = '" + Convert.ToString(sessionCompanyCode) + strExchange + "'";
            else
                strsql += " and left(ld_DPID,1) = '" + Convert.ToString(sessionCompanyCode) + "'";

            strsql += " Group By MTFC_CMcd ) a ";
            strsql += " Where MTFC_CMcd = Tmp_Clientcd ";
            ExecuteSQLTmp(strsql, objConnectionTmp);

            strsql = "Update " + strTempRMSSummary;
            strsql += " set Tmp_CollateralFund = A.ld_amount ";
            strsql += " from (select MTFC_CMcd,Sum(-ld_amount) ld_amount ";
            strsql += " from Ledger,MrgTdgFin_Clients,Client_master";
            strsql += " Where MTFC_CMcd = cm_Cd and ld_clientcd = Rtrim(MTFC_FillerB) and ld_documentType in ('P','R') ";
            strsql += " and ld_dt <='" + strDT + "'";
            if (strExchange != "")
                strsql += " and left(ld_DPID,2) = '" + Convert.ToString(sessionCompanyCode) + strExchange + "'";
            else
                strsql += " and left(ld_DPID,1) = '" + Convert.ToString(sessionCompanyCode) + "'";

            strsql += " Group By MTFC_CMcd Having Sum(-ld_amount) >  0 ) a ";
            strsql += " Where MTFC_CMcd = Tmp_Clientcd ";
            ExecuteSQLTmp(strsql, objConnectionTmp);

            strsql = "Update " + strTempRMSSummary;
            strsql += " set Tmp_TradeValue = A.TradeValue ";
            strsql += " from (select MTtd_clientcd, Round(Sum((MTtd_bqty-MTtd_sqty)*MTtd_Rate),2) TradeValue ";
            strsql += " from MrgTdgFin_TRX,Settlements,Client_master ";
            strsql += " Where MTtd_clientcd = cm_Cd and MTtd_companycode = '" + Convert.ToString(sessionCompanyCode) + "' and MTTd_stlmnt = se_stlmnt and se_shpayoutdt ='" + strDT + "' and MTtd_TrxFlag <> 'X'";
            strsql += " Group By MTtd_clientcd ) a ";
            strsql += " Where MTtd_clientcd = Tmp_Clientcd ";
            ExecuteSQLTmp(strsql, objConnectionTmp);

            strsql = " Insert into " + strTempRMSDetail;
            strsql += " select 'M' type,MTtd_Exchange,";
            strsql += " MTtd_clientcd,MTtd_scripcd,'N',Sum(MTtd_bqty-MTtd_sqty),Round(Sum((MTtd_bqty-MTtd_sqty)*MTtd_rate)/Sum(MTtd_bqty-MTtd_sqty),2),0,0,0,0 ";
            strsql += " from MrgTdgFin_TRX,Client_master ";
            strsql += " Where MTtd_clientcd = cm_Cd and MTtd_companycode = '" + Convert.ToString(sessionCompanyCode) + "'";
            strsql += " and MTtd_dt <='" + strDT + "' and MTtd_TrxFlag <> 'X'" + strWhere;
            if (strExchange != "")
                strsql += " and MTtd_Exchange = '" + strExchange + "'";

            strsql += " Group by MTtd_clientcd,MTtd_scripcd,MTtd_Exchange,MTtd_CntStlmnt";
            strsql += " Having Sum(MTtd_bqty-MTtd_sqty) > 0 ";
            ExecuteSQLTmp(strsql, objConnectionTmp);


            if (GetSysParmSt("MTFPLDCOLLAT", "") == "N" && Conversion.Val(strDT) >= Conversion.Val("20200901"))
            {
                string strLastCollatDT = fnFireQueryTradeWeb("Fmargins_MTF", "isNull(max(Fmt_Dt),'')", "Fmt_Dt <='" + strDT + "' and Fmt_Companycode", Convert.ToString(sessionCompanyCode), true);

                strsql = " Insert into " + strTempRMSDetail;
                strsql += " select 'C' type,Fmt_Exchange,";
                strsql += " Fmt_Clientcd,Fmt_scripcd,'N',Sum(Fmt_qty),0,0,0,0,0 ";
                strsql += " from Fmargins_MTF,Client_master ";
                strsql += " Where Fmt_Clientcd = cm_Cd and Fmt_Companycode = '" + Convert.ToString(sessionCompanyCode) + "'";
                strsql += " and Fmt_Dt = '" + strLastCollatDT + "'" + strWhere;
                if (strExchange != "")
                    strsql += " and Fmt_Exchange = '" + strExchange + "'";

                strsql += " Group by Fmt_Clientcd,Fmt_scripcd,Fmt_Exchange,Fmt_Type";
                strsql += " Having Sum(Fmt_qty) > 0 ";
                ExecuteSQLTmp(strsql, objConnectionTmp);
            }
            else
            {
                strsql = " Insert into " + strTempRMSDetail;
                strsql += " select 'C' type,case MPT_OurDP When '" + strDMTColBSE + "' Then 'B' When '" + strDMTColNSE + "' Then 'N' else '' end MPT_Exchange,";
                strsql += " MPT_clientcd,MPT_scripcd,'N',Sum(case MPT_DRCR When 'C' Then MPT_Qty else -MPT_Qty end),0,0,0,0,0 ";
                strsql += " from MrgTdgFin_Pledge,Client_master,OurDPS ";
                strsql += " Where MPT_clientcd = cm_Cd and MPT_companycode = '" + Convert.ToString(sessionCompanyCode) + "'";
                strsql += " and MPT_dt <= '" + strDT + "' and MPT_TrxFlag = 'P'" + strWhere;
                if (strExchange != "")
                    strsql += " and MPT_OurDP = '" + (strExchange == "B" ? strDMTColBSE : strDMTColNSE) + "'";

                strsql += " and od_cd = MPT_OurDP and od_ActType = 'H' ";
                strsql += " Group by MPT_clientcd,MPT_scripcd,MPT_OurDP";
                strsql += " Having Sum(case MPT_DRCR When 'C' Then MPT_Qty else -MPT_Qty end) > 0 ";
                ExecuteSQLTmp(strsql, objConnectionTmp);
            }

            if (strExchange == "")
            {
                strsql = "Delete " + strTempRMSDetail + " Where Tmp_Type = 'C' and Tmp_Scripcd not in (select MTFD_SecCD from MrgTdgFin_SecuritiesDt ";
                strsql += " Where MTFD_SecDt = (Select max(MTFD_SecDt) from MrgTdgFin_SecuritiesDt where MTFD_SecDt <= '" + strDT + "') and (MTFD_BSE = 'Y' Or MTFD_NSE = 'Y'))";
                ExecuteSQLTmp(strsql, objConnectionTmp);
            }
            else
            {
                strsql = "Delete " + strTempRMSDetail + " Where Tmp_Type = 'C' and Tmp_Scripcd not in (select MTFD_SecCD from MrgTdgFin_SecuritiesDt ";
                strsql += " Where MTFD_SecDt = (Select max(MTFD_SecDt) from MrgTdgFin_SecuritiesDt where MTFD_SecDt <= '" + strDT + "') and MTFD_" + strExchange + "SE = 'Y')";
                ExecuteSQLTmp(strsql, objConnectionTmp);
            }

            char[] ArrSeparators = new char[1];
            ArrSeparators[0] = '/';
            string strExch = ((blnBSE == true ? "B/" : "") + (blnNSE == true ? "N/" : ""));
            strExch = strExch.Trim() != "" ? Strings.Left(strExch, strExch.Length - 1) : "";
            string[] arrExch = strExch.Split(ArrSeparators);



            strsql = "Update " + strTempRMSDetail;
            strsql += " Set Tmp_RegForFO = 'Y'  ";
            strsql += " From Product_master, Securities  ";
            strsql += " Where pm_assetcd = Case pm_Exchange When 'B' Then ss_Bsymbol Else ss_nsymbol End and pm_segment = 'F' ";
            strsql += " and Tmp_Scripcd  = ss_cd ";
            strsql += " and exists ( select pe_assetcd from Product_Expiry Where pe_assetcd = pm_assetcd and pe_exchange = pm_exchange and pe_segment = 'F' and pe_expirydt >= '" + strDT + "')";
            ExecuteSQLTmp(strsql, objConnectionTmp);
            string strLastRateDt = "";

            for (int i = 0; i <= arrExch.Length - 1; i++)
            {
                strLastRateDt = strDT;
                if (Conversion.Val(fnFireQueryTradeWeb("market_rates", "Count(0)", "mk_exchange = '" + arrExch[i] + "' and mk_dt", strLastRateDt, true)) == 0)
                    strLastRateDt = GetTMinusPlusdt(strLastRateDt, -1, arrExch[i]);


                strsql = "Update " + strTempRMSDetail;
                strsql += " set Tmp_MarketRate = mk_closerate from market_rates a with (nolock) where mk_exchange = '" + arrExch[i] + "' and mk_scripcd=Tmp_Scripcd ";
                strsql += " and mk_dt= (select max(mk_dt) from market_rates with(nolock) where mk_exchange = '" + arrExch[i] + "'";
                strsql += " and mk_dt <='" + strLastRateDt + "') ";
                strsql += " and Tmp_Type= 'M' and Tmp_Exchange = '" + arrExch[i] + "'";
                ExecuteSQLTmp(strsql, objConnectionTmp);

                strsql = "Update " + strTempRMSDetail;
                strsql += " set Tmp_MarketRate = mk_closerate from market_rates a with (nolock) where mk_exchange = '" + arrExch[i] + "' and mk_scripcd=Tmp_Scripcd ";
                strsql += " and mk_dt= (select max(mk_dt) from market_rates with(nolock) where mk_exchange = '" + arrExch[i] + "'";
                strsql += " and mk_dt <= '" + GetTMinusPlusdt(strLastRateDt, -1, arrExch[i]) + "') ";
                strsql += " and Tmp_Type <> 'M' and Tmp_Exchange = '" + arrExch[i] + "'";
                ExecuteSQLTmp(strsql, objConnectionTmp);

                strsql = "update " + strTempRMSDetail;
                strsql += " set Tmp_MrgHairCut = ";
                strsql += " case Tmp_Type When 'C' Then Case When vm_exchange = 'N' then vm_applicable_var else vm_margin_rate end else (Case When vm_exchange = 'N' then vm_applicable_var else vm_margin_rate end * " + GetSysParmSt("MTFP_VARMAR", "") + " ) + (vm_max_loss * case Tmp_RegForFO When 'Y' Then " + GetSysParmSt("MTFP_ELMRGFO", "") + " else " + GetSysParmSt("MTFP_ELMNTFO", "") + " end ) end ";
                strsql += " from VarMargin  ";
                strsql += " where vm_scripcd = Tmp_Scripcd and vm_exchange = '" + arrExch[i] + "'";
                strsql += " and vm_dt =(select max(vm_dt) from VarMargin ";
                strsql += " Where vm_exchange = '" + arrExch[i] + "' ";
                strsql += " and vm_dt <= '" + GetTMinusPlusdt(strLastRateDt, -1, arrExch[i]) + "')";
                strsql += " and Tmp_Exchange = '" + arrExch[i] + "'";
                ExecuteSQLTmp(strsql, objConnectionTmp);
            }

            strsql = "update " + strTempRMSDetail + "  set Tmp_MrgHairCut = " + dblHairCut + " Where Tmp_MrgHairCut < " + dblHairCut;
            ExecuteSQLTmp(strsql, objConnectionTmp);

            strsql = "update " + strTempRMSDetail + "  set Tmp_MrgHairCut = 100 Where Tmp_MrgHairCut > 100 ";
            ExecuteSQLTmp(strsql, objConnectionTmp);

            ExecuteSQLTmp("Alter Table " + strTempRMSDetail + " Add Tmp_NonAppoved varchar(1) ", objConnectionTmp);
            ExecuteSQLTmp("Update " + strTempRMSDetail + " Set Tmp_NonAppoved = '' ", objConnectionTmp);

            strsql = "Update " + strTempRMSDetail;
            strsql += " set Tmp_NonAppoved = 'Y' ";
            strsql += " Where Tmp_Type = 'M' and Tmp_Scripcd not in (select MTFD_SecCD from MrgTdgFin_SecuritiesDt ";
            strsql += " Where MTFD_SecDt = (Select max(MTFD_SecDt) from MrgTdgFin_SecuritiesDt where MTFD_SecDt <= '" + strDT + "') ";
            if (strExchange == "")
                strsql += " and (MTFD_BSE = 'Y' Or MTFD_NSE = 'Y')) ";
            else
                strsql += " and MTFD_" + strExchange + "SE = 'Y') ";

            ExecuteSQLTmp(strsql, objConnectionTmp);

            strsql = "Update " + strTempRMSDetail;
            strsql += " set Tmp_NonAppoved = 'Y', Tmp_MrgHairCut = 0, Tmp_MarketRate = 0 ";
            strsql += " Where Tmp_Type = 'C' and Tmp_Scripcd not in (select MTFD_SecCD from MrgTdgFin_SecuritiesDt ";
            strsql += " Where MTFD_SecDt = (Select max(MTFD_SecDt) from MrgTdgFin_SecuritiesDt where MTFD_SecDt <= '" + strDT + "') ";
            if (strExchange == "")
                strsql += " and (MTFD_BSE = 'Y' Or MTFD_NSE = 'Y')) ";
            else
                strsql += " and MTFD_" + strExchange + "SE = 'Y') ";

            ExecuteSQLTmp(strsql, objConnectionTmp);

            strsql = "update " + strTempRMSDetail + "  set Tmp_Value = Round(Tmp_Qty*Tmp_Rate,2) Where Tmp_Type= 'M' ";
            ExecuteSQLTmp(strsql, objConnectionTmp);

            strsql = "update " + strTempRMSDetail + "  set Tmp_Value = Round(Tmp_Qty*Tmp_MarketRate,2) Where Tmp_Type <> 'M' ";
            ExecuteSQLTmp(strsql, objConnectionTmp);

            strsql = "update " + strTempRMSDetail + "  set Tmp_NetValue = Round(Tmp_Value*((Tmp_MrgHairCut)/100),2) Where Tmp_Type = 'M' ";
            ExecuteSQLTmp(strsql, objConnectionTmp);

            strsql = "update " + strTempRMSDetail + "  set Tmp_NetValue = Round(Tmp_Value*((100-Tmp_MrgHairCut)/100),2) Where Tmp_Type <> 'M' ";
            ExecuteSQLTmp(strsql, objConnectionTmp);

            strsql = "Update " + strTempRMSSummary + "  set Tmp_FundedAmount = A.Tmp_FundedAmount, Tmp_FundedMrgReq  = A.Tmp_FundedMrgReq, ";
            strsql += " Tmp_CollateralValue = A.Tmp_CollateralValue ";
            strsql += " from (select Tmp_Clientcd Clientcd,";
            strsql += " Round(Sum(case Tmp_Type When 'M' Then Tmp_Value else 0 end),2) Tmp_FundedAmount, ";
            strsql += " Round(Sum(case Tmp_Type When 'M' Then Tmp_NetValue else 0 end),2) Tmp_FundedMrgReq , ";
            strsql += " Round(Sum(case Tmp_Type When 'C' Then Tmp_NetValue else 0 end),2) Tmp_CollateralValue ";
            strsql += " from " + strTempRMSDetail;
            strsql += " Group By Tmp_Clientcd ) a ";
            strsql += " Where Clientcd = Tmp_Clientcd ";
            ExecuteSQLTmp(strsql, objConnectionTmp);

            strsql = "Update " + strTempRMSSummary + "  set Tmp_M2MLoss = b.M2MLoss From ( ";
            strsql += " select Tmpclientcd,Sum(M2MLoss) M2MLoss from ( ";
            strsql += " select Tmp_clientcd Tmpclientcd,Tmp_Scripcd,Round(Sum((Tmp_Rate-Tmp_MarketRate)*Tmp_Qty),2) M2MLoss ";
            strsql += " From " + strTempRMSDetail;
            strsql += " Where Tmp_type = 'M' ";
            strsql += " Group By Tmp_clientcd,Tmp_Scripcd Having Round(Sum((Tmp_Rate-Tmp_MarketRate)*Tmp_Qty),2)  > 0 ) A Group By Tmpclientcd ) b ";
            strsql += " Where Tmp_Clientcd = Tmpclientcd ";
            ExecuteSQLTmp(strsql, objConnectionTmp);

            strsql = "Update " + strTempRMSSummary + "  set Tmp_ShortFallExcess = (Tmp_CollateralFund+Tmp_CollateralValue)-(Tmp_FundedMrgReq+Tmp_M2MLoss) ";
            ExecuteSQLTmp(strsql, objConnectionTmp);
        }
        public void prProcess(string strDT, string strExchange, string strWhere, bool blnCollExcessOppExch, bool blnisFundTransfer, bool blnIncludeUNDEL, SqlConnection objConnectionTmp)
        {
            string strsql;
            double dblHairCut = Conversion.Val(GetSysParmSt("MTFP_HAIRCUT", ""));
            string strT2Date = string.Empty;

            bool blnBSE;
            bool blnNSE;
            blnBSE = (fnFireQueryTradeWeb("Sysparameter", "sp_sysvalue", "sp_parmcd", "MTFP_LICBSE", true) == "Y");
            blnNSE = (fnFireQueryTradeWeb("Sysparameter", "sp_sysvalue", "sp_parmcd", "MTFP_LICNSE", true) == "Y");

            dblHairCut = 0;

            prCreateTempTable(blnCollExcessOppExch, objConnectionTmp);

            strsql = "Insert into " + strTempRMSSummary;
            strsql += " select MTFC_CMcd,MTFC_AllowLimit,0,0,0,0,0,0,0,0,0";
            strsql += " from MrgTdgFin_Clients,Client_master ";
            strsql += " Where cm_cd = MTFC_CMcd and MTFC_Status='A' and MTFC_RegDt<='" + strDT + "'" + strWhere;
            ExecuteSQLTmp(strsql, objConnectionTmp);

            strsql = "Update " + strTempRMSSummary;
            strsql += " set Tmp_TplusBAL = A.ld_amount ";
            strsql += " from (select MTFC_CMcd,Sum(ld_amount) ld_amount ";
            strsql += " from Ledger,MrgTdgFin_Clients,Client_master";
            strsql += " Where cm_cd = MTFC_CMcd and (ld_clientcd = cm_cd " + (GetSysParmSt("MTFP_MRGNBAL", "") == "Y" ? " or ld_clientcd= cm_brkggroup" : "") + ")";
            strsql += " and ld_dt <='" + strDT + "' "; // and " & mfnGetLedType(cmbTPLedgerBal)
            strsql += " Group By MTFC_CMcd ) a ";
            strsql += " Where MTFC_CMcd = Tmp_Clientcd ";
            ExecuteSQLTmp(strsql, objConnectionTmp);

            string strT2 = mfnGetT2Dt(strExchange, strDT);

            strsql = "Update " + strTempRMSSummary + Constants.vbNewLine;
            strsql += " set Tmp_LoanBal = A.ld_amount " + Constants.vbNewLine;
            strsql += " from (select MTFC_CMcd,Sum(ld_amount) ld_amount " + Constants.vbNewLine;
            strsql += " from Ledger,MrgTdgFin_Clients" + Constants.vbNewLine;
            strsql += " Where ld_clientcd = Rtrim(MTFC_CMcd) + '" + GetSysParmSt("MTFP_SUFFIX", "") + "' and ld_documentType not in ('P','R') ";
            strsql += " and ld_dt <= '" + strT2 + "' " + Constants.vbNewLine;
            strsql += " Group By MTFC_CMcd ) a " + Constants.vbNewLine;
            strsql += " Where MTFC_CMcd = Tmp_Clientcd " + Constants.vbNewLine;
            ExecuteSQLTmp(strsql, objConnectionTmp);

            strsql = "Update " + strTempRMSSummary;
            strsql += " set Tmp_CollateralFund = A.ld_amount ";
            strsql += " from (select MTFC_CMcd,Sum(-ld_amount) ld_amount ";
            strsql += " from Ledger,MrgTdgFin_Clients";
            strsql += " Where ld_clientcd = Rtrim(MTFC_CMcd) + '" + GetSysParmSt("MTFP_SUFFIX", "") + "' and ld_documentType in ('P','R') ";
            strsql += " and ld_dt <='" + strDT + "'";
            strsql += " Group By MTFC_CMcd Having Sum(-ld_amount) >  0 ) a ";
            strsql += " Where MTFC_CMcd = Tmp_Clientcd ";
            ExecuteSQLTmp(strsql, objConnectionTmp);

            strsql = "Update " + strTempRMSSummary;
            strsql += " set Tmp_TradeValue = A.TradeValue ";
            strsql += " from (select MTtd_clientcd, Round(Sum((MTtd_bqty-MTtd_sqty)*MTtd_Rate),2) TradeValue ";
            strsql += " from MrgTdgFin_TRX,Settlements ";
            strsql += " Where MTTd_stlmnt = se_stlmnt and se_shpayoutdt ='" + strDT + "'";
            strsql += " Group By MTtd_clientcd ) a ";
            strsql += " Where MTtd_clientcd = Tmp_Clientcd ";
            ExecuteSQLTmp(strsql, objConnectionTmp);

            string strSelectedOurDPS = "";
            string strOurDPSWhere = "";
            if (!blnisFundTransfer)
            {
                if (strExchange != "")
                {
                    if (strExchange == "B")
                    {
                        strSelectedOurDPS += GetSysParmSt("MTFP_DMTBSE", "");
                        strSelectedOurDPS += "," + GetSysParmSt("MTFP_DMTBSEC", "");
                    }
                    else
                    {
                        strSelectedOurDPS += GetSysParmSt("MTFP_DMTNSE", "");
                        strSelectedOurDPS += "," + GetSysParmSt("MTFP_DMTNSEC", "");
                    }
                }
                else
                {
                    strSelectedOurDPS += GetSysParmSt("MTFP_DMTBSE", "") + "," + GetSysParmSt("MTFP_DMTNSE", "");
                    strSelectedOurDPS += "," + GetSysParmSt("MTFP_DMTBSEC", "") + "," + GetSysParmSt("MTFP_DMTNSEC", "");
                }
                string[] OurDPS = strSelectedOurDPS.Split(',');

                for (int i = 0; i <= OurDPS.Length - 1; i++)
                {
                    if (OurDPS[i] != "")
                        strOurDPSWhere += "'" + OurDPS[i] + "',";
                }
            }

            if (strOurDPSWhere.Length > 0)
            {
                strOurDPSWhere = Strings.Left(strOurDPSWhere, (strOurDPSWhere.Length) - 1);
                strOurDPSWhere = " and dm_ourDP in (" + strOurDPSWhere + ")";
            }

            strsql = " Insert into " + strTempRMSDetail;
            strsql += " select case When od_actType = 'C' Then 'C' else 'X' End type,";
            strsql += " case dm_OurDP When '" + GetSysParmSt("MTFP_DMTBSE", "").Trim().Trim() + "' Then 'B' When '" + GetSysParmSt("MTFP_DMTNSE", "").Trim().Trim() + "' Then 'N'";
            strsql += " When '" + GetSysParmSt("MTFP_DMTBSEC", "").Trim().Trim() + "' Then 'B' When '" + GetSysParmSt("MTFP_DMTNSEC", "").Trim().Trim() + "' Then 'N' else '' end,";
            strsql += " dm_clientcd,dm_Scripcd,'N',Sum(Qty),0,0,0,0,0 from ( ";
            strsql += " select 'X' type,dm_OurDP,dm_clientcd,dm_Scripcd,Sum(-dm_qty) Qty";
            strsql += " from Demat,Settlements,Client_master";
            strsql += " Where dm_clientcd = cm_cd and dm_stlmnt=se_stlmnt ";
            strsql += " and se_shpayoutdt <='" + strDT + "'";
            strsql += " and dm_Dt <='" + strDT + "'" + strOurDPSWhere;
            strsql += " and dm_type ='BC' and dm_locked ='N' and dm_transfered = 'N'";
            strsql += " Group By dm_OurDP,dm_clientcd,dm_Scripcd";

            //Future Date Payout to Client & Benf to Pool
            strsql += " union all ";
            strsql += " select 'X' type,dm_OurDP,dm_clientcd,dm_Scripcd,Sum(-dm_qty) Qty";
            strsql += " from Demat,Settlements,Client_master";
            strsql += " Where dm_clientcd = cm_cd and dm_stlmnt=se_stlmnt ";
            strsql += " and se_shpayoutdt <='" + strDT + "'";
            strsql += " and dm_Dt <='" + strDT + "'";
            strsql += " and dm_execdt > '" + strDT + "'" + strOurDPSWhere;
            strsql += " and dm_type ='BC' and (dm_locked <>'N' Or dm_transfered <> 'N') ";
            strsql += " Group By dm_OurDP,dm_clientcd,dm_Scripcd";

            //Expected
            strsql += " union all ";
            strsql += " select 'X',dm_OurDP,dm_clientcd,dm_Scripcd,Sum(-dm_qty)";
            strsql += " from Demat,Settlements,Client_master";
            strsql += " Where dm_stlmnt=se_stlmnt and dm_clientcd = cm_cd " + strWhere;
            strsql += " and se_stdt <= '" + strDT + "'";
            strsql += " and se_shpayoutdt > '" + strDT + "'";
            strsql += " and exists ( select MTtd_Stlmnt,MTtd_dt,MTtd_TrxFlag,MTtd_clientcd,MTtd_scripcd,MTtd_bsflag from MrgTdgFin_TRX,Settlements ";
            strsql += " Where MTtd_Stlmnt=se_stlmnt and se_stdt <= '" + strDT + "' and se_shpayoutdt > '" + strDT + "' ";
            strsql += " and MTtd_Stlmnt = dm_stlmnt and MTtd_TrxFlag = 'N' and MTtd_bsflag = 'B' and MTtd_clientcd = dm_clientcd and MTtd_scripcd = dm_scripcd  and MTtd_bsflag = case When dm_type = 'BC' Then 'B' else 'S' end ) " + strOurDPSWhere;
            strsql += " and dm_type ='BC'";
            strsql += " Group By dm_OurDP,dm_clientcd,dm_Scripcd";
            strsql += " ) a,ourDPS Where dm_OurDP = od_cd Group By type,dm_OurDP,dm_clientcd,dm_Scripcd,od_actType";
            ExecuteSQLTmp(strsql, objConnectionTmp);

            if (GetSysParmSt("MTFP_COLLAT", "") == "Y")
            {
                strsql = " Delete " + strTempRMSDetail + " Where Tmp_Type = 'X' ";
                ExecuteSQLTmp(strsql, objConnectionTmp);
            }

            strsql = "Insert into " + strTempRMSDetail;
            strsql += " select 'M',MTtd_Exchange,MTtd_clientcd,MTtd_scripcd,'N',Sum(MTtd_bqty-MTtd_sqty),Round(Sum((MTtd_bqty-MTtd_sqty)*MTtd_rate)/Sum(MTtd_bqty-MTtd_sqty),2),0,0,0,0";
            strsql += " from MrgTdgFin_TRX,client_master ";
            strsql += " Where MTtd_clientcd=cm_cd and MTtd_dt <= '" + strDT + "'" + strWhere;
            if (strExchange != "")
                strsql += " and MTtd_Exchange = '" + strExchange + "'";
            strsql += " Group By MTtd_Exchange,MTtd_clientcd,MTtd_scripcd";   // ,MTtd_CntStlmnt
            strsql += " Having Sum(MTtd_bqty-MTtd_sqty) <> 0 ";
            ExecuteSQLTmp(strsql, objConnectionTmp);

            strsql = "Insert into " + strTempRMSDetail;
            strsql += " select 'C',Tmp_Exchange,Tmp_Clientcd,Tmp_Scripcd,'N',Sum(case Tmp_Type When 'X' Then Tmp_Qty else -Tmp_Qty end),0,0,0,0,0";
            strsql += " from " + strTempRMSDetail;
            strsql += " Group By Tmp_Exchange,Tmp_Clientcd,Tmp_Scripcd ";
            strsql += " Having Sum(case Tmp_Type When 'X' Then Tmp_Qty else -Tmp_Qty end) > 0 ";
            ExecuteSQLTmp(strsql, objConnectionTmp);

            strsql = " Delete " + strTempRMSDetail + " where Tmp_Type = 'X' ";
            ExecuteSQLTmp(strsql, objConnectionTmp);

            if (strExchange == "")
            {
                strsql = "Delete " + strTempRMSDetail + Constants.vbNewLine + " Where Tmp_Type = 'C' and Tmp_Scripcd not in (select MTFD_SecCD from MrgTdgFin_SecuritiesDt ";
                strsql += " Where MTFD_SecDt = (Select max(MTFD_SecDt) from MrgTdgFin_SecuritiesDt where MTFD_SecDt <= '" + strDT + "') and (MTFD_BSE = 'Y' Or MTFD_NSE = 'Y'))";
                ExecuteSQLTmp(strsql, objConnectionTmp);
            }
            else
            {
                strsql = "Delete " + strTempRMSDetail + Constants.vbNewLine + " Where Tmp_Type = 'C' and Tmp_Scripcd not in (select MTFD_SecCD from MrgTdgFin_SecuritiesDt ";
                strsql += " Where MTFD_SecDt = (Select max(MTFD_SecDt) from MrgTdgFin_SecuritiesDt where MTFD_SecDt <= '" + strDT + "') and MTFD_" + strExchange + "SE = 'Y')";
                ExecuteSQLTmp(strsql, objConnectionTmp);
            }

            if (blnIncludeUNDEL)
            {
                strsql = "Insert into " + strTempRMSDetail;
                strsql += "  select 'U' type,left(dm_tstlmnt,1) ,dm_clientcd,dm_Scripcd,'N',sum(dm_qty)  ,0,0,0,0,0 ";
                strsql += "  From Demat, OurDps, Settlements, Client_master ";
                strsql += " Where dm_clientcd = cm_cd And dm_ourdp = od_cd  And dm_tstlmnt = se_stlmnt and dm_type = 'BC' ";
                strsql += " and od_acttype = 'T' and dm_locked = 'B' and se_stdt <= '" + strDT + "' and se_shpayindt >= '" + strDT + "'" + strWhere;
                strsql += " and dm_execdt > '" + strDT + "' and dm_execdt <= se_shpayindt Group By left(dm_tstlmnt,1),dm_clientcd,dm_Scripcd";
                ExecuteSQLTmp(strsql, objConnectionTmp);
            }

            char[] ArrSeparators = new char[1];
            ArrSeparators[0] = '/';
            string strExch = ((blnBSE == true ? "B/" : "") + (blnNSE == true ? "N/" : ""));
            strExch = strExch.Trim() != "" ? Strings.Left(strExch, strExch.Length - 1) : "";
            string[] arrExch = strExch.Split(ArrSeparators);

            for (int i = 0; i <= arrExch.Length - 1; i++)
            {
                strsql = "Update " + strTempRMSDetail;
                strsql += " set Tmp_MarketRate = mk_closerate from market_rates a with (nolock) where mk_exchange = '" + arrExch[i] + "' and mk_scripcd=Tmp_Scripcd ";
                strsql += " and mk_dt= (select max(mk_dt) from market_rates with(nolock) where mk_exchange = '" + arrExch[i] + "'";
                strsql += " and mk_dt <='" + strDT + "') ";
                strsql += " and Tmp_Type= 'M' and Tmp_Exchange = '" + arrExch[i] + "'";
                ExecuteSQLTmp(strsql, objConnectionTmp);

                strsql = "Update " + strTempRMSDetail;
                strsql += " set Tmp_MarketRate = mk_closerate from market_rates a with (nolock) where mk_exchange = '" + arrExch[i] + "' and mk_scripcd=Tmp_Scripcd ";
                strsql += " and mk_dt= (select max(mk_dt) from market_rates with(nolock) where mk_exchange = '" + arrExch[i] + "'";
                strsql += " and mk_dt <'" + strDT + "') ";
                strsql += " and Tmp_Type <> 'M' and Tmp_Exchange = '" + arrExch[i] + "'";
                ExecuteSQLTmp(strsql, objConnectionTmp);

                strsql = "Update " + strTempRMSDetail;
                strsql += " Set Tmp_RegForFO = 'Y'  ";
                strsql += " From Product_master, Securities  ";
                strsql += " Where pm_Exchange = '" + arrExch[i] + "' and pm_assetcd = " + Interaction.IIf(arrExch[i] == "B", "ss_Bsymbol", "ss_nsymbol");
                strsql += " and Tmp_Scripcd  = ss_cd and Tmp_Exchange = '" + arrExch[i] + "'";
                if (Convert.ToInt32(fnFireQueryTradeWeb("Product_Expiry", "Count(0)", "pe_expirydt >= '" + strDT + "' and pe_exchange", arrExch[i], true)) > 0)
                    strsql += " and pm_assetcd in ( select pe_assetcd from Product_Expiry Where pe_exchange = '" + arrExch[i] + "' and pe_expirydt >= '" + strDT + "')";
                ExecuteSQLTmp(strsql, objConnectionTmp);

                strsql = "update " + strTempRMSDetail;
                strsql += " set Tmp_MrgHairCut = ";
                strsql += " case Tmp_Type When 'C' Then Case When vm_exchange = 'N' then vm_applicable_var else vm_margin_rate end else (Case When vm_exchange = 'N' then vm_applicable_var else vm_margin_rate end * " + GetSysParmSt("MTFP_VARMAR", "") + " ) + (vm_max_loss * case Tmp_RegForFO When 'Y' Then " + GetSysParmSt("MTFP_ELMRGFO", "") + " else " + GetSysParmSt("MTFP_ELMNTFO", "") + " end ) end ";
                strsql += " from VarMargin  ";
                strsql += " where vm_scripcd = Tmp_Scripcd and vm_exchange = '" + arrExch[i] + "'";
                strsql += " and vm_dt =(select max(vm_dt) from VarMargin ";
                strsql += " Where vm_exchange = '" + arrExch[i] + "' ";
                strsql += " and vm_dt <'" + strDT + "')";
                strsql += " and Tmp_Exchange = '" + arrExch[i] + "'";
                ExecuteSQLTmp(strsql, objConnectionTmp);
            }

            strsql = "update " + strTempRMSDetail + "  set Tmp_MrgHairCut = " + dblHairCut + " Where Tmp_MrgHairCut < " + dblHairCut;
            ExecuteSQLTmp(strsql, objConnectionTmp);

            strsql = "update " + strTempRMSDetail + "  set Tmp_Value = Round(Tmp_Qty*Tmp_Rate,2) Where Tmp_Type= 'M' ";
            ExecuteSQLTmp(strsql, objConnectionTmp);

            strsql = "update " + strTempRMSDetail + "  set Tmp_Value = Round(Tmp_Qty*Tmp_MarketRate,2) Where Tmp_Type <> 'M' ";
            ExecuteSQLTmp(strsql, objConnectionTmp);

            strsql = "update " + strTempRMSDetail + "  set Tmp_NetValue = Round(Tmp_Value*((Tmp_MrgHairCut)/100),2) Where Tmp_Type = 'M' ";
            ExecuteSQLTmp(strsql, objConnectionTmp);

            strsql = "update " + strTempRMSDetail + "  set Tmp_NetValue = Round(Tmp_Value*((100-Tmp_MrgHairCut)/100),2) Where Tmp_Type <> 'M' ";
            ExecuteSQLTmp(strsql, objConnectionTmp);

            strsql = "Update " + strTempRMSSummary + "  set Tmp_FundedAmount = A.Tmp_FundedAmount, Tmp_FundedMrgReq  = A.Tmp_FundedMrgReq, ";
            strsql += " Tmp_CollateralValue = A.Tmp_CollateralValue ";
            strsql += " from (select Tmp_Clientcd Clientcd,";
            strsql += " Round(Sum(case Tmp_Type When 'M' Then Tmp_Value else 0 end),2) Tmp_FundedAmount, ";
            strsql += " Round(Sum(case Tmp_Type When 'M' Then Tmp_NetValue else 0 end),2) Tmp_FundedMrgReq , ";
            strsql += " Round(Sum(case Tmp_Type When 'C' Then Tmp_NetValue else 0 end),2) Tmp_CollateralValue ";
            strsql += " from " + strTempRMSDetail;
            strsql += " Group By Tmp_Clientcd ) a ";
            strsql += " Where Clientcd = Tmp_Clientcd ";
            ExecuteSQLTmp(strsql, objConnectionTmp);

            strsql = "Update " + strTempRMSSummary + "  set Tmp_M2MLoss = A.M2MLoss ";
            strsql += " From ( select Tmp_clientcd Tmpclientcd, Round(Sum((Tmp_Rate-Tmp_MarketRate)*Tmp_Qty),2) M2MLoss ";
            strsql += " From " + strTempRMSDetail;
            strsql += " Where Tmp_type = 'M' and Tmp_Rate > Tmp_MarketRate  ";
            strsql += " Group By Tmp_clientcd) A ";
            strsql += " Where Tmp_Clientcd = Tmpclientcd ";
            ExecuteSQLTmp(strsql, objConnectionTmp);

            strsql = "Update " + strTempRMSSummary + "  set Tmp_ShortFallExcess = (Tmp_CollateralFund+Tmp_CollateralValue)-(Tmp_FundedMrgReq+Tmp_M2MLoss) ";
            ExecuteSQLTmp(strsql, objConnectionTmp);
        }
        public string GetTMinusPlusdt(string strDate, int intDay, string strExch)
        {
            string strDate1 = strDate;
            for (int intCnt = 0; intCnt < Math.Abs(intDay); intCnt++)
            {
            Againdt:
                strDate1 = ConvertDT(strDate1).AddDays(intDay).ToString("yyyyMMdd");
                if (Conversion.Val(fnFireQueryTradeWeb("THoliday_master", "count(0)", "hm_exchange = '" + strExch + "' and hm_dt", strDate1, true)) > 0)
                    goto Againdt;
            }
            return strDate1;
        }
        public string mfnGetT2Dt(string strExchange, string strDT)
        {
            string strT2 = strDT;
            string strSql = string.Empty;
            for (int i = 0; i < 2; i++)
            {
                strT2 = ConvertDT(strT2).AddDays(1).ToString("yyyyMMdd");
            AgaindtT2:
                strSql = "select * from Tholiday_master";
                strSql += " where Replace(hm_dt,'BH','20') = '" + strT2 + "'";
                if (strExchange != "")
                    strSql += " and  hm_exchange = '" + strExchange + "'";
                DataTable dstemp = new DataTable();
                dstemp = OpenDataTable(strSql);
                if (dstemp.Rows.Count > 0)
                {
                    strT2 = ConvertDT(strT2).AddDays(1).ToString("yyyyMMdd");
                    goto AgaindtT2;
                }
            }
            return strT2;
        }
        public string GetCommExch(string strExch)
        {
            string Exch = "";
            switch (strExch)
            {
                case "MCX":
                    Exch = "M";
                    break;
                case "NCDEX":
                    Exch = "N";
                    break;
                case "ICEX":
                    Exch = "C";
                    break;
                case "NCME":
                    Exch = "A";
                    break;
                case "NSEL":
                    Exch = "S";
                    break;
                case "NSX":
                    Exch = "D";
                    break;
            }
            return Exch;
        }
        public string GetSlashDateNew(string Date)
        {
            if (Date.ToString().Trim() == "")
                return string.Empty;
            int Year = int.Parse(Date.Substring(0, 4));
            int Month = int.Parse(Date.Substring(4, 2));
            int Day = int.Parse(Date.Substring(6, 2));
            return new DateTime(Year, Month, Day).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("en-GB"));
        }

        public string fnCheckInterOperability(string strDate, string strSegment)
        {
            DataSet DsInterOP;
            string StrSql;

            if (Convert.ToInt16(fnFireQueryTradeWeb("sysobjects", "count(0)", "name", "ClearingHouse", true)) == 0)
            {
                return "";
            }

            StrSql = " select ClearingHouse.* from ClearingHouse ";
            StrSql += " Where CH_CompanyCode = ";
            if (ConfigurationManager["IsTradeWeb"] == "O")//live database
            {
                StrSql += "'A'"; // "'" + Convert.ToString(sessionCompanyCode) + "'";
            }
            else
            {
                StrSql += " (select min(em_cd) from Entity_master Where Len(Ltrim(Rtrim(em_cd))) = 1) ";
            }
            StrSql += "and CH_Segment = '" + strSegment + "'";
            StrSql += " and CH_EffDt = (Select max(CH_EffDt) from ClearingHouse ";
            StrSql += " Where CH_Segment = '" + strSegment + "' and CH_EffDt <='" + strDate + "')";
            DsInterOP = OpenDataSet(StrSql);

            if (DsInterOP.Tables[0].Rows.Count == 0)
            {
                return "";
            }
            else
            {
                if (DsInterOP.Tables[0].Rows[0]["CH_ClgHs"].ToString().Trim() == "")
                {
                    return "";
                }
                else
                    return "TRUE";
            }
        }
        public string fnGetInterOpStlmnts(string strStlmnt, string StrDate, Boolean blnIncludeT2T)
        {
            // Dim RsInterOP As New ADODB.Recordset, rstemp As New ADODB.Recordset, RsAucStlmnt As New ADODB.Recordset
            DataSet DsInterOP;
            DataSet DsAucStlmnt;
            DataSet DsStlmnt1;
            DataSet DsStlmnt2;
            string strData;
            string strSTDt = StrDate.Trim();
            bool blnInteOP;
            string StrSql;
            int j;
            DsInterOP = null/* TODO Change to default(_) if this is not a reference type */;

            if (",BR,BW,BC,NA,NN,NZ,MA,MN,MZ,".Contains("," + Strings.Left(strStlmnt, 2) + ","))
            {
                strData = "";

                StrSql = " select * from SEttlements Where se_stdt = '" + strSTDt + "'";
                if (",BW,NN,MN,".Contains("," + Strings.Left(strStlmnt, 2) + ","))
                    StrSql = StrSql + " and left(se_stlmnt,2) in ('BW','NN','MN')";
                else if (",BC,NZ,MZ,".Contains("," + Strings.Left(strStlmnt, 2) + ","))
                    StrSql = StrSql + " and left(se_stlmnt,2) in ('BC','NZ','MZ')";
                else
                    StrSql = StrSql + " and left(se_stlmnt,2) in ('BR','NA','MA')";
                DsInterOP = OpenDataSet(StrSql);
                if (DsInterOP.Tables[0].Rows.Count == 0)
                    strData = strStlmnt + ",";
                else
                {
                    strData = "";
                    if (",BR,NA,MA,".Contains("," + Strings.Left(strStlmnt, 2) + ","))
                    {
                        StrSql = " select isNull(Sum(case Left(se_stlmnt,2) When 'BR' Then 1 else 0 end),0) BR,  ";
                        StrSql = StrSql + " isNull(Sum(case Left(se_stlmnt,2) When 'NA' Then 1 else 0 end),0) NA  ";
                        StrSql = StrSql + " from Settlements Where se_stdt = '" + strSTDt + "'";
                        DsAucStlmnt = OpenDataSet(StrSql);
                        int intBR;
                        int intNA;
                        intBR = DsAucStlmnt.Tables[0].Rows.Count; intNA = DsAucStlmnt.Tables[0].Rows.Count;
                        if (intBR > 1 | intNA > 1)
                        {
                            if (intBR == intNA)
                            {
                                string[] ArrExch;
                                ArrExch = (Strings.Left(strStlmnt, 1) == "B" ? "B,N" : "N,B").Split(',');

                                StrSql = " select * from SEttlements Where se_stdt = '" + strSTDt + "'";
                                StrSql = StrSql + " and left(se_stlmnt,2) in ('BR','NA','MA') and se_exchange = '" + ArrExch[0] + "'";
                                StrSql = StrSql + " Order by se_No ";
                                DsStlmnt1 = OpenDataSet(StrSql);

                                StrSql = " select * from SEttlements Where se_stdt = '" + strSTDt + "'";
                                StrSql = StrSql + " and left(se_stlmnt,2) in ('BR','NA','MA') and se_exchange = '" + ArrExch[1] + "'";
                                StrSql = StrSql + " Order by se_No ";
                                DsStlmnt2 = OpenDataSet(StrSql);
                                j = 0;
                                while (j < DsStlmnt1.Tables[0].Rows.Count)
                                {
                                    if (DsStlmnt1.Tables[0].Rows[j]["se_stlmnt"].ToString() == strStlmnt)
                                    {
                                        strData = strData + DsStlmnt1.Tables[0].Rows[j]["se_stlmnt"].ToString() + ",";
                                        strData = strData + DsStlmnt2.Tables[0].Rows[j]["se_stlmnt"].ToString() + ",";
                                        break;
                                    }
                                    j = j + 1;
                                }
                                DsStlmnt1 = null/* TODO Change to default(_) if this is not a reference type */;
                                DsStlmnt2 = null/* TODO Change to default(_) if this is not a reference type */;
                            }
                            else
                                strData = strStlmnt + ",";
                        }
                        else
                        {
                            j = 0;
                            while (j < DsInterOP.Tables[0].Rows.Count)
                            {
                                strData = strData + DsInterOP.Tables[0].Rows[j]["se_stlmnt"].ToString().Trim() + ",";
                                j = j + 1;
                            }
                        }
                    }
                    else
                    {
                        j = 0;
                        while (j < DsInterOP.Tables[0].Rows.Count)
                        {
                            strData = strData + DsInterOP.Tables[0].Rows[j]["se_stlmnt"].ToString().Trim() + ",";
                            j = j + 1;
                        }
                    }
                }
                DsInterOP = null/* TODO Change to default(_) if this is not a reference type */;
                j = 0;
                if (blnIncludeT2T)
                {
                    StrSql = " select * from SEttlements Where se_stdt = '" + strSTDt + "'";
                    StrSql = StrSql + " and left(se_stlmnt,2) in ('BC','NZ','MZ')";
                    DsInterOP = OpenDataSet(StrSql);
                    if (DsInterOP.Tables[0].Rows.Count == 0)
                        strData = strStlmnt + ",";
                    else
                        while (j < DsInterOP.Tables[0].Rows.Count)
                        {
                            strData = strData + DsInterOP.Tables[0].Rows[j]["se_stlmnt"].ToString() + ",";
                            j = j + 1;
                        }
                    DsInterOP = null/* TODO Change to default(_) if this is not a reference type */;
                }
                strData = Strings.Left(strData, Strings.Len(strData) - 1);
                return strData;
            }
            else if (Convert.ToInt16(fnFireQueryTradeWeb("settlements,settlement_type", "count(0)", "se_exchange=sy_exchange and se_type=sy_type and sy_maptype in ('O','P') and se_stdt >= '20200801' and se_stlmnt", strStlmnt, true)) > 0)
            {

                StrSql = " select * from SEttlements,settlement_type Where se_stdt = '" + strSTDt + "'";
                StrSql += " and se_exchange=sy_exchange and se_type=sy_type ";
                StrSql += " and sy_maptype = '" + fnFireQueryTradeWeb("settlements,settlement_type", "sy_maptype", "se_exchange=sy_exchange and se_type=sy_type and se_stlmnt", strStlmnt, true) + "'";
                DsInterOP = null;
                strData = "";
                DsInterOP = OpenDataSet(StrSql);
                if (DsInterOP.Tables[0].Rows.Count == 0)
                {
                    strData = strStlmnt + ",";
                }
                else
                {
                    j = 0;

                    while (j < DsInterOP.Tables[0].Rows.Count)
                    {
                        strData = strData + DsInterOP.Tables[0].Rows[j]["se_stlmnt"].ToString() + ",";
                        j = j + 1;
                    }
                }
                DsInterOP = null;
                strData = Strings.Left(strData, Strings.Len(strData) - 1);
                return strData;
            }
            else
                return strStlmnt;
        }

        public string fnGetInterOpExchange(string strSegment)
        {
            DataSet DsInterOP;
            string strSQL;
            string strData;

            strSQL = " select substring(CES_Cd,2,1) as CES_Cd from CompanyExchangeSegments Where CES_CompanyCd = 'A'  and substring(CES_Cd,2,1) in ('B','N') and Right(CES_Cd,1) = '" + strSegment + "'";
            DsInterOP = OpenDataSet(strSQL);
            if (DsInterOP.Tables[0].Rows.Count == 0)
            {
                strData = "";
            }
            else
            {
                strData = "";

                for (int j = 0; j < DsInterOP.Tables[0].Rows.Count; j++)
                {
                    strData += DsInterOP.Tables[0].Rows[j]["CES_Cd"].ToString().Trim() + ",";
                }
            }
            strData = Strings.Left(strData, Strings.Len(strData) - 1);
            return strData;
        }
        public string mfnGetSegmentCode2Desc(string strSegment)
        {

            strSegment = strSegment.Trim().ToUpper();
            if (strSegment == "K")
            {
                return "FX";
            }
            else if (strSegment == "C")
            {
                return "CASH";
            }
            else if (strSegment == "X")
            {
                return "COMM";
            }
            else if (strSegment == "M")
            {
                return "MF";
            }
            else
            {
                return "F&O";
            }
        }

        public string mfnGetSegmentDesc2Code(string strSegment)
        {
            strSegment = strSegment.Trim().ToUpper();
            if (strSegment == "FX")
            {
                return "K";
            }
            else if (strSegment == "CASH")
            {
                return "C";
            }
            else if (strSegment == "COMMEX" || strSegment == "COMM")
            {
                return "X";
            }
            else if (strSegment == "MF")
            {
                return "M";
            }
            else
            {
                return "F";
            }
        }
        public string GetCommexConnectionNew(string Commex)
        {
            string StrCommexConn = "";
            if (Commex != null && Commex != string.Empty)
            {
                char[] ArrSeparators = new char[1];
                ArrSeparators[0] = '/';
                string[] ArrCommex = Commex.Split(ArrSeparators);
                Connection con = new Connection();
                var csb = con.GetDataSource();
                // var csb = new SqlConnectionStringBuilder(ConfigurationManager.ConnectionStrings["TradeWebConnectionString"].ConnectionString);
                string StrServer = csb.DataSource.ToUpper().ToString().Trim();
                StrServer = "[" + StrServer + "]";
                if (StrServer.Trim() == ArrCommex[0].ToUpper().Trim())
                {
                    StrCommexConn = ArrCommex[1].Trim() + "." + ArrCommex[2].Trim();
                }
                else
                { StrCommexConn = ArrCommex[0].Trim() + "." + ArrCommex[1].Trim() + "." + ArrCommex[2].Trim(); }
            }
            return StrCommexConn;
        }
        public string GetCommexConnection()
        {
            string StrCommexConn = "";
            if (GetWebParameter("Commex") != null && GetWebParameter("Commex") != string.Empty)
            {
                char[] ArrSeparators = new char[1];
                ArrSeparators[0] = '/';
                string[] ArrCommex = GetWebParameter("Commex").Split(ArrSeparators);
                var con = new Connection();//new SqlConnectionStringBuilder(ConfigurationManager.ConnectionStrings["TradeWebConnectionString"].ConnectionString);
                var csb = con.GetDataSource();
                string StrServer = csb.DataSource.ToUpper().ToString().Trim();
                // StrServer = "[" + StrServer + "]";
                if (StrServer.Trim() == ArrCommex[0].ToUpper().Trim())
                {
                    StrCommexConn = ArrCommex[1].Trim() + "." + ArrCommex[2].Trim();
                }
                else
                { StrCommexConn = ArrCommex[0].Trim() + "." + ArrCommex[1].Trim() + "." + ArrCommex[2].Trim(); }
            }
            return StrCommexConn;
        }
        public string GetSysParmStComm(string strParmcd, string strTableName)//GetSysParmSt("ACREPORT", "Stationary");
        {
            string strsql = string.Empty;
            string strReturn = string.Empty;
            if (strTableName == "" || strTableName == null)
            {
                strsql = "Select sp_sysvalue from " + GetCommexConnection() + ".Sysparameter where sp_parmcd= '" + strParmcd + "'";
                DataSet ObjDataset = OpenDataSet(strsql);
                if (ObjDataset.Tables[0].Rows.Count > 0)
                {
                    strReturn = ObjDataset.Tables[0].Rows[0]["sp_sysvalue"].ToString().Trim();
                    return strReturn;
                }
                else
                {
                    return strReturn;
                }
            }
            return strReturn;
        }

        public DataSet OpenDataSetTmp(string strFireQry, SqlConnection objConnection)
        {
            using (SqlCommand objSqlCommand = new SqlCommand(strFireQry, objConnection))
            {
                objSqlCommand.Connection = objConnection;
                objSqlCommand.CommandText = strFireQry;
                objSqlCommand.CommandTimeout = 5000;
                DataSet MyDataSet = new DataSet();
                using (SqlDataAdapter MyAdapter = new SqlDataAdapter(objSqlCommand))
                {
                    MyAdapter.Fill(MyDataSet);
                    MyAdapter.Dispose();
                    return MyDataSet;
                }
            }
        }
        public DataTable OpenNewDataTableTmp(string strFireQry, SqlConnection objConnection)
        {
            using (SqlCommand objSqlCommand = new SqlCommand(strFireQry, objConnection))
            {
                objSqlCommand.Connection = objConnection;
                objSqlCommand.CommandText = strFireQry;
                objSqlCommand.CommandTimeout = 5000;
                using (SqlDataAdapter MyAdapter = new SqlDataAdapter(objSqlCommand))
                {
                    DataTable MyDataTable = new DataTable();
                    MyAdapter.Fill(MyDataTable);
                    MyAdapter.Dispose();
                    return MyDataTable;
                }
            }
        }
        public void ExecuteSQLTmp(string Str_Qry, SqlConnection objConnection)
        {
            using (SqlCommand MyCmd = new SqlCommand())
            {
                MyCmd.Connection = objConnection;
                MyCmd.CommandTimeout = 5000;
                MyCmd.CommandText = Str_Qry;
                MyCmd.ExecuteNonQuery();
                MyCmd.Dispose();
            }
        }
        public DataTable OpenDataTableTmp(string strFireQry, SqlConnection objConnection)
        {
            using (SqlCommand objSqlCommand = new SqlCommand(strFireQry, objConnection))
            {
                objSqlCommand.Connection = objConnection;
                objSqlCommand.CommandText = strFireQry;
                objSqlCommand.CommandTimeout = 5000;
                using (SqlDataAdapter MyAdapter = new SqlDataAdapter(objSqlCommand))
                {
                    DataSet MyDataSet = new DataSet();
                    MyAdapter.Fill(MyDataSet);
                    MyAdapter.Dispose();
                    return MyDataSet.Tables[0];
                }
            }
        }

        public DataTable OpenDataTable(string strFireQry)
        {
            DataSet ds = new DataSet();
            try
            {
                Connection con = new Connection();
                try
                {
                    con.OpenConection();
                    ds = con.ReturnDataSet(strFireQry);
                    con.CloseConnection();
                    return ds.Tables[0];
                }
                catch (Exception e)
                {
                    con.DisposeConnection();
                    throw e;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            //using (SqlConnection ObjConnection = new SqlConnection(StrConn))
            //{
            //    ObjConnection.Open();
            //    using (SqlCommand objSqlCommand = new SqlCommand(strFireQry, ObjConnection))
            //    {
            //        objSqlCommand.Connection = ObjConnection;
            //        objSqlCommand.CommandText = strFireQry;
            //        objSqlCommand.CommandTimeout = 5000;
            //        using (SqlDataAdapter MyAdapter = new SqlDataAdapter(objSqlCommand))
            //        {
            //            DataSet MyDataSet = new DataSet();
            //            MyAdapter.Fill(MyDataSet);
            //            MyAdapter.Dispose();
            //            return MyDataSet.Tables[0];
            //        }
            //    }
            //}
        }

        public DataTable OpenNewDataTable(string strFireQry)
        {
            DataTable ds = new DataTable();
            try
            {
                Connection con = new Connection();
                try
                {
                    con.OpenConection();
                    ds = con.ReturnNewDataTable(strFireQry);
                    con.CloseConnection();
                    return ds;
                }
                catch (Exception e)
                {
                    con.DisposeConnection();
                    throw e;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }


        public void ReportLog(string StrDescription, string StrFromDt, string StrToDt, string StrBOID, string StrClientCD)
        {
            string strsql = "";
            strsql = "Insert Into process_log";
            strsql += " select convert(char(8),getdate(),112) pr_dt,'REPORTLOG' pr_process,'" + StrDescription.Trim() + "' pr_description,";
            strsql += " convert(char(8),getdate(),112)+ ' ' + LEFT(convert(char(8),getdate(),108),8) pr_starttime,convert(char(8),getdate(),112)+ ' ' + LEFT(convert(char(8),getdate(),108),8) pr_endtime,'" + Strings.Left(ReturnsHost().ToString(), 30) + "' pr_computername,'S' pr_status,";
            strsql += " '" + StrBOID + "' pr_filler1,'" + StrFromDt.Trim() + "' pr_filler2,'" + StrToDt.Trim() + "' pr_filler3,'" + StrClientCD + "' mkrid,convert(char(8),getdate(),112) mkrdt";
            ExecuteSQL(strsql);
        }
        private string ReturnsHost()
        {
            string strHost = string.Empty;
            try
            {
                strHost = "";// System.Net.Dns.GetHostEntry(HttpContext.Current.Request.ServerVariables["remote_addr"]).HostName.Trim().ToUpper();

            }
            catch (Exception ex)
            {
                strHost = "";// HttpContext.Current.Request.ServerVariables["remote_Addr"].ToString();
            }
            return strHost;
        }
        public string FnCheckDt(string StrID)
        {
            string strDate = "20201101";
            return strDate;
        }
        public void prCreateTempSecTable(SqlConnection objConnectionTmp)
        {
            string strSql = "";
            try
            {
                strSql = "Drop Table #TmpSecurity";
                ExecuteSQLTmp(strSql, objConnectionTmp);
            }
            catch
            {
                strSql = "Create Table #TmpSecurity (";
                strSql += " Tmp_Scripcd Char(6),";
                strSql += " Tmp_BSE VarChar(3),";
                strSql += " Tmp_NSE VarChar(3),";
                strSql += " Tmp_MSEI VarChar(3),";
                strSql += " Tmp_RegForFO VarChar(1),";
                strSql += " Tmp_VarMarginBSE Money,";
                strSql += " Tmp_ELMBSE Money,";
                strSql += " Tmp_VarMarginNSE Money,";
                strSql += " Tmp_ELMNSE Money )";
                ExecuteSQLTmp(strSql, objConnectionTmp);
            }
        }
        public DataSet prSecurityListRpt(string StrfrDt, Boolean blnBSE, Boolean blnNSE)
        {
            string strSql = "";
            Connection con = new Connection();
            var db = new DataContext();

            SqlConnection objConnectionTmp = new SqlConnection((db.Database.GetDbConnection()).ConnectionString);
            objConnectionTmp.Open();
            prCreateTempSecTable(objConnectionTmp);

            strSql = "Insert Into #TmpSecurity ";
            strSql += " select MTFD_SecCD,";
            strSql += " Case MTFD_BSE When 'Y' then 'Yes' else 'No' end , ";
            strSql += " Case MTFD_NSE When 'Y' then 'Yes' else 'No' end , ";
            strSql += " Case MTFD_MSEI When 'Y' then 'Yes' else 'No' end ,'', 0, 0,0,0 ";
            strSql += " From MrgTdgFin_SecuritiesDt";
            strSql += " Where MTFD_SecDt = (Select max(MTFD_SecDt) from MrgTdgFin_SecuritiesDt where MTFD_SecDt <= '" + StrfrDt.Trim() + "') ";
            strSql += " and (MTFD_BSE= 'Y' or MTFD_NSE='Y' or MTFD_MSEI= 'Y')";
            ExecuteSQLTmp(strSql, objConnectionTmp);

            strSql = "Update #TmpSecurity";
            strSql += " Set Tmp_RegForFO = 'Y'  ";
            strSql += " From Product_master, Securities  ";
            strSql += " Where pm_assetcd = Case pm_Exchange When 'B' Then ss_Bsymbol Else ss_nsymbol End and pm_segment = 'F' ";
            strSql += " and Tmp_Scripcd  = ss_cd ";
            strSql += " and exists ( select pe_assetcd from Product_Expiry Where pe_assetcd = pm_assetcd and pe_exchange = pm_exchange and pe_segment = 'F' and pe_expirydt >= '" + StrfrDt.Trim() + "')";
            ExecuteSQLTmp(strSql, objConnectionTmp);

            char[] ArrSeparators = new char[1];
            ArrSeparators[0] = '/';
            string strExch = ((blnBSE == true ? "B/" : "") + (blnNSE == true ? "N/" : ""));
            strExch = strExch.Trim() != "" ? Strings.Left(strExch, strExch.Length - 1) : "";
            string[] arrExch = strExch.Split(ArrSeparators);

            for (int i = 0; i <= arrExch.Length - 1; i++)
            {
                strSql = "Update #TmpSecurity  ";
                strSql += " Set Tmp_VarMargin" + arrExch[i] + "SE=";
                strSql += " (Case When vm_exchange = 'N' then vm_applicable_var else vm_margin_rate end * " + GetSysParmSt("MTFP_VARMAR", "") + " )";
                strSql += " From VarMargin  ";
                strSql += " where vm_scripcd = Tmp_Scripcd and vm_exchange = '" + arrExch[i] + "'";
                strSql += " and vm_dt =(select max(vm_dt) from VarMargin ";
                strSql += " Where vm_exchange = '" + arrExch[i] + "' ";
                strSql += " and vm_dt <='" + StrfrDt.Trim() + "')";
                ExecuteSQLTmp(strSql, objConnectionTmp);

                strSql = "Update #TmpSecurity  ";
                strSql += "Set Tmp_ELM" + arrExch[i] + "SE=";
                strSql += " (vm_max_loss * case Tmp_RegForFO When 'Y' Then " + GetSysParmSt("MTFP_ELMRGFO", "") + " else " + GetSysParmSt("MTFP_ELMNTFO", "") + "  end ) ";
                strSql += " From VarMargin  ";
                strSql += " where vm_scripcd = Tmp_Scripcd and vm_exchange = '" + arrExch[i] + "'";
                strSql += " and vm_dt =(select max(vm_dt) from VarMargin ";
                strSql += " Where vm_exchange = '" + arrExch[i] + "' ";
                strSql += " and vm_dt <='" + StrfrDt.Trim() + "')";
                ExecuteSQLTmp(strSql, objConnectionTmp);
            }

            strSql = "Select MTFS_SecCD, MTFS_Lname,MTFS_NSymbol, Tmp_BSE , Tmp_NSE , Tmp_MSEI , ";
            strSql += " Case Tmp_RegForFO When 'Y' then 'Yes' else 'No' end Tmp_RegForFO ,";
            strSql += " cast(Tmp_VarMarginBSE as decimal(15,2)) Tmp_VarMarginBSE ,cast(Tmp_ELMBSE as decimal(15,2)) Tmp_ELMBSE,cast ((Tmp_VarMarginBSE + Tmp_ELMBSE) as decimal(15,2)) TotalBSE,case when (Tmp_VarMarginBSE + Tmp_ELMBSE)>0 then cast((100/(Tmp_VarMarginBSE + Tmp_ELMBSE))as decimal(15,2)) else 0 end TotalBSE1, ";
            strSql += " cast(Tmp_VarMarginNSE as decimal(15,2)) Tmp_VarMarginNSE, cast(Tmp_ELMNSE as decimal(15,2)) Tmp_ELMNSE,cast ((Tmp_VarMarginNSE + Tmp_ELMNSE) as decimal(15,2)) TotalNSE,case when (Tmp_VarMarginNSE + Tmp_ELMNSE)>0 then cast((100/(Tmp_VarMarginNSE + Tmp_ELMNSE))as decimal(15,2)) else 0 end TotalNSE1 ";
            strSql += " From #TmpSecurity, MrgTdgFin_Securities";
            strSql += " Where Tmp_Scripcd = MTFS_SecCD ";
            strSql += " Order by MTFS_Lname,MTFS_SecCD";
            DataSet dtSec = new DataSet();
            dtSec = OpenDataSetTmp(strSql, objConnectionTmp);
            return dtSec;
        }
        public double fnPeakFactor(string strForDate)
        {
            double dblPeakFactor = 25;
            if (Conversion.Val(strForDate) >= 20210901)
                dblPeakFactor = 100;
            else if (Conversion.Val(strForDate) >= 20210601)
                dblPeakFactor = 75;
            else if (Conversion.Val(strForDate) >= 20210301)
                dblPeakFactor = 50;
            else if (Conversion.Val(strForDate) >= 20201201)
                dblPeakFactor = 25;
            return dblPeakFactor;
        }

        //// Added new methods
        public string AddBracket(string strparm)
        {
            string temp = "";
            if (strparm != "")
            {
                int count = 0;
                string[] Arrstring = strparm.Split('/');
                foreach (string str in Arrstring)
                {
                    if (count < 3)
                    { temp += "[" + str + "]"; }
                    else { temp += str; }
                    if (count != Arrstring.Length - 1)
                    { temp = temp + "/"; }
                    count++;
                }
            }
            return temp;
        }
        // Added new methods for TradeMobileWebService
        public string fnFireQuery(string strTable, string strSelect, string strParam1, string strParam2, bool blnconn)
        {
            DataSet ds = new DataSet();
            string strsql;
            strsql = "select " + strSelect + " from " + strTable + " with (nolock) where " + strParam1 + " = '" + Strings.Trim(strParam2) + "'";
            ds = OpenDataSet(strsql);
            if (ds.Tables[0].Rows.Count > 0)
                return ds.Tables[0].Rows[0][0].ToString();
            else
                return "";
        }
        public string fnFireQuery(string strTable, string strSelect, string strParam1, string strParam2)
        {
            DataSet ds = new DataSet();
            string strsql;
            strsql = "select " + strSelect + " from " + strTable + " with (nolock) where " + strParam1 + " = '" + Strings.Trim(strParam2) + "'";
            ds = OpenDataSet(strsql);
            if (ds.Tables[0].Rows.Count > 0)
                return ds.Tables[0].Rows[0][0].ToString();
            else
                return "";
        }
        public string mfnGetTPlusDt(string strExchange, string strDT, int intDays)
        {
            string strSQL = "";
            string strT2 = strDT;

            string strCompanyCode = OpenDataSet("select min(em_cd) from Entity_master").Tables[0].Rows[0][0].ToString().Trim();

            for (int intLoop = 1; intLoop <= intDays; intLoop++)
            {
                strT2 = Strings.Format(ctod(strT2).AddDays(1), "yyyyMMdd");
            AgaindtT2:
                ;
                strSQL = "select * from Tholiday_master ";
                strSQL += " where hm_companycode = '" + strCompanyCode + "' ";
                if (strExchange != "")
                    strSQL += " and hm_exchange = '" + strExchange + "' ";
                strSQL += " and Replace(hm_dt,'BH','20') = '" + strT2 + "'";
                DataTable dtT2 = OpenDataTable(strSQL);
                if (dtT2.Rows.Count > 0)
                {
                    strT2 = Strings.Format(ctod(strT2).AddDays(1), "yyyyMMdd");
                    goto AgaindtT2;
                }
            }
            return strT2;
        }
        public DateTime ctod(string strDate)
        {
            var rtnDate = default(DateTime);
            if (Strings.Len(strDate) == 8 & !strDate.Contains("/"))
            {
                rtnDate = DateTime.Parse("1980-01-01").AddYears((int)Math.Round(Conversion.Val(Strings.Left(strDate, 4)) - 1980d)).AddMonths((int)Math.Round(Conversion.Val(Strings.Mid(strDate, 5, 2)) - 1d)).AddDays(Conversion.Val(Strings.Right(strDate, 2)) - 1d);
            }
            else if (Information.UBound(Strings.Split(strDate, "/")) == 2)
            {
                if (Conversion.Val(Strings.Split(strDate, "/")[2]) < 100d)
                {
                    rtnDate = DateTime.Parse("1980-01-01").AddYears((int)Math.Round(Conversion.Val(Strings.Split(strDate, "/")[2]) - 80d)).AddMonths((int)Math.Round(Conversion.Val(Strings.Split(strDate, "/")[1]) - 1d)).AddDays(Conversion.Val(Strings.Split(strDate, "/")[0]) - 1d);
                }
                else
                {
                    rtnDate = DateTime.Parse("1980-01-01").AddYears((int)Math.Round(Conversion.Val(Strings.Split(strDate, "/")[2]) - 1980d)).AddMonths((int)Math.Round(Conversion.Val(Strings.Split(strDate, "/")[1]) - 1d)).AddDays(Conversion.Val(Strings.Split(strDate, "/")[0]) - 1d);
                }
            }
            return rtnDate;
        }

    }
}
