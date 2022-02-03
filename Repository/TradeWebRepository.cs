using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.VisualBasic;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;
using TradeWeb.API.Data;
using static TradeWeb.API.Repository.UtilityCommon;

namespace TradeWeb.API.Repository
{
    public interface ITradeWebRepository
    {
        public dynamic UserDetails(string userId, string password);

        public dynamic Login_validate_USER(string userId);

        public dynamic Login_validate_Password(string userId, string password);

        public dynamic Login_GetPassword(string userId);

        public dynamic GetUserDetais(string userId);

        public dynamic Transaction_Summary(string userId, string type, string FromDate, string ToDate);

        public dynamic Transaction_Accounts(string userId, string type, string fromDate, string toDate);

        public dynamic Transaction_AGTS(string userId, string seg, string fromDate, string toDate);

        public dynamic ItemWiseDetails(string cm_cd, string td_type, string LinkCode, string td_scripnm, string FromDt, string ToDt);

        public dynamic DateWiseDetails(string cm_cd, string td_type, string LinkCode, string td_stlmnt, string Dt, string FromDt, string ToDt, string txtheader);

        public dynamic Ledger_Summary(string userId, string type, string fromdate, string toDate);

        public dynamic Ledger_Detail(string userId, string fromDate, string toDate, string type_cesCd);

        public dynamic OutStandingPosition(string userId, string AsOnDt);

        public dynamic OutStandingPosition_Detail(string userId, string seriesid, string CESCd, string AsOnDt);

        public dynamic ProfitLoss_Cash_Summary(string userId, string fromDate, string toDate);

        public dynamic ProfitLoss_Cash_Detail(string userId, string fromDate, string toDate, string scripcd);

        public dynamic ProfitLoss_FO_Summary(string userId, string exchange, string segment, string fromDate, string toDate);

        public dynamic ProfitLoss_Commodity_Summary(string userId, string exchange, string fromDate, string toDate);

        public dynamic Holding_Broker_Current(string userid);

        public dynamic Holding_Broker_Ason(string userid, string AsOnDt);

        public dynamic Holding_Demat_Current(string userid);

        public dynamic GetDropDownComboAsOnDataHandler(string strTable);

        public dynamic AddUnPledgeRequest(string userId, string unPledge, string dmScripcd, string txtReqQty);

        public dynamic GetSettelmentType(string syExchange, string syStatus);

        public dynamic CommonGetSysParmStHandler(string param1, string param2);

        public dynamic GetBillMainData(string strClientId, string strClient, string exchangeType, string stlmntType, string fromDt, string strCompanyCode);

        public dynamic GetCumulativeDetailsHandler(string td_clientcd, string StrOrder, string StrScripCode, string Strbsflag, string StrDate, string StrLookup);

        public dynamic GetConfirmationDetailsHandler(string td_clientcd, string StrOrder, string StrLoopUp);

        public dynamic GetConfirmationMainDataHandler(string userId, int lstConfirmationSelectIndex, string tdDt);

    }


    public class TradeWebRepository : ITradeWebRepository
    {
        #region Class level declarations.
        private readonly IConfiguration _configuration;
        private readonly UtilityCommon objUtility;
        private string strsql = "";
        private string strConnecton = "";
        #endregion

        #region Constructor
        public TradeWebRepository(IConfiguration configuration, UtilityCommon objUtility)
        {
            _configuration = configuration;
            this.objUtility = objUtility;
        }
        #endregion

        #region Method
        // get user details
        public dynamic UserDetails(string userId, string password)
        {
            try
            {
                string qury = "select cm_cd ClientCode, cm_Name ClientName from Client_master with (nolock) where cm_cd='" + userId + "'  and cm_pwd='" + password + "'";
                var ds = CommonRepository.FillDataset(qury);
                if (ds != null)
                {
                    if (ds.Tables.Count > 0)
                    {
                        DataTable Dt = new DataTable();
                        Dt = ds.Tables[0];
                        return JsonConvert.SerializeObject(Dt);
                    }
                }
                return null;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public dynamic Login_validate_USER(string userId)
        {
            try
            {
                string qury = "select cm_cd ClientCode, cm_Name ClientName from Client_master with (nolock) where cm_cd='" + userId + "'";
                var ds = objUtility.OpenDataSet(qury);
                if (ds != null)
                {
                    if (ds.Tables[0].Rows.Count > 0)
                    {
                        return "success";
                    }
                }
                return "failed";
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public dynamic Login_validate_Password(string userId, string password)
        {
            try
            {
                string qury = "select cm_cd 'ClientCode',cm_Name 'ClientName', cm_mobile 'Mobile' from Client_master with (nolock) where cm_cd='" + userId + "'  and cm_pwd='" + password + "'";
                var ds = objUtility.OpenDataSet(qury);
                if (ds != null)
                {
                    if (ds.Tables[0].Rows.Count > 0)
                    {
                        DataTable Dt = new DataTable();
                        Dt = ds.Tables[0];
                        return JsonConvert.SerializeObject(Dt);
                    }
                }
                return "failed";
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public dynamic Login_GetPassword(string userId)
        {
            try
            {
                string result = ResetPassword(userId); //"select cm_mobile from Client_master with (nolock) where cm_cd='" + userId + "'  and cm_pwd='" + password + "'";
                if (result != null)
                {
                    return JsonConvert.SerializeObject(result);
                }
                return "failed";
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public dynamic GetUserDetais(string userId)
        {
            try
            {
                string qury = "Select cm_cd as ClientCode, LTrim(RTrim(cm_name)) as ClientName , LTrim(RTrim(cm_add1)) As Address1, LTrim(RTrim(cm_add2)) As Address2, LTrim(RTrim(cm_add3)) As Address3, LTrim(RTrim(cm_add4)) as Address4, LTrim(RTrim(cm_pincode)) as Pincode, LTrim(RTrim(cm_state)) as State, LTrim(RTrim(cm_pcountry)) as Country, LTrim(RTrim(cm_email)) as Email, LTrim(RTrim(cm_mobile)) as Mobile, LTrim(RTrim(cm_tele1)) as Telephone1, LTrim(RTrim(cm_tele2)) as Telephone2 , LTrim(RTrim(cm_panno)) as PanNo, LTrim(RTrim(bk_name)) as BankName, LTrim(RTrim(ba_actno)) as BankAccountNo, LTrim(RTrim(ba_micr)) as MICR, LTrim(RTrim(ba_ifsccode)) as IFSC, LTrim(RTrim(da_dpid)) as DPID, LTrim(RTrim(da_actno)) as DematAccountNo from client_master,client_info,Bankact,Bank_master,Dematact with (nolock) where cm_cd=cm2_cd and cm_cd='" + userId + "' and da_clientcd='" + userId + "' and da_defaultyn='y' and ba_clientcd='" + userId + "' and ba_default='y' and ba_micr=bk_micr  and ba_ifsccode = bk_IFCCode;";
                var ds = CommonRepository.FillDataset(qury);
                if (ds != null)
                {
                    if (ds.Tables.Count > 0)
                    {
                        DataTable Dt = new DataTable();
                        Dt = ds.Tables[0];
                        return JsonConvert.SerializeObject(Dt);
                    }
                }
                return null;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #region Transaction Handler methods
        //TODO : For getting Itemwise details Transaction data
        public dynamic ItemWiseDetails(string cm_cd, string td_type, string LinkCode, string td_scripnm, string FromDt, string ToDt)
        {
            List<string> EntityList = new List<string>();
            string qury = ItemWiseDetailsQuery(cm_cd, td_type, LinkCode, td_scripnm, FromDt, ToDt);
            try
            {
                var ds = CommonRepository.FillDataset(qury);
                if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                {
                    var json = JsonConvert.SerializeObject(ds.Tables[0], Formatting.Indented);

                    return json;
                }
                return EntityList.ToList();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //TODO : For getting Datewise details Transaction data
        public dynamic DateWiseDetails(string cm_cd, string td_type, string LinkCode, string td_stlmnt, string Dt, string FromDt, string ToDt, string txtheader)
        {
            List<string> entityList = new List<string>();
            string qury = DateWiseDetailsQuery(cm_cd, td_type, LinkCode, td_stlmnt, Dt, FromDt, ToDt, txtheader);
            try
            {
                var ds = CommonRepository.FillDataset(qury);
                if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                {
                    var json = JsonConvert.SerializeObject(ds.Tables[0], Formatting.Indented);
                    return json;
                }
                return entityList.ToList();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //TODO : For getting Transaction data
        public dynamic Transaction_Summary(string userId, string type, string FromDate, string ToDate)
        {
            string strsql;
            if (type.ToUpper() == "2")
            {
                strsql = Transaction_Trade_Datewise(userId, FromDate, ToDate);
            }
            else
            {
                strsql = Transaction_Trade_Itemwise(userId, FromDate, ToDate);
            }

            try
            {
                var ds = CommonRepository.FillDataset(strsql);
                if (ds != null)
                {
                    if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                    {
                        var json = JsonConvert.SerializeObject(ds.Tables[0], Formatting.Indented);
                        return json;
                    }
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }

        public dynamic Transaction_Accounts(string userId, string type, string fromDate, string toDate)
        {
            string strsql;

            if (type.ToLower() == "1" || type.ToLower() == "2")
            {
                strsql = " select '" + type + "' Type,ld_documentno DocumentNo, ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) Date , ld_Particular Particular, ld_Chequeno Chequeno,";
                strsql = strsql + "convert(decimal(15,2),case ld_documenttype When 'R' Then (-1) else 1 end*ld_amount)  Amount from ledger with (nolock)";
                strsql = strsql + "where ld_documenttype=";
                if (type.ToLower() == "receipts")
                {
                    strsql = strsql + "'R'";
                }
                else
                {
                    strsql = strsql + "'P'";
                }
                strsql = strsql + "and ld_clientcd='" + userId + "' and ld_dt between '" + fromDate + "' and '" + toDate + "'";
                strsql = strsql + "order by ld_dt desc ";
            }
            else
            {
                strsql = "select '" + type + "' Type,ld_documentno DocumentNo, ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) Date , ";
                strsql = strsql + " ld_Particular Particular , case ld_debitflag when 'D' then convert(decimal(15,2),ld_amount) else 0 end  Debit,";
                strsql = strsql + " case ld_debitflag when 'D' then 0 else convert(decimal(15,2),-ld_amount) end  Credit";
                strsql = strsql + " from ledger with (nolock) where ld_documenttype=";
                if (type.ToLower() == "3")
                {
                    strsql = strsql + "'J'";
                }
                else
                {
                    strsql = strsql + "'B'";
                }
                strsql = strsql + " and ld_clientcd='" + userId + "'";
                strsql = strsql + " and ld_dt between '" + fromDate + "' and '" + toDate + "'";
                strsql = strsql + " order by ld_dt desc";
            }

            try
            {
                var ds = CommonRepository.FillDataset(strsql);
                if (ds != null)
                {
                    if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                    {
                        var json = JsonConvert.SerializeObject(ds.Tables[0], Formatting.Indented);
                        return json;
                    }
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }

        private string Transaction_Trade_Itemwise(string userId, string fromDate, string toDate)
        {
            string StrTradesIndex = "";
            string StrComTradesIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
            { StrTradesIndex = "index(idx_trades_clientcd),"; }

            string StrTRXIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'Trx' and b.name", "idx_Trx_Clientcd", true)) == 1)
            { StrTRXIndex = "index(idx_trx_clientcd),"; }

            DataSet dt = new DataSet();

            strsql = " select 1 ListOrder,'' as td_ac_type,'' as td_trxdate,'' as td_isin_code,'' as sc_company_name,";
            strsql = strsql + " cast((case when sum(td_bqty-td_sqty)=0 then 0 else sum((td_bqty-td_sqty)*td_rate)/sum(td_bqty-td_sqty) end)as";
            strsql = strsql + " decimal(15,4) )   as rate,'Equity' Td_Type,'' as FScripNm,'' as FExDt,";
            strsql = strsql + " rtrim(td_scripcd)td_scripnm , rtrim(ss_name) snm,";
            strsql = strsql + " sum(td_bqty) Bqty, convert(decimal(15,2), sum(td_bqty*td_rate)) BAmt, ";
            strsql = strsql + " sum(td_sqty) Sqty, convert(decimal(15,2), sum(td_sqty*td_rate)) SAmt, sum(td_bqty-td_sqty) NQty, convert(decimal(15,2), sum((td_bqty-td_sqty)*td_rate)) NAmt, ";
            strsql = strsql + " '' as td_debit_credit,0 as sm_strikeprice,'' as sm_callput,'Equity|'+''+'|'+td_scripcd  LinkCode ";
            strsql = strsql + " from Trx with (" + StrTRXIndex + "nolock) , securities with(nolock)";
            strsql = strsql + " where td_clientcd='" + userId + "' and td_dt between '" + fromDate + "' and '" + toDate + "' ";
            strsql = strsql + " and td_Scripcd = ss_cd";
            strsql = strsql + " group by td_scripcd, ss_name,'Equity|'+''+'|'+td_scripcd ";
            strsql = strsql + " union all ";
            strsql = strsql + " select case left(sm_productcd,1) when 'F' then 2 else 3 end,'', '','' as td_isin_code,'' as sc_company_name, ";
            strsql = strsql + " cast((case when  sum(td_bqty-td_sqty)=0 then 0 else sum((td_bqty-td_sqty)*td_rate)/sum(td_bqty-td_sqty) end)as decimal(15,4) ) as rate,";
            strsql = strsql + " Case When td_segment='K' then 'Currency ' else 'Equity ' end + ";
            strsql = strsql + " Case left(sm_productcd,1) when 'F' Then 'Future ' else 'Option ' end Td_Type,rtrim(sm_symbol), sm_expirydt,rtrim(sm_symbol), case left(sm_productcd,1) when 'F' then 'Fut ' else 'Opt ' end+ rtrim(sm_symbol)+'  Exp: '+ ltrim(rtrim(convert(char,convert(datetime,sm_expirydt),103))) + case left(sm_productcd,1) when 'F' then '' else ''+rtrim(convert(char(9),sm_strikeprice))+sm_callput+sm_optionstyle end, sum(td_bqty) Bqty, convert(decimal(15,2),sum(td_bqty*td_rate*sm_multiplier)) BAmt,  ";
            strsql = strsql + " sum(td_sqty) Sqty, convert(decimal(15,2),sum(td_sqty*td_rate*sm_multiplier)) SAmt, sum(td_bqty-td_sqty) NQty,  ";
            strsql = strsql + " convert(decimal(15,2),sum((td_bqty-td_sqty)*td_rate *sm_multiplier)) NAmt,'' as td_debit_credit ,sm_strikeprice, sm_callput,";
            strsql = strsql + " Case When td_segment='K' then 'Currency' else 'Equity' end + ";
            strsql = strsql + " Case left(sm_productcd,1) when 'F' Then 'Future' else 'Option' end + '|' + '' + '|' + replace(sm_symbol,'&','-')  + '|' + left(sm_productcd,1) + '|' + sm_expirydt + '|' + Rtrim(Ltrim(Convert(char,sm_strikeprice))) + '|' +  sm_callput + '|' +  sm_optionstyle + '|' +  td_Segment LinkCode";
            strsql = strsql + " from trades with (" + StrTradesIndex + "nolock), series_master with (nolock) ";
            strsql = strsql + " where td_clientcd='" + userId + "' and sm_exchange=td_exchange and sm_Segment=td_Segment and td_seriesid=sm_seriesid ";
            strsql = strsql + " and td_dt between '" + fromDate + "' and '" + toDate + "' and td_trxflag <> 'O'  ";
            strsql = strsql + " group by sm_symbol, sm_productcd,td_exchange,td_Segment, sm_expirydt, sm_strikeprice, sm_callput ,sm_optionstyle";
            strsql = strsql + " union all ";
            strsql = strsql + " select 4 ,'','' as td_trxdate,'' as td_isin_code,'' as sc_company_name,cast((case when  sum(ex_aqty-ex_eqty)=0 then 0 else sum((ex_aqty-ex_eqty)*ex_diffrate *case ex_eaflag When 'A' Then -1 else 1 end)/sum(ex_aqty-ex_eqty) end)as decimal(15,2) ) as rate , ";
            strsql = strsql + " Case When ex_Segment='K' then 'Currency ' else 'Equity ' end + case ex_eaflag when 'E' then 'Exercise ' else 'Assignment ' end Td_Type, '','', rtrim(sm_symbol), case left(sm_productcd,1) when 'F' then 'Fut ' else 'Opt ' end+ rtrim(sm_symbol)+'  Exp: '+ ltrim(rtrim(convert(char,convert(datetime,sm_expirydt),103))) + case left(sm_productcd,1) when 'F' then '' else ''+rtrim(convert(char(9),sm_strikeprice))+sm_callput+sm_optionstyle end, sum(ex_aqty) Bqty, ";
            strsql = strsql + " convert(decimal(15,2),sum(ex_aqty*ex_diffrate *case ex_eaflag When 'A' Then -1 else 1 end *sm_multiplier)) BAmt, sum(ex_eqty) Sqty, convert(decimal(15,2),sum(ex_eqty*ex_diffrate *case ex_eaflag When 'A' Then -1 else 1 end  *sm_multiplier)) SAmt, ";
            strsql = strsql + " sum(ex_aqty-ex_eqty) NQty, convert(decimal(15,2),sum((ex_aqty-ex_eqty)*ex_diffrate *case ex_eaflag When 'A' Then -1 else 1 end    *sm_multiplier)) NAmt,'' as td_debit_credit,0,'', ";
            strsql = strsql + " Case When ex_segment='K' then 'Currency' else 'Equity' end + ";
            strsql = strsql + " case ex_eaflag when 'E' then 'Exercise' else 'Assignment' end + '|' + '' + '|' + replace(sm_symbol,'&','-')  + '|' + left(sm_productcd,1) + '|' + sm_expirydt + '|' + Rtrim(Ltrim(Convert(char,sm_strikeprice))) + '|' +  sm_callput + '|' +  sm_optionstyle + '|' +  ex_Segment LinkCode";
            strsql = strsql + " from exercise with (nolock), series_master with (nolock) where ex_clientcd='" + userId + "' ";
            strsql = strsql + " and ex_exchange=sm_exchange and ex_Segment=sm_Segment and ex_seriesid=sm_seriesid ";
            strsql = strsql + " and ex_dt between '" + fromDate + "' and '" + toDate + "' group by ex_eaflag, sm_symbol,ex_Segment,sm_productcd,sm_expirydt, sm_strikeprice, sm_callput ,sm_optionstyle ";
            if (_configuration["Commex"] != null && _configuration["Commex"] != string.Empty)
            {
                string StrCommexConn = "";
                StrCommexConn = objUtility.GetCommexConnectionNew(_configuration["Commex"]);
                if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb(StrCommexConn + ".sysobjects a, " + StrCommexConn + ".sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
                { StrComTradesIndex = "index(idx_trades_clientcd),"; }

                strsql = strsql + " union all ";
                strsql = strsql + " select case left(sm_productcd,1) when 'F' then 5 else 6 end,'', '','' as td_isin_code,";
                strsql = strsql + " '' as sc_company_name,   cast((case when  sum(td_bqty-td_sqty)=0 then 0 else ";
                strsql = strsql + " sum((td_bqty-td_sqty)*td_rate)/sum(td_bqty-td_sqty) end)as decimal(15,4) ) as rate,";
                strsql = strsql + " case left(sm_productcd,1) when 'F' then 'Future (Commodities)' else 'Option (Commodities)' end Td_Type,rtrim(sm_symbol), sm_expirydt,rtrim(sm_symbol), case left(sm_productcd,1) ";
                strsql = strsql + " when 'F' then 'Fut ' else 'Opt ' end+ rtrim(sm_symbol)+'  Exp: '+ ";
                strsql = strsql + " ltrim(rtrim(convert(char,convert(datetime,sm_expirydt),103))) + ";
                strsql = strsql + " case left(sm_productcd,1) when 'F' then '' else ''+rtrim(convert(char(9),sm_strikeprice))+sm_callput end, ";
                strsql = strsql + " sum(td_bqty) Bqty, convert(decimal(15,2), sum(td_bqty*td_rate *sm_multiplier)) BAmt,  sum(td_sqty) Sqty, convert(decimal(15,2), sum(td_sqty*td_rate*sm_multiplier)) SAmt, ";
                strsql = strsql + " sum(td_bqty-td_sqty) NQty, convert(decimal(15,2),sum((td_bqty-td_sqty)*td_rate*sm_multiplier)) NAmt,'' as td_debit_credit ,sm_strikeprice, sm_callput,'Commodities' + '|' + '' + '|' + replace(sm_symbol,'&','-')  + '|' + sm_expirydt LinkCode ";
                strsql = strsql + " from " + StrCommexConn + ".trades with(" + StrComTradesIndex + "nolock), " + StrCommexConn + ".series_master with (nolock) ";
                strsql = strsql + " where td_clientcd='" + userId + "' and sm_exchange=td_exchange and td_seriesid=sm_seriesid and td_dt ";
                strsql = strsql + " between '" + fromDate + "' and '" + toDate + "'";
                strsql = strsql + " group by sm_symbol, sm_productcd,sm_expirydt, sm_strikeprice, sm_callput  ";
            }

            strsql = " select ListOrder,Td_Type Type,td_scripnm ScripCode ,snm ScripName,Bqty Buy,BAmt BuyAmount,Sqty Sell,SAmt SellAmount,NQty Net,NAmt NetAmount,rate AvgRate,LinkCode from (" + strsql + ") a order by ListOrder, td_type, snm ,td_scripnm ";

            return strsql;
        }

        private string Transaction_Trade_Datewise(string userId, string fromDate, string toDate)
        {
            string StrTradesIndex = "";
            string StrComTradesIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
            { StrTradesIndex = "index(idx_trades_clientcd),"; }

            string StrTRXIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'Trx' and b.name", "idx_Trx_Clientcd", true)) == 1)
            { StrTRXIndex = "index(idx_trx_clientcd),"; }

            char[] ArrSeparators = new char[1];
            ArrSeparators[0] = '/';
            DataSet dt = new DataSet();
            strsql = "select 1 Td_order,'Equity [' + Case When left(td_stlmnt,1) = 'B' Then 'BSE' else 'NSE' end + ']' Td_Type,ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) Dt, rtrim(td_stlmnt)   td_stlmnt, ";
            strsql = strsql + "sum(td_bqty) Bqty, convert(decimal(15,2),sum(td_bqty*td_rate)) BAmt, sum(td_sqty) Sqty, convert(decimal(15,2),sum(td_sqty*td_rate)) SAmt, ";
            strsql = strsql + "sum(td_bqty-td_sqty) NQty, ";
            strsql = strsql + "convert(decimal(15,2),sum((td_bqty-td_sqty)*td_rate)) NAmt, 'Equity|'+Left(td_Stlmnt,1)+'|' LinkCode, td_dt Dt1  from trx with (" + StrTRXIndex + "nolock) where td_clientcd='" + userId + "' and ";
            strsql = strsql + "td_dt between '" + fromDate + "' and '" + toDate + "' group by  td_stlmnt,td_dt  ";
            strsql = strsql + "union all ";
            strsql = strsql + "select case sm_prodtype when 'CF' then 4 else case left(sm_productcd,1) when 'F' then 2 else 3 end end,  ";
            strsql = strsql + "Case When td_segment='K' then 'Currency ' else 'Equity ' end + ";
            strsql = strsql + "Case left(sm_productcd,1) when 'F' Then 'Future ' else 'Option ' end + '[' + Case left(td_exchange,1) when 'B'";
            strsql = strsql + "Then 'BSE' when 'N' then 'NSE' else 'MCX' end + ']' Td_Type,";
            strsql = strsql + " ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) Dt, case left(sm_prodtype,1) when 'I' then 'Index' when 'E' then 'Stock' else 'Currency' end + case right(sm_prodtype,1) when 'F' then ' Future' else ' Option' end  , sum(td_bqty) Bqty, ";
            strsql = strsql + " convert(decimal(15,2),sum(td_bqty*td_rate*sm_multiplier)) BAmt, sum(td_sqty) Sqty, convert(decimal(15,2),sum(td_sqty*td_rate*sm_multiplier)) SAmt,  ";
            strsql = strsql + "sum(td_bqty-td_sqty) NQty, ";
            strsql = strsql + "convert(decimal(15,2),sum((td_bqty-td_sqty)*td_rate*sm_multiplier)) NAmt,Case When td_segment='K' then 'Currency' else 'Equity' end +  Case left(sm_productcd,1) ";
            strsql = strsql + "when 'F' Then 'Future' else 'Option' end + '|' + td_exchange + '|' +  + '|' +  + '|' ";
            strsql = strsql + "+  + '|' +  '' + '|' +  sm_optionstyle + '|' +  td_segment LinkCode ,td_dt Dt1  "; //--, sm_prodtype
            strsql = strsql + "from trades with (" + StrTradesIndex + "nolock), series_master with (nolock) ";
            strsql = strsql + "where td_clientcd='" + userId + "' and sm_exchange=td_exchange and sm_Segment=td_Segment and td_seriesid=sm_seriesid and  ";
            strsql = strsql + "td_dt between '" + fromDate + "' and '" + toDate + "' and td_trxflag <> 'O' ";
            strsql = strsql + "group by  td_Dt,sm_productcd,td_exchange,td_Segment, sm_prodtype,td_exchange,sm_optionstyle ,sm_prodtype   ";
            strsql = strsql + " union all  ";
            strsql = strsql + "select 5 ,Case When ex_segment='K' then 'Currency ' else 'Equity ' end + case ex_eaflag when 'E' then";
            strsql = strsql + "'Exercise ' else 'Assignment ' end + '[' + Case left(ex_exchange,1) when 'B' Then 'BSE' when 'N' then 'NSE' else 'MCX' end + ']' Td_Type,ltrim(rtrim(convert(char,convert(datetime,ex_Dt),103))) Dt,";
            strsql = strsql + " (case left(sm_prodtype,1) when 'I' then 'Index' when 'E' then 'Stock' else 'Currency' end + case right(sm_prodtype,1) when 'F' then ' Future' else ' Option' end ),";
            strsql = strsql + "sum(ex_aqty) Bqty, convert(decimal(15,2),sum(ex_aqty*ex_diffrate *case ex_eaflag When 'A' Then -1 else 1 end *sm_multiplier)) BAmt, sum(ex_eqty) Sqty, convert(decimal(15,2),sum(ex_eqty*ex_diffrate *case ex_eaflag When 'A' Then -1 else 1 end *sm_multiplier)) SAmt, ";
            strsql = strsql + "sum(ex_aqty-ex_eqty) NQty,";
            strsql = strsql + "convert(decimal(15,2),sum((ex_aqty-ex_eqty)*ex_diffrate *case ex_eaflag When 'A' Then -1 else 1 end *sm_multiplier)) NAmt, Case ";
            strsql = strsql + "When ex_segment='K' then 'Currency' else 'Equity' end +  case ex_eaflag when 'E' then 'Exercise'";
            strsql = strsql + "else 'Assignment' end + '|' + ex_exchange + '|' + ex_segment + '|'  LinkCode ,ex_Dt Dt1 from exercise with (nolock), series_master with (nolock) ";
            strsql = strsql + "where ex_clientcd='" + userId + "' and ex_exchange=sm_exchange and ex_Segment=sm_Segment and ex_seriesid=sm_seriesid and  ";
            strsql = strsql + "ex_dt between '" + fromDate + "' and '" + toDate + "' group by ex_Dt,ex_eaflag ,ex_exchange,ex_Segment,sm_prodtype   ";

            if (_configuration["Commex"] != null && _configuration["Commex"] != string.Empty)
            {

                string StrCommexConn = "";
                StrCommexConn = objUtility.GetCommexConnectionNew(_configuration["Commex"]);
                if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb(StrCommexConn + ".sysobjects a, " + StrCommexConn + ".sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
                { StrComTradesIndex = "index(idx_trades_clientcd),"; }

                strsql = strsql + "union all  ";
                strsql = strsql + "select case left(sm_productcd,1) when 'F' then 6 else 7 end , ";
                strsql = strsql + " 'Commodities ' + '[' + case td_exchange When 'M' Then 'MCX' When 'N' Then 'NCDEX' else '' end + ']' Td_Type, ";
                strsql = strsql + " ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) Dt, 'Commodity' + case right(sm_prodtype,1) when 'F' then ' Future' else ' Option' end, sum(td_bqty) Bqty,  ";
                strsql = strsql + " convert(decimal(15,2),sum(td_bqty*td_rate*sm_multiplier)) BAmt, sum(td_sqty) Sqty, convert(decimal(15,2),sum(td_sqty*td_rate*sm_multiplier)) SAmt,  ";
                strsql = strsql + "sum(td_bqty-td_sqty) NQty, ";
                strsql = strsql + "convert(decimal(15,2),sum((td_bqty-td_sqty)*td_rate*sm_multiplier)) NAmt,'Commodities' + '|' + td_exchange + '|' +   + '|'  LinkCode ,td_dt Dt1 ";
                strsql = strsql + "from " + StrCommexConn + ".trades with(" + StrComTradesIndex + "nolock)," + StrCommexConn + ".series_master with (nolock) ";
                strsql = strsql + "where td_clientcd='" + userId + "' and sm_exchange=td_exchange and td_seriesid=sm_seriesid and  ";
                strsql = strsql + "td_dt between '" + fromDate + "' and '" + toDate + "' group by td_Dt,sm_productcd,td_exchange ,sm_prodtype  ";
            }

            strsql = " select Td_order ListOrder ,Td_Type type,Dt Date,td_stlmnt Stlmnt,Bqty Buy,BAmt BuyAmount,Sqty Sell,SAmt SellAmount,NQty Net,NAmt NetAmount,LinkCode,Dt1 Date2 from (" + strsql + ") a ";
            return strsql;
        }

        private void Transaction_Trade_MF(string userId, string fromDate, string toDate)
        {
            string strSql = "";

            strSql = "select MTd_srno,ltrim(rtrim(convert(char,convert(datetime,MTd_dt),103))) MTd_dt, MTd_ISIN , MTd_Bqty,MTd_Sqty,MTd_Rate,cast(Mtd_MarketRate as decimal(15,4)) Mtd_MarketRate,MTd_Brokerage,MTd_OrderDt,MTd_OrderTime,MTd_TerminalCd,MTd_Billdt,";
            strSql += "case When MTd_Exchange = 'B' Then MFS_BSchemeName else MFS_NSchemeName End SchemeName,MTd_OldClientcd,";
            strSql += "case when Mtd_MarketRate=0 then 0 else (mtd_Brokerage*100)/Mtd_MarketRate End Brokper,mtd_broktype,MTd_FolioNumber,MTd_Stlmnt,";
            strSql += "((mtd_bqty+mtd_sqty)*mtd_Brokerage) as Brokerage,cm_name,cm_cd,Mtd_marginyn,cast(((MTd_Sqty-MTd_Bqty) * MTd_Rate) as decimal(15,2)) Value ";
            strSql += " From MFTrades, MFSecurities, Client_master left outer join SubBrokers on cm_subbroker = rm_cd  ,Group_master a,Family_master,Branch_master";
            strSql += " Where MTd_ISIN=MFS_ISIN and MTd_ClientCd = cm_cd and MTd_dt between '" + fromDate + "' and '" + toDate + "' ";
            strSql += " and cm_brboffcode = bm_branchcd ";
            strSql += " and cm_groupcd = a.gr_cd and cm_familycd  = fm_cd ";
            strSql += " and MTd_ClientCd = '" + userId + "'";
            strSql += " order by MTd_dt,MFS_NSchemeName";
            DataSet dt = new DataSet();
        }

        public dynamic Transaction_AGTS(string userId, string seg, string fromDate, string toDate)
        {
            SqlConnection ObjConnection;
            DataSet ObjDataSetCash = new DataSet();
            using (var db = new DataContext())
            {
                ObjConnection = new SqlConnection((db.Database.GetDbConnection()).ConnectionString);
                if (ObjConnection.State == ConnectionState.Closed)
                    ObjConnection.Open();
            }

            // TODO : We replace _config to string type Cash_Type.
            if (seg == "C")
            {

                strsql = "if OBJECT_ID('tempdb..##tmpGlobalC') is not null Drop Table ##tmpGlobalC";
                objUtility.ExecuteSQLTmp(strsql, ObjConnection);

                strsql = "Create Table ##tmpGlobalC(";
                strsql += " td_dt varchar(8) Not null ,";
                strsql += " td_stlmnt varchar(9) Not null ,";
                strsql += " td_clientcd varchar(8) Not null , ";
                strsql += " td_Scripcd varchar(6) Not null , ";
                strsql += " td_BSFlag varchar(1) Not null , ";
                strsql += " td_Qty numeric (18,3) Not null, ";
                strsql += " td_Value numeric (18,3) Not null, ";
                strsql += " td_MarketRate MONEY Not null, ";
                strsql += " td_Brokerage MONEY Not null, ";
                strsql += " td_Netrate MONEY Not null, ";
                strsql += " td_Chrg1_1 varchar(10) Not null, ";
                strsql += " td_Chrg1_2 varchar(10) Not null, ";
                strsql += " td_Chrg2 varchar(10) Not null, ";
                strsql += " td_Chrg3 varchar(10) Not null, ";
                strsql += " td_Chrg4 varchar(10) Not null, ";
                strsql += " td_Chrg5 varchar(10) Not null, ";
                strsql += " td_Chrg6 varchar(10) Not null, ";
                strsql += " td_Chrg7 varchar(10) Not null, ";
                strsql += " td_Exch varchar(8) Not null, ";
                strsql += " td_ExchTrx varchar(20) Not null , ";
                strsql += " td_TRDType char(2) Not null, ";
                strsql += " td_TotalChrg numeric (18,3) Not null ";
                strsql += " )";
                objUtility.ExecuteSQLTmp(strsql, ObjConnection);

                strsql = "Select GLOBAL_TRX.* ";
                strsql += " , Case td_bsflag When 'B' then td_bqty When 'S' then td_sqty else 0 end BSQuantity ";
                strsql += "  , Case td_bsflag When 'B' then Round((td_bqty * td_rate),2) When 'S' then Round((td_sqty * td_rate),2) else 0 end BSValue  ";
                strsql += " From GLOBAL_TRX, Client_master  ";
                strsql += " Where td_clientcd = cm_cd and td_dt between '" + fromDate + "' and '" + toDate + "'";
                strsql += " and td_clientcd = '" + userId + "'";
                DataSet dt = new DataSet();
                dt = objUtility.OpenDataSet(strsql);

                if (dt.Tables[0].Rows.Count > 0)
                {
                    int i = 0;
                    for (i = 0; i < dt.Tables[0].Rows.Count; i++)
                    {
                        strsql = " Insert Into ##tmpGlobalC Values (";
                        strsql += "'" + dt.Tables[0].Rows[i]["td_dt"].ToString() + "', ";
                        strsql += "'" + dt.Tables[0].Rows[i]["td_stlmnt"].ToString() + "', ";
                        strsql += "'" + dt.Tables[0].Rows[i]["td_clientcd"].ToString() + "', ";
                        strsql += "'" + dt.Tables[0].Rows[i]["td_scripcd"].ToString() + "', ";
                        strsql += "'" + dt.Tables[0].Rows[i]["td_bsflag"].ToString() + "', ";
                        strsql += dt.Tables[0].Rows[i]["BSQuantity"].ToString() + " , ";
                        strsql += dt.Tables[0].Rows[i]["BSValue"].ToString() + " , ";
                        strsql += dt.Tables[0].Rows[i]["td_marketrate"].ToString() + " , ";
                        strsql += dt.Tables[0].Rows[i]["td_brokerage"].ToString() + ",";
                        strsql += dt.Tables[0].Rows[i]["td_rate"].ToString() + ", ";
                        strsql += "'" + DecryptN(dt.Tables[0].Rows[i]["td_Chrg1_1"].ToString()) + "', ";
                        strsql += "'" + DecryptN(dt.Tables[0].Rows[i]["td_Chrg1_2"].ToString()) + "', ";
                        strsql += "'" + DecryptN(dt.Tables[0].Rows[i]["td_Chrg2"].ToString()) + "', ";
                        strsql += "'" + DecryptN(dt.Tables[0].Rows[i]["td_Chrg3"].ToString()) + "', ";
                        strsql += "'" + DecryptN(dt.Tables[0].Rows[i]["td_Chrg4"].ToString()) + "', ";
                        strsql += "'" + DecryptN(dt.Tables[0].Rows[i]["td_Chrg5"].ToString()) + "', ";
                        strsql += "'" + DecryptN(dt.Tables[0].Rows[i]["td_Chrg6"].ToString()) + "', ";
                        strsql += "'" + DecryptN(dt.Tables[0].Rows[i]["td_Chrg7"].ToString()) + "', ";
                        strsql += "'" + mfnGetExchangeCode2Desc(Strings.Left(dt.Tables[0].Rows[i]["td_stlmnt"].ToString(), 1)) + "',";
                        strsql += "'" + (Conversion.Val(DecryptN(dt.Tables[0].Rows[i]["td_Chrg1_1"].ToString())) + Conversion.Val(DecryptN(dt.Tables[0].Rows[i]["td_Chrg1_2"].ToString()))) + "',";
                        strsql += "'" + dt.Tables[0].Rows[i]["td_TRDType"].ToString() + "',0";
                        strsql += " )";
                        objUtility.ExecuteSQLTmp(strsql, ObjConnection);
                    }

                    strsql = " Update ##tmpGlobalC set td_TotalChrg =  Convert(numeric(18,3),td_Chrg2) + Convert(numeric(18,3),td_Chrg3) +Convert(numeric(18,3),td_Chrg4) + Convert(numeric(18,3),td_Chrg5) + Convert(numeric(18,3),td_Chrg6) + Convert(numeric(18,3),td_ExchTrx) ";
                    objUtility.ExecuteSQLTmp(strsql, ObjConnection);

                    strsql = " select td_clientcd ClientCode,td_dt Date, td_stlmnt Stlmnt,td_Scripcd ScripCd, ss_Name ScripName, td_BSFlag BSFlag, td_Qty Quantity, ";
                    strsql += " td_MarketRate MarketRate, td_Brokerage Brokerage,td_Netrate NetRate, td_ExchTRX ExchTRX_Chrg ,";
                    strsql += " td_Chrg2 StampDuty, td_Chrg3 SEBITO ,td_Chrg4 Others, td_Chrg5 STT,td_Chrg7 GST, td_Exch Exchange,td_TRDType TRDType";
                    strsql += " from ##tmpGlobalC, securities ";
                    strsql += " Where td_scripcd = ss_cd ";

                    ObjDataSetCash = objUtility.OpenDataSetTmp(strsql, ObjConnection);

                }
            }
            else if (seg == "F" || seg == "K")
            {
                strsql = "if OBJECT_ID('tempdb..##tmpGlobalFO') is not null Drop Table ##tmpGlobalFO";
                objUtility.ExecuteSQLTmp(strsql, ObjConnection);

                strsql = "Create Table ##tmpGlobalFO(";
                strsql += " td_exchange char(1) Not null ,";
                strsql += " td_segment char(1) Not null ,";
                strsql += " td_Date varchar(8) Not null ,";
                strsql += " td_clientcd varchar(8) Not null , ";
                strsql += " td_seriesId numeric (18,0) Not null , ";
                strsql += " td_BSFlag varchar(1) Not null , ";
                strsql += " td_Qty numeric (18,3) Not null, ";
                strsql += " td_Value numeric (18,3) Not null, ";
                strsql += " td_MarketRate MONEY Not null, ";
                strsql += " td_Brokerage MONEY Not null, ";
                strsql += " td_Netrate MONEY Not null, ";
                strsql += " td_Chrg1 varchar(10) Not null, ";
                strsql += " td_Chrg2 varchar(10) Not null, ";
                strsql += " td_Chrg3 varchar(10) Not null, ";
                strsql += " td_Chrg4 varchar(10) Not null, ";
                strsql += " td_Chrg5 varchar(10) Not null, ";
                strsql += " td_Chrg6 varchar(10) Not null, ";
                strsql += " td_Chrg7 varchar(10) Not null, ";
                strsql += " td_exch varchar(8) Not null ";
                strsql += " )";
                objUtility.ExecuteSQLTmp(strsql, ObjConnection);

                strsql = "Select Global_Trades.* ,";
                strsql += " Case td_bsflag When 'B' then td_bqty When 'S' then td_sqty else 0 end BSQuantity, ";
                strsql += " Case td_bsflag When 'B' then Round((td_bqty * td_rate * sm_multiplier),2) When 'S' then Round((td_sqty * td_rate * sm_multiplier),2) else 0 end BSValue  ";
                strsql += " From Global_Trades, client_master, Series_Master ";
                strsql += " Where  td_seriesId = sm_seriesid and td_exchange = SM_exchange and td_Segment = sm_Segment ";
                strsql += " and td_clientcd = cm_cd and td_dt between '" + fromDate + "' and '" + toDate + "' and td_Segment = '" + seg + "'";
                strsql += " and td_clientcd = '" + userId + "'";

                DataSet ObjDataSet = new DataSet();
                ObjDataSet = objUtility.OpenDataSet(strsql);

                if (ObjDataSet.Tables[0].Rows.Count > 0)
                {
                    int i = 0;
                    for (i = 0; i < ObjDataSet.Tables[0].Rows.Count; i++)
                    {
                        strsql = " Insert Into ##tmpGlobalFO Values (";
                        strsql += "'" + ObjDataSet.Tables[0].Rows[i]["td_exchange"].ToString() + "',";
                        strsql += "'" + ObjDataSet.Tables[0].Rows[i]["td_segment"].ToString() + "',";
                        strsql += "'" + ObjDataSet.Tables[0].Rows[i]["td_dt"].ToString() + "',";
                        strsql += "'" + ObjDataSet.Tables[0].Rows[i]["td_clientcd"].ToString() + "',";
                        strsql += "'" + ObjDataSet.Tables[0].Rows[i]["td_seriesid"].ToString() + "',";
                        strsql += "'" + ObjDataSet.Tables[0].Rows[i]["td_bsflag"].ToString() + "',";
                        strsql += ObjDataSet.Tables[0].Rows[i]["BSQuantity"].ToString() + ",";
                        strsql += ObjDataSet.Tables[0].Rows[i]["BSValue"].ToString() + ",";
                        strsql += ObjDataSet.Tables[0].Rows[i]["td_marketrate"].ToString() + ",";
                        strsql += ObjDataSet.Tables[0].Rows[i]["td_brokerage"].ToString() + ",";
                        strsql += ObjDataSet.Tables[0].Rows[i]["td_rate"].ToString() + ",";
                        strsql += "'" + DecryptN(ObjDataSet.Tables[0].Rows[i]["td_Chrg1"].ToString()) + "', ";
                        strsql += "'" + DecryptN(ObjDataSet.Tables[0].Rows[i]["td_Chrg2"].ToString()) + "', ";
                        strsql += "'" + DecryptN(ObjDataSet.Tables[0].Rows[i]["td_Chrg3"].ToString()) + "', ";
                        strsql += "'" + DecryptN(ObjDataSet.Tables[0].Rows[i]["td_Chrg4"].ToString()) + "', ";
                        strsql += "'" + DecryptN(ObjDataSet.Tables[0].Rows[i]["td_Chrg5"].ToString()) + "', ";
                        strsql += "'" + DecryptN(ObjDataSet.Tables[0].Rows[i]["td_Chrg6"].ToString()) + "', ";
                        strsql += "'" + DecryptN(ObjDataSet.Tables[0].Rows[i]["td_Chrg7"].ToString()) + "', ";
                        strsql += "'" + mfnGetExchangeCode2Desc(ObjDataSet.Tables[0].Rows[i]["td_exchange"].ToString()) + "') ";
                        objUtility.ExecuteSQLTmp(strsql, ObjConnection);
                    }

                    strsql = " select td_clientcd ClientCode,td_date Date, td_seriesid seriesid, sm_desc SeriesName, td_BSFlag BSFlag, td_Qty Quantity,";
                    strsql += " td_MarketRate MarketRate, td_Brokerage Brokerage, td_Netrate NetRate, td_Chrg1 ExchTRX_Chrg,";
                    strsql += " td_Chrg2 StampDuty, td_Chrg3 SEBITO, td_Chrg4 Others, td_Chrg5 STT, td_Chrg7 GST, td_Exch Exchange";
                    strsql += " from ##tmpGlobalFO, Series_Master ";
                    strsql += " Where td_seriesId = sm_seriesid  and td_exchange = SM_exchange and td_Segment = sm_Segment ";

                    ObjDataSetCash = objUtility.OpenDataSetTmp(strsql, ObjConnection);

                }
            }
            else if ((seg == "X") && (_configuration["Commex"] != null && _configuration["Commex"] != string.Empty))
            {
                string StrCommexConn = "";
                StrCommexConn = objUtility.GetCommexConnection();

                strsql = "if OBJECT_ID('tempdb..##tmpGlobalFO') is not null Drop Table ##tmpGlobalFO";
                objUtility.ExecuteSQLTmp(strsql, ObjConnection);

                strsql = "Create Table ##tmpGlobalFO(";
                strsql += " td_exchange char(1) Not null ,";
                strsql += " td_segment char(1) Not null ,";
                strsql += " td_Date varchar(8) Not null ,";
                strsql += " td_clientcd varchar(8) Not null , ";
                strsql += " td_seriesId numeric (18,0) Not null , ";
                strsql += " td_BSFlag varchar(1) Not null , ";
                strsql += " td_Qty numeric (18,3) Not null, ";
                strsql += " td_Value numeric (18,3) Not null, ";
                strsql += " td_MarketRate MONEY Not null, ";
                strsql += " td_Brokerage MONEY Not null, ";
                strsql += " td_Netrate MONEY Not null, ";
                strsql += " td_Chrg1 varchar(10) Not null, ";
                strsql += " td_Chrg2 varchar(10) Not null, ";
                strsql += " td_Chrg3 varchar(10) Not null, ";
                strsql += " td_Chrg4 varchar(10) Not null, ";
                strsql += " td_Chrg5 varchar(10) Not null, ";
                strsql += " td_Chrg6 varchar(10) Not null, ";
                strsql += " td_Chrg7 varchar(10) Not null, ";
                strsql += " td_exch varchar(8) Not null ";
                strsql += " )";
                objUtility.ExecuteSQLTmp(strsql, ObjConnection);

                strsql = "Select " + StrCommexConn + ".Global_Trades.* ,";
                strsql += " Case td_bsflag When 'B' then td_bqty When 'S' then td_sqty else 0 end BSQuantity, ";
                strsql += " Case td_bsflag When 'B' then Round((td_bqty * td_rate * sm_multiplier),2) When 'S' then Round((td_sqty * td_rate * sm_multiplier),2) else 0 end BSValue  ";
                strsql += " From  " + StrCommexConn + ".Global_Trades,  " + StrCommexConn + ".client_master,  " + StrCommexConn + ".Series_Master ";
                strsql += " Where  td_seriesId = sm_seriesid and td_exchange = SM_exchange ";
                strsql += " and td_clientcd = cm_cd and td_dt between '" + fromDate + "' and '" + toDate + "'";
                strsql += " and td_clientcd = '" + userId + "'";

                DataSet ObjDataSet = new DataSet();
                ObjDataSet = objUtility.OpenDataSet(strsql);

                if (ObjDataSet.Tables[0].Rows.Count > 0)
                {
                    int i = 0;
                    for (i = 0; i < ObjDataSet.Tables[0].Rows.Count; i++)
                    {
                        strsql = " Insert Into ##tmpGlobalFO Values (";
                        strsql += "'" + ObjDataSet.Tables[0].Rows[i]["td_exchange"].ToString() + "',";
                        strsql += "'',";
                        strsql += "'" + ObjDataSet.Tables[0].Rows[i]["td_dt"].ToString() + "',";
                        strsql += "'" + ObjDataSet.Tables[0].Rows[i]["td_clientcd"].ToString() + "',";
                        strsql += "'" + ObjDataSet.Tables[0].Rows[i]["td_seriesid"].ToString() + "',";
                        strsql += "'" + ObjDataSet.Tables[0].Rows[i]["td_bsflag"].ToString() + "',";
                        strsql += ObjDataSet.Tables[0].Rows[i]["BSQuantity"].ToString() + ",";
                        strsql += ObjDataSet.Tables[0].Rows[i]["BSValue"].ToString() + ",";
                        strsql += ObjDataSet.Tables[0].Rows[i]["td_marketrate"].ToString() + ",";
                        strsql += ObjDataSet.Tables[0].Rows[i]["td_brokerage"].ToString() + ",";
                        strsql += ObjDataSet.Tables[0].Rows[i]["td_rate"].ToString() + ",";
                        strsql += "'" + DecryptN(ObjDataSet.Tables[0].Rows[i]["td_Chrg1"].ToString()) + "', ";
                        strsql += "'" + DecryptN(ObjDataSet.Tables[0].Rows[i]["td_Chrg2"].ToString()) + "', ";
                        strsql += "'" + DecryptN(ObjDataSet.Tables[0].Rows[i]["td_Chrg3"].ToString()) + "', ";
                        strsql += "'" + DecryptN(ObjDataSet.Tables[0].Rows[i]["td_Chrg4"].ToString()) + "', ";
                        strsql += "'" + DecryptN(ObjDataSet.Tables[0].Rows[i]["td_Chrg5"].ToString()) + "', ";
                        strsql += "'" + DecryptN(ObjDataSet.Tables[0].Rows[i]["td_Chrg6"].ToString()) + "', ";
                        strsql += "'" + DecryptN(ObjDataSet.Tables[0].Rows[i]["td_Chrg7"].ToString()) + "', ";
                        strsql += "'" + mfnGetExchangeCode2DescComm(ObjDataSet.Tables[0].Rows[i]["td_exchange"].ToString()) + "') ";
                        objUtility.ExecuteSQLTmp(strsql, ObjConnection);
                    }

                    strsql = " select td_clientcd ClientCode,td_date Date, td_seriesid seriesid, sm_desc SeriesName, td_BSFlag BSFlag, td_Qty Quantity,";
                    strsql += " td_MarketRate MarketRate, td_Brokerage Brokerage, td_Netrate NetRate, td_Chrg1 ExchTRX_Chrg,";
                    strsql += " td_Chrg2 StampDuty, td_Chrg3 SEBITO, td_Chrg4 Others, td_Chrg5 STT, td_Chrg7 GST, td_Exch Exchange";
                    strsql += " from ##tmpGlobalFO, " + StrCommexConn + ".Series_Master ";
                    strsql += " Where td_seriesId = sm_seriesid  and td_exchange = SM_exchange ";

                    ObjDataSetCash = objUtility.OpenDataSetTmp(strsql, ObjConnection);

                }
            }

            try
            {
                if (ObjDataSetCash != null)
                {
                    if (ObjDataSetCash?.Tables?.Count > 0 && ObjDataSetCash?.Tables[0]?.Rows?.Count > 0)
                    {
                        var json = JsonConvert.SerializeObject(ObjDataSetCash.Tables[0], Formatting.Indented);
                        return json;
                    }
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }


        }

        private void BindGridDPHolding(string cm_cd, string Trade_Type, String Select_Type, string FromDate, string ToDate)
        {
            if (Trade_Type == "Trades" || Trade_Type == "Deliveries")
            {
                if (Select_Type == "0")
                {
                    ShowItemWiseDPHolding(cm_cd, Trade_Type, Select_Type, FromDate, ToDate);
                }
                else
                {
                    ShowDateWiseDPHolding(cm_cd, Trade_Type, Select_Type, FromDate, ToDate);
                }
            }
        }

        private void ShowItemWiseDPHolding(string cm_cd, string Trade_Type, string Select_Type, string FromDate, string ToDate)
        {
            if (_configuration["IsTradeWeb"] == "O") //liveDB
            {
                if (_configuration["Estro"] != "" && _configuration["Cross"] != "")
                {
                    DataSet dt = new DataSet();
                    strsql = "select da_DPID from dematact with (nolock) where da_clientcd='" + cm_cd + "' and da_defaultYN = 'Y'";
                    dt = objUtility.OpenDataSet(strsql);
                    strConnecton = dt.Tables[0].Rows[0][0].ToString().Trim();
                    if (Strings.Left(strConnecton, 2) == "IN")
                    { GetEstroItemWiseDPHolding(cm_cd, FromDate, ToDate); }
                    else { GetCrossItemWiseDPHolding(cm_cd, FromDate, ToDate); }
                }
                else
                {
                    if (_configuration["Estro"] != "")
                        GetEstroItemWiseDPHolding(cm_cd, FromDate, ToDate);
                    if (_configuration["Cross"] != "")
                        GetCrossItemWiseDPHolding(cm_cd, FromDate, ToDate);
                }
            }
        }

        private void ShowDateWiseDPHolding(string cm_cd, string Trade_Type, string Select_Type, string FromDate, string ToDate)
        {
            DataSet dt = new DataSet();
            string strConnecton = string.Empty;
            if (_configuration["IsTradeWeb"] == "O") //liveDB
            {
                strsql = "select da_DPID from dematact with (nolock) where da_clientcd='" + cm_cd + "' and da_defaultYN = 'Y'";
                dt = objUtility.OpenDataSet(strsql);
                strConnecton = dt.Tables[0].Rows[0][0].ToString().Trim();
                if (_configuration["Estro"] != "" && _configuration["Cross"] != "")
                {
                    if (Strings.Left(strConnecton, 2) == "IN")
                    { GetEstroDateWiseDPHolding(cm_cd, FromDate, ToDate); }
                    else
                    { GetCrossDateWiseDPHolding(cm_cd, FromDate, ToDate); }

                }
                else
                {
                    if (_configuration["Estro"] != "")
                        GetEstroDateWiseDPHolding(cm_cd, FromDate, ToDate);
                    if (_configuration["Cross"] != "")
                        GetCrossDateWiseDPHolding(cm_cd, FromDate, ToDate);
                }
            }
            else //tradeweb  
            {
                strsql = "  select convert (char,convert(datetime,td_trxdate),103) td_trxdate,td_reference, sc_isinname , td_isin_code as ISINCode, ";
                strsql += "da_name + '  [ '+ td_dpid+td_ac_code +' ]' as DPID,";
                strsql += "Case td_debit_credit  when 'D' then cast((td_qty)as decimal(15,0)) else 0 end  'Debit', ";
                strsql += "Case td_debit_credit  when 'C' then cast((td_qty)as decimal(15,0)) else 0 end  'Credit',";
                strsql += "bt_description as acdesc ,td_isin_code, isnull((select sum(case td_debit_credit when 'C' then td_qty else td_qty * (-1) end) ";
                strsql += "From trxweb with (nolock) Where td_ac_code = a.td_ac_code and td_dpid = a.td_dpid and td_isin_code = a.td_isin_code ";
                strsql += "and  td_ac_type = a.td_ac_type   and td_trxdate <    '" + FromDate + "'),0) 'holding',td_ac_code from trxweb a with (nolock) ,";
                strsql += "Security with (nolock),Client_master with (nolock),Beneficiary_type with (nolock),DematAct with(nolock) ";
                strsql += "where td_isin_code = sc_isincode  And td_ac_type = bt_code  and td_trxdate between  '" + FromDate + "' and '" + ToDate + "'";
                strsql += "and td_dpid+td_ac_code =  da_actno and da_clientcd = cm_cd and cm_cd = '" + cm_cd + "'  ";
                strsql += "Order By DPID,convert(datetime,td_trxdate) ,sc_company_name, sc_isinname, td_isin_code, td_ac_type,td_debit_credit ";
            }
            return;
        }


        private string DecryptN(string strT)
        {
            string strEKey;
            string strRtn;
            int ix;
            strRtn = "";
            strEKey = "TPLUSBACKOFFICE";
            for (ix = 1; ix <= Strings.Len(strT); ix++)
                strRtn = strRtn + Strings.Chr(Strings.Asc(Strings.Mid(strT, ix, 1)) - Strings.Asc(Strings.Mid(strEKey, (ix % Strings.Len(strEKey)) + 1, 1)) + 65);
            return strRtn;
        }

        private string mfnGetExchangeCode2Desc(string strExch)
        {

            strExch = strExch.Trim().ToUpper();
            if (strExch == "N")
            {
                return "NSE";
            }
            else if (strExch == "B")
            {
                return "BSE";
            }
            else
                return "";

        }


        private string mfnGetExchangeCode2DescComm(string strExch)
        {

            strExch = strExch.Trim().ToUpper();
            if (strExch == "M")
            {
                return "MCX";
            }
            else if (strExch == "N")
            {
                return "NCDEX";
            }
            else if (strExch == "A")
            {
                return "Ahm-NMCE";
            }
            else
                return "";

        }

        private void GetEstroDateWiseDPHolding(string cm_cd, string FromDate, string ToDate)
        {
            char[] ArrSeparators = new char[1];
            ArrSeparators[0] = '/';
            string[] ArrEstro = _configuration["Estro"].Split(ArrSeparators);
            strsql = "select convert (char,convert(datetime,td_trxdate),103) td_trxdate,td_reference, sc_company_name as 'sc_isinname' , td_isin_code as ISINCode, ";
            strsql += "da_name + '  [ ' + td_ac_code + ' ]' as DPID,";
            strsql += "Case td_debit_credit  when 'D' then cast((td_qty)as decimal(15,0)) else 0 end  'Debit', ";
            strsql += "Case td_debit_credit  when 'C' then cast((td_qty)as decimal(15,0)) else 0 end  'Credit',";
            strsql += "bt_description as acdesc ,td_isin_code, isnull((select sum(case td_debit_credit when 'C' then td_qty else td_qty * (-1) end) ";
            strsql += "From " + ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".TrxDetail with (nolock) Where td_ac_code = a.td_ac_code and td_isin_code = a.td_isin_code ";
            strsql += "and  td_ac_type = a.td_ac_type   and td_trxdate <    '" + FromDate + "'),0) 'holding',td_ac_code from " + ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".TrxDetail a with (nolock) ,";
            strsql += " DematAct with(nolock) , " + ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".Security   with (nolock),Client_master with (nolock), " + ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".Beneficiary_type  with (nolock),";
            strsql += " " + ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".sysParameter   with (nolock)";
            strsql += "where td_isin_code = sc_isincode  And td_ac_type = bt_code  and td_trxdate between  '" + FromDate + "' and '" + ToDate + "'";
            strsql += "and td_ac_code = da_actno and da_clientcd = cm_cd and da_DPId = sp_sysValue  and sp_parmcd = 'DPID'     and cm_cd = '" + cm_cd + "'  ";
            strsql += "Order By DPID,convert(datetime,td_trxdate) ,sc_company_name,td_isin_code, td_ac_type,td_debit_credit ";
        }

        private void GetEstroItemWiseDPHolding(string cm_cd, string FromDate, string ToDate)
        {
            char[] ArrSeparators = new char[1];
            ArrSeparators[0] = '/';
            string[] ArrEstro = _configuration["Estro"].Split(ArrSeparators);
            strsql = " select convert (char,convert(datetime,td_trxdate),103) td_trxdate,td_reference, sc_company_name + '   [ ' + td_isin_code + ' ]' ISINCode,";
            strsql += " da_name + '  [ '+td_ac_code +' ]' as DPID,cm_cd, td_ac_code,da_name, isnull((select sum(case td_debit_credit when 'C' then td_qty else td_qty * (-1) end) From";
            strsql += " " + ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".TrxDetail with (nolock)";
            strsql += " Where td_ac_code = a.td_ac_code  and td_isin_code = a.td_isin_code  ";
            strsql += " and  td_ac_type = a.td_ac_type   and td_trxdate <  '" + FromDate + "'),0) 'holding',";
            strsql += " case Rtrim(td_settlement) when '' Then '' else ' / '+ td_settlement End 'td_text',";
            strsql += " Case td_debit_credit  when 'D' then cast((td_qty)as decimal(15,0)) else 0 end  'Debit', ";
            strsql += " Case td_debit_credit  when 'C' then cast((td_qty)as decimal(15,0)) else 0 end  'Credit',";
            strsql += " '0' 'Balance',td_qty,td_ac_type,bt_description as acdesc ,td_isin_code,";
            strsql += " td_ac_code,cm_name 'Boid',  td_narration,td_description,td_beneficiery,td_settlement ,td_debit_credit,td_counterdp,td_market_type ,td_booking_type,td_clear_corpn,td_countercmbpid ,mt_description from " + ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".TrxDetail a with (nolock) ,";
            strsql += " " + ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".Security with (nolock),Client_master with (nolock), ";
            strsql += " " + ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".Beneficiary_type  with (nolock),DematAct with(nolock) ,";
            strsql += " " + ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".sysParameter   with (nolock), ";
            strsql += " " + ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".market_type   with (nolock)";
            strsql += " where td_isin_code = sc_isincode  And td_ac_type = bt_code and (td_market_type = mt_code and ltrim(rtrim(isnull(td_clear_corpn,''))) = ltrim(rtrim(mt_ccid)))";
            strsql += " and td_trxdate between  '" + FromDate + "' and '" + ToDate + "'";
            strsql += " and td_ac_code =  da_actno and da_clientcd = cm_cd and da_DPId = sp_sysValue  and sp_parmcd = 'DPID'";
            strsql += " and cm_cd = '" + cm_cd + "'  ";
            strsql += " Order By cm_cd ,da_name, sc_company_name, td_isin_code, td_ac_type,convert(datetime,td_trxdate) ,td_debit_credit ";
        }

        private void GetCrossDateWiseDPHolding(string cm_cd, string FromDate, string ToDate)
        {
            char[] ArrSeparators = new char[1];
            ArrSeparators[0] = '/';
            string[] ArrCross = _configuration["Cross"].Split(ArrSeparators);
            strsql = "    select convert (char,convert(datetime,td_trxdate),103) td_trxdate,td_reference, sc_isinname , td_isin_code as ISINCode, ";
            strsql += "da_name + '  [ ' + td_ac_code + ' ]' as DPID,";
            strsql += "Case td_debit_credit  when 'D' then cast((td_qty)as decimal(15,0)) else 0 end  'Debit', ";
            strsql += "Case td_debit_credit  when 'C' then cast((td_qty)as decimal(15,0)) else 0 end  'Credit',";
            strsql += "bt_description as acdesc ,td_isin_code, isnull((select sum(case td_debit_credit when 'C' then td_qty else td_qty * (-1) end) ";
            strsql += "From " + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".TrxDetail with (nolock) Where td_ac_code = a.td_ac_code and td_isin_code = a.td_isin_code ";
            strsql += "and  td_ac_type = a.td_ac_type   and td_trxdate <    '" + FromDate + "'),0) 'holding',td_ac_code from " + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".TrxDetail a with (nolock) ,";
            strsql += " " + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".Security   with (nolock),Client_master with (nolock), " + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".Beneficiary_type  with (nolock),DematAct with(nolock) ";
            strsql += "where td_isin_code = sc_isincode  And td_ac_type = bt_code  and td_trxdate between  '" + FromDate + "' and '" + ToDate + "'";
            strsql += "and td_ac_code = da_actno and da_clientcd = cm_cd and cm_cd = '" + cm_cd + "'  ";
            strsql += "Order By DPID,convert(datetime,td_trxdate) ,sc_company_name, sc_isinname, td_isin_code, td_ac_type,td_debit_credit ";
        }

        private void GetCrossItemWiseDPHolding(string cm_cd, string FromDate, string ToDate)
        {
            char[] ArrSeparators = new char[1];
            ArrSeparators[0] = '/';
            string[] ArrCross = _configuration["Cross"].Split(ArrSeparators);
            DataSet dt = new DataSet();
            strsql = " select convert (char,convert(datetime,td_trxdate),103) td_trxdate,td_reference, sc_isinname + '   [ ' + td_isin_code + ' ]' ISINCode,";
            strsql += " da_name + '  [ '+td_ac_code +' ]' as DPID,cm_cd, td_ac_code,da_name,";
            strsql += " isnull((select sum(case td_debit_credit when 'C' then td_qty else td_qty * (-1) end) From " + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".TrxDetail with (nolock)";
            strsql += " Where td_ac_code = a.td_ac_code  and td_isin_code = a.td_isin_code  ";
            strsql += " and  td_ac_type = a.td_ac_type   and td_trxdate <  '" + FromDate + "'),0) 'holding',";
            strsql += "  ''  'td_text',";
            strsql += " Case td_debit_credit  when 'D' then cast((td_qty)as decimal(15,0)) else 0 end  'Debit', ";
            strsql += " Case td_debit_credit  when 'C' then cast((td_qty)as decimal(15,0)) else 0 end  'Credit',";
            strsql += " '0' 'Balance',td_qty,td_ac_type,bt_description as acdesc ,td_isin_code,";
            strsql += " td_ac_code,cm_name 'Boid', td_narration,td_description,td_beneficiery,td_settlement ,td_debit_credit,td_counterdp ,td_market_type ";
            strsql += " from " + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".TrxDetail a with (nolock) ," + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".Security with (nolock),Client_master with (nolock), " + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".Beneficiary_type  with (nolock),DematAct with(nolock)";
            strsql += " where td_isin_code = sc_isincode  And td_ac_type = bt_code ";
            strsql += " and td_trxdate between  '" + FromDate + "' and '" + ToDate + "'";
            strsql += " and td_ac_code =  da_actno and da_clientcd = cm_cd";
            strsql += " and cm_cd = '" + cm_cd + "'  ";
            strsql += " Order By cm_cd ,da_name,sc_company_name, sc_isinname, td_isin_code, td_ac_type,convert(datetime,td_trxdate) ,td_debit_credit ";
        }
        #endregion

        #region Transaction details Query

        ////// TODO : For ItemWise transaction details
        private string ItemWiseDetailsQuery(string cm_cd, string td_type, string LinkCode, string td_scripnm, string FromDt, string ToDt)
        {
            string strLinkCode = LinkCode;
            string StrTRXIndex = string.Empty;
            string linkCodeVal = strLinkCode.Split('|')[0].ToString();

            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'Trx' and b.name", "idx_Trx_Clientcd", true)) == 1)
            { StrTRXIndex = "index(idx_trx_clientcd),"; }

            if (linkCodeVal == "Equity")
            {
                strsql = "select  td_orderid,td_tradeid,td_time, ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) as td_dt,convert(decimal(15,4), case when sum(td_bqty) >0 then  sum(td_bqty*td_rate)/sum(td_bqty) else 0 end )rate1,td_stlmnt, sum(td_bqty) Bqty, convert(decimal(15,2),sum(td_bqty*td_rate)) BAmt, ";
                strsql = strsql + "sum(td_sqty) Sqty, convert(decimal(15,2),sum(td_sqty*td_rate))  SAmt,convert(decimal(15,4),case when sum(td_sqty) > 0 then sum(td_sqty*td_rate)/sum(td_sqty) else  0 end)  rate2,";
                strsql = strsql + "sum(td_bqty-td_sqty) NQty, convert(decimal(15,2),sum((td_bqty-td_sqty)*td_rate)) NAmt ";
                strsql = strsql + " from Trx with(" + StrTRXIndex + "nolock)  where td_clientcd='" + cm_cd + "'";
                //strsql = strsql + " and left(td_stlmnt,1) = '" + strLinkCode.Split('|')[1].ToString() + "'";
                strsql = strsql + " and td_scripcd='" + strLinkCode.Split('|')[2].ToString() + "' and td_dt between '" + FromDt + "' and '" + ToDt + "' ";
                strsql = strsql + "group by td_dt, td_stlmnt,td_orderid,td_tradeid,td_time ";

            }

            else if ((linkCodeVal == "EquityFuture") || (linkCodeVal == "CurrencyFuture") || (linkCodeVal == "EquityOption") || (linkCodeVal == "CurrencyOption"))
            {

                strsql = "select td_orderid,td_tradeid,td_time,case left(sm_productcd,1) when 'F' then 'Future' else 'Option' end td_Type,";
                strsql = strsql + " case left(sm_productcd,1) when 'F' then '' else rtrim(sm_callput)+''+ltrim(convert(char,round(sm_strikeprice,0))) end callput, ";
                strsql = strsql + " ltrim(rtrim(convert(char,convert(datetime,td_dt),103)))as td_dt, ltrim(rtrim(convert(char,convert(datetime,sm_expirydt),103)))  as expiry, sum(td_bqty) Bqty, convert(decimal(15,2),sum(td_bqty*td_rate*sm_multiplier))  BAmt,";
                strsql = strsql + " sum(td_sqty) Sqty,convert(decimal(15,2),sum(td_sqty*td_rate*sm_multiplier)) SAmt, sum(td_bqty-td_sqty) NQty,";
                strsql = strsql + " convert(decimal(15,2),sum((td_bqty-td_sqty)*td_rate *sm_multiplier)) NAmt ";
                strsql = strsql + " from trades with (index(idx_trades_clientcd),nolock), series_master with (nolock) ";
                strsql = strsql + " where td_exchange=sm_exchange and td_Segment=sm_Segment";
                strsql = strsql + " and td_seriesid=sm_seriesid and td_clientcd='" + cm_cd + "'";
                //strsql = strsql + " and td_exchange = '" + strLinkCode.Split('|')[1].ToString() + "'";
                strsql = strsql + " and td_segment = '" + strLinkCode.Split('|')[8].ToString() + "'";
                strsql = strsql + " and sm_symbol='" + strLinkCode.Split('|')[2].ToString() + "'";
                strsql = strsql + " and left(sm_productcd,1)='" + strLinkCode.Split('|')[3].ToString() + "'";
                strsql = strsql + " and td_dt between '" + FromDt + "' and '" + ToDt + "'";
                strsql = strsql + " and sm_expirydt='" + strLinkCode.Split('|')[4].ToString() + "'";
                if (strLinkCode.Split('|')[3].ToString() == "O")
                {
                    strsql = strsql + " and sm_strikeprice=" + strLinkCode.Split('|')[5].ToString().Trim() + "";
                    strsql = strsql + " and sm_callput='" + strLinkCode.Split('|')[6].ToString() + "'";
                    strsql = strsql + " and sm_optionstyle='" + strLinkCode.Split('|')[7].ToString() + "'";
                }
                strsql = strsql + " and td_trxflag <> 'O' ";
                strsql = strsql + " group by td_dt, sm_expirydt, sm_productcd, sm_callput, ";
                strsql = strsql + " sm_strikeprice,td_orderid,td_tradeid,td_time";

            }

            else if ((linkCodeVal == "Commodities"))
            {
                if (_configuration["IsTradeWeb"] == "O")//Live
                {
                    string StrCommexConn = "";
                    StrCommexConn = objUtility.GetCommexConnection();

                    strsql = "select td_orderid,td_tradeid,td_time,  case left(sm_productcd,1) when 'F' then 'Future' else 'Option' end td_Type,";
                    strsql = strsql + " case left(sm_productcd,1) when 'F' then '' else rtrim(sm_callput)+''+ltrim(convert(char,round(sm_strikeprice,0))) end callput, ";
                    strsql = strsql + " ltrim(rtrim(convert(char,convert(datetime,td_dt),103)))as td_dt, ltrim(rtrim(convert(char,convert(datetime,sm_expirydt),103)))  as expiry, sum(td_bqty) Bqty, convert(decimal(15,2), sum(td_bqty*td_rate *sm_multiplier)) BAmt,";
                    strsql = strsql + " sum(td_sqty) Sqty,  convert(decimal(15,2), sum(td_sqty*td_rate*sm_multiplier))SAmt, sum(td_bqty-td_sqty) NQty,";
                    strsql = strsql + " convert(decimal(15,2),sum((td_bqty-td_sqty)*td_rate*sm_multiplier)) NAmt ";
                    strsql = strsql + " from " + StrCommexConn + ".trades, " + StrCommexConn + ".series_master";
                    strsql = strsql + " where td_exchange=sm_exchange";
                    strsql = strsql + " and td_seriesid=sm_seriesid and td_clientcd='" + cm_cd + "'";
                    //strsql = strsql + " and td_exchange = '" + strLinkCode.Split('|')[1].ToString() + "'";
                    strsql = strsql + " and sm_symbol='" + strLinkCode.Split('|')[2].ToString() + "'";
                    strsql = strsql + " and td_dt between '" + FromDt + "' and '" + ToDt + "'";
                    strsql = strsql + " and sm_expirydt='" + strLinkCode.Split('|')[3].ToString() + "'";
                    strsql = strsql + " and td_trxflag <> 'O' ";
                    strsql = strsql + " group by td_dt, sm_expirydt, sm_productcd, sm_callput,td_orderid,td_tradeid,td_time,";
                    strsql = strsql + " sm_strikeprice ";

                }
                else
                {
                    strsql = "select td_orderid,td_tradeid,td_time, case left(sm_productcd,1) when 'F' then 'Future' else 'Option' end td_Type,";
                    strsql = strsql + " case left(sm_productcd,1) when 'F' then '' else rtrim(sm_callput)+''+ltrim(convert(char,round(sm_strikeprice,0))) end callput, ";
                    strsql = strsql + " ltrim(rtrim(convert(char,convert(datetime,td_dt),103)))as td_dt, ltrim(rtrim(convert(char,convert(datetime,sm_expirydt),103)))  as expiry, sum(td_bqty) Bqty, convert(decimal(15,2), sum(td_bqty*td_rate *sm_multiplier)) BAmt,";
                    strsql = strsql + " sum(td_sqty) Sqty,  convert(decimal(15,2), sum(td_sqty*td_rate*sm_multiplier))SAmt, sum(td_bqty-td_sqty) NQty,";
                    strsql = strsql + " convert(decimal(15,2),sum((td_bqty-td_sqty)*td_rate*sm_multiplier)) NAmt ";
                    strsql = strsql + " from Ctrades with (nolock) , Cseries_master with (nolock) ";
                    strsql = strsql + " where td_exchange=sm_exchange";
                    strsql = strsql + " and td_seriesid=sm_seriesid and td_clientcd='" + cm_cd + "'";
                    //strsql = strsql + " and td_exchange = '" + strLinkCode.Split('|')[1].ToString() + "'";
                    strsql = strsql + " and sm_symbol='" + strLinkCode.Split('|')[2].ToString() + "'";
                    strsql = strsql + " and td_dt between '" + FromDt + "' and '" + ToDt + "'";
                    strsql = strsql + " and sm_expirydt='" + strLinkCode.Split('|')[3].ToString() + "'";
                    strsql = strsql + " and td_trxflag <> 'O' ";
                    strsql = strsql + " group by td_dt, sm_expirydt, sm_productcd, sm_callput,td_orderid,td_tradeid,td_time, ";
                    strsql = strsql + " sm_strikeprice ";
                }
            }

            else if ((linkCodeVal == "EquityExercise") || (linkCodeVal == "EquityAssignment") || (linkCodeVal == "CurrencyExercise") || (linkCodeVal == "CurrencyAssignment"))
            {
                strsql = "select case ex_eaflag when 'E' then 'Exercise' else 'Assignment' end td_Type, ";
                strsql = strsql + " rtrim(sm_callput)+' '+ltrim(convert(char,round(sm_strikeprice,0))) callput, ltrim(rtrim(convert(char,convert(datetime,ex_dt),103)))  as td_dt,   ltrim(rtrim(convert(char,convert(datetime,sm_expirydt),103)))  as expiry, ";
                strsql = strsql + " sum(ex_aqty) Bqty, convert(decimal(15,2),sum(ex_aqty*ex_diffrate*sm_multiplier)) BAmt,sum(ex_eqty) Sqty,convert(decimal(15,2),sum(ex_eqty*ex_diffrate*sm_multiplier))SAmt, ";
                strsql = strsql + " sum(ex_aqty-ex_eqty) NQty, convert(decimal(15,2),sum((ex_aqty-ex_eqty)*ex_diffrate *sm_multiplier))  NAmt";
                strsql = strsql + " from exercise with (nolock), series_master with (nolock) ";
                strsql = strsql + " where ex_exchange=sm_exchange and ex_Segment=sm_Segment and ex_seriesid=sm_seriesid ";
                strsql = strsql + " and ex_clientcd='" + cm_cd + "' and sm_symbol='" + strLinkCode.Split('|')[2].ToString() + "' ";
                //strsql = strsql + " and ex_exchange = '" + strLinkCode.Split('|')[1].ToString() + "'";
                strsql = strsql + " and ex_Segment = '" + strLinkCode.Split('|')[8].ToString() + "'";
                strsql = strsql + " and ex_dt between '" + FromDt + "' and '" + ToDt + "'";
                strsql = strsql + " and left(sm_productcd,1)='" + strLinkCode.Split('|')[3].ToString() + "'";
                strsql = strsql + " and sm_strikeprice=" + strLinkCode.Split('|')[5].ToString().Trim() + "";
                strsql = strsql + " and sm_callput='" + strLinkCode.Split('|')[6].ToString() + "'";
                strsql = strsql + " and sm_optionstyle='" + strLinkCode.Split('|')[7].ToString() + "'";
                strsql = strsql + " and sm_expirydt='" + strLinkCode.Split('|')[4].ToString() + "'";
                strsql = strsql + " group by ex_dt, sm_expirydt, ex_eaflag, sm_callput,sm_strikeprice order by td_dt,expiry, td_type ";
            }

            else if (linkCodeVal == "NBFC")
            {
                string StrNBFCTRXIndex = "";
                if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'NBFC_TRX' and b.name", "idx_NBFC_TRX_Clientcd", true)) == 1)
                { StrNBFCTRXIndex = "index(idx_NBFC_TRX_clientcd),"; }

                strsql = "select  ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) as td_dt,convert(decimal(15,2), case when sum(td_bqty) >0 then  sum(td_bqty*td_rate)/sum(td_bqty) else 0 end )rate1,td_stlmnt, sum(td_bqty) Bqty, convert(decimal(15,2),sum(td_bqty*td_rate)) BAmt, ";
                strsql = strsql + "sum(td_sqty) Sqty, convert(decimal(15,2),sum(td_sqty*td_rate))  SAmt,convert(decimal(15,2),case when sum(td_sqty) > 0 then sum(td_sqty*td_rate)/sum(td_sqty) else  0 end)  rate2,";
                strsql = strsql + "sum(td_bqty-td_sqty) NQty, convert(decimal(15,2),sum((td_bqty-td_sqty)*td_rate)) NAmt ";
                strsql = strsql + " from NBFC_TRX with (" + StrNBFCTRXIndex + "nolock) where td_clientcd='" + cm_cd + "'";
                strsql = strsql + " and td_scripcd='" + strLinkCode.Split('|')[1].ToString() + "' and td_dt between '" + FromDt + "' and '" + ToDt + "' ";
                strsql = strsql + "group by td_dt,td_stlmnt";  //order by td_dt, td_stlmnt 

            }

            else if (linkCodeVal == "MTF")
            {
                string StrMTFTRXIndex = "";
                if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'MrgTdgFin_TRX' and b.name", "idx_MrgTdgFin_TRX_Clientcd", true)) == 1)
                { StrMTFTRXIndex = "index(idx_MrgTdgFin_TRX_clientcd),"; }

                strsql = " select ltrim(rtrim(convert(char,convert(datetime,MTtd_dt),103))) as td_dt,convert(decimal(15,2), case  ";
                strsql = strsql + "when sum(MTtd_bqty) >0 then  sum(MTtd_bqty*MTtd_rate)/sum(MTtd_bqty) else 0 end )rate1,MTtd_Stlmnt, sum(MTtd_bqty) Bqty, convert(decimal(15,2),";
                strsql = strsql + "sum(MTtd_bqty*MTtd_rate)) BAmt, sum(MTtd_sqty) Sqty, convert(decimal(15,2),sum(MTtd_sqty*MTtd_rate))  SAmt,convert(decimal(15,2),case when sum";
                strsql = strsql + " (MTtd_sqty) > 0 then sum(MTtd_sqty*MTtd_rate)/sum(MTtd_sqty) else  0 end)  rate2,sum(MTtd_bqty-MTtd_sqty) NQty, convert(decimal(15,2),sum((MTtd_bqty-MTtd_sqty)*MTtd_rate)) NAmt ";
                strsql = strsql + " from MrgTdgFin_TRX with (" + StrMTFTRXIndex + "nolock) where MTtd_clientcd='" + cm_cd + "'";
                strsql = strsql + " and MTtd_scripcd='" + strLinkCode.Split('|')[1].ToString() + "' and MTtd_dt between '" + FromDt + "' and '" + ToDt + "' ";
                strsql = strsql + "group by MTtd_dt, MTtd_stlmnt";  //order by td_dt, td_stlmnt 

            }

            return strsql;
        }

        ////// TODO : For DateWise transaction details
        private string DateWiseDetailsQuery(string cm_cd, string td_type, string LinkCode, string td_stlmnt, string Dt, string FromDt, string ToDt, string txtheader)
        {
            string strLinkCode = LinkCode;
            string StrTRXIndex = string.Empty;
            string linkCodeVal = strLinkCode.Split('|')[0].ToString();

            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'Trx' and b.name", "idx_Trx_Clientcd", true)) == 1)
            { StrTRXIndex = "index(idx_trx_clientcd),"; }

            string StrTradesIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
            { StrTradesIndex = "index(idx_trades_clientcd),"; }

            if (linkCodeVal == "Equity")
            {
                if (_configuration["IsTradeWeb"] == "O")//Live DB
                {
                    strsql = "select  td_orderid,td_tradeid,td_time,  rtrim(ss_name) as ScripNm,td_scripcd, td_stlmnt,ltrim(rtrim(convert(char,convert(datetime,td_dt),103)))as td_dt, ";
                    strsql = strsql + "sum(td_bqty) Bqty, convert(decimal(15,2),sum(td_bqty*td_rate)) BAmt, sum(td_sqty) Sqty, convert(decimal(15,2),sum(td_sqty*td_rate)) SAmt, ";
                    strsql = strsql + "sum(td_bqty-td_sqty) NQty, convert(decimal(15,2),sum((td_bqty-td_sqty)*td_rate)) NAmt from Trx with(" + StrTRXIndex + "nolock) ,securities with(nolock) where td_clientcd='" + cm_cd + "' ";
                    strsql = strsql + "and td_stlmnt='" + td_stlmnt + "' and td_dt between '" + FromDt + "' and '" + ToDt + "' and td_Scripcd = ss_cd group by ss_name,td_scripcd,td_orderid,td_tradeid,td_time, ";
                    strsql = strsql + "td_stlmnt,td_dt order by ss_name,td_scripcd,td_stlmnt ";
                }
                else
                {
                    strsql = "select  td_orderid,td_tradeid,td_time, td_scripnm as ScripNm,td_scripcd, td_stlmnt,ltrim(rtrim(convert(char,convert(datetime,td_dt),103)))as td_dt, ";
                    strsql = strsql + "sum(td_bqty) Bqty, convert(decimal(15,2),sum(td_bqty*td_rate)) BAmt, sum(td_sqty) Sqty, convert(decimal(15,2),sum(td_sqty*td_rate)) SAmt, ";
                    strsql = strsql + "sum(td_bqty-td_sqty) NQty, convert(decimal(15,2),sum((td_bqty-td_sqty)*td_rate)) NAmt from  Trx with(" + StrTRXIndex + "nolock) where td_clientcd='" + cm_cd + "' ";
                    strsql = strsql + "and td_stlmnt='" + td_stlmnt + "' and td_dt between '" + FromDt + "' and '" + ToDt + "' group by td_scripnm,td_scripcd,td_orderid,td_tradeid,td_time, ";
                    strsql = strsql + "td_stlmnt,td_dt order by td_scripnm,td_scripcd,td_stlmnt ";

                }
            }
            else if ((linkCodeVal == "EquityFuture") || (linkCodeVal == "CurrencyFuture") || (linkCodeVal == "EquityOption") || (linkCodeVal == "CurrencyOption"))
            {

                strsql = "select td_orderid,td_tradeid,td_time, sm_symbol as ScripNm,case left(sm_productcd,1) when 'F' then 'Future' else 'Option' end td_Type, ";
                strsql = strsql + " case left(sm_productcd,1) when 'F' then '' else rtrim(sm_callput)+''+ ";
                strsql = strsql + "ltrim(convert(char,round(sm_strikeprice,0))) end callput, ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) as td_dt,ltrim(rtrim(convert(char,convert(datetime,sm_expirydt),103)))  as expiry, ";
                strsql = strsql + "sum(td_bqty) Bqty,convert(decimal(15,2),sum(td_bqty*td_rate*sm_multiplier)) BAmt, sum(td_sqty) Sqty, convert(decimal(15,2),sum(td_sqty*td_rate*sm_multiplier)) SAmt,  ";
                strsql = strsql + "sum(td_bqty-td_sqty) NQty, convert(decimal(15,2),sum((td_bqty-td_sqty)*td_rate *sm_multiplier)) NAmt from trades with(" + StrTradesIndex + "nolock) , series_master with (nolock) ";
                strsql = strsql + "where td_exchange=sm_exchange and td_Segment=sm_segment and td_seriesid=sm_seriesid and td_clientcd='" + cm_cd + "'  ";
                strsql = strsql + "and  td_trxflag <> 'O' and td_exchange='" + strLinkCode.Split('|')[1].ToString() + "'  and td_dt='" + Dt + "'  ";

                if (txtheader == "Stock Future")
                {
                    strsql = strsql + " and sm_prodtype='EF' ";
                }
                if (txtheader == "Index Future")
                {
                    strsql = strsql + " and sm_prodtype='IF' ";
                }
                if (txtheader == "Stock Option")
                {
                    strsql = strsql + " and sm_prodtype='EO' ";
                }
                if (txtheader == "Index Option")
                {
                    strsql = strsql + " and sm_prodtype='IO' ";
                }
                if (txtheader == "Currency Future")
                {
                    strsql = strsql + " and sm_prodtype='CF' ";
                }

                strsql = strsql + " and td_trxflag <> 'O' ";
                strsql = strsql + "group by td_dt, sm_expirydt, sm_productcd, sm_callput, sm_strikeprice,sm_symbol, td_orderid,td_tradeid,td_time";

            }

            else if (linkCodeVal == "Commodities")
            {
                strsql = "select  td_orderid,td_tradeid,td_time,sm_symbol as ScripNm,case left(sm_productcd,1) when 'F' then 'Future' else 'Option' end td_Type, ";
                strsql = strsql + " case left(sm_productcd,1) when 'F' then '' else rtrim(sm_callput)+''+ ";
                strsql = strsql + "ltrim(convert(char,round(sm_strikeprice,0))) end callput, ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) as td_dt,ltrim(rtrim(convert(char,convert(datetime,sm_expirydt),103)))  as expiry, ";
                strsql = strsql + "sum(td_bqty) Bqty, convert(decimal(15,2),sum(td_bqty*td_rate)) BAmt, sum(td_sqty) Sqty, convert(decimal(15,2),sum(td_sqty*td_rate)) SAmt,  ";
                strsql = strsql + "sum(td_bqty-td_sqty) NQty, convert(decimal(15,2),sum((td_bqty-td_sqty)*td_rate)) NAmt from Ctrades with (nolock), Cseries_master with (nolock) ";
                strsql = strsql + "where td_exchange=sm_exchange and td_seriesid=sm_seriesid and td_clientcd='" + cm_cd + "'  ";
                strsql = strsql + "and  td_trxflag <> 'O'   and td_dt='" + Dt + "'  ";
                strsql = strsql + " and td_trxflag <> 'O' and td_exchange='" + strLinkCode.Split('|')[1].ToString() + "' ";

                strsql = strsql + "group by td_dt, sm_expirydt, sm_productcd, sm_callput, sm_strikeprice,sm_symbol, td_orderid,td_tradeid,td_time";

            }

            else if ((linkCodeVal == "EquityExercise") || (linkCodeVal == "EquityAssignment") || (linkCodeVal == "CurrencyExercise") || (linkCodeVal == "CurrencyAssignment"))
            {
                strsql = "select sm_symbol as ScripNm,case  ex_eaflag   when 'A' then 'Assignment' else 'Exercise' end td_Type, ";
                strsql = strsql + " case  ex_eaflag  when 'A' then '' else rtrim(sm_callput)+''+ ltrim(convert(char,round(sm_strikeprice,0))) end callput,  ";
                strsql = strsql + "ltrim(rtrim(convert(char,convert(datetime,ex_dt),103)))  as ex_dt,";
                strsql = strsql + "ltrim(rtrim(convert(char,convert(datetime,sm_expirydt),103)))  as expiry, sum(ex_aqty) Bqty, ";
                strsql = strsql + "convert(decimal(15,2),sum(ex_aqty*ex_diffrate)) BAmt, sum(ex_eqty) Sqty, convert(decimal(15,2),sum(ex_eqty*ex_diffrate)) SAmt,  ";
                strsql = strsql + "convert(decimal(15,2),sum((ex_aqty-ex_eqty)*ex_diffrate)) NAmt from exercise with (nolock) , series_master with (nolock) where ex_exchange=sm_exchange and ex_Segment=sm_Segment and   ";
                strsql = strsql + "ex_seriesid=sm_seriesid and ex_clientcd='" + cm_cd + "'  and  ex_eaflag <> 'O' and ex_exchange='" + strLinkCode.Split('|')[1].ToString() + "' and ex_dt='" + Dt + "'  ";
                if (td_type == "Assignment")
                {
                    strsql = strsql + " and ex_eaflag='A'";
                }
                if (td_type == "Exercise")
                {
                    strsql = strsql + " and ex_eaflag='E'";
                }
                strsql = strsql + " group by sm_symbol,ex_dt,ex_eaflag,sm_callput,sm_strikeprice,sm_expirydt, ";
                strsql = strsql + "ex_aqty,ex_aqty,ex_eqty,ex_diffrate ";

            }
            else if (linkCodeVal == "NBFC")
            {
                string StrNBFCTRXIndex = "";
                if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'NBFC_TRX' and b.name", "idx_Trx_Clientcd", true)) == 1)
                { StrNBFCTRXIndex = "index(idx_NBFC_TRX_clientcd),"; }

                if (_configuration["IsTradeWeb"] == "O")//Live DB
                {
                    strsql = "select  rtrim(ss_name) as ScripNm,td_scripcd, td_stlmnt,ltrim(rtrim(convert(char,convert(datetime,td_dt),103)))as td_dt, ";
                    strsql = strsql + "sum(td_bqty) Bqty, convert(decimal(15,2),sum(td_bqty*td_rate)) BAmt, sum(td_sqty) Sqty, convert(decimal(15,2),sum(td_sqty*td_rate)) SAmt, ";
                    strsql = strsql + "sum(td_bqty-td_sqty) NQty, convert(decimal(15,2),sum((td_bqty-td_sqty)*td_rate)) NAmt ";
                    strsql = strsql + " from NBFC_TRX with (" + StrNBFCTRXIndex + "nolock),securities with(nolock)";
                    strsql = strsql + " where td_clientcd='" + cm_cd + "' ";
                    strsql = strsql + "and td_stlmnt='" + td_stlmnt + "' and td_dt between '" + FromDt + "' and '" + ToDt + "' and td_Scripcd = ss_cd group by ss_name,td_scripcd, ";
                    strsql = strsql + "td_stlmnt,td_dt order by ss_name,td_scripcd,td_stlmnt ";
                }
                else
                {
                    strsql = "select  rtrim(ss_name) as ScripNm,td_scripcd, td_stlmnt,ltrim(rtrim(convert(char,convert(datetime,td_dt),103)))as td_dt, ";
                    strsql = strsql + "sum(td_bqty) Bqty, convert(decimal(15,2),sum(td_bqty*td_rate)) BAmt, sum(td_sqty) Sqty, convert(decimal(15,2),sum(td_sqty*td_rate)) SAmt, ";
                    strsql = strsql + "sum(td_bqty-td_sqty) NQty, convert(decimal(15,2),sum((td_bqty-td_sqty)*td_rate)) NAmt ";
                    strsql = strsql + " from NBFC_TRX with (" + StrNBFCTRXIndex + "nolock),TPSecurities with(nolock)";
                    strsql = strsql + " where td_clientcd='" + cm_cd + "' ";
                    strsql = strsql + "and td_stlmnt='" + td_stlmnt + "' and td_dt between '" + FromDt + "' and '" + ToDt + "' and td_Scripcd = ss_cd group by ss_name,td_scripcd, ";
                    strsql = strsql + "td_stlmnt,td_dt order by ss_name,td_scripcd,td_stlmnt ";

                }

            }
            else if (linkCodeVal == "MTF")
            {
                string StrMTFTRXIndex = "";
                if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'MrgTdgFin_TRX' and b.name", "idx_MrgTdgFin_TRX_Clientcd", true)) == 1)
                { StrMTFTRXIndex = "index(idx_MrgTdgFin_TRX_clientcd),"; }

                if (_configuration["IsTradeWeb"] == "O")//Live DB
                {
                    strsql = "select  rtrim(ss_name) as ScripNm,MTtd_scripcd, MTtd_Stlmnt,ltrim(rtrim(convert(char,convert(datetime,MTtd_dt),103)))as MTtd_dt, ";
                    strsql = strsql + "sum(MTtd_bqty) Bqty, convert(decimal(15,2),sum(MTtd_bqty*MTtd_rate)) BAmt, sum(MTtd_sqty) Sqty, convert(decimal(15,2),sum(MTtd_sqty*MTtd_rate)) SAmt, ";
                    strsql = strsql + "sum(MTtd_bqty-MTtd_sqty) NQty, convert(decimal(15,2),sum((MTtd_bqty-MTtd_sqty)*MTtd_rate)) NAmt ";
                    strsql = strsql + " from MrgTdgFin_TRX with (" + StrMTFTRXIndex + "nolock),securities with(nolock)";
                    strsql = strsql + " where MTtd_clientcd ='" + cm_cd + "' ";
                    strsql = strsql + "and MTtd_Stlmnt='" + td_stlmnt + "' and MTtd_dt between '" + FromDt + "' and '" + ToDt + "' and MTtd_scripcd = ss_cd group by ss_name,MTtd_scripcd, ";
                    strsql = strsql + "MTtd_Stlmnt,MTtd_dt order by ss_name,MTtd_scripcd,MTtd_Stlmnt ";
                }
                else
                {
                    strsql = "select  rtrim(ss_name) as ScripNm,MTtd_scripcd, MTtd_Stlmnt,ltrim(rtrim(convert(char,convert(datetime,MTtd_dt),103)))as MTtd_dt, ";
                    strsql = strsql + "sum(MTtd_bqty) Bqty, convert(decimal(15,2),sum(MTtd_bqty*MTtd_rate)) BAmt, sum(MTtd_sqty) Sqty, convert(decimal(15,2),sum(MTtd_sqty*MTtd_rate)) SAmt, ";
                    strsql = strsql + "sum(MTtd_bqty-MTtd_sqty) NQty, convert(decimal(15,2),sum((MTtd_bqty-MTtd_sqty)*MTtd_rate)) NAmt ";
                    strsql = strsql + " from MrgTdgFin_TRX with (" + StrMTFTRXIndex + "nolock),TPSecurities with(nolock)";
                    strsql = strsql + " where MTtd_clientcd ='" + cm_cd + "' ";
                    strsql = strsql + "and MTtd_Stlmnt='" + td_stlmnt + "' and MTtd_dt between '" + FromDt + "' and '" + ToDt + "' and MTtd_scripcd = ss_cd group by ss_name,MTtd_scripcd, ";
                    strsql = strsql + "MTtd_Stlmnt,MTtd_dt order by ss_name,MTtd_scripcd,MTtd_Stlmnt ";

                }

            }

            return strsql;
        }

        #endregion

        #region Ledger handler methods
        public dynamic Ledger_Summary(string userId, string type, string fromdate, string toDate)
        {

            if (String.IsNullOrEmpty(type))
                type = "";

            if (String.IsNullOrEmpty(fromdate))
                fromdate = "";

            if (String.IsNullOrEmpty(toDate))
                toDate = "";

            string strsql = "";
            if (type == "" || type.ToUpper() == "1")
            {
                strsql = strsql + "select 'Trading' as [Type],ld_clientcd as [ClientCode], sum(case sign(datediff(d,'" + fromdate + "',ld_dt)) when -1 then ld_amount else 0 end) OpeningBalance,sum(case sign(datediff(d,'" + fromdate + "',ld_dt)) when -1 then 0 else case ld_debitflag when 'D' then ld_amount else 0 end end) Debit, sum(case sign(datediff(d,'" + fromdate + "',ld_dt)) when -1 then 0 else case ld_debitflag     when 'D' then 0 else ld_amount end end) Credit, sum(ld_amount) Balance ,";
                strsql = strsql + " Rtrim(CES_Exchange) + '-' + CES_Segment [ExchSeg], ";
                strsql = strsql + " LD_DPID as CESCD";
                strsql = strsql + " from ledger with (nolock) , Companyexchangesegments where LD_DPID = CES_Cd and ld_clientcd='" + userId + "' and ld_dt <= '" + toDate + "' group by ld_dpid , ld_clientcd,CES_Exchange,CES_Segment";
                strsql = strsql + " union all ";
                strsql = strsql + " select 'Trading-Margin' as [Type],ld_clientcd as [ClientCode], sum(case sign(datediff(d,'" + fromdate + "',ld_dt)) when -1 then ld_amount else 0 end) OpeningBalance,sum(case sign(datediff(d,'" + fromdate + "',ld_dt)) when -1 then 0 else case ld_debitflag  ";
                strsql = strsql + " when 'D' then ld_amount else 0 end end) Debit, sum(case sign(datediff(d,'" + fromdate + "',ld_dt)) when -1 then 0 else case ld_debitflag when 'D' then 0 else ld_amount end end) Credit, sum(ld_amount) Balance, ";
                strsql = strsql + " Rtrim(CES_Exchange) + '-' + CES_Segment [ExchSeg], ";
                strsql = strsql + " LD_DPID as CompanyCode";
                strsql = strsql + " from ledger , Companyexchangesegments where LD_DPID = CES_Cd and ld_clientcd=(select distinct cm_brkggroup from client_master with (nolock) where cm_cd='" + userId + "') ";
                strsql = strsql + " and ld_dt <= '" + toDate + "' group by ld_dpid , ld_clientcd,CES_Exchange,CES_Segment ";
            }

            if (type == "" || type.ToUpper() == "3")
            {
                if (Convert.ToInt32(objUtility.fnFireQueryTradeWeb("sysobjects", "count(*)", "name", "MrgTdgFin_TRX", true)) > 0)
                {
                    if (strsql.Length > 0)
                    {
                        strsql = strsql + " union all ";
                    }
                    strsql = strsql + "select 'MTF' as [Type],ld_clientcd [ClientCode], sum(case sign(datediff(d,'" + fromdate + "',ld_dt)) when -1 then ld_amount else 0 end) OpeningBalance,sum(case sign(datediff(d,'" + fromdate + "',ld_dt)) when -1 then 0 else case ld_debitflag when 'D' then ld_amount else 0 end end) Debit, sum(case sign(datediff(d,'" + fromdate + "',ld_dt)) when -1 then 0 else case ld_debitflag     when 'D' then 0 else ld_amount end end) Credit, sum(ld_amount) Balance ,";
                    strsql = strsql + " Rtrim(CES_Exchange) + '- MTF' [ExchSeg] , ";
                    strsql = strsql + " LD_DPID as CESCD";
                    strsql = strsql + " from ledger with (nolock),MrgTdgFin_Clients with (nolock) , Companyexchangesegments where LD_DPID = CES_Cd and ld_clientcd='" + userId + objUtility.GetSysParmSt("MTFP_SUFFIX", "") + "' and ld_dt <= '" + toDate + "'";
                    strsql = strsql + " and ld_clientcd =  Rtrim(MTFC_CMCD) + '" + objUtility.GetSysParmSt("MTFP_SUFFIX", "") + "' ";
                    strsql = strsql + " group by ld_dpid , ld_clientcd , CES_Exchange ";
                }

            }
            //MTF ledger

            // NBFC
            if (type == "" || type.ToUpper() == "4")
            {
                if (Convert.ToInt32(objUtility.fnFireQueryTradeWeb("sysobjects", "count(*)", "name", "nbfc_clients", true)) > 0)
                {
                    if (strsql.Length > 0)
                    {
                        strsql = strsql + " union all ";
                    }
                    strsql += "select 'NBFC' as [Type],ld_clientcd [ClientCode], sum(case sign(datediff(d,'" + fromdate + "',ld_dt)) when -1 then ld_amount else 0 end) OpeningBalance,sum(case sign(datediff(d,'" + fromdate + "',ld_dt)) when -1 then 0 else case ld_debitflag when 'D' then ld_amount else 0 end end) Debit, sum(case sign(datediff(d,'" + fromdate + "',ld_dt)) when -1 then 0 else case ld_debitflag     when 'D' then 0 else ld_amount end end) Credit, sum(ld_amount) Balance ,";
                    strsql += " 'NBFC' [ExchSeg],";
                    strsql += " 'NBFC' as CESCD";
                    strsql = strsql + " from NBFC_Ledger with (nolock) where ld_clientcd='" + userId + "' and ld_dt <= '" + toDate + "' group by ld_dpid , ld_clientcd";

                }
            }
            if (type == "" || type.ToUpper() == "2")
            {
                if (_configuration["Commex"] != null && _configuration["Commex"] != string.Empty)
                {

                    string StrCommexConn = objUtility.GetCommexConnectionNew(_configuration["Commex"]);

                    if (strsql.Length > 0)
                    {
                        strsql = strsql + " union all ";
                    }
                    strsql = strsql + " select 'Commodity' as [Type],ld_clientcd [ClientCode], sum(case sign(datediff(d,'" + fromdate + "',ld_dt)) when -1 then ld_amount else 0 end) OpeningBalance,sum(case sign(datediff(d,'" + fromdate + "',ld_dt)) when -1 then 0 else case ld_debitflag when 'D' then ld_amount else 0 end end) Debit, sum(case sign(datediff(d,'" + fromdate + "',ld_dt)) when -1 then 0 else case ld_debitflag when 'D' then 0 else ld_amount end end) Credit, sum(ld_amount) Balance, ";
                    strsql = strsql + " Rtrim(CES_Exchange) + '-Comm' [ExchSeg] , ";
                    strsql = strsql + " Left(ld_DPID,2)+'X' as CESCD";
                    strsql = strsql + " from " + StrCommexConn + ".ledger," + StrCommexConn + ".Companyexchangesegments";
                    strsql = strsql + " where LD_DPID = CES_Cd and ld_clientcd='" + userId + "' and ld_dt <= '" + toDate + "' ";
                    strsql = strsql + " group by ld_dpid , ld_clientcd , CES_Exchange ";

                    strsql = strsql + " union all ";
                    strsql = strsql + " select 'Commodity-Margin' as [Type],ld_clientcd [ClientCode], sum(case sign(datediff(d,'" + fromdate + "',ld_dt)) when -1 then ld_amount else 0 end) OpeningBalance,sum(case sign(datediff(d,'" + fromdate + "',ld_dt)) when -1 then 0 else case ld_debitflag when 'D' then ld_amount else 0 end end) Debit, sum(case sign(datediff(d,'" + fromdate + "',ld_dt)) when -1 then 0 else case ld_debitflag when 'D' then 0 else ld_amount end end) Credit, sum(ld_amount) Balance, ";
                    strsql = strsql + " Rtrim(CES_Exchange) + '-Comm' [ExchSeg],";
                    strsql = strsql + " Left(ld_DPID,2)+'X' as CESCD";
                    strsql = strsql + " from   " + StrCommexConn + ".ledger," + StrCommexConn + ".Companyexchangesegments";
                    strsql = strsql + " where LD_DPID = CES_Cd and ld_clientcd=(select distinct cm_brkggroup from " + StrCommexConn + ".client_master where cm_cd='" + userId + "') and ld_dt <= '" + toDate + "' ";
                    strsql = strsql + " group by ld_dpid , ld_clientcd , CES_Exchange ";
                }
            }
            strsql = "select * from ( " + strsql;
            strsql = strsql + " ) a ";
            strsql = strsql + " order by [Type],ClientCode,CESCD ";

            try
            {
                var ds = objUtility.OpenDataSet(strsql);
                if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                {
                    var data = ds.Tables[0];
                    return JsonConvert.SerializeObject(data, Formatting.Indented);
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public dynamic Ledger_Detail(string userId, string fromDate, string toDate, string type_cesCd)
        {

            string strTable = " Ledger ";
            string strsql = "";
            string strCommTable = string.Empty;
            string strCommClientMaster = string.Empty;

            if (_configuration["Commex"] != null && _configuration["Commex"] != string.Empty)
            {
                string StrCommexConn = "";
                StrCommexConn = objUtility.GetCommexConnectionNew(_configuration["Commex"]);
                strCommTable = StrCommexConn + ".ledger";
                strCommClientMaster = StrCommexConn + ".Client_master";
            }


            strsql = string.Empty;

            for (int i = 0; i < type_cesCd.Split("|").Length; i++)
            {
                if (strsql.Length > 0 && type_cesCd.Split("|")[i] != "")
                {
                    strsql = strsql + " union all ";
                }

                if (type_cesCd.Split("|")[i].Split(",")[0].ToUpper() == "1")
                {
                    strsql = strsql + " select 'Trading' as [Type],ld_clientcd,convert(char,convert(datetime,'" + fromDate + "'),103) ld_dt,Rtrim(CES_Exchange) + '-' + CES_Segment [ExchSeg],";
                    strsql = strsql + " cast(sum(case sign(datediff(d,'" + fromDate + "',ld_dt)) when -1 then ld_amount else 0 end)as decimal(15,2)) as ld_amount,'Opening Balance' ld_particular, case sign(sum(ld_amount)) when 1 then 'D' else 'C' end as ";
                    strsql = strsql + " ld_debitflag ,'' ld_chequeno, 'O' ld_documenttype,''ld_common ,convert(datetime,'" + fromDate + "') Ldate,ld_dpid";
                    strsql = strsql + " from " + strTable + " with (nolock) , Companyexchangesegments ";
                    strsql = strsql + " where LD_DPID = CES_Cd and ld_clientcd = '" + userId + "'";
                    strsql = strsql + " and ld_dt < '" + fromDate + "' and ld_dpid = '" + type_cesCd.Split("|")[i].Split(",")[1] + "'";
                    strsql = strsql + " group by ld_clientcd,ld_dpid,CES_Exchange,CES_Segment having sum(ld_amount)<> 0 ";
                    strsql = strsql + " union all ";
                    strsql = strsql + " select 'Trading' as [Type],ld_clientcd, ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) as ld_dt,Rtrim(CES_Exchange) + '-' + CES_Segment [ExchSeg],";
                    strsql = strsql + " cast(ld_amount as decimal(15,2)),ld_particular,ld_debitflag,ld_chequeno,ld_documenttype,ld_common,convert(datetime,ld_dt) Ldate,ld_dpid ";
                    strsql = strsql + " from " + strTable + " with (nolock), Companyexchangesegments ";
                    strsql = strsql + " where LD_DPID = CES_Cd and  ld_clientcd = '" + userId + "'";
                    strsql = strsql + "  and ld_dt between '" + fromDate + "' and '" + toDate + "' and ld_dpid = '" + type_cesCd.Split("|")[i].Split(",")[1] + "'";
                }

                if (type_cesCd.Split("|")[i].Split(",")[0].ToUpper() == "2")
                {
                    strsql = strsql + " select 'Trading-Margin' as [Type],ld_clientcd,convert(char,convert(datetime,'" + fromDate + "'),103) ld_dt,Rtrim(CES_Exchange) + '-' + CES_Segment [ExchSeg],";
                    strsql = strsql + " cast(sum(case sign(datediff(d,'" + fromDate + "',ld_dt)) when -1 then ld_amount else 0 end)as decimal(15,2)) as ld_amount,'Opening Balance' ld_particular, case sign(sum(ld_amount)) when 1 then 'D' else 'C' end as ";
                    strsql = strsql + " ld_debitflag ,'' ld_chequeno, 'O' ld_documenttype,''ld_common ,convert(datetime,'" + fromDate + "') Ldate,ld_dpid";
                    strsql = strsql + " from " + strTable + " with (nolock) , Companyexchangesegments ";
                    strsql = strsql + " where LD_DPID = CES_Cd and ld_clientcd in (select distinct cm_brkggroup from client_master where cm_cd='" + userId + "' and cm_brkggroup <> '' ) ";
                    strsql = strsql + " and ld_dt < '" + fromDate + "' and ld_dpid = '" + type_cesCd.Split("|")[i].Split(",")[1] + "'";
                    strsql = strsql + " group by ld_clientcd,ld_dpid,CES_Exchange,CES_Segment having sum(ld_amount)<> 0 ";
                    strsql = strsql + " union all ";
                    strsql = strsql + " select 'Trading-Margin' as [Type],ld_clientcd, ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) as ld_dt,Rtrim(CES_Exchange) + '-' + CES_Segment [ExchSeg],";
                    strsql = strsql + " cast(ld_amount as decimal(15,2)),ld_particular,ld_debitflag,ld_chequeno,ld_documenttype,ld_common,convert(datetime,ld_dt) Ldate,ld_dpid ";
                    strsql = strsql + " from " + strTable + " with (nolock), Companyexchangesegments ";
                    strsql = strsql + " where LD_DPID = CES_Cd and ld_clientcd in (select distinct cm_brkggroup from client_master where cm_cd='" + userId + "' and cm_brkggroup <> '' ) ";
                    strsql = strsql + "  and ld_dt between '" + fromDate + "' and '" + toDate + "' and ld_dpid = '" + type_cesCd.Split("|")[i].Split(",")[1] + "'";
                }


                if (type_cesCd.Split("|")[i].Split(",")[0].ToUpper() == "3")
                {
                    strsql = strsql + " select 'Trading' as [Type],ld_clientcd,convert(char,convert(datetime,'" + fromDate + "'),103) ld_dt,Rtrim(CES_Exchange) + '-Comm' [ExchSeg],";
                    strsql = strsql + " cast(sum(case sign(datediff(d,'" + fromDate + "',ld_dt)) when -1 then ld_amount else 0 end)as decimal(15,2)) as ld_amount,'Opening Balance' ld_particular, case sign(sum(ld_amount)) when 1 then 'D' else 'C' end as ";
                    strsql = strsql + " ld_debitflag ,'' ld_chequeno, 'O' ld_documenttype,''ld_common ,convert(datetime,'" + fromDate + "') Ldate,ld_dpid";
                    strsql = strsql + " from " + strCommTable + " with (nolock) , Companyexchangesegments ";
                    strsql = strsql + " where LD_DPID = CES_Cd and ld_clientcd = '" + userId + "'";
                    strsql = strsql + " and ld_dt < '" + fromDate + "' and ld_dpid = '" + type_cesCd.Split("|")[i].Split(",")[1] + "'";
                    strsql = strsql + " group by ld_clientcd,ld_dpid,CES_Exchange,CES_Segment having sum(ld_amount)<> 0 ";
                    strsql = strsql + " union all ";
                    strsql = strsql + " select 'Trading' as [Type],ld_clientcd, ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) as ld_dt,Rtrim(CES_Exchange) + '-Comm' [ExchSeg],";
                    strsql = strsql + " cast(ld_amount as decimal(15,2)),ld_particular,ld_debitflag,ld_chequeno,ld_documenttype,ld_common,convert(datetime,ld_dt) Ldate,ld_dpid ";
                    strsql = strsql + " from " + strCommTable + " with (nolock), Companyexchangesegments ";
                    strsql = strsql + " where LD_DPID = CES_Cd and  ld_clientcd = '" + userId + "'";
                    strsql = strsql + "  and ld_dt between '" + fromDate + "' and '" + toDate + "' and ld_dpid = '" + type_cesCd.Split("|")[i].Split(",")[1] + "'";
                }

                if (type_cesCd.Split("|")[i].Split(",")[0].ToUpper() == "4")
                {
                    strsql = strsql + " select 'Commodity-Margin' as [Type],ld_clientcd,convert(char,convert(datetime,'" + fromDate + "'),103) ld_dt,Rtrim(CES_Exchange) + '-Comm' [ExchSeg],";
                    strsql = strsql + " cast(sum(case sign(datediff(d,'" + fromDate + "',ld_dt)) when -1 then ld_amount else 0 end)as decimal(15,2)) as ld_amount,'Opening Balance' ld_particular, case sign(sum(ld_amount)) when 1 then 'D' else 'C' end as ";
                    strsql = strsql + " ld_debitflag ,'' ld_chequeno, 'O' ld_documenttype,''ld_common ,convert(datetime,'" + fromDate + "') Ldate,ld_dpid";
                    strsql = strsql + " from " + strCommTable + " with (nolock) , Companyexchangesegments ";
                    strsql = strsql + " where LD_DPID = CES_Cd and ld_clientcd in (select distinct cm_brkggroup from client_master where cm_cd='" + userId + "' and cm_brkggroup <> '' ) ";
                    strsql = strsql + " and ld_dt < '" + fromDate + "' and ld_dpid = '" + type_cesCd.Split("|")[i].Split(",")[1] + "'";
                    strsql = strsql + " group by ld_clientcd,ld_dpid,CES_Exchange,CES_Segment having sum(ld_amount)<> 0 ";
                    strsql = strsql + " union all ";
                    strsql = strsql + " select 'Commodity-Margin' as [Type],ld_clientcd, ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) as ld_dt,Rtrim(CES_Exchange) + '-Comm' [ExchSeg],";
                    strsql = strsql + " cast(ld_amount as decimal(15,2)),ld_particular,ld_debitflag,ld_chequeno,ld_documenttype,ld_common,convert(datetime,ld_dt) Ldate,ld_dpid ";
                    strsql = strsql + " from " + strCommTable + " with (nolock), Companyexchangesegments ";
                    strsql = strsql + " where LD_DPID = CES_Cd and ld_clientcd in (select distinct cm_brkggroup from client_master where cm_cd='" + userId + "' and cm_brkggroup <> '' ) ";
                    strsql = strsql + "  and ld_dt between '" + fromDate + "' and '" + toDate + "' and ld_dpid = '" + type_cesCd.Split("|")[i].Split(",")[1] + "'";
                }

                if (type_cesCd.Split("|")[i].Split(",")[0].ToUpper() == "5")
                {
                    strsql = strsql + " select 'MTF' as [Type],ld_clientcd,convert(char,convert(datetime,'" + fromDate + "'),103) ld_dt,Rtrim(CES_Exchange) + '-MTF' [ExchSeg],";
                    strsql = strsql + " cast(sum(case sign(datediff(d,'" + fromDate + "',ld_dt)) when -1 then ld_amount else 0 end)as decimal(15,2)) as ld_amount,'Opening Balance' ld_particular, case sign(sum(ld_amount)) when 1 then 'D' else 'C' end as ";
                    strsql = strsql + " ld_debitflag ,'' ld_chequeno, 'O' ld_documenttype,''ld_common ,convert(datetime,'" + fromDate + "') Ldate,ld_dpid";
                    strsql = strsql + " from " + strTable + " with (nolock) , Companyexchangesegments ";
                    strsql = strsql + " where LD_DPID = CES_Cd and ld_clientcd in (select distinct MTFC_FillerB from MrgTdgFin_Clients where MTFC_CMCD ='" + userId + "') ";
                    strsql = strsql + " and ld_dt < '" + fromDate + "' and ld_dpid = '" + type_cesCd.Split("|")[i].Split(",")[1] + "'";
                    strsql = strsql + " group by ld_clientcd,ld_dpid,CES_Exchange,CES_Segment having sum(ld_amount)<> 0 ";
                    strsql = strsql + " union all ";
                    strsql = strsql + " select 'MTF' as [Type],ld_clientcd, ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) as ld_dt,Rtrim(CES_Exchange) + '-MTF' [ExchSeg],";
                    strsql = strsql + " cast(ld_amount as decimal(15,2)),ld_particular,ld_debitflag,ld_chequeno,ld_documenttype,ld_common,convert(datetime,ld_dt) Ldate,ld_dpid ";
                    strsql = strsql + " from " + strTable + " with (nolock), Companyexchangesegments ";
                    strsql = strsql + " where LD_DPID = CES_Cd and ld_clientcd in (select distinct cm_brkggroup from client_master where cm_cd='" + userId + "' and cm_brkggroup <> '' ) ";
                    strsql = strsql + "  and ld_dt between '" + fromDate + "' and '" + toDate + "' and ld_dpid = '" + type_cesCd.Split("|")[i].Split(",")[1] + "'";

                }

                if (type_cesCd.Split("|")[i].Split(",")[0].ToUpper() == "6")
                {
                    strsql = strsql + " select 'Trading-Margin' as [Type],ld_clientcd,convert(char,convert(datetime,'" + fromDate + "'),103) ld_dt,'NBFC' [ExchSeg],";
                    strsql = strsql + " cast(sum(case sign(datediff(d,'" + fromDate + "',ld_dt)) when -1 then ld_amount else 0 end)as decimal(15,2)) as ld_amount,'Opening Balance' ld_particular, case sign(sum(ld_amount)) when 1 then 'D' else 'C' end as ";
                    strsql = strsql + " ld_debitflag ,'' ld_chequeno, 'O' ld_documenttype,''ld_common ,convert(datetime,'" + fromDate + "') Ldate,ld_dpid";
                    strsql = strsql + " from NBFC_Ledger with (nolock) ";
                    strsql = strsql + " where ld_clientcd = '" + userId + "'";
                    strsql = strsql + " and ld_dt < '" + fromDate + "'";
                    strsql = strsql + " group by ld_clientcd,ld_dpid having sum(ld_amount)<> 0 ";
                    strsql = strsql + " union all ";
                    strsql = strsql + " select 'MTF' as [Type],ld_clientcd, ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) as ld_dt,'NBFC' [ExchSeg],";
                    strsql = strsql + " cast(ld_amount as decimal(15,2)),ld_particular,ld_debitflag,ld_chequeno,ld_documenttype,ld_common,convert(datetime,ld_dt) Ldate,ld_dpid ";
                    strsql = strsql + " from NBFC_Ledger with (nolock) ";
                    strsql = strsql + " where ld_clientcd = '" + userId + "'";
                    strsql = strsql + " and ld_dt between '" + fromDate + "' and '" + toDate + "' and ld_dpid = '" + type_cesCd.Split("|")[i].Split(",")[1] + "'";
                }

            }
            strsql = " select Type [Type],ld_clientcd [ClientCode],ld_dt [Date],ExchSeg,ld_amount Amount,ld_particular Particular,ld_debitflag Debitflag,ld_chequeno Chequeno,ld_documenttype Documenttype,ld_common Common,Ldate,ld_dpid CESCD from (" + strsql + ") a ";
            strsql = strsql + " order by Type,Ldate";
            try
            {
                var ds = CommonRepository.FillDataset(strsql);
                if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                {
                    //DataTable dt = new DataTable();
                    //EntityList = ConvertData.ConvertDataTable<LedgerDetailsEntity>(ds.Tables[0]);
                    var json = JsonConvert.SerializeObject(ds.Tables[0], Formatting.Indented);
                    return json;
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region OutStanding Handler Method
        // For getting Outstanding data
        public dynamic OutStandingPosition(string userId, string AsOnDt)
        {
            String Strsql;
            String StrCommexConn = objUtility.GetCommexConnection();

            string StrTradesIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
            { StrTradesIndex = "index(idx_trades_clientcd),"; }


            //Query To Fecth Record From TradePlus DataBase
            Strsql = " Select case right(sm_prodtype,1) when 'F' then 'Future' else 'Option' end+ case td_segment when 'X' then '(Commodities)' when 'K' then ' (Currency)'  else ''end ";
            Strsql = Strsql + " as [Type],ltrim(rtrim(sm_desc)) LongName,sm_sname ShortName,";
            Strsql = Strsql + " sum(td_bqty) Buy, sum(td_sqty) Sell,sum(td_bqty-td_sqty) as Net,td_companycode+td_exchange+td_Segment CESCD, ";
            Strsql = Strsql + " convert(decimal(15,2), case sum(td_bqty -td_sqty) when 0 then 0 else abs(sum((td_bqty -td_sqty)*td_rate)/sum(td_bqty-td_sqty)) end) AvgRate,";
            Strsql = Strsql + " convert(decimal(15,2), (isnull((select ms_lastprice from Market_summary with (nolock) where ms_exchange = td_exchange and ms_Segment = td_Segment and ms_seriesid = td_seriesid and ms_dt = (select max(ms_dt) from Market_summary with (nolock) where ms_exchange = td_exchange and ms_Segment = td_Segment and ms_seriesid = td_seriesid and  ms_dt <= '" + AsOnDt + "')),0)";
            Strsql = Strsql + " + case  when right(sm_prodtype,1) <> 'F' then  sm_strikeprice  else 0 end) ) Closeprice,";
            Strsql = Strsql + " convert(decimal(15,2), (isnull((select ms_lastprice from Market_summary with (nolock) where ms_exchange = td_exchange and ms_Segment = td_Segment and ms_seriesid = td_seriesid ";
            Strsql = Strsql + " and ms_dt = (select max(ms_dt) from Market_summary with (nolock) where ms_exchange = td_exchange and ms_Segment = td_Segment and ms_seriesid = td_seriesid and  ms_dt <= '" + AsOnDt + "')),0) ";
            Strsql = Strsql + "  + case when right(sm_prodtype,1) <> 'F' then sm_strikeprice  else 0 end)	";
            Strsql = Strsql + " *sum(td_bqty-td_sqty) * sm_multiplier ) Closing,";
            Strsql = Strsql + " case sm_prodtype when 'IF' then 1 when 'EF' then 2 when 'IO' then 3 when 'EO' then 4 else 5 end SortOrder,";
            Strsql = Strsql + " Rtrim(CES_Exchange) +'-'+CES_Segment ExchSeg,td_seriesid SeriesID";
            Strsql = Strsql + " from Trades with (" + StrTradesIndex + "nolock), Series_master with (nolock) , CompanyExchangeSegments ";
            Strsql = Strsql + " where td_companyCode+td_exchange+td_Segment = CES_Cd and td_seriesid=sm_seriesid and td_exchange = sm_exchange and td_Segment = sm_Segment and td_clientcd='" + userId + "'";
            Strsql = Strsql + " and td_dt <= '" + AsOnDt + "' and sm_expirydt >= '" + AsOnDt + "'";
            Strsql = Strsql + " Group by td_companyCode,td_clientcd ,td_seriesid,td_exchange,td_Segment,sm_sname,sm_prodtype,sm_desc,sm_multiplier,sm_strikeprice,CES_Exchange,CES_Segment";

            string StrComTradesIndex = "";
            //Query To Fecth Record From Commex DataBase
            if (objUtility.GetWebParameter("Commex") != null && objUtility.GetWebParameter("Commex") != string.Empty)
            {
                if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb(StrCommexConn + ".sysobjects a, " + StrCommexConn + ".sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
                { StrComTradesIndex = "index(idx_trades_clientcd),"; }

                Strsql = Strsql + " Union all";
                Strsql = Strsql + " Select case right(sm_prodtype,1)when 'F' then 'Future (Commodities)' else 'Option (Commodities)' ";
                Strsql = Strsql + " end as [Type],ltrim(rtrim(sm_desc)) LongName,sm_sname ShortName,sum(td_bqty) Buy,sum(td_sqty) Sell,sum(td_bqty-td_sqty) as Net,td_companycode+td_Exchange+'X' CESCD,";
                Strsql = Strsql + " convert(decimal(15,2), case sum(td_bqty -td_sqty) when 0 then 0 else abs(sum((td_bqty -td_sqty)*td_rate)/sum(td_bqty-td_sqty)) end) AvgRate,";
                Strsql = Strsql + " convert(decimal(15,2), (isnull((select ms_lastprice from " + StrCommexConn + ".Market_summary with (nolock) where ms_exchange = td_exchange and ms_seriesid = td_seriesid and ms_dt = (select max(ms_dt) from " + StrCommexConn + ".Market_summary with (nolock) where ms_exchange = td_exchange and ms_seriesid = td_seriesid and  ms_dt <= '" + AsOnDt + "')),0) ";
                Strsql = Strsql + "  + case  when right(sm_prodtype,1) <> 'F' then  sm_strikeprice  else 0 end) ) Closeprice,";
                Strsql = Strsql + " convert(decimal(15,2), (isnull((select ms_lastprice from " + StrCommexConn + ".Market_summary with (nolock) where ms_exchange = td_exchange and ms_seriesid = td_seriesid ";
                Strsql = Strsql + " and ms_dt = (select max(ms_dt) from " + StrCommexConn + ".Market_summary with (nolock) where ms_exchange = td_exchange and ms_seriesid = td_seriesid and  ms_dt <= '" + AsOnDt + "')),0) ";
                Strsql = Strsql + "  + case  when right(sm_prodtype,1) <> 'F' then sm_strikeprice  else 0 end)	";
                Strsql = Strsql + "  *sum(td_bqty-td_sqty) * sm_multiplier ) Closing,";
                Strsql = Strsql + " case sm_prodtype when 'CF'then 11 else 12 end SortOrder,";
                Strsql = Strsql + " Rtrim(CES_Exchange) +'-Comm' ExchSeg,td_seriesid SeriesID";
                Strsql = Strsql + " from " + StrCommexConn + ".Trades with(" + StrComTradesIndex + "nolock), " + StrCommexConn + ".Series_master with (nolock) , " + StrCommexConn + ".CompanyExchangeSegments with (nolock)";
                Strsql = Strsql + " where td_companyCode+td_exchange+'F' = CES_Cd and td_seriesid=sm_seriesid and td_exchange = sm_exchange and td_clientcd='" + userId + "'";
                Strsql = Strsql + " and td_dt <= '" + AsOnDt + "' and sm_expirydt > '" + AsOnDt + "'";
                Strsql = Strsql + " Group by td_companyCode,td_clientcd ,td_seriesid,td_exchange,sm_sname,sm_prodtype,sm_desc,sm_multiplier,sm_strikeprice,CES_Exchange";
            }

            Strsql = " select * from (" + Strsql + ") a order by SortOrder,LongName";


            try
            {
                var ds = CommonRepository.FillDataset(Strsql);
                if (ds != null)
                {
                    if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                    {
                        var json = JsonConvert.SerializeObject(ds.Tables[0], Formatting.Indented);
                        return json;
                    }
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //For getting Outstanding details data
        public dynamic OutStandingPosition_Detail(string userId, string seriesid, string CESCd, string AsOnDt)
        {
            var qury = "";
            try
            {
                var ds = CommonRepository.FillDataset(qury);
                if (ds != null)
                {
                    if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                    {
                        var json = JsonConvert.SerializeObject(ds.Tables[0], Formatting.Indented);
                        return json;
                    }
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region ProfitLoss Handler Method
        public dynamic ProfitLoss_Cash_Summary(string userId, string fromDate, string toDate)
        {
            try
            {
                SqlConnection con;
                using (var db = new DataContext())
                {
                    con = new SqlConnection((db.Database.GetDbConnection()).ConnectionString);
                    con.Open();
                    ProfitLoss_Cash_Process(userId, fromDate, toDate, con);

                    strsql = " select '1' as Ord, case td_flag When 'Y' Then 1 else 2 end Flag,ss_Lname ScripName,sum(td_bqty) Buy, convert(decimal(15,2),sum(td_bqty*td_rate)) BuyAmount ,  case when SUM(td_bqty) <> 0 then convert(decimal(15,2),sum(td_bqty*td_rate)/sum(td_bqty)) else 0 end BuyRate,  sum(td_sqty) Sell , convert(decimal(15,2),sum(td_sqty*td_rate)) SellAmount ,  case when SUM(td_sqty) <> 0 then convert(decimal(15,2),sum(td_sqty*td_rate)/sum(td_sqty)) else 0 end SellRate, ";
                    strsql += " convert(decimal(15,2), sum( case When td_flag <> 'X' Then ((td_sqty-td_bqty) *td_rate) else 0 end)) Realised,  ";
                    strsql += " convert(decimal(15,2), sum( case When td_flag = 'X' Then (td_bqty-td_sqty)* ((Case Left(td_stlmnt,1) when 'N' then ss_NSERate else ss_BSERate end) - td_rate) else 0 end)) UnRealised,  ";
                    strsql += " convert(decimal(15,2), sum( case When td_flag = 'X' Then ((td_bqty-td_sqty)) else 0 end)) Net,  convert(decimal(15,2), sum( case When td_flag = 'X' Then ((td_sqty-td_bqty) * (Case Left(td_stlmnt,1) when 'N' then ss_NSERate else ss_BSERate end) ) else 0 end))  NetAmount,  ";
                    strsql += " case When sum( case when td_flag = 'X'  Then (td_bqty-td_sqty) else 0 end) <> 0 then abs(convert(decimal(15,2),sum(Case When td_flag = 'X' Then (td_sqty-td_bqty)*(Case Left(td_stlmnt,1) when 'N' then ss_NSERate else ss_BSERate end) else 0 end )/sum(td_bqty-td_sqty))) else 0 end NetRate, ";
                    strsql += " isnull(td_desc,'') TrdDlv,td_scripcd Scrip ";
                    strsql += " from ##VX,securities with(nolock) where td_scripcd=ss_cd ";
                    strsql += " Group by ss_Lname,case td_flag When 'Y' Then 1 else 2 end ,td_desc,td_scripcd ";
                    strsql += " Union all ";
                    strsql += " select '2' Ord,'','Charges' ic_desc,0,0,0,0,0,0,convert(decimal(15,2),sum(ic_amount)) * (-1),0,0,0,0 amt ,'','' from ##invcharges";
                    strsql += " where ic_amount > 0";
                    strsql += " order by Ord,ss_Lname,flag desc";
                    var ds = objUtility.OpenDataSetTmp(strsql, con);
                    if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                    {
                        var data = ds.Tables[0];
                        return JsonConvert.SerializeObject(data, Formatting.Indented);
                    }
                    return new List<string>();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public dynamic ProfitLoss_Cash_Detail(string userId, string fromDate, string toDate, string scripcd)
        {
            try
            {
                SqlConnection con;
                using (var db = new DataContext())
                {
                    con = new SqlConnection((db.Database.GetDbConnection()).ConnectionString);
                    con.Open();
                    ProfitLoss_Cash_Process(userId, fromDate, toDate, con);

                    strsql = "select '1' as Ord, ss_Lname td_scripnm ,td_stlmnt , sum(td_bqty) BQty, convert(decimal(15,2),sum(td_bqty*td_rate)) BAmount ,  case when SUM(td_bqty) <> 0 then convert(decimal(15,2),sum(td_bqty*td_rate)/sum(td_bqty)) else 0 end BRate,  sum(td_sqty) SQty , convert(decimal(15,2),sum(td_sqty*td_rate)) SAmount ,  case when SUM(td_sqty) <> 0 then convert(decimal(15,2),sum(td_sqty*td_rate)/sum(td_sqty)) else 0 end SRate,isnull(td_desc,'') td_desc";
                    strsql += ", Convert(char,Convert(datetime,td_dt ),103) td_dt , td_dt as dt ";
                    strsql += " from ##VX,securities with(nolock) where td_scripcd=ss_cd";
                    strsql += " Group by ss_Lname,td_stlmnt,td_desc,td_dt ";
                    var ds = objUtility.OpenDataSetTmp(strsql, con);
                    if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                    {
                        var data = ds.Tables[0];
                        return JsonConvert.SerializeObject(data, Formatting.Indented);
                    }
                    return new List<string>();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public dynamic ProfitLoss_FO_Summary(string userId, string exchange, string segment, string fromDate, string toDate)
        {
            try
            {
                SqlConnection con;
                using (var db = new DataContext())
                {
                    con = new SqlConnection((db.Database.GetDbConnection()).ConnectionString);
                    con.Open();
                    ProfitLoss_FO_Process(userId, exchange, segment, fromDate, toDate, con);

                    strsql = " select Convert(char(12), sm_seriesid) SeriesID,sm_symbol Symbol,sm_desc SeriesName , ";
                    strsql += " sum(case fi_balfield when 'O' then fi_bqty - fi_sqty else 0 end) BF,";
                    strsql += " sum(case fi_type when 'N' then case fi_controlflag when 2 then fi_bqty else 0 end else 0 end) Buy,";
                    strsql += " sum(case fi_type when 'N' then case fi_controlflag when 2 then fi_sqty else 0 end else 0 end) Sell,";
                    strsql += " sum(case fi_type when 'N' then case fi_controlflag when 3 then abs(fi_bqty) else 0 end else 0 end) Exer,";
                    strsql += " sum(case fi_type when 'N' then case fi_controlflag when 4 then abs(fi_sqty) else 0 end else 0 end) Assign,";
                    strsql += " sum(case when sm_expirydt <= '" + toDate + "' then 0 else case fi_balfield when 'N' then  0 else (fi_bqty)*case fi_controlflag when 3 then -1 else 1 end  - (fi_sqty)*case fi_controlflag when 4 then -1 else 1 end end end) Out,";
                    strsql += " sum(case fi_type when 'R' then  fi_sqty - fi_bqty else 0 end) Closing,";
                    strsql += " convert(decimal(15,2), sum(fi_mtm)) MTM,";
                    strsql += " convert(decimal(15,2),sum(case fi_type when 'N' then case fi_controlflag when 2 then fi_bvalue else 0 end else 0 end)) BuyValue,";
                    strsql += " convert(decimal(15,2),sum(case fi_type when 'N' then case fi_controlflag when 2 then fi_svalue else 0 end else 0 end)) SellValue,";
                    strsql += " convert(decimal(15,2),case when (sum(case fi_type when 'N' then case fi_controlflag when 2 then fi_bqty else 0 end else 0 end) >0) then ((sum(fi_bvalue)/sum(fi_bqty))/ fi_multiplier) else 0 end) BuyRate, ";
                    strsql += " convert(decimal(15,2),case when (sum(case fi_type when 'N' then case fi_controlflag when 2 then fi_sqty else 0 end else 0 end) >0) then ((sum(fi_svalue) /sum(fi_sqty ))/ fi_multiplier) else 0 end) SellRate";
                    strsql += " ,convert(decimal(15,2),case when (sum(case when sm_expirydt <= '" + toDate + "' then 0 else case fi_balfield when 'N' then  0 else (fi_bqty)*case fi_controlflag when 3 then -1 else 1 end  - (fi_sqty)*case fi_controlflag when 4 then -1 else 1 end end end) >0 and fi_multiplier > 0) then ((sum(fi_netvalue) /sum(case when sm_expirydt <= '" + toDate + "' then 0 else case fi_balfield when 'N' then  0 else (fi_bqty)*case fi_controlflag when 3 then -1 else 1 end  - (fi_sqty)*case fi_controlflag when 4 then -1 else 1 end end end)) / fi_multiplier) else 0 end) OutRate, ";
                    strsql += " case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'TF' then 2 when 'RF' then 2 when 'EF' then 2 when 'IO' then 3 else 4 end ListOrder ";
                    strsql += " from ##tmpfinvestorrep   a,Client_master with (nolock) ,Series_master with (nolock)";
                    strsql += " where fi_clientcd = cm_cd and fi_exchange = sm_exchange ";
                    strsql += " and fi_segment = sm_segment ";
                    strsql += " and fi_seriesid = sm_seriesid ";
                    strsql += " group by fi_clientcd,fi_seriesid,cm_name,sm_seriesid,sm_prodtype,sm_symbol,sm_sname,sm_desc,fi_multiplier";
                    strsql += "  Union all ";
                    strsql += "select '','',bc_desc,0,0,0,0,0,0,0,convert(decimal(15,2),sum(bc_amount*(-1)))  chrg,0,0,0,0,0,'5' from ##tmpFinvcharges";
                    strsql += " group by bc_desc ";
                    strsql += " Having sum(bc_amount*(-1)) <> 0 ";
                    strsql += " order by listorder,sm_symbol";

                    var ds = objUtility.OpenDataSetTmp(strsql, con);
                    if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                    {
                        var data = ds.Tables[0];
                        return JsonConvert.SerializeObject(data, Formatting.Indented);
                    }
                    return new List<string>();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public dynamic ProfitLoss_Commodity_Summary(string userId, string exchange, string fromDate, string toDate)
        {
            try
            {

                string StrCommexConn = "";
                StrCommexConn = objUtility.GetCommexConnectionNew(_configuration["Commex"]);

                SqlConnection con;
                using (var db = new DataContext())
                {
                    con = new SqlConnection((db.Database.GetDbConnection()).ConnectionString);
                    con.Open();
                    ProfitLoss_Commodity_Process(userId, exchange, fromDate, toDate, con);

                    strsql = " select Convert(char(12), sm_seriesid) SeriesID,sm_symbol Symbol,sm_desc SeriesName , ";
                    strsql += " sum(case fi_balfield when 'O' then fi_bqty - fi_sqty else 0 end) BF,";
                    strsql += " sum(case fi_type when 'N' then case fi_controlflag when 2 then fi_bqty else 0 end else 0 end) Buy,";
                    strsql += " sum(case fi_type when 'N' then case fi_controlflag when 2 then fi_sqty else 0 end else 0 end) Sell,";
                    strsql += " sum(case fi_type when 'N' then case fi_controlflag when 3 then abs(fi_bqty) else 0 end else 0 end) Exer,";
                    strsql += " sum(case fi_type when 'N' then case fi_controlflag when 4 then abs(fi_sqty) else 0 end else 0 end) Assign,";
                    strsql += " sum(case when sm_expirydt <= '" + toDate + "' then 0 else case fi_balfield when 'N' then  0 else (fi_bqty)*case fi_controlflag when 3 then -1 else 1 end  - (fi_sqty)*case fi_controlflag when 4 then -1 else 1 end end end) Out,";
                    strsql += " sum(case fi_type when 'R' then  fi_sqty - fi_bqty else 0 end) Closing,";
                    strsql += " convert(decimal(15,2), sum(fi_mtm)) MTM,";
                    strsql += " convert(decimal(15,2),sum(case fi_type when 'N' then case fi_controlflag when 2 then fi_bvalue else 0 end else 0 end)) BuyValue,";
                    strsql += " convert(decimal(15,2),sum(case fi_type when 'N' then case fi_controlflag when 2 then fi_svalue else 0 end else 0 end)) SellValue,";
                    strsql += " convert(decimal(15,2),case when (sum(case fi_type when 'N' then case fi_controlflag when 2 then fi_bqty else 0 end else 0 end) >0) then ((sum(fi_bvalue)/sum(fi_bqty))/ fi_multiplier) else 0 end) BuyRate, ";
                    strsql += " convert(decimal(15,2),case when (sum(case fi_type when 'N' then case fi_controlflag when 2 then fi_sqty else 0 end else 0 end) >0) then ((sum(fi_svalue) /sum(fi_sqty ))/ fi_multiplier) else 0 end) SellRate";
                    strsql += " ,convert(decimal(15,2),case when (sum(case when sm_expirydt <= '" + toDate + "' then 0 else case fi_balfield when 'N' then  0 else (fi_bqty)*case fi_controlflag when 3 then -1 else 1 end  - (fi_sqty)*case fi_controlflag when 4 then -1 else 1 end end end) >0 and fi_multiplier > 0) then ((sum(fi_netvalue) /sum(case when sm_expirydt <= '" + toDate + "' then 0 else case fi_balfield when 'N' then  0 else (fi_bqty)*case fi_controlflag when 3 then -1 else 1 end  - (fi_sqty)*case fi_controlflag when 4 then -1 else 1 end end end)) / fi_multiplier) else 0 end) OutRate, ";
                    strsql += " case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'TF' then 2 when 'RF' then 2 when 'EF' then 2 when 'IO' then 3 else 4 end ListOrder ";
                    strsql += " from ##tmpfinvestorrep a," + StrCommexConn + ".Client_master with (nolock) ," + StrCommexConn + ".Series_master with (nolock)";
                    strsql += " where fi_clientcd = cm_cd and fi_exchange = sm_exchange ";
                    strsql += " and fi_seriesid = sm_seriesid ";
                    strsql += " group by fi_clientcd,fi_seriesid,cm_name,sm_seriesid,sm_prodtype,sm_symbol,sm_sname,sm_desc,fi_multiplier";
                    strsql += "  Union all ";
                    strsql += "select '','',bc_desc,0,0,0,0,0,0,0,convert(decimal(15,2),sum(bc_amount*(-1)))  chrg,0,0,0,0,0,'5' from ##tmpFinvcharges";
                    strsql += " group by bc_desc ";
                    strsql += " Having sum(bc_amount*(-1)) <> 0 ";
                    strsql += " order by listorder,sm_symbol";

                    var ds = objUtility.OpenDataSetTmp(strsql, con);
                    if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                    {
                        var data = ds.Tables[0];
                        return JsonConvert.SerializeObject(data, Formatting.Indented);
                    }
                    return new List<string>();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region Holding Handler Method
        public dynamic Holding_Broker_Current(string userid)
        {

            SqlConnection con;
            using (var db = new DataContext())
            {
                con = new SqlConnection((db.Database.GetDbConnection()).ConnectionString);
                con.Open();
            }
            // if (con.Open()) con.Open();
            string strsql = string.Empty;
            SqlCommand ObjCommand = new SqlCommand();
            SqlDataAdapter ObjAdapter = new SqlDataAdapter();
            DataSet ObjDataSet = new DataSet();
            string strdate = DateTime.Now.ToString("yyyyMMdd");

            strsql = "if OBJECT_ID('tempdb..##TmpVarMargin') is not null Drop Table ##TmpVarMargin";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "Create Table ##TmpVarMargin (Tmp_scripcd VarChar(6), Tmp_haricut money )";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "Create index #idx_TmpVarMargin_scripcd on ##TmpVarMargin (Tmp_scripcd)";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "insert into ##TmpVarMargin select ss_cd , 0 From securities";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = " update ##TmpVarMargin set Tmp_haricut = case When lv_BseDt > lv_NseDt Then lv_BseVarMargin else lv_NseVarMargin end from LVarMargin with (nolock) where lv_Scripcd = Tmp_scripcd ";
            objUtility.ExecuteSQLTmp(strsql, con);


            strsql = "Select em_name from entity_master with (nolock) where len(em_cd)=1 ";
            DataSet ObjDataSet1 = new DataSet();
            ObjCommand.CommandText = strsql;
            ObjCommand.Connection = con;
            ObjAdapter.SelectCommand = ObjCommand;
            ObjAdapter.Fill(ObjDataSet1);
            if (ObjDataSet1.Tables[0].Rows.Count > 0)
            {
                if (ObjDataSet1.Tables[0].Rows[0][0].ToString().Contains("BP"))
                {
                    strsql = "  update ##TmpVarMargin set Tmp_haricut = 25.00 where  Tmp_haricut < 25";
                    objUtility.ExecuteSQLTmp(strsql, con);
                }
            }

            strsql = string.Empty;
            strsql = "select dm_clientcd,dm_scripcd,sum(dm_qty * (-1)) as qty,dm_isin,";
            strsql = strsql + " ss_name,ss_bserate,convert(decimal(15,2),(ss_bserate*sum(dm_qty * (-1)))) as valuation ,'Beneficiary Holding' as bh_type,cast((Tmp_haricut) as decimal(15,2)) HairCut,convert(decimal(15,2), 0) as AfterHaricut ,(case dm_tmkttype  when 'X' then 'Marked For Sale against debit' else '' end)  as Remark ";
            strsql = strsql + " ,'' unpledge,'' lnkUnPledge ";
            strsql = strsql + " from demat with (nolock), ourdps with (nolock) ,settlements with (nolock) ,client_master with (nolock),securities with (nolock),##TmpVarMargin";
            strsql = strsql + " where cm_cd = dm_clientcd and dm_stlmnt = se_stlmnt and dm_scripcd = ss_cd";
            strsql = strsql + " and dm_ourdp = od_cd";
            strsql = strsql + " and Tmp_scripcd = dm_scripcd and od_acttype in ('B','M') and dm_type = 'BC' and dm_locked = 'N' and dm_transfered = 'N'";
            strsql = strsql + " and dm_clientcd = '" + userid + "' and od_actType <> 'M'";
            strsql = strsql + " group by dm_clientcd,dm_scripcd,dm_isin, ss_name,Tmp_haricut,ss_bserate,dm_tmkttype having abs(sum(dm_qty)) > 0 ";
            strsql = strsql + " union all ";

            strsql = strsql + " select dm_clientcd,dm_scripcd,sum(dm_qty * (-1)) as qty,dm_isin,ss_name,ss_bserate, convert(decimal(15,2),(ss_bserate*sum(dm_qty * (-1))))";
            strsql = strsql + " as valuation ,'COLLAT Holding' as bh_type,cast((Tmp_haricut) as decimal(15,2)) HairCut,convert(decimal(15,2), 0) as AfterHaricut,(case dm_tmkttype  when 'X' then 'Marked For Sale against debit' else '' end)  as Remark ";
            strsql = strsql + " ,'' unpledge,'' lnkUnPledge from demat with (nolock),ourdps with (nolock) ,settlements with (nolock) ,client_master with (nolock) ,securities with (nolock) ,##TmpVarMargin where cm_cd = dm_clientcd and dm_stlmnt = se_stlmnt and dm_scripcd = ss_cd and dm_ourdp = od_cd and Tmp_scripcd = dm_scripcd";
            strsql = strsql + " and od_acttype in ('B','M') and dm_type = 'BC' and dm_locked = 'N' and dm_transfered = 'N'";
            strsql = strsql + " and dm_clientcd = '" + userid + "' and od_actType <> 'M'";
            strsql = strsql + " group by dm_clientcd,dm_scripcd,dm_isin, ss_name,Tmp_haricut,ss_bserate,dm_tmkttype";
            strsql = strsql + " having abs(sum(dm_qty)) > 0 ";
            strsql = strsql + " Union all  ";

            strsql = strsql + " select dm_clientcd,dm_scripcd,sum(dm_qty * (-1)) as qty,dm_isin,ss_name,ss_bserate, convert(decimal(15,2),(ss_bserate*sum(dm_qty * (-1)))) as valuation ,'Expected Holding' as bh_type,cast((Tmp_haricut) as decimal(15,2)) HairCut,convert(decimal(15,2), 0) as AfterHaricut ,(case dm_tmkttype  when 'X' then 'Marked For Sale against debit' else '' end)  as Remark ";
            strsql = strsql + " ,'' unpledge,'' lnkUnPledge from demat with (nolock), ourdps with (nolock) ,settlements with (nolock) ,client_master with (nolock) ,securities  with (nolock) ,##TmpVarMargin";
            strsql = strsql + " where cm_cd = dm_clientcd and dm_stlmnt = se_stlmnt and dm_scripcd = ss_cd and dm_ourdp = od_cd and od_acttype = 'P'and dm_type = 'BC' and dm_locked = 'N' and dm_transfered <> 'S' and se_shpayoutdt > '" + strdate + "' ";
            strsql = strsql + " and od_actType <> 'M'";
            strsql = strsql + " and dm_clientcd = '" + userid + "' and Tmp_scripcd = dm_scripcd";
            strsql = strsql + " group by dm_clientcd,dm_scripcd,dm_isin, ss_name,se_stlmnt,Tmp_haricut,ss_bserate,dm_tmkttype";
            strsql = strsql + " having abs(sum(dm_qty)) > 0 ";
            strsql = strsql + " Union all  ";

            strsql = strsql + " select dm_clientcd,dm_scripcd,sum(dm_qty * (-1)) as qty,dm_isin,ss_name,ss_bserate, convert(decimal(15,2),(ss_bserate*sum(dm_qty * (-1)))) as valuation ,'Pool Holding' as bh_type,cast((Tmp_haricut) as decimal(15,2)) HairCut,convert(decimal(15,2), 0) as AfterHaricut ,(case dm_tmkttype  when 'X' then 'Marked For Sale against debit' else '' end)  as Remark ";
            strsql = strsql + " ,'' unpledge,'' lnkUnPledge from demat with (nolock), ourdps with (nolock) ,settlements with (nolock) ,client_master with (nolock) ,securities  with (nolock) ,##TmpVarMargin";
            strsql = strsql + " where cm_cd = dm_clientcd and dm_stlmnt = se_stlmnt and dm_scripcd = ss_cd and dm_ourdp = od_cd and od_acttype = 'P'and dm_type = 'BC' and dm_locked = 'N' and dm_transfered ='N' and se_shpayoutdt <= '" + strdate + "'";
            strsql = strsql + " and od_actType <> 'M'";
            strsql = strsql + " and dm_clientcd = '" + userid + "' and Tmp_scripcd = dm_scripcd";
            strsql = strsql + " group by dm_clientcd,dm_scripcd,dm_isin, ss_name,se_stlmnt,Tmp_haricut,ss_bserate,dm_tmkttype";
            strsql = strsql + " having abs(sum(dm_qty)) > 0 ";
            strsql = strsql + " Union all  ";

            strsql = strsql + " select dm_clientcd,dm_scripcd,sum(dm_qty * (-1)) as qty,dm_isin,ss_name,ss_bserate, convert(decimal(15,2),(ss_bserate*sum(dm_qty * (-1)))) as valuation ,'Undelivered Holding' as bh_type,cast((Tmp_haricut) as decimal(15,2)) HairCut,convert(decimal(15,2), 0) as AfterHaricut,(case dm_tmkttype  when 'X' then 'Marked For Sale against debit' else '' end)  as Remark";
            strsql = strsql + " ,'' unpledge,'' lnkUnPledge from demat with (nolock), ourdps with (nolock),settlements with (nolock), client_master with (nolock),securities  with (nolock),##TmpVarMargin";
            strsql = strsql + " where cm_cd = dm_clientcd and dm_stlmnt = se_stlmnt and dm_scripcd = ss_cd and dm_ourdp = od_cd and od_acttype = 'P' and dm_type = 'CB' and dm_locked = 'N' and dm_transfered <> 'S'";
            strsql = strsql + " and dm_clientcd = '" + userid + "' and od_actType <> 'M'";
            strsql = strsql + " and Tmp_scripcd = dm_scripcd group by dm_clientcd,dm_scripcd,dm_isin, ss_name,Tmp_haricut,ss_bserate,dm_tmkttype having abs(sum(dm_qty)) > 0 ";

            strsql = strsql + " union all ";
            strsql = strsql + " select dm_clientcd,dm_scripcd,sum(dm_qty * (-1)) as qty,dm_isin,";
            strsql = strsql + " ss_name,ss_bserate,convert(decimal(15,2),(ss_bserate*sum(dm_qty * (-1)))) as valuation ,(case od_acttype when 'T' then 'Margin Trading' when 'C' then 'MTF Collateral' else '' end ) as bh_type,cast((Tmp_haricut) as decimal(15,2)) HairCut,convert(decimal(15,2), 0) as AfterHaricut ,(case dm_tmkttype  when 'X' then 'Marked For Sale against debit' else '' end)  as Remark ";
            strsql = strsql + " ,'' unpledge,'' lnkUnPledge from demat with (nolock), ourdps with (nolock) ,settlements with (nolock) ,client_master with (nolock),securities with (nolock),##TmpVarMargin";
            strsql = strsql + " where cm_cd = dm_clientcd and dm_stlmnt = se_stlmnt and dm_scripcd = ss_cd";
            strsql = strsql + " and dm_ourdp = od_cd";
            strsql = strsql + " and Tmp_scripcd = dm_scripcd and od_acttype in ('T','C') and dm_type = 'BC' and dm_locked = 'N' and dm_transfered = 'N'";
            strsql = strsql + " and dm_clientcd = '" + userid + "' ";
            strsql = strsql + " group by dm_clientcd,dm_scripcd,dm_isin, ss_name,Tmp_haricut,ss_bserate,dm_tmkttype,od_acttype ";
            strsql = strsql + " having abs(sum(dm_qty)) > 0 ";

            DataSet DsMPTrx = new DataSet();
            DsMPTrx = objUtility.OpenDataSet("select * from SysObjects where name= 'MrgPledge_TRX'");
            if (DsMPTrx.Tables[0].Rows.Count > 0)
            {
                string strOurdp = string.Empty;
                DataTable dt = objUtility.OpenDataTable("select distinct MPT_OurDP from MrgPledge_TRX Where MPT_TRXFlag ='P'");
                if (dt.Rows.Count > 0)
                {
                    for (int i = 0; i <= dt.Rows.Count - 1; i++)
                    {
                        strOurdp += "'" + dt.Rows[i]["MPT_OurDP"].ToString().Trim() + "',";
                    }
                    strOurdp = strOurdp.Remove(strOurdp.Length - 1);
                }
                else
                {
                    strOurdp = "''";
                }
                strsql = strsql + " union all ";
                strsql = strsql + " select MPT_clientcd dm_clientcd,MPT_scripcd dm_scripcd,sum(case When MPT_DRCR ='C' Then MPT_Qty else -MPT_Qty  end) as qty,im_isin,ss_name,ss_bserate, ";
                strsql = strsql + " (ss_bserate*sum(case When MPT_DRCR ='C' Then MPT_Qty else -MPT_Qty  end)) as valuation ,'Margin Pledge' bh_type,cast((Tmp_haricut) as decimal(15,2)) HairCut, ";
                strsql = strsql + " convert(decimal(15,2), 0) as AfterHaricut ,''  ";
                strsql = strsql + " ,'Un-Pledge ' unpledge,";
                strsql = strsql + " MPT_clientcd +'/'+ MPT_scripcd +'/' + ss_name +'/'+ convert(varchar,sum(case When MPT_DRCR ='C' Then MPT_Qty else -MPT_Qty  end)) +'/P' lnkUnPledge ";
                strsql = strsql + " from MrgPledge_TRX a,securities with (nolock),##TmpVarMargin ,Isin with (nolock) ";
                strsql = strsql + " where im_scripcd = ss_cd and im_active = 'Y' and im_priority = (select min(im_priority) from Isin Where im_scripcd = MPT_scripcd and im_active = 'Y' )  ";
                strsql = strsql + " and Tmp_scripcd = im_scripcd ";
                strsql = strsql + " and  Tmp_scripcd = MPT_scripcd and ss_cd = MPT_scripcd ";
                strsql = strsql + " and MPT_clientcd = '" + userid + "' ";
                strsql += " and MPT_OurDP in (" + strOurdp + ") ";
                strsql = strsql + " group by MPT_clientcd,MPT_scripcd,im_isin,ss_name,ss_bserate,Tmp_haricut ";
                strsql = strsql + " having abs(sum(case When MPT_DRCR ='C' Then MPT_Qty else -MPT_Qty  end)) > 0 ";
                strsql = strsql + " union all ";
                strsql = strsql + " select MPT_clientcd dm_clientcd,MPT_scripcd dm_scripcd,Sum(case MPT_DRCR when 'D' Then MPT_Qty else -MPT_Qty end) as qty,im_isin,ss_name,ss_bserate, ";
                strsql = strsql + " (ss_bserate*Sum(case MPT_DRCR when 'D' Then MPT_Qty else -MPT_Qty end)) as valuation ,'Margin Re-Pledge' bh_type,cast((Tmp_haricut) as decimal(15,2)) HairCut, ";
                strsql = strsql + " convert(decimal(15,2), 0) as AfterHaricut ,'' ";
                strsql = strsql + " ,'Un-Re-Pledge '  unpledge,";
                strsql = strsql + "  MPT_clientcd +'/'+ MPT_scripcd +'/' + ss_name +'/'+ convert(varchar,sum(case When MPT_DRCR ='C' Then MPT_Qty else -MPT_Qty  end))+'/R' lnkUnPledge ";
                strsql = strsql + " from MrgPledge_TRX a,securities with (nolock),##TmpVarMargin ,Isin with (nolock) ";
                strsql = strsql + " where im_scripcd = ss_cd and im_active = 'Y' and im_priority = (select min(im_priority) from Isin Where im_scripcd = MPT_scripcd and im_active = 'Y' ) ";
                strsql = strsql + " and Tmp_scripcd = im_scripcd ";
                strsql = strsql + " and  Tmp_scripcd = MPT_scripcd and ss_cd = MPT_scripcd ";
                strsql = strsql + " and MPT_clientcd = '" + userid + "' and MPT_TRXFlag ='R'";
                strsql += " and MPT_OurDP in (" + strOurdp + ") ";
                strsql = strsql + " group by MPT_clientcd,MPT_scripcd,im_isin,ss_name,ss_bserate,Tmp_haricut ";
                strsql = strsql + " having abs(Sum(case MPT_DRCR when 'D' Then MPT_Qty else -MPT_Qty end)) > 0 ";
            }

            strsql = " select dm_clientcd ClientCode,dm_scripcd ScripCode ,qty Quantity ,dm_isin ISIN,ss_name ScripName ,bh_type HoldingType,ss_bserate Rate,HairCut,unpledge from (" + strsql + ") a ";


            try
            {
                var ds = objUtility.OpenDataSetTmp(strsql, con);
                if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                {
                    var data = ds.Tables[0];
                    return JsonConvert.SerializeObject(data, Formatting.Indented);
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public dynamic Holding_Broker_Ason(string userid, string AsOnDt)
        {

            SqlConnection con;
            using (var db = new DataContext())
            {
                con = new SqlConnection((db.Database.GetDbConnection()).ConnectionString);
                con.Open();
            }
            string strsql = string.Empty;

            SqlCommand ObjCommand = new SqlCommand();
            SqlDataAdapter ObjAdapter = new SqlDataAdapter();
            DataSet ObjDataSet = new DataSet();
            string strRMSVALATLTRT;
            strRMSVALATLTRT = objUtility.GetSysParmSt("RMSVALATLTRT", "").ToUpper().Trim();

            strsql = "if OBJECT_ID('tempdb..##TmpRates') is not null Drop Table ##TmpRates";
            objUtility.ExecuteSQLTmp(strsql, con);

            objUtility.ExecuteSQLTmp("Create Table ##TmpRates (dm_scripcd VarChar(6), dm_bserate money )", con);

            strsql = "if OBJECT_ID('tempdb..##PreserveHld') is not null Drop Table ##PreserveHld";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "Create table ##PreserveHld(";
            strsql += " dm_clientcd char(8),";
            strsql += " dm_scripcd char(6),";
            strsql += " dm_stlmnt char(9),";
            strsql += " dm_bcqty Numeric,";
            strsql += " dm_isin char(12),";
            strsql += " dm_Scripname char(12),";
            strsql += " dm_bserate money,";
            strsql += " dm_valuation money,";
            strsql += " dm_Type char(3))";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "Insert into ##PreserveHld ";
            strsql += " select dm_clientcd,dm_scripcd,se_stlmnt,abs(sum(dm_qty)) dm_qty,dm_isin,ss_name,0,0,'BEN' bh_type  ";
            strsql += " from Demat,Settlements,Client_master,ourdps,securities";
            strsql += " where dm_stlmnt = se_stlmnt and dm_clientcd = cm_cd and dm_scripcd = ss_cd and dm_Dt <= '" + AsOnDt + "'";
            strsql += " and od_cd = dm_ourdp and od_acttype in ('B','T') ";
            strsql += " and dm_type = 'BC' and dm_locked = 'N' and dm_transfered = 'N' and cm_type Not in ('C','I','B') ";
            strsql += " and cm_cd='" + userid + "'";
            strsql += " group by dm_clientcd,dm_scripcd,dm_isin, ss_name,se_stlmnt";
            strsql += " Having sum(dm_qty)<>0";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "Insert into ##PreserveHld ";
            strsql += " select dm_clientcd,dm_scripcd,se_stlmnt,abs(sum(dm_qty)) dm_qty,dm_isin,ss_name,0,0,'BEN' bh_type ";
            strsql += " from Demat,Settlements,client_master,ourdps,securities where ";
            strsql += " dm_stlmnt = se_stlmnt and dm_clientcd = cm_cd and dm_scripcd = ss_cd and cm_type Not in ('C','I','B') ";
            strsql += " and od_cd = dm_ourdp and od_acttype in ('B','T') ";
            strsql += " and dm_type = 'BC' and dm_locked in ('B','C','X','B') ";
            strsql += " and ( dm_execdt > '" + AsOnDt + "' Or IsNull(dm_execdt,0) = 0 ) and dm_dt <= '" + AsOnDt + "'";
            strsql += " and cm_cd='" + userid + "' ";
            strsql += " group by dm_clientcd,dm_scripcd,dm_isin, ss_name,se_stlmnt";
            strsql += " Having sum(dm_qty)<>0";
            objUtility.ExecuteSQLTmp(strsql, con);


            strsql = "Insert into ##PreserveHld ";
            strsql += " select dm_clientcd,dm_scripcd,se_stlmnt,abs(sum(dm_qty)) dm_qty ,dm_isin,ss_name,0,0,'BEN'  bh_type";
            strsql += " from Demat,Settlements,client_master,ourdps,securities ";
            strsql += " Where dm_stlmnt = se_stlmnt and dm_clientcd = cm_cd and dm_scripcd = ss_cd and cm_type Not in ('C','I','B') ";
            strsql += " and od_cd = dm_ourdp and od_acttype <> 'M' ";
            strsql += " and dm_type = 'BC'  and dm_locked = 'N' and dm_transfered in ('Y','S') ";
            strsql += " and ( dm_execdt > '" + AsOnDt + "' Or IsNull(dm_execdt,0) = 0 ) and dm_dt <= '" + AsOnDt + "'";
            strsql += " and cm_cd='" + userid + "'";
            strsql += " group by dm_clientcd,dm_scripcd,dm_isin, ss_name,se_stlmnt";
            strsql += " Having sum(dm_qty)<>0";
            objUtility.ExecuteSQLTmp(strsql, con);

            DataSet DsMPTrx = new DataSet();
            DsMPTrx = objUtility.OpenDataSet("select * from SysObjects where name= 'MrgPledge_TRX'");
            if (DsMPTrx.Tables[0].Rows.Count > 0)
            {
                string strOurdp = string.Empty;
                DataTable dt = objUtility.OpenDataTable("select distinct MPT_OurDP from MrgPledge_TRX Where MPT_TRXFlag ='P'");
                if (dt.Rows.Count > 0)
                {
                    for (int i = 0; i <= dt.Rows.Count - 1; i++)
                    {
                        strOurdp += "'" + dt.Rows[i]["MPT_OurDP"].ToString().Trim() + "',";
                    }
                    strOurdp = strOurdp.Remove(strOurdp.Length - 1);
                }
                else
                {
                    strOurdp = "''";
                }
                strsql = "Insert into ##PreserveHld ";
                strsql += " select MPT_clientcd,MPT_scripcd,'',sum(case When MPT_DRCR ='C' Then MPT_Qty else -MPT_Qty  end) MPT_Qty ,im_isin,ss_name,0,0,'MP' bh_type ";
                strsql += " from Isin with (nolock) ,securities ,MrgPledge_TRX  ";
                strsql += " Where im_scripcd = ss_cd and im_active = 'Y' and im_priority = (select min(im_priority) from Isin Where im_scripcd = MPT_scripcd and im_active = 'Y' )";
                strsql += " and MPT_scripcd = im_scripcd and ss_cd = MPT_scripcd ";
                strsql += " and MPT_OurDP in (" + strOurdp + ") ";
                strsql += " and MPT_clientcd='" + userid + "' ";
                strsql += " group by MPT_clientcd,MPT_scripcd,im_isin, ss_name ";
                strsql += " having abs(sum(case When MPT_DRCR ='C' Then MPT_Qty else -MPT_Qty  end)) > 0 ";
                objUtility.ExecuteSQLTmp(strsql, con);
                strsql = "Insert into ##PreserveHld ";
                strsql += " select MPT_clientcd,MPT_scripcd,'',sum(case When MPT_DRCR ='D' Then MPT_Qty else -MPT_Qty  end) MPT_Qty ,im_isin,ss_name,0,0,'MR' bh_type ";
                strsql += " from Isin with (nolock) ,securities ,MrgPledge_TRX  ";
                strsql += " Where im_scripcd = ss_cd and im_active = 'Y' and im_priority = (select min(im_priority) from Isin Where im_scripcd = MPT_scripcd and im_active = 'Y' )";
                strsql += " and MPT_scripcd = im_scripcd and ss_cd = MPT_scripcd ";
                strsql += " and MPT_OurDP in (" + strOurdp + ") ";
                strsql += " and MPT_TRXFlag = 'R' and MPT_clientcd='" + userid + "' ";
                strsql += " group by MPT_clientcd,MPT_scripcd,im_isin, ss_name ";
                strsql += " having abs(sum(case When MPT_DRCR ='D' Then MPT_Qty else -MPT_Qty  end)) > 0 ";
                objUtility.ExecuteSQLTmp(strsql, con);
            }
            strsql = " Insert into ##TmpRates ";
            strsql += " select distinct dm_scripcd , 0 from ##PreserveHld ";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "update ##TmpRates set dm_bserate = mk_closerate from Market_rates";
            strsql += " where mk_scripcd = dm_scripcd and mk_exchange = 'B'";
            strsql += " and mk_dt = (select max(mk_dt) from Market_rates where mk_exchange = 'B'";
            strsql += " and mk_scripcd = dm_scripcd ";
            if (Conversion.Val(strRMSVALATLTRT) > 0)
            {
                string StrNewRMSVALATLTRT;
                DateTime DtRMSVALATLTRT = objUtility.ConvertDT(AsOnDt).AddDays(-Conversion.Val(strRMSVALATLTRT));
                StrNewRMSVALATLTRT = DtRMSVALATLTRT.ToString("yyyyMMdd");
                strsql += " and mk_dt >='" + StrNewRMSVALATLTRT + "'";
            }
            strsql += " and mk_dt <='" + AsOnDt + "' )";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "update ##TmpRates set dm_bserate = mk_closerate from Market_rates";
            strsql += " where mk_scripcd = dm_scripcd and mk_exchange ='N'";
            strsql += " and mk_dt = (select max(mk_dt) from Market_rates where mk_exchange = 'N'";
            strsql += " and mk_scripcd = dm_scripcd ";
            if (Conversion.Val(strRMSVALATLTRT) > 0)
            {
                string StrNewRMSVALATLTRT;
                DateTime DtRMSVALATLTRT = objUtility.ConvertDT(AsOnDt).AddDays(-Conversion.Val(strRMSVALATLTRT));
                StrNewRMSVALATLTRT = DtRMSVALATLTRT.ToString("yyyyMMdd");
                strsql += " and mk_dt >='" + StrNewRMSVALATLTRT + "'";
            }
            strsql += " and mk_dt <='" + AsOnDt + "' )";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "update A set A.dm_bserate = B.dm_bserate";
            strsql += " from ##PreserveHld A, ##TmpRates B Where A.dm_scripcd = B.dm_scripcd ";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = " update ##PreserveHld set dm_valuation = (dm_bcqty*dm_bserate) ";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = " select dm_clientcd ClientCode, dm_scripcd ScripCode , dm_isin ISIN,dm_Scripname ScripName,  sum(dm_bcqty) Quantity , convert(decimal(15,2) ,dm_bserate) Rate , convert(decimal(15,2), sum(dm_valuation)) Value ";
            strsql += " from ##PreserveHld ";
            strsql += " group by dm_clientcd,dm_scripcd,dm_isin,dm_Scripname,dm_bserate ";

            try
            {
                var ds = objUtility.OpenDataSetTmp(strsql, con);
                if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                {
                    var data = ds.Tables[0];
                    return JsonConvert.SerializeObject(data, Formatting.Indented);
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public dynamic Holding_Demat_Current(string userid)
        {
            string strsql = string.Empty;

            if (_configuration["Cross"] != "")
            {
                string[] ArrCross = _configuration["Cross"].Split("/");
                strsql = "select cm_blsavingcd ClientCode ,hld_ac_code DematAc, a.hld_isin_code ISIN,b.sc_company_name CompanyName,b.sc_isinname ISINName,cast((a.hld_ac_pos) as decimal(15,3)) Quantity, ";
                strsql = strsql + " d.bt_description BalanceType ,cast((sc_rate) as decimal(15,2)) as Rate, ";
                strsql = strsql + " cast(( ( a.hld_ac_pos * sc_Rate)) as decimal(15,2))  as Value ";
                strsql = strsql + " from " + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".Holding a,";
                strsql = strsql + " " + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".Security b ,";
                strsql = strsql + " " + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".Client_master c ,";
                strsql = strsql + "" + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".Beneficiary_type d ";
                strsql = strsql + " where a.hld_ac_code = cm_cd and cm_blsavingcd = '" + userid + "' and a.hld_isin_code = b.sc_isincode ";
                strsql = strsql + " and d.bt_code = a.hld_ac_type order by a.hld_ac_code,b.sc_company_name ";
            }
            try
            {
                var ds = objUtility.OpenDataSet(strsql);
                if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                {
                    var data = ds.Tables[0];
                    return JsonConvert.SerializeObject(data, Formatting.Indented);
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        // Get data for bind dropdownlist combo as on
        public dynamic GetDropDownComboAsOnDataHandler(string strTable)
        {
            string strX = string.Empty;
            if (_configuration["IsTradeWeb"] == "O")
            {
                if (strTable == "Holding")
                {
                    char[] ArrSeparators = new char[1];
                    ArrSeparators[0] = '/';
                    if (objUtility.GetWebParameter("Cross") != "") // strBoid  LEft 2 <>IN
                    {
                        string[] ArrCross = objUtility.GetWebParameter("Cross").Split(ArrSeparators);
                        strX = " select name from [" + ArrCross[0].Trim() + "].[" + ArrCross[1].Trim() + "].[" + ArrCross[2].Trim() + "].sysobjects where xtype = 'U' and name like ('Holding_%') and ISDATE(RIGHT(name,8))=1 ";
                    }
                    if (objUtility.GetWebParameter("Estro") != "")
                    {
                        if (strX != "")
                        { strX += "  Union All "; }
                        string[] ArrEstro = objUtility.GetWebParameter("Estro").Split(ArrSeparators);
                        strX += " select name from [" + ArrEstro[0].Trim() + "].[" + ArrEstro[1].Trim() + "].[" + ArrEstro[2].Trim() + "].sysobjects where xtype = 'U' and name like ('Holding_%') and ISDATE(RIGHT(name,8))=1 ";
                    }
                }
            }
            else
            {
                if (strTable == "Holding")
                    strX = "Select Name From SysObjects where xtype = 'U' and name like 'Holding_%' and ISDATE(RIGHT(name,8))=1  order by name desc";
                else
                    strX = "Select Name From SysObjects where xtype = 'U' and name like 'BenHolding_%' and ISDATE(RIGHT(name,8))=1  order by name desc";
            }

            try
            {
                if (strX != "")
                {
                    var ds = CommonRepository.OpenDataSetTmp(strX);
                    if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                    {
                        var json = JsonConvert.SerializeObject(ds.Tables[0], Formatting.Indented);
                        return json;
                    }
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        // insert unpledge request
        public dynamic AddUnPledgeRequest(string userId, string unPledge, string dmScripcd, string txtReqQty)
        {
            try
            {
                string strHostAdd = Dns.GetHostName();
                bool blnIdentityOn = false;
                string strsql = "";
                blnIdentityOn = false;
                DataSet Dstemp = new DataSet();
                long intcnt = 0;
                //DataSet Dstemp = new DataSet();
                strsql = "delete from PledgeRequest where Rq_Clientcd='" + userId.ToUpper() + "' and Rq_Status1='X' and Rq_Status3 ='" + (unPledge == "Un-Re-Pledge" ? "R" : "P") + "'";
                objUtility.ExecuteSQL(strsql);

                Dstemp = objUtility.OpenDataSet("SELECT isnull (IDENT_CURRENT('PledgeRequest'),0)");
                intcnt = 0;

                if (Convert.ToInt64(Dstemp.Tables[0].Rows[0][0]) > 0)
                {
                    blnIdentityOn = true;
                    DataSet DsReqId = new DataSet();
                    DsReqId = objUtility.OpenDataSet("SELECT IDENT_CURRENT('PledgeRequest')");
                    intcnt = Convert.ToInt64(DsReqId.Tables[0].Rows[0][0]);
                }

                if (blnIdentityOn)
                    strsql = "insert into PledgeRequest values ( ";
                else
                    strsql = "insert into PledgeRequest values ( " + intcnt + ",";

                strsql += " '" + userId.ToUpper() + "','','" + dmScripcd.Trim() + "','" + Conversion.Val(txtReqQty) + "','" + strHostAdd + "',";
                strsql += " '" + DateTime.Today.ToString("yyyyMMdd") + "',";
                strsql += " convert(char(8),getdate(),108),";
                strsql += " 'X','P','" + (unPledge == "Un-Re-Pledge" ? "R" : "P") + "','" + objUtility.Encrypt((DateTime.Today.ToString("yyyyMMdd")).ToString().Trim()) + "','')";
                objUtility.ExecuteSQL(strsql);


                var json = JsonConvert.SerializeObject("Success");
                return json;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region ProfitLoss Process

        private void ProfitLoss_Cash_Process(string userId, string fromDate, string toDate, SqlConnection con)
        {
            string StrTRXIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'Trx' and b.name", "idx_Trx_Clientcd", true)) == 1)
            { StrTRXIndex = "index(idx_trx_clientcd),"; }
            string Fromdt = fromDate.ToString();
            string Todt = toDate.ToString();
            DataSet ObjDataSet = new DataSet();

            strsql = "if OBJECT_ID('tempdb..##VX') is not null Drop Table ##VX";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = " CREATE TABLE [##VX] (";
            strsql += " [td_companycode] [char] (1) NOT NULL ,";
            strsql += " [td_stlmnt] [char] (9) NOT NULL ,";
            strsql += " [td_clientcd] [char] (8) NOT NULL ,";
            strsql += " [td_scripcd] [char] (6) NOT NULL ,";
            strsql += " [td_dt] [char] (8) NOT NULL ,";
            strsql += " [td_srno] [numeric](18, 0) IDENTITY(1,1) NOT NULL ,";
            strsql += " [td_bsflag] [char] (1) NOT NULL ,";
            strsql += " [td_bqty] [numeric](18, 0) NOT NULL ,";
            strsql += " [td_sqty] [numeric](18, 0) NOT NULL ,";
            strsql += " [td_rate] [money] NOT NULL ,";
            strsql += " [td_marketrate] [money] NOT NULL ,";
            strsql += " [td_flag] [VarChar](1) Not null, ";
            strsql += " [td_scripnm] [VarChar](12) Not null, ";
            strsql += " [td_desc] [VarChar](15) null ";
            strsql += " PRIMARY KEY CLUSTERED (td_srno))";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "if OBJECT_ID('tempdb..##invcharges') is not null Drop Table ##invcharges";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = " CREATE TABLE [##invcharges] (";
            strsql += " [ic_stlmnt] [char] (9) NOT NULL ,";
            strsql += " [ic_clientcd] [char] (8) NOT NULL ,";
            strsql += " [ic_desc] [char] (20) NOT NULL ,";
            strsql += " [ic_amount] money";
            strsql += " ) ON [PRIMARY]";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = " CREATE index #idx_invcharges_clientcd on ##invcharges (ic_clientcd) ";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "insert into ##VX SELECT ";
            strsql += " td_companycode ,td_stlmnt ,td_clientcd ,td_scripcd,";
            strsql += " td_dt , td_bsflag , sum(td_bqty) ,sum(td_sqty) ,sum(td_rate*(td_bqty+td_sqty)), sum(td_marketrate*(td_bqty+td_sqty)),'Y' td_flag,ss_name,''";
            strsql += " FROM Trx with(" + StrTRXIndex + "nolock) , securities with(nolock) where td_dt between '" + Fromdt + "' and '" + Todt + "'";
            strsql += " and td_cfflag = 'N' and td_clientcd = '" + userId + "' and td_Scripcd = ss_cd";
            strsql += " group by td_companycode ,td_stlmnt ,td_clientcd ,td_scripcd, td_dt , td_bsflag,ss_name ";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "insert into ##invcharges";
            strsql += " select sh_stlmnt,sh_clientcd,left(sh_desc,12),sh_amount ";
            strsql += " from Specialcharges with (nolock),Settlements with (nolock) ";
            strsql += " Where sh_stlmnt = se_stlmnt and se_endt between '" + Fromdt + "' and '" + Todt + "' and sh_clientcd =('" + userId + "')";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "insert into ##invcharges";
            strsql += " select sh_stlmnt,sh_clientcd,left('Service Tax',12),sh_servicetax ";
            strsql += " from Specialcharges with (nolock),Settlements with (nolock) where sh_stlmnt = se_stlmnt";
            strsql += " and sh_servicetaxyn = 'Y' and se_endt between '" + Fromdt + "' and '" + Todt + "' ";
            strsql += " and sh_clientcd ='" + userId + "' ";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "Insert into  ##invcharges";
            strsql += " select bc_stlmnt,bc_clientcd,left(cg_desc,12),";
            strsql += " bc_amount from Cbilled_charges with (nolock) ,Charges_master with (nolock) ,Settlements with (nolock)";
            strsql += " Where bc_companycode = cg_companycode And Left(bc_stlmnt, 1) = cg_exchange";
            strsql += " and bc_chargecode = cg_cd and bc_stlmnt = se_stlmnt ";
            strsql += " and se_endt between '" + Fromdt + "' and '" + Todt + "' ";
            strsql += " and bc_clientcd ='" + userId + "' ";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "update ##VX set td_rate = Case when (td_bqty+td_sqty) > 0 then td_rate/(td_bqty+td_sqty) else 0 end , td_marketrate= Case When (td_bqty+td_sqty) > 0 then td_marketrate/(td_bqty+td_sqty) else 0 end ";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "Create ";
            strsql += " INDEX [VX_clientscripstlmnt] ON [dbo].[##VX]";
            strsql += " ([td_clientcd], [td_scripcd],[td_dt],[td_stlmnt])";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "update a set a.td_flag = 'N' from ##VX a";
            strsql += " where a.td_clientcd + a.td_scripcd + a.td_stlmnt";
            strsql += " in(select b.td_clientcd + b.td_scripcd + b.td_stlmnt";
            strsql += " from ##VX b where a.td_clientcd = b.td_clientcd";
            strsql += " and a.td_scripcd = b.td_scripcd";
            strsql += " and a.td_stlmnt = b.td_stlmnt";
            strsql += " group by td_clientcd,td_scripcd,td_stlmnt";
            strsql += " having sum(td_bqty) = 0 or sum(td_sqty) = 0)";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "SELECT td_Stlmnt,td_clientcd , td_scripcd,cm_name,td_scripnm,";
            strsql += " sum(td_bqty) td_bqty ,sum(td_sqty) td_sqty, sum(td_bqty-td_sqty) net";
            strsql += " FROM ##VX,Client_master with (nolock) ";
            strsql += " where td_clientcd = cm_cd and td_flag = 'Y'";
            strsql += " group by td_stlmnt,td_clientcd,cm_name,td_scripcd,td_scripnm";
            strsql += " having sum(td_bqty - td_sqty) <> 0";
            strsql += " ORDER BY td_clientcd , td_scripcd  ";

            ObjDataSet = objUtility.OpenDataSetTmp(strsql, con);
            if (ObjDataSet.Tables[0].Rows.Count > 0)
            {
                string strclient = string.Empty;
                string strscrip = string.Empty;
                string strstlmnt = string.Empty;
                string strDelSide = string.Empty;
                long lngDelQty = 0;
                long lngLoop = 0;
                long lngCurSerial = 0;
                long lngBalqty = 0;
                foreach (DataRow objdatarow in ObjDataSet.Tables[0].Rows)
                {
                    strclient = (string)objdatarow["td_Clientcd"];
                    strscrip = (string)objdatarow["td_scripcd"];
                    strstlmnt = (string)objdatarow["td_stlmnt"];
                    if (long.Parse(objdatarow["Net"].ToString()) > 0)
                    {
                        strDelSide = "B";
                    }
                    else
                    {
                        strDelSide = "S";
                    }
                    lngDelQty = System.Math.Abs(long.Parse(objdatarow["Net"].ToString()));
                    strsql = "select * from ##VX where td_clientcd = '" + strclient + "' and td_scripcd = '" + strscrip + "'";
                    strsql += " and td_stlmnt = '" + strstlmnt + "'";
                    strsql += " and td_bsflag = '" + strDelSide + "' order by td_dt desc,td_stlmnt desc";
                    //SqlDataAdapter ObjAdapter1 = new SqlDataAdapter(strsql, con);
                    DataSet ObjDataSet1 = new DataSet();
                    //ObjAdapter1.Fill(ObjDataSet1);
                    ObjDataSet1 = objUtility.OpenDataSetTmp(strsql, con);
                    if (ObjDataSet1.Tables[0].Rows.Count > 0)
                    {
                        foreach (DataRow objdatarow1 in ObjDataSet1.Tables[0].Rows)
                        {
                            lngLoop += 1;
                            lngCurSerial = long.Parse(objdatarow1["td_SrNo"].ToString());
                            if (long.Parse(objdatarow1["td_bqty"].ToString()) + long.Parse(objdatarow1["td_sqty"].ToString()) > lngDelQty)
                            {
                                lngBalqty = long.Parse(objdatarow1["td_bqty"].ToString()) + long.Parse(objdatarow1["td_sqty"].ToString()) - lngDelQty;
                                strsql = " insert into ##VX select td_companycode ,td_stlmnt,td_clientcd , td_scripcd, td_dt, td_bsflag,";
                                if (strDelSide == "B")
                                {
                                    strsql += lngBalqty + ", td_sqty";
                                }
                                else
                                {
                                    strsql += " td_bqty ," + lngBalqty;
                                }
                                strsql += ", td_rate, td_marketrate,td_flag,td_scripnm,'' from ##VX where td_srno =" + lngCurSerial;
                                objUtility.ExecuteSQLTmp(strsql, con);

                                strsql = "update ##VX set td_flag = 'N' ";
                                if (strDelSide == "B")
                                {
                                    strsql += ",td_bqty = ";
                                }
                                else
                                {
                                    strsql += ",td_sqty = ";
                                }
                                strsql += +lngDelQty + " where td_srno = " + lngCurSerial;
                                objUtility.ExecuteSQLTmp(strsql, con);
                                lngDelQty = 0;
                            }
                            else
                            {
                                strsql = "update ##VX set td_flag = 'N' where td_srno = " + lngCurSerial;
                                objUtility.ExecuteSQLTmp(strsql, con);
                                lngDelQty = lngDelQty - (long.Parse(objdatarow1["td_bqty"].ToString()) + long.Parse(objdatarow1["td_Sqty"].ToString()));
                            }

                            if (lngDelQty <= 0)
                            { break; }
                        }
                    }
                }
            }
            ObjDataSet.Dispose();

            strsql = "SELECT td_clientcd , td_scripcd,cm_name,td_scripnm,";
            strsql += " sum(td_bqty) td_bqty ,sum(td_sqty) td_sqty, sum(td_bqty-td_sqty) net";
            strsql += " FROM ##VX,Client_master with (nolock) ";
            strsql += " where td_clientcd = cm_cd ";
            strsql += " group by td_clientcd,cm_name,td_scripcd,td_scripnm";
            strsql += " having sum(td_bqty - td_sqty) <> 0";
            strsql += " ORDER BY td_clientcd , td_scripcd  ";

            DataSet ObjDataSet2 = new DataSet();
            ObjDataSet2 = objUtility.OpenDataSetTmp(strsql, con);

            if (ObjDataSet2.Tables[0].Rows.Count > 0)
            {
                string strclient = string.Empty;
                string strscrip = string.Empty;
                string strstlmnt = string.Empty;
                string strDelSide = string.Empty;
                long lngDelQty = 0;
                long lngLoop = 0;
                long lngCurSerial = 0;
                long lngBalqty = 0;
                foreach (DataRow objdatarow in ObjDataSet2.Tables[0].Rows)
                {
                    strclient = (string)objdatarow["td_Clientcd"];
                    strscrip = (string)objdatarow["td_scripcd"];
                    if (long.Parse(objdatarow["Net"].ToString()) > 0)
                    {
                        strDelSide = "B";
                    }
                    else
                    {
                        strDelSide = "S";
                    }
                    lngDelQty = System.Math.Abs(long.Parse(objdatarow["Net"].ToString()));
                    strsql = "select * from ##VX where td_clientcd = '" + strclient + "' and td_scripcd = '" + strscrip + "'";
                    strsql += " and td_bsflag = '" + strDelSide + "' and td_flag = 'N' order by td_dt desc,td_stlmnt desc";
                    //SqlDataAdapter ObjAdapter3 = new SqlDataAdapter(strsql, con);
                    DataSet ObjDataSet1 = new DataSet();
                    //ObjAdapter3.Fill(ObjDataSet1);
                    ObjDataSet1 = objUtility.OpenDataSetTmp(strsql, con);
                    if (ObjDataSet1.Tables[0].Rows.Count > 0)
                    {
                        foreach (DataRow objdatarow1 in ObjDataSet1.Tables[0].Rows)
                        {
                            lngLoop += 1;
                            lngCurSerial = long.Parse(objdatarow1["td_SrNo"].ToString());
                            if (long.Parse(objdatarow1["td_bqty"].ToString()) + long.Parse(objdatarow1["td_sqty"].ToString()) > lngDelQty)
                            {
                                lngBalqty = long.Parse(objdatarow1["td_bqty"].ToString()) + long.Parse(objdatarow1["td_sqty"].ToString()) - lngDelQty;
                                strsql = " insert into ##VX select td_companycode ,td_stlmnt,td_clientcd , td_scripcd, td_dt, td_bsflag,";
                                if (strDelSide == "B")
                                {
                                    strsql += lngBalqty + ", td_sqty";
                                }
                                else
                                {
                                    strsql += " td_bqty ," + lngBalqty;
                                }
                                strsql += ", td_rate, td_marketrate,td_flag,td_scripnm,'' from ##VX where td_srno =" + lngCurSerial;
                                objUtility.ExecuteSQLTmp(strsql, con);

                                strsql = "update ##VX set td_flag = 'X' ";
                                if (strDelSide == "B")
                                {
                                    strsql += ",td_bqty = ";
                                }
                                else
                                {
                                    strsql += ",td_sqty = ";
                                }
                                strsql += +lngDelQty + " where td_srno = " + lngCurSerial;
                                objUtility.ExecuteSQLTmp(strsql, con);
                                lngDelQty = 0;
                            }
                            else
                            {
                                strsql = "update ##VX set td_flag = 'X' where td_srno = " + lngCurSerial;
                                objUtility.ExecuteSQLTmp(strsql, con);
                                lngDelQty = lngDelQty - (long.Parse(objdatarow1["td_bqty"].ToString()) + long.Parse(objdatarow1["td_Sqty"].ToString()));
                            }
                            if (lngDelQty <= 0)
                            { break; }
                        }
                    }
                }
            }

            strsql = "update ##VX set td_desc = CAse td_flag when 'Y' then 'Square off' else 'Delivery' end ";
            objUtility.ExecuteSQLTmp(strsql, con);

        }

        private void ProfitLoss_Commodity_Process(string userId, string exchange, string fromDate, string toDate, SqlConnection con)
        {

            DataSet ObjDatasetH;

            string strFirstDate = string.Empty;
            string strLastDate = string.Empty;
            string Fromdt = fromDate;
            string Todt = toDate;

            DateTime strBillstDt;
            DateTime strBillenDt;
            string strdate = string.Empty;

            strsql = "if OBJECT_ID('tempdb..##finvdates') is not null Drop Table ##finvdates";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "CREATE TABLE [##finvdates] (";
            strsql += "[bd_dt] [char] (8) NOT NULL ";
            strsql += ")";
            objUtility.ExecuteSQLTmp(strsql, con);

            strBillstDt = objUtility.stod(fromDate.ToString());
            strBillenDt = objUtility.stod(toDate.ToString());

            while (strBillstDt <= strBillenDt)
            {
                strdate = objUtility.dtos(strBillstDt.ToString());
                strsql = "select count(*) cnt from Fholiday_master with (nolock) where ";
                strsql += " hm_exchange = '" + exchange + "'";
                strsql += " and hm_dt = '" + strdate + "'";
                ObjDatasetH = new DataSet();
                ObjDatasetH = objUtility.OpenDataSetTmp(strsql, con);
                if (Convert.ToInt64(ObjDatasetH.Tables[0].Rows[0]["cnt"]) == 0)
                {
                    objUtility.ExecuteSQLTmp("insert into ##finvdates values('" + strdate + "')", con);
                }
                strBillstDt = strBillstDt.AddDays(1);
            }

            strsql = "if OBJECT_ID('tempdb..##tmpFinvcharges') is not null Drop Table ##tmpFinvcharges";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "CREATE TABLE [##tmpFinvcharges] (";
            strsql += "[bc_dt] [char] (8) NOT NULL,";
            strsql += "[bc_clientcd] [char] (8) NOT NULL,";
            strsql += "[bc_desc] [char] (40) NOT NULL,";
            strsql += "[bc_amount] [money] NOT NULL,";
            strsql += "[bc_billno] [numeric] NOT NULL";
            strsql += ")";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "if OBJECT_ID('tempdb..##tmpfinvestorrep') is not null Drop Table ##tmpfinvestorrep";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "Create table ##tmpfinvestorrep  (";
            strsql += " fi_dt char(8) not null,";
            strsql += " fi_clientcd char(8) not null,";
            strsql += " fi_exchange char(1) not null,";
            strsql += " fi_seriesid numeric not null,";
            strsql += " fi_bqty numeric not null,";
            strsql += " fi_bvalue money not null,";
            strsql += " fi_sqty numeric not null,";
            strsql += " fi_svalue money not null,";
            strsql += " fi_netqty numeric not null,";
            strsql += " fi_netvalue money not null,";
            strsql += " fi_rate money not null,";
            strsql += " fi_closeprice money not null,";
            strsql += " fi_mtm money not null,";
            strsql += " fi_listorder numeric not null,";
            strsql += " fi_controlflag numeric not null,";
            strsql += " fi_prodtype char(2) not null,";
            strsql += " fi_type char(1) not null,";
            strsql += " fi_balfield char(1) not null,";
            strsql += " fi_multiplier money,";
            strsql += " fi_segment char(1) not null)";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "select isnull(min(bd_dt),'" + Fromdt + "'),isNull(max(bd_dt),'') from ##finvdates ";
            ObjDatasetH = new DataSet();

            ObjDatasetH = objUtility.OpenDataSetTmp(strsql, con);
            strFirstDate = ObjDatasetH.Tables[0].Rows[0][0].ToString();
            strLastDate = ObjDatasetH.Tables[0].Rows[0][1].ToString();

            string StrCommexConn = "";
            StrCommexConn = objUtility.GetCommexConnection();
            string StrComTradesIndex = string.Empty;

            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb(StrCommexConn + ".sysobjects a, " + StrCommexConn + ".sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
            { StrComTradesIndex = "index(idx_trades_clientcd),"; }

            //Futures opening
            strsql = "Insert into ##tmpfinvestorrep  ";
            strsql += " Select '" + strFirstDate + "',td_clientcd,td_exchange,";
            strsql += " td_seriesid,case sign(sum(td_bqty - td_sqty)) when 1 then abs(sum(td_bqty - td_sqty)) else 0 end  td_bqty,0,";
            strsql += " case sign(sum(td_bqty - td_sqty)) when 1 then 0 else abs(sum(td_bqty - td_sqty)) end td_sqty,0,0,0,0,0 td_closeprice,0,";
            strsql += " case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'TF' then 2 when 'RF' then 2 when 'EF' then 2 when 'IO' then 3 else 4 end td_sortlist,";
            strsql += " 1 td_controlflag,sm_prodtype,'N','O',sm_multiplier,'X'";
            strsql += " From " + StrCommexConn + ".Trades with (" + StrComTradesIndex + "nolock)," + StrCommexConn + ".Series_master with (nolock)," + StrCommexConn + ".Client_master with (nolock)";
            strsql += " Where td_clientcd = cm_cd and td_exchange = sm_exchange And td_seriesid = sm_seriesid";
            strsql += " and td_clientcd = '" + userId + "'";
            strsql += " and sm_expirydt >= '" + strFirstDate + "' and  td_dt < '" + strFirstDate + "'";
            strsql += " and td_exchange = '" + exchange + "' and sm_prodtype in('IF','EF','CF','RF','TF')";
            strsql += " and cm_type <> 'C'";
            strsql += " group by td_exchange,td_clientcd,td_seriesid,sm_prodtype,sm_multiplier";
            strsql += " having sum(td_bqty - td_sqty) <> 0";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            //Opening for Options
            strsql = "insert into ##tmpfinvestorrep  ";
            strsql += " select '" + strFirstDate + "',td_clientcd,td_exchange,";
            strsql += " td_seriesid, sum(case sale when 0 then buy else 0 end) td_bqty,";
            strsql += " 0,";
            strsql += " sum(case sale when 0 then 0 else sale end) td_sqty,0,";
            strsql += " 0,0,sum((buy+sale)*td_rate) / sum((buy+sale)),0 td_closeprice,0,";
            strsql += " case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'TF' then 2 when 'RF' then 2 when 'EF' then 2 when 'IO' then 3 else 4 end td_sortlist,";
            strsql += " 1 td_controlflag,sm_prodtype,'N','O',sm_multiplier,'X'";
            strsql += " From " + StrCommexConn + ".vwFoutstandingpos  ";
            strsql += " Where sm_expirydt >= '" + Fromdt + "' and  td_dt < '" + Fromdt + "'";
            strsql += " and td_clientcd = '" + userId + "'";
            strsql += " and td_exchange = '" + exchange + "' and sm_prodtype in('IO','EO','CO')";
            strsql += " and cm_type <> 'C'";
            strsql += " group by td_exchange,td_clientcd,td_seriesid,sm_prodtype,sm_multiplier";
            strsql += " having sum(sale - buy) <> 0 ";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            //Futures/Options running
            strsql = "insert into ##tmpfinvestorrep  ";
            strsql += " select td_dt,td_clientcd,td_exchange,";
            strsql += " td_seriesid,td_bqty,0,td_sqty,0,0,0,";
            strsql += " td_rate,0.0000 td_closeprice,0 mtm,";
            strsql += " case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'TF' then 2 when 'RF' then 2 when 'EF' then 2 when 'IO' then 3 else 4 end td_sortlist,";
            strsql += " 2,sm_prodtype,'N','Y',sm_multiplier,'X'";
            strsql += " From " + StrCommexConn + ".Trades with (" + StrComTradesIndex + "nolock) , " + StrCommexConn + ".Series_master with (nolock)," + StrCommexConn + ".Client_master with (nolock) ";
            strsql += " Where td_clientcd = cm_cd and td_exchange = sm_exchange and td_seriesid = sm_seriesid";
            strsql += " and td_clientcd = '" + userId + "'";
            strsql += " and td_exchange = '" + exchange + "'";
            strsql += " and sm_expirydt >= '" + strFirstDate + "' and td_dt between '" + strFirstDate + "' and '" + strLastDate + "'";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            //Exercise/Assignments
            strsql = "insert into ##tmpfinvestorrep  ";
            strsql += " select ex_dt,ex_clientcd,ex_exchange,";
            strsql += " ex_seriesid,ex_eqty,0,ex_aqty,0,0,0,";
            strsql += " ex_diffbrokrate,ex_settlerate,0,";
            strsql += " case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'TF' then 2  when 'RF' then 2 when 'EF' then 2 when 'IO' then 3 else 4 end + 5 td_sortlist,";
            strsql += " case ex_eaflag when 'E' then 3 else 4 end td_controlflag,sm_prodtype,'N','Y',sm_multiplier,'X'";
            strsql += " From " + StrCommexConn + ".Exercise with (nolock), " + StrCommexConn + ".Series_master with (nolock)," + StrCommexConn + ".Client_master with (nolock)";
            strsql += " Where ex_clientcd = cm_cd and ex_exchange = sm_exchange And ex_seriesid = sm_seriesid";
            strsql += " and ex_clientcd = '" + userId + "'";
            strsql += " and ex_exchange = '" + exchange + "'";
            strsql += " and sm_expirydt >= '" + strFirstDate + "' and  ex_dt between '" + strFirstDate + "' and '" + strLastDate + "'";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            //Reverse Future Outstanding
            strsql = "insert into ##tmpfinvestorrep  ";
            strsql += " select '" + strLastDate + "',fi_clientcd,fi_exchange,";
            strsql += " fi_seriesid,case sign(sum(case fi_controlflag when 1 then case fi_dt when '" + strFirstDate + "' then fi_bqty - fi_sqty else 0 end when 2 then fi_bqty - fi_sqty else fi_sqty - fi_bqty end)) when -1 then abs(sum(case fi_controlflag when 1 then case fi_dt when '" + strFirstDate + "' then fi_bqty - fi_sqty else 0 end when 2 then fi_bqty - fi_sqty else fi_sqty - fi_bqty end)) else 0 end td_bqty,";
            strsql += " 0,";
            strsql += " case sign(sum(case fi_controlflag when 1 then case fi_dt when '" + strFirstDate + "' then fi_bqty - fi_sqty else 0 end when 2 then fi_bqty - fi_sqty else fi_sqty - fi_bqty end)) when 1 then abs(sum(case fi_controlflag when 1 then case fi_dt when '" + strFirstDate + "' then fi_bqty - fi_sqty else 0 end when 2 then fi_bqty - fi_sqty else fi_sqty - fi_bqty end)) else 0 end td_sqty,0,";
            strsql += " 0,0,0,0 td_closeprice,0,";
            strsql += " case fi_prodtype when 'IF' then 1 when 'CF' then 1 when 'TF' then 2 when 'RF' then 2 when 'EF' then 2 when 'IO' then 3 else 4 end + 6 td_sortlist,";
            strsql += " 5 td_controlflag,fi_prodtype,'R','N',sm_multiplier,'X'";
            strsql += " From ##tmpfinvestorrep  ," + StrCommexConn + ".Series_master with (nolock)";
            strsql += " Where fi_exchange = sm_exchange and sm_seriesid = fi_seriesid and fi_prodtype in('IF','EF','CF','RF','TF') ";
            strsql += " and sm_expirydt <= '" + Todt + "'";
            strsql += " group by fi_exchange,fi_clientcd,fi_seriesid,fi_prodtype,sm_multiplier";
            strsql += " having sum(case fi_controlflag when 1 then case fi_dt when '" + strFirstDate + "' then fi_bqty - fi_sqty else 0 end when 2 then fi_bqty - fi_sqty else fi_sqty - fi_bqty end) <> 0";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            //Update Last Market Price for Options
            strsql = "update ##tmpfinvestorrep   set fi_rate = case fi_type when 'R' then ms_lastprice else fi_rate end,fi_closeprice = ms_lastprice from ##tmpfinvestorrep  ,Market_summary with (nolock)";
            strsql += " where ms_seriesid = fi_seriesid ";
            strsql += " and ms_exchange = fi_exchange ";
            strsql += " and ms_dt = (select max(ms_dt) from " + StrCommexConn + ".Market_summary with (nolock) where ms_exchange = fi_exchange ";
            strsql += " and ms_seriesid = fi_seriesid and ms_lastprice <> 0 and ms_dt <= '" + Todt + "' )";
            strsql += " and fi_prodtype in('IO','EO','CO')";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            //Update Previous close and today's close prices
            strsql = "update ##tmpfinvestorrep   set fi_closeprice =  isNull((select ms_lastprice From " + StrCommexConn + ".Market_summary with (nolock) ";
            strsql += " Where ms_exchange = '" + exchange + "' and ms_seriesid = fi_seriesid ";
            strsql += " and ms_dt = (select Max(ms_dt) from " + StrCommexConn + ".Market_Summary with (nolock) ";
            strsql += " Where ms_exchange = '" + exchange + "' and ms_seriesid = fi_seriesid ";
            strsql += " and ms_dt <='" + Todt + "')),0) ";
            strsql += " where fi_controlflag in('1','2') and fi_prodtype in('IF','EF','CF','RF','TF') ";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            //Update close Price If Expiry Trade is Not Generated.
            strsql = "update ##tmpfinvestorrep   set fi_rate =  ms_lastprice  , fi_closeprice = ms_lastprice ";
            strsql += " from ##tmpfinvestorrep  ," + StrCommexConn + ".Market_summary with (nolock) , " + StrCommexConn + ".Series_master with (nolock) ";
            strsql += " where sm_Exchange= '" + exchange + "' and sm_seriesid = fi_seriesid ";
            strsql += " and sm_exchange = ms_exchange and sm_seriesid = ms_seriesid  and sm_expirydt = ms_dt ";
            strsql += " and ms_dt < '" + Todt + "'";
            strsql += " and fi_prodtype in('IF','EF','CF','RF','TF') and fi_type = 'R' ";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            strsql = "update ##tmpfinvestorrep   set fi_rate = ms_prcloseprice from ##tmpfinvestorrep  ," + StrCommexConn + ".Market_summary with (nolock) ";
            strsql += " where ms_seriesid = fi_seriesid and fi_controlflag = 1";
            strsql += " and ms_exchange = '" + exchange + "' ";
            strsql += " and ms_dt = fi_dt";
            strsql += " and fi_prodtype in('IF','EF','CF','RF','TF')";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            //End of updation of close prices
            //Service tax here for Trades
            strsql = "insert into ##tmpFinvcharges select td_dt,td_clientcd,'SERVICE TAX',round(sum(td_servicetax),2),0 from " + StrCommexConn + ".Trades with (" + StrComTradesIndex + "nolock) ,##finvdates," + StrCommexConn + ".Client_master with (nolock)," + StrCommexConn + ".Series_master with (nolock)";
            strsql += " where td_clientcd = cm_cd and td_dt = bd_dt";
            strsql += " and td_exchange = sm_exchange ";
            strsql += " and td_seriesid = sm_seriesid";
            strsql += " and td_exchange = '" + exchange + "' ";
            strsql += " and td_clientcd = '" + userId + "'";
            strsql += " group by td_dt,td_clientcd having sum(td_servicetax) <> 0";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            //Service tax here for Exercise
            strsql = "insert into ##tmpFinvcharges select ex_dt,ex_clientcd,'SERVICE TAX',round(sum(ex_servicetax),2),0 from " + StrCommexConn + ".Exercise with (nolock),##finvdates," + StrCommexConn + ".Client_master with (nolock)," + StrCommexConn + ".Series_master with (nolock) ";
            strsql += " where ex_clientcd = cm_cd and ex_dt = bd_dt";
            strsql += " and ex_exchange = sm_exchange ";
            strsql += " and ex_seriesid = sm_seriesid";
            strsql += " and ex_clientcd = '" + userId + "'";
            strsql += " and ex_exchange = '" + exchange + "' ";
            strsql += " group by ex_dt,ex_clientcd having sum(ex_servicetax) <> 0";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            //Charges here
            //-----------from specialcharges start
            strsql = "insert into ##tmpFinvcharges select fc_dt,fc_clientcd,fc_desc,round(sum(fc_amount),2),0 from " + StrCommexConn + ".Fspecialcharges with (nolock),##finvdates," + StrCommexConn + ".Client_master with (nolock) ";
            strsql += " where fc_clientcd = cm_cd and fc_dt = bd_dt";
            strsql += " and fc_clientcd = '" + userId + "'";
            strsql += " and fc_desc not like '%{New%' and fc_exchange='" + exchange + "' and fc_desc not like '%{Old%'";
            strsql += " and fc_chargecode not in ('EMR') ";
            strsql += " group by fc_dt,fc_clientcd,fc_desc having round(sum(fc_amount),2) <> 0";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            strsql = "insert into ##tmpFinvcharges select fc_dt,fc_clientcd,'SERVICE TAX',round(sum(fc_servicetax),2),0 from " + StrCommexConn + ".Fspecialcharges with (nolock) ,##finvdates," + StrCommexConn + ".Client_master with (nolock) ";
            strsql += " where fc_exchange='" + exchange + "' and fc_clientcd = cm_cd and fc_dt = bd_dt";
            strsql += " and fc_clientcd = '" + userId + "'";
            strsql += " group by fc_dt,fc_clientcd,fc_desc having round(sum(fc_servicetax),2) <> 0";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            //-----------from specialcharges end        
            strsql = "update ##tmpfinvestorrep  set fi_bvalue = fi_bqty*fi_rate*fi_multiplier,fi_svalue = fi_sqty*fi_rate*fi_multiplier,";
            strsql += "fi_netqty = fi_bqty - fi_sqty,fi_netvalue = (fi_bqty - fi_sqty)*fi_rate*fi_multiplier";
            strsql += " where fi_controlflag not in(3,4)";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            strsql = "update ##tmpfinvestorrep  set fi_bvalue = fi_bqty*fi_rate*fi_multiplier,fi_svalue = fi_sqty*fi_rate*fi_multiplier,";
            strsql += "fi_netqty = fi_sqty - fi_bqty,fi_netvalue = (fi_bqty + fi_sqty)*fi_rate*fi_multiplier";
            strsql += " where fi_controlflag in(3,4)";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            strsql = "update ##tmpfinvestorrep  set fi_mtm = round((((fi_sqty - fi_bqty)*fi_rate*fi_multiplier) - ((fi_sqty - fi_bqty)*fi_closeprice*fi_multiplier)),4)";
            strsql += " where fi_prodtype in('IF','EF','CF','RF','TF')";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            strsql = "update ##tmpfinvestorrep  set fi_mtm = round(((case fi_controlflag when 3  then (fi_bqty + fi_sqty) when 4 then (fi_bqty + fi_sqty) else (fi_bqty - fi_sqty)*(-1) end) *fi_rate*fi_multiplier),4)";
            strsql += " where fi_prodtype in('IO','EO','CO')";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            strsql = "update ##tmpfinvestorrep  set fi_netvalue = 0 where fi_type = 'R'";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            strsql = "update ##tmpfinvestorrep  ";
            strsql += " set fi_netvalue = (fi_bqty - fi_sqty)*";
            strsql += " case sm_callput when 'C' then fi_closeprice - fi_rate else fi_rate - fi_closeprice end*fi_multiplier";
            strsql += " from ##tmpfinvestorrep ,Series_master";
            strsql += " where fi_exchange = sm_exchange and fi_segment = sm_segment and fi_seriesid = sm_seriesid ";
            strsql += " and fi_type = 'R'";
            strsql += " and (fi_bqty - fi_sqty) < 0";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            strsql = "update ##tmpfinvestorrep  ";
            strsql += " set fi_netvalue = (fi_bqty - fi_sqty)*";
            strsql += " case sm_callput when 'C' then fi_rate - fi_closeprice else fi_closeprice - fi_rate end*fi_multiplier";
            strsql += " from ##tmpfinvestorrep ,Series_master";
            strsql += " where fi_exchange = sm_exchange and fi_segment = sm_segment and fi_seriesid = sm_seriesid ";
            strsql += " and fi_type = 'R'";
            strsql += " and (fi_bqty - fi_sqty) > 0";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            strsql = "update ##tmpfinvestorrep  set fi_mtm = fi_netvalue *(-1) where fi_type = 'R'";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            //DataSet abc = new DataSet();
            //strsql = "select * from ##tmpfinvestorrep";
            //abc = objUtility.OpenDataSetTmp(strsql, con);
            //var a = JsonConvert.SerializeObject(abc.Tables[0]);
        }

        private void ProfitLoss_FO_Process(string userId, string exchange, string segment, string fromDate, string toDate, SqlConnection con)
        {
            DataSet ObjDatasetH;
            string Fromdt = fromDate;
            string Todt = toDate;
            string StrTradesIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
            { StrTradesIndex = "index(idx_trades_clientcd),"; }

            string strFirstDate = string.Empty;
            string strLastDate = string.Empty;

            DateTime strBillstDt;
            DateTime strBillenDt;
            string strdate = string.Empty;

            strsql = "if OBJECT_ID('tempdb..##finvdates') is not null Drop Table ##finvdates";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "CREATE TABLE [##finvdates] (";
            strsql += "[bd_dt] [char] (8) NOT NULL ";
            strsql += ")";
            objUtility.ExecuteSQLTmp(strsql, con);

            strBillstDt = objUtility.stod(fromDate.ToString());
            strBillenDt = objUtility.stod(toDate.ToString());

            while (strBillstDt <= strBillenDt)
            {
                strdate = objUtility.dtos(strBillstDt.ToString());
                strsql = "select count(*) cnt from Fholiday_master with (nolock) where ";
                strsql += " hm_exchange = '" + exchange + "'";
                strsql += " and hm_segment ='" + segment + "'";
                strsql += " and hm_dt = '" + strdate + "'";
                ObjDatasetH = new DataSet();
                ObjDatasetH = objUtility.OpenDataSetTmp(strsql, con);
                if (Convert.ToInt64(ObjDatasetH.Tables[0].Rows[0]["cnt"]) == 0)
                {
                    objUtility.ExecuteSQLTmp("insert into ##finvdates values('" + strdate + "')", con);
                }
                strBillstDt = strBillstDt.AddDays(1);
            }

            strsql = "if OBJECT_ID('tempdb..##tmpFinvcharges') is not null Drop Table ##tmpFinvcharges";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "CREATE TABLE [##tmpFinvcharges] (";
            strsql += "[bc_dt] [char] (8) NOT NULL,";
            strsql += "[bc_clientcd] [char] (8) NOT NULL,";
            strsql += "[bc_desc] [char] (40) NOT NULL,";
            strsql += "[bc_amount] [money] NOT NULL,";
            strsql += "[bc_billno] [numeric] NOT NULL";
            strsql += ")";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "if OBJECT_ID('tempdb..##tmpfinvestorrep') is not null Drop Table ##tmpfinvestorrep";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "Create table ##tmpfinvestorrep  (";
            strsql += " fi_dt char(8) not null,";
            strsql += " fi_clientcd char(8) not null,";
            strsql += " fi_exchange char(1) not null,";
            strsql += " fi_seriesid numeric not null,";
            strsql += " fi_bqty numeric not null,";
            strsql += " fi_bvalue money not null,";
            strsql += " fi_sqty numeric not null,";
            strsql += " fi_svalue money not null,";
            strsql += " fi_netqty numeric not null,";
            strsql += " fi_netvalue money not null,";
            strsql += " fi_rate money not null,";
            strsql += " fi_closeprice money not null,";
            strsql += " fi_mtm money not null,";
            strsql += " fi_listorder numeric not null,";
            strsql += " fi_controlflag numeric not null,";
            strsql += " fi_prodtype char(2) not null,";
            strsql += " fi_type char(1) not null,";
            strsql += " fi_balfield char(1) not null,";
            strsql += " fi_multiplier money,";
            strsql += " fi_segment char(1) not null)";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "select isnull(min(bd_dt),'" + Fromdt + "'),isNull(max(bd_dt),'') from ##finvdates ";
            ObjDatasetH = new DataSet();

            ObjDatasetH = objUtility.OpenDataSetTmp(strsql, con);
            strFirstDate = ObjDatasetH.Tables[0].Rows[0][0].ToString();
            strLastDate = ObjDatasetH.Tables[0].Rows[0][1].ToString();

            //Futures opening       
            strsql = "Insert into ##tmpfinvestorrep ";
            strsql += " Select '" + strFirstDate + "',td_clientcd,td_exchange,";
            strsql += " td_seriesid,case sign(sum(td_bqty - td_sqty)) when 1 then abs(sum(td_bqty - td_sqty)) else 0 end  td_bqty,";
            strsql += " 0,";
            strsql += " case sign(sum(td_bqty - td_sqty)) when 1 then 0 else abs(sum(td_bqty - td_sqty)) end td_sqty,0,";
            strsql += " 0,0,0,0 td_closeprice,0,";
            strsql += " case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'TF' then 2 when 'RF' then 2 when 'EF' then 2 when 'IO' then 3 else 4 end td_sortlist,";
            strsql += " 1 td_controlflag,sm_prodtype,'N','O',sm_multiplier,td_segment";
            strsql += " From Trades with(" + StrTradesIndex + "nolock),Series_master,Client_master";
            strsql += " Where td_clientcd = cm_cd and td_exchange = sm_exchange and td_segment = sm_segment And td_seriesid = sm_seriesid";
            strsql += " and sm_expirydt >= '" + strFirstDate + "' and  td_dt < '" + strFirstDate + "'";
            strsql += " and td_exchange = '" + exchange + "' and td_segment = '" + segment + "' and sm_prodtype in('IF','EF','CF','RF','TF')";
            strsql += " and td_clientcd = '" + userId + "'";
            strsql += " and cm_type <> 'C'";
            strsql += " group by td_exchange,td_clientcd,td_seriesid,sm_prodtype,sm_multiplier,td_segment";
            strsql += " having sum(td_bqty - td_sqty) <> 0";
            objUtility.ExecuteSQLTmp(strsql, con);

            //Opening for Options
            strsql = "insert into ##tmpfinvestorrep ";
            strsql += " select '" + strFirstDate + "',td_clientcd,td_exchange,";
            strsql += " td_seriesid, sum(case sale when 0 then buy else 0 end) td_bqty,";
            strsql += " 0,";
            strsql += " sum(case sale when 0 then 0 else sale end) td_sqty,0,";
            strsql += " 0,0,0,0 td_closeprice,0,";
            strsql += " case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'TF' then 2 when 'RF' then 2 when 'EF' then 2 when 'IO' then 3 else 4 end td_sortlist,";
            strsql += " 1 td_controlflag,sm_prodtype,'N','O',sm_multiplier,td_segment";
            strsql += " From vwFoutstandingpos  ";
            strsql += " Where sm_expirydt >= '" + Fromdt + "' and  td_dt < '" + Fromdt + "'";
            strsql += " and td_clientcd = '" + userId + "'";
            strsql += " and td_exchange = '" + exchange + "' and td_segment = '" + segment + "' and sm_prodtype in('IO','EO','CO')";
            strsql += " and cm_type <> 'C'";
            strsql += " group by td_exchange,td_clientcd,td_seriesid,sm_prodtype,sm_multiplier,td_segment";
            strsql += " having sum(sale - buy) <> 0 ";
            objUtility.ExecuteSQLTmp(strsql, con);

            //Futures/Options running
            strsql = "insert into ##tmpfinvestorrep ";
            strsql += " select td_dt,td_clientcd,td_exchange,";
            strsql += " td_seriesid,td_bqty,0,td_sqty,0,0,0,";
            strsql += " td_rate,0.0000 td_closeprice,0 mtm,";
            strsql += " case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'TF' then 2 when 'RF' then 2 when 'EF' then 2 when 'IO' then 3 else 4 end td_sortlist,";
            strsql += " 2,sm_prodtype,'N','Y',sm_multiplier,td_segment";
            strsql += " From Trades with(index(idx_trades_dt_clientcd)) , Series_master,Client_master";
            strsql += " Where td_clientcd = cm_cd and td_exchange = sm_exchange and td_segment = sm_segment and td_seriesid = sm_seriesid";
            strsql += " and td_exchange = '" + exchange + "' and td_segment = '" + segment + "'";
            strsql += " and sm_expirydt >= '" + strFirstDate + "' and td_dt between '" + strFirstDate + "' and '" + strLastDate + "'";
            strsql += " and td_clientcd = '" + userId + "'";
            objUtility.ExecuteSQLTmp(strsql, con);

            //Exercise/Assignments
            strsql = "insert into ##tmpfinvestorrep ";
            strsql += " select ex_dt,ex_clientcd,ex_exchange,";
            strsql += " ex_seriesid,ex_eqty,0,ex_aqty,0,0,0,";
            strsql += " ex_diffbrokrate,ex_settlerate,0,";
            strsql += " case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'TF' then 2  when 'RF' then 2 when 'EF' then 2 when 'IO' then 3 else 4 end + 5 td_sortlist,";
            strsql += " case ex_eaflag when 'E' then 3 else 4 end td_controlflag,sm_prodtype,'N','Y',sm_multiplier,ex_segment";
            strsql += " From Exercise, Series_master,Client_master";
            strsql += " Where ex_clientcd = cm_cd and ex_exchange = sm_exchange and ex_segment = sm_segment And ex_seriesid = sm_seriesid";
            strsql += " and ex_exchange = '" + exchange + "' and ex_segment ='" + segment + "'";
            strsql += " and sm_expirydt >= '" + strFirstDate + "' and  ex_dt between '" + strFirstDate + "' and '" + strLastDate + "'";
            strsql += " and ex_clientcd = '" + userId + "'";
            objUtility.ExecuteSQLTmp(strsql, con);

            //Reverse Future Outstanding
            strsql = "insert into ##tmpfinvestorrep ";
            strsql += " select '" + strLastDate + "',fi_clientcd,fi_exchange,";
            strsql += " fi_seriesid,case sign(sum(case fi_controlflag when 1 then case fi_dt when '" + strFirstDate + "' then fi_bqty - fi_sqty else 0 end when 2 then fi_bqty - fi_sqty else fi_sqty - fi_bqty end)) when -1 then abs(sum(case fi_controlflag when 1 then case fi_dt when '" + strFirstDate + "' then fi_bqty - fi_sqty else 0 end when 2 then fi_bqty - fi_sqty else fi_sqty - fi_bqty end)) else 0 end td_bqty,";
            strsql += " 0,";
            strsql += " case sign(sum(case fi_controlflag when 1 then case fi_dt when '" + strFirstDate + "' then fi_bqty - fi_sqty else 0 end when 2 then fi_bqty - fi_sqty else fi_sqty - fi_bqty end)) when 1 then abs(sum(case fi_controlflag when 1 then case fi_dt when '" + strFirstDate + "' then fi_bqty - fi_sqty else 0 end when 2 then fi_bqty - fi_sqty else fi_sqty - fi_bqty end)) else 0 end td_sqty,0,";
            strsql += " 0,0,0,0 td_closeprice,0,";
            strsql += " case fi_prodtype when 'IF' then 1 when 'CF' then 1 when 'TF' then 2 when 'RF' then 2 when 'EF' then 2 when 'IO' then 3 else 4 end + 6 td_sortlist,";
            strsql += " 5 td_controlflag,fi_prodtype,'R','N',sm_multiplier,fi_segment";
            strsql += " From ##tmpfinvestorrep ,Series_master";
            strsql += " Where fi_exchange = sm_exchange and fi_segment = sm_segment and sm_seriesid = fi_seriesid and fi_prodtype in('IF','EF','CF','RF','TF') ";
            strsql += " and sm_expirydt <= '" + strLastDate + "'";
            strsql += " group by fi_exchange,fi_clientcd,fi_seriesid,fi_prodtype,sm_multiplier,fi_segment";
            strsql += " having sum(case fi_controlflag when 1 then case fi_dt when '" + strFirstDate + "' then fi_bqty - fi_sqty else 0 end when 2 then fi_bqty - fi_sqty else fi_sqty - fi_bqty end) <> 0";
            objUtility.ExecuteSQLTmp(strsql, con);

            //Update Last Market Price for Options
            strsql = "update ##tmpfinvestorrep  set fi_rate = case fi_type when 'R' then ms_lastprice else fi_rate end,fi_closeprice = ms_lastprice from ##tmpfinvestorrep ,Market_summary";
            strsql += " where ms_seriesid = fi_seriesid ";
            strsql += " and ms_exchange = fi_exchange and ms_segment = fi_segment ";
            strsql += " and ms_dt = (select max(ms_dt) from Market_summary where ms_exchange = fi_exchange and ms_segment = fi_segment ";
            strsql += " and ms_seriesid = fi_seriesid and ms_lastprice <> 0 and ms_dt <= '" + strLastDate + "' )";
            strsql += " and fi_prodtype in('IO','EO','CO')";
            objUtility.ExecuteSQLTmp(strsql, con);

            //Update Previous close and today's close prices
            strsql = "update ##tmpfinvestorrep  set fi_closeprice =  isNull((select ms_lastprice From Market_summary ";
            strsql += " Where ms_exchange = '" + exchange + "' and ms_segment ='" + segment + "' and ms_seriesid = fi_seriesid ";
            strsql += " and ms_dt = (select Max(ms_dt) From Market_Summary ";
            strsql += " Where ms_exchange = '" + exchange + "' and ms_segment ='" + segment + "' and ms_seriesid = fi_seriesid ";
            strsql += " and ms_dt <='" + strLastDate + "')),0) ";
            strsql += " where fi_controlflag in('1','2') and fi_prodtype in('IF','EF','CF','RF','TF') ";
            objUtility.ExecuteSQLTmp(strsql, con);

            //Update close Price If Expiry Trade is Not Generated.
            strsql = "update ##tmpfinvestorrep  set fi_rate =  ms_lastprice  , fi_closeprice = ms_lastprice ";
            strsql += " from ##tmpfinvestorrep ,Market_summary , Series_master ";
            strsql += " where sm_Exchange= '" + exchange + "' and sm_segment = '" + segment + "' and sm_seriesid = fi_seriesid ";
            strsql += " and sm_exchange = ms_exchange and sm_segment = ms_segment and sm_seriesid = ms_seriesid  and sm_expirydt = ms_dt ";
            strsql += " and ms_dt < '" + strLastDate + "'";
            strsql += " and fi_prodtype in('IF','EF','CF','RF','TF') and fi_type = 'R' ";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "update ##tmpfinvestorrep  set fi_rate = ms_prcloseprice from ##tmpfinvestorrep ,Market_summary";
            strsql += " where ms_seriesid = fi_seriesid and fi_controlflag = 1";
            strsql += " and ms_exchange = '" + exchange + "' and ms_segment ='" + segment + "'";
            strsql += " and ms_dt = fi_dt";
            strsql += " and fi_prodtype in('IF','EF','CF','RF','TF')";
            objUtility.ExecuteSQLTmp(strsql, con);
            //End of updation of close prices

            //Service tax here for Trades
            strsql = "insert into ##tmpFinvcharges select td_dt,td_clientcd,'SERVICE TAX',round(sum(td_servicetax),2),0 from Trades with(index(idx_trades_dt_clientcd)) ,##finvdates,Client_master,Series_master";
            strsql += " where td_clientcd = cm_cd and td_dt = bd_dt";
            strsql += " and td_exchange = sm_exchange and td_segment = sm_segment ";
            strsql += " and td_seriesid = sm_seriesid";
            strsql += " and td_exchange = '" + exchange + "' and td_segment = '" + segment + "'";
            strsql += " group by td_dt,td_clientcd having sum(td_servicetax) <> 0";
            objUtility.ExecuteSQLTmp(strsql, con);

            //Service tax here for Exercise
            strsql = "insert into ##tmpFinvcharges select ex_dt,ex_clientcd,'SERVICE TAX',round(sum(ex_servicetax),2),0 from Exercise,##finvdates,Client_master,Series_master";
            strsql += " where ex_clientcd = cm_cd and ex_dt = bd_dt";
            strsql += " and ex_exchange = sm_exchange and ex_segment = sm_segment ";
            strsql += " and ex_seriesid = sm_seriesid";
            strsql += " and ex_exchange = '" + exchange + "' and ex_segment ='" + segment + "'";
            strsql += " group by ex_dt,ex_clientcd having sum(ex_servicetax) <> 0";
            objUtility.ExecuteSQLTmp(strsql, con);

            //Charges here
            //-----------from specialcharges start;
            strsql = "insert into ##tmpFinvcharges select fc_dt,fc_clientcd,fc_desc,round(sum(fc_amount),2),0 from Fspecialcharges,##finvdates,Client_master";
            strsql += " where  fc_clientcd = cm_cd and fc_dt = bd_dt";
            strsql += " and fc_clientcd = '" + userId + "'";
            strsql += " and fc_desc not like '%{New%' and fc_exchange='" + exchange + "' and fc_segment='" + segment + "' and fc_desc not like '%{Old%'";
            strsql += " and fc_chargecode not in ('EMR') ";
            strsql += " group by fc_dt,fc_clientcd,fc_desc having round(sum(fc_amount),2) <> 0";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "insert into ##tmpFinvcharges select fc_dt,fc_clientcd,'SERVICE TAX',round(sum(fc_servicetax),2),0 from Fspecialcharges,##finvdates,Client_master";
            strsql += " where fc_exchange='" + exchange + "' and fc_segment='" + segment + "' and fc_clientcd = cm_cd and fc_dt = bd_dt";
            strsql += " and fc_clientcd = '" + userId + "'";
            strsql += " group by fc_dt,fc_clientcd,fc_desc having round(sum(fc_servicetax),2) <> 0";
            objUtility.ExecuteSQLTmp(strsql, con);
            //-----------from specialcharges end

            //---------------Update values for MTM and Premium
            strsql = "update ##tmpfinvestorrep  set fi_bvalue = fi_bqty*fi_rate*fi_multiplier,fi_svalue = fi_sqty*fi_rate*fi_multiplier,";
            strsql += "fi_netqty = fi_bqty - fi_sqty,fi_netvalue = (fi_bqty - fi_sqty)*fi_rate*fi_multiplier";
            strsql += " where fi_controlflag not in(3,4)";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "update ##tmpfinvestorrep  set fi_bvalue = fi_bqty*fi_rate*fi_multiplier,fi_svalue = fi_sqty*fi_rate*fi_multiplier,";
            strsql += "fi_netqty = fi_sqty - fi_bqty,fi_netvalue = (fi_bqty + fi_sqty)*fi_rate*fi_multiplier";
            strsql += " where fi_controlflag in(3,4)";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "update ##tmpfinvestorrep  set fi_mtm = round((((fi_sqty - fi_bqty)*fi_rate*fi_multiplier) - ((fi_sqty - fi_bqty)*fi_closeprice*fi_multiplier)),4)";
            strsql += " where fi_prodtype in('IF','EF','CF','RF','TF')";
            objUtility.ExecuteSQLTmp(strsql, con);


            strsql = "update ##tmpfinvestorrep  set fi_mtm = round(((case fi_controlflag when 3  then (fi_bqty + fi_sqty) when 4 then (fi_bqty + fi_sqty) else (fi_bqty - fi_sqty)*(-1) end) *fi_rate*fi_multiplier),4)";
            strsql += " where fi_prodtype in('IO','EO','CO')";
            objUtility.ExecuteSQLTmp(strsql, con);
        }
        #endregion

        #region Bill Handler Method

        // get settlement type for dropdown
        public dynamic GetSettelmentType(string syExchange, string syStatus)
        {
            var query = "select sy_desc,sy_type from Settlement_type with (nolock) where sy_exchange='" + syExchange + "' and sy_Status = '" + syStatus + "' Order by case sy_maptype  when 'N' Then 1 When 'C' Then 2 else 3 end,sy_desc";
            try
            {
                var ds = objUtility.OpenDataSet(query);
                if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                {
                    var json = JsonConvert.SerializeObject(ds.Tables[0], Formatting.Indented);
                    return json;
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public dynamic CommonGetSysParmStHandler(string param1, string param2)
        {
            try
            {
                string ds = objUtility.GetSysParmSt(param1, param2);
                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        // get bill main data
        public dynamic GetBillMainData(string strClientId, string strClient, string exchangeType, string stlmntType, string fromDt, string strCompanyCode)
        {
            try
            {
                var ds = GetBillMainDataMehod(strClientId, strClient, exchangeType, stlmntType, fromDt, strCompanyCode);
                if (ds != null)
                {
                    if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                    {
                        var json = JsonConvert.SerializeObject(ds.Tables[0], Formatting.Indented);
                        return json;
                    }
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #region Bill usefull methods
        public DataSet GetBillMainDataMehod(string clientId, string strClient, string exchangeType, string stlmntType, string fromDt, string strCompanyCode)
        {
            string StrTradesIndex = "";
            Boolean blnInterOP = new Boolean();
            List<string> arrexchange = new List<string>();
            List<string> arrStlmnt = new List<string>();
            string Excode = exchangeType;
            DataSet ObjDataSet = new DataSet();

            var result = objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true);

            if (Convert.ToInt16(result.Trim()) == 1)
            { StrTradesIndex = "index(idx_trades_clientcd),"; }

            if ((exchangeType == "MCX") || (exchangeType == "NCDEX") || (exchangeType == "ICEX") || (exchangeType == "NCME") || (exchangeType == "NSEL") || (exchangeType == "NSX")) // 5-MCX  6-NCDEX  7-ICEX   8-NCME
            {
                string exchange = string.Empty;
                string Segment = string.Empty;
                if (exchangeType == "MCX")
                { exchange = "M"; }
                else if (exchangeType == "NCDEX")
                {
                    exchange = (objUtility.GetSysParmStComm("CHGNCDEXCD", "") == "Y" ? "F" : "N");
                }
                else if ((exchangeType == "ICEX"))
                { exchange = "C"; }
                else if ((exchangeType == "NCME"))
                { exchange = "A"; }
                else if ((exchangeType == "NSEL"))
                { exchange = "S"; }
                else if ((exchangeType == "NSX"))
                { exchange = "D"; }

                if (_configuration["IsTradeWeb"] == "O")//live database
                {
                    string StrConn = _configuration.GetConnectionString("DefaultConnection");
                    using (SqlConnection ObjConnectionTmp = new SqlConnection(StrConn))
                    {
                        ObjConnectionTmp.Open();
                        ObjDataSet = objUtility.fnForBill(clientId, exchange, fromDt, fromDt, Excode, Segment, ObjConnectionTmp);
                    }

                    if (ObjDataSet.Tables[0].Rows.Count == 0)
                    {
                        return ObjDataSet;
                    }
                    else
                    {
                        string strCharges = "";
                        decimal TotalDrCr = 0;
                        for (int i = 0; i <= ObjDataSet.Tables[0].Rows.Count - 1; i++)
                        {
                            if (strCharges == "")
                            {
                                if (Conversion.Val(ObjDataSet.Tables[0].Rows[i]["tx_sortlist"].ToString().Trim()) >= 10)
                                {
                                    strCharges = "Charges";
                                    DataRow ObjRow1 = ObjDataSet.Tables[0].NewRow();
                                    ObjRow1["sm_sname"] = "Value Before Adding Charges :";
                                    //ObjRow1["dt"] = ObjDataSet.Tables[0].Rows[i]["dt"].ToString().Trim();
                                    ObjRow1["NetValue"] = objUtility.mfnFormatCurrency(TotalDrCr, 2);
                                    ObjDataSet.Tables[0].Rows.InsertAt(ObjRow1, i);
                                    i = i + 1;
                                }
                            }
                            TotalDrCr = TotalDrCr + (decimal)ObjDataSet.Tables[0].Rows[i]["drcr"];
                        }
                        DataRow ObjRow2 = ObjDataSet.Tables[0].NewRow();

                        //ObjRow2["dt"] = ObjDataSet.Tables[0].Rows[ObjDataSet.Tables[0].Rows.Count - 1]["dt"].ToString().Trim();
                        ObjRow2["NetValue"] = objUtility.mfnFormatCurrency(TotalDrCr, 2);
                        ObjDataSet.Tables[0].Rows.InsertAt(ObjRow2, ObjDataSet.Tables[0].Rows.Count);
                        ObjDataSet.AcceptChanges();
                    }
                }
                else
                {
                    if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'Ctrades' and b.name", "idx_Ctrades_clientcd", true)) == 1)
                    { StrTradesIndex = "index(idx_Ctrades_clientcd),"; }

                    strsql = " Select 'Bill For : ' + ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) as  dt,td_orderid as [Order#], td_tradeid [Trade#], left(case td_trxflag when 'O' then 'b/f' else '' end,5) Time,";
                    strsql = strsql + " sm_symbol, left(sm_desc,3)+' '+rtrim(sm_symbol)+' Exp:'+ convert(char(5),";
                    strsql = strsql + " convert(datetime,sm_expirydt),103)+ case sm_strikeprice when 0 then '' else ' '+ltrim(convert(char(10),sm_strikeprice))+";
                    strsql = strsql + " lower(sm_callput) end as sm_sname,  convert(decimal(15,2),td_lastclose)td_lastclose,td_bqty,td_sqty,";
                    strsql = strsql + " convert(decimal(15,2),td_marketrate) td_marketrate,";
                    strsql = strsql + " convert(decimal(15,2),td_brokerage*(td_bqty+td_sqty)*sm_multiplier) Brokerage, convert(decimal(15,2),td_rate) td_rate,";
                    strsql = strsql + " convert(decimal(15,2),case right(sm_prodtype,1) when 'F' then (((td_bqty-td_sqty)*td_rate*sm_multiplier)-((td_bqty-td_sqty)*td_lastclose*sm_multiplier)) else ((td_bqty-td_sqty)*td_rate*sm_multiplier) end) drcr,";
                    strsql = strsql + " convert(decimal(15,2),((td_bqty-td_sqty) * td_rate*sm_multiplier)) value,td_trxflag,td_seriesid,td_exchange as exchange,td_servicetax stax,'' as NetValue";
                    strsql = strsql + " from ctrades with (nolock), cseries_master with (nolock) ";
                    strsql = strsql + " where td_seriesid=sm_seriesid and td_clientcd=@clcd and td_exchange = sm_exchange and td_dt between @FromDt and @ToDt and td_exchange=@Exch";
                    strsql = strsql + " Union all ";
                    strsql = strsql + " Select 'Bill For : ' + ltrim(rtrim(convert(char,convert(datetime,fc_dt),103)))  as dt,'0','0','Charges','zzzyy',fc_desc sm_sname,0 td_lastclose, 0 td_bqty,0 td_sqty,0,0,0 td_rate,convert(decimal(15,2),sum(fc_amount)) drcr,0 value,'C' td_trxflag,0 td_seriesid,fc_exchange as exchange,0 stax ,'' as NetValue";
                    strsql = strsql + " from cfspecialcharges with (nolock) ";
                    strsql = strsql + " where fc_clientcd=@clcd and fc_exchange=@Exch and fc_dt between @FromDt and @ToDt group by fc_dt,fc_dt,fc_desc,fc_exchange ";
                    strsql = strsql + " Union all ";
                    strsql = strsql + " Select 'Bill For : ' + ltrim(rtrim(convert(char,convert(datetime,fb_billdt),103)))  as dt ,'0','0','OpnMrg','zzzzz','[Prev. Day Mrgn.]' sm_sname, 0 td_lastclose,0 td_bqty,0 td_sqty,0,0,0 td_rate,convert(decimal(15,2),fb_margin1) drcr,0 value,'-2' td_trxflag,0 td_seriesid,fb_exchange  as exchange,0 stax,'' as NetValue ";
                    strsql = strsql + " from cFbills with (nolock) ";
                    strsql = strsql + " where fb_clientcd=@clcd and fb_exchange=@Exch and fb_billdt between @FromDt and @ToDt and fb_postmrgyn = 'Y' and fb_margin1 <> 0  ";
                    strsql = strsql + " Union all ";
                    strsql = strsql + " Select 'Bill For : ' + ltrim(rtrim(convert(char,convert(datetime,fb_billdt),103)))  as dt,'0','0','CurrMrgn','zzzzz', '[Curr. Day Mrgn.]' sm_sname, 0 td_lastclose,0 td_bqty,0 td_sqty,0,0,0 td_rate,convert(decimal(15,2),fb_margin2) drcr,0 value,'-1' td_trxflag,0 td_seriesid,fb_exchange as exchange,0 stax,'' as NetValue ";
                    strsql = strsql + " from cFbills with (nolock) ";
                    strsql = strsql + " where fb_clientcd=@clcd and fb_exchange=@Exch and fb_billdt between @FromDt and @ToDt and fb_postmrgyn = 'Y' and fb_margin2 <> 0 ";
                    strsql = strsql + " order by dt,sm_symbol, sm_sname,td_trxflag desc, td_seriesid ";

                    strsql = strsql.Replace("@FromDt", "'" + fromDt + "'").Replace("@clcd", "'" + clientId + "'").Replace("@Exch", "'" + exchange + "'").Replace("@ToDt", "'" + fromDt + "'");
                    ObjDataSet = objUtility.OpenDataSet(strsql);

                    if (ObjDataSet.Tables[0].Rows.Count == 0)
                    {
                        return ObjDataSet;
                    }
                    else
                    {
                        string strCharges = "";
                        decimal TotalDrCr = 0;
                        for (int i = 0; i <= ObjDataSet.Tables[0].Rows.Count - 1; i++)
                        {
                            if (strCharges == "")
                            {
                                if (ObjDataSet.Tables[0].Rows[i]["Time"].ToString().Trim() == "Charges" || ObjDataSet.Tables[0].Rows[i]["Time"].ToString().Trim() == "OpnMrg" || ObjDataSet.Tables[0].Rows[i]["Time"].ToString().Trim() == "CurrMrgn")
                                {
                                    strCharges = "Charges";
                                    DataRow ObjRow1 = ObjDataSet.Tables[0].NewRow();
                                    ObjRow1["sm_sname"] = "Value Before Adding Charges :";
                                    ObjRow1["dt"] = ObjDataSet.Tables[0].Rows[i]["dt"].ToString().Trim();
                                    ObjRow1["NetValue"] = objUtility.mfnFormatCurrency(TotalDrCr, 2);
                                    ObjDataSet.Tables[0].Rows.InsertAt(ObjRow1, i);
                                    i = i + 1;
                                }
                            }
                            TotalDrCr = TotalDrCr + (decimal)ObjDataSet.Tables[0].Rows[i]["drcr"];
                        }
                    }
                }
            }
            if ((exchangeType == "MCXFX") || (exchangeType == "BSEFX") || (exchangeType == "NSEFX") || (exchangeType == "NSEDERV") || (exchangeType == "MCXDERV") || (exchangeType == "BSEDERV"))
            {
                string exchange = string.Empty;
                string Segment = string.Empty;
                string StrExchWhere = string.Empty;

                if (exchangeType == "NSEDERV")
                { exchange = "N"; Segment = "F"; }
                else if (exchangeType == "MCXFX")
                { exchange = "M"; Segment = "K"; }
                else if (exchangeType == "BSEFX")
                { exchange = "B"; Segment = "K"; }
                else if (exchangeType == "NSEFX")
                { exchange = "N"; Segment = "K"; }
                else if (exchangeType == "MCXDERV")
                { exchange = "M"; Segment = "F"; }
                else if (exchangeType == "BSEDERV")
                { exchange = "B"; Segment = "F"; }


                string StrMsg;
                StrMsg = objUtility.fnCheckInterOperability(fromDt, Segment);

                if (StrMsg.ToUpper().Trim() == "TRUE")
                {
                    string apiRes7 = objUtility.fnGetInterOpExchange(Segment);

                    blnInterOP = true;
                    arrexchange = apiRes7.Split(',').ToList();
                    StrExchWhere += "('" + arrexchange[0] + "'";
                    for (int j = 1; j < arrexchange.Count; j++)
                    {
                        StrExchWhere += ",'" + arrexchange[j] + "'";
                    }
                    StrExchWhere += ")";
                }


                if (_configuration["IsTradeWeb"] == "O")//live database
                {
                    if (blnInterOP)
                    { exchange = "IOP" + exchange; }

                    string StrConn = _configuration.GetConnectionString("DefaultConnection");
                    using (SqlConnection ObjConnectionTmp = new SqlConnection(StrConn))
                    {
                        ObjConnectionTmp.Open();
                        ObjDataSet = objUtility.fnForBill(clientId, exchange, fromDt, fromDt, Excode, Segment, ObjConnectionTmp);
                    }

                    if (ObjDataSet.Tables[0].Rows.Count == 0)
                    {
                        return ObjDataSet;
                    }
                    else
                    {
                        string strCharges = "";
                        decimal TotalDrCr = 0;
                        for (int i = 0; i <= ObjDataSet.Tables[0].Rows.Count - 1; i++)
                        {
                            if (strCharges == "")
                            {
                                if (Conversion.Val(ObjDataSet.Tables[0].Rows[i]["tx_sortlist"].ToString().Trim()) >= 10)
                                {
                                    strCharges = "Charges";

                                    DataRow ObjRow1 = ObjDataSet.Tables[0].NewRow();
                                    ObjRow1["tx_desc"] = "Value Before Adding Charges :";
                                    ObjRow1["dt"] = ObjDataSet.Tables[0].Rows[i]["dt"].ToString().Trim();
                                    ObjRow1["NetValue"] = objUtility.mfnFormatCurrency(TotalDrCr, 2);
                                    ObjDataSet.Tables[0].Rows.InsertAt(ObjRow1, i);
                                    i = i + 1;
                                }
                            }
                            TotalDrCr = TotalDrCr + (decimal)ObjDataSet.Tables[0].Rows[i]["drcr"];
                        }
                        DataRow ObjRow2 = ObjDataSet.Tables[0].NewRow();

                        ObjRow2["dt"] = ObjDataSet.Tables[0].Rows[ObjDataSet.Tables[0].Rows.Count - 1]["dt"].ToString().Trim();
                        ObjRow2["NetValue"] = objUtility.mfnFormatCurrency(TotalDrCr, 2);
                        ObjDataSet.Tables[0].Rows.InsertAt(ObjRow2, ObjDataSet.Tables[0].Rows.Count);
                        ObjDataSet.AcceptChanges();

                        return ObjDataSet;
                    }
                }
                else
                {
                    if (blnInterOP)
                    {
                        strsql = " Select 'Bill For : ' + ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) as  dt,td_orderid as [Order#], td_tradeid [Trade#], left(case td_trxflag when 'O' then 'b/f' else '' end,5) Time, ";
                        strsql = strsql + " sm_symbol, left(sm_desc,3)+' '+rtrim(sm_symbol)+' Exp:'+ convert(char(5),convert(datetime,sm_expirydt),103)+ ";
                        strsql = strsql + " case sm_strikeprice when 0 then '' else ' '+ltrim(convert(char(10),sm_strikeprice))+lower(sm_callput) end as sm_sname,";
                        strsql = strsql + " convert(decimal(15,2),td_lastclose) td_lastclose,td_bqty,td_sqty,convert(decimal(15,2),td_marketrate) td_marketrate,";
                        strsql = strsql + "  convert(decimal(15,2),td_brokerage*(td_bqty+td_sqty)*sm_multiplier) Brokerage, convert(decimal(15,2),td_rate)td_rate,";
                        strsql = strsql + " convert(decimal(15,2),case right(sm_prodtype,1) when 'F' then (((td_bqty-td_sqty)*td_rate*sm_multiplier)-((td_bqty-td_sqty)*td_lastclose*";
                        strsql = strsql + " sm_multiplier)) else ((td_bqty-td_sqty)*td_rate) end) drcr, convert(decimal(15,2),((td_bqty-td_sqty) * td_rate*sm_multiplier)) value,";
                        strsql = strsql + " td_trxflag,td_seriesid,td_exchange as exchange,td_Segment as Segment,convert(decimal(15,2),td_servicetax) stax,'' NetValue  ";
                        strsql = strsql + " from trades with(" + StrTradesIndex + "nolock), series_master with (nolock) ";
                        strsql = strsql + " where td_seriesid=sm_seriesid and td_clientcd=@clcd and td_exchange = sm_exchange and td_Segment = sm_Segment and td_dt = @FromDt  and td_exchange=@Exch  and td_Segment=@Seg";

                        strsql = strsql + " union all";
                        strsql = strsql + " select 'Bill For : ' + ltrim(rtrim(convert(char,convert(datetime,ex_dt),103))) as  dt,0,0,case ex_eaflag when 'E' then 'Exercise' else 'Assignment' end ,sm_symbol,  ";
                        strsql = strsql + "left(sm_desc,3)+' '+rtrim(sm_symbol)+' Exp:'+ convert(char(5),convert(char,convert(decimal(15,2), convert(datetime,sm_expirydt),103))+ case sm_strikeprice when 0 then '' else ' '+  ltrim(convert(char(10),sm_strikeprice))+lower(sm_callput) end), ";
                        //strsql = strsql + " left(sm_desc,3)+' '+rtrim(sm_symbol)+' Exp:'+ convert(char(5),convert(decimal(15,2),convert(datetime,sm_expirydt),103)+ case sm_strikeprice when 0 then '' else ' '+ltrim(convert(char(10),sm_strikeprice))+lower(sm_callput) end),";
                        strsql = strsql + "  0  ,ex_aqty as td_bqty,ex_eqty as td_sqty, 0, convert(decimal(15,2),ex_brokerage*(ex_eqty+ex_aqty)), ";
                        strsql = strsql + " convert(decimal(15,2),ex_diffbrokrate )as td_rate,convert(decimal(15,2),(-(ex_eqty+ex_aqty) * ex_diffbrokrate)) drcr, ";
                        strsql = strsql + " convert(decimal(15,2),((ex_eqty+ex_aqty) * ex_diffbrokrate)) as value, '' as td_trxflag, ex_seriesid as td_seriesid,";
                        strsql = strsql + " ex_exchange as exchange,ex_Segment as Segment,0 stax,'' NetValue  ";
                        strsql = strsql + " from exercise with (nolock),series_master with (nolock)  ";
                        strsql = strsql + " where ex_seriesid=sm_seriesid and ex_clientcd=@clcd and ex_exchange = sm_exchange and ex_Segment = sm_Segment and ex_dt = @FromDt  and ex_exchange=@Exch and ex_Segment=@Seg ";

                        strsql = strsql + " Union all  ";
                        strsql = strsql + " Select 'Bill For : ' + ltrim(rtrim(convert(char,convert(datetime,fc_dt),103))) as  dt,0,0,'Charges','zzzyy',fc_desc sm_sname,0 td_lastclose, 0 td_bqty,0 td_sqty,0,0,0 td_rate,";
                        strsql = strsql + " convert(decimal(15,2),convert(decimal(15,2),sum(fc_amount))) drcr,0 value,'C' td_trxflag,0 td_seriesid,fc_exchange as exchange,fc_Segment as Segment,0 stax,'' NetValue  ";
                        strsql = strsql + " from fspecialcharges with (nolock)  ";
                        strsql = strsql + " where fc_clientcd=@clcd and fc_exchange=@Exch and fc_Segment=@Seg and fc_dt = @FromDt";
                        strsql = strsql + " group by fc_dt,fc_dt,fc_desc,fc_exchange,fc_Segment ";

                        strsql = strsql + " Union all";
                        strsql = strsql + " Select  'Bill For : ' + ltrim(rtrim(convert(char,convert(datetime,fb_billdt),103))) as  dt,0,0,'OpnMrg','zzzzz','[Prev. Day Mrgn.]' sm_sname, 0 td_lastclose,0 td_bqty,0 td_sqty,0,0,";
                        strsql = strsql + " 0 td_rate,convert(decimal(15,2),fb_margin1) drcr,0 value,'-2' td_trxflag,0 td_seriesid,fb_exchange  as exchange,fb_Segment  as Segment,0 stax,'' NetValue  ";
                        strsql = strsql + " from Fbills with (nolock)  ";
                        strsql = strsql + " where fb_clientcd=@clcd and fb_exchange=@Exch and fb_Segment=@Seg and fb_billdt = @FromDt  and fb_postmrgyn = 'Y' and fb_margin1 <> 0";

                        strsql = strsql + " Union all  ";
                        strsql = strsql + " Select 'Bill For : ' + ltrim(rtrim(convert(char,convert(datetime,fb_billdt),103))) as  dt,0,0,'CurrMrgn','zzzzz', '[Curr. Day Mrgn.]' sm_sname, 0 td_lastclose,0 td_bqty,0 td_sqty,0,0,";
                        strsql = strsql + " 0 td_rate,convert(decimal(15,2),fb_margin2) drcr,0 value,'-1' td_trxflag,0 td_seriesid,fb_exchange as exchange,fb_Segment as Segment,0 stax,'' NetValue  ";
                        strsql = strsql + " from Fbills with (nolock)  ";
                        strsql = strsql + " where fb_clientcd=@clcd and fb_exchange=@Exch and fb_Segment=@Seg and fb_billdt = @FromDt  and fb_postmrgyn = 'Y' and fb_margin2 <> 0  ";
                        strsql = strsql + " order by dt,sm_symbol, sm_sname,td_trxflag desc, td_seriesid ";

                        strsql = strsql.Replace("@FromDt", "'" + fromDt + "'").Replace("@clcd", "'" + clientId + "'").Replace("=@Exch", " in " + StrExchWhere).Replace("@Seg", "'" + Segment + "'");
                    }
                    else
                    {
                        strsql = " Select 'Bill For : ' + ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) as  dt,td_orderid as [Order#], td_tradeid [Trade#], left(case td_trxflag when 'O' then 'b/f' else '' end,5) Time, ";
                        strsql = strsql + " sm_symbol, left(sm_desc,3)+' '+rtrim(sm_symbol)+' Exp:'+ convert(char(5),convert(datetime,sm_expirydt),103)+ ";
                        strsql = strsql + " case sm_strikeprice when 0 then '' else ' '+ltrim(convert(char(10),sm_strikeprice))+lower(sm_callput) end as sm_sname,";
                        strsql = strsql + " convert(decimal(15,2),td_lastclose) td_lastclose,td_bqty,td_sqty,convert(decimal(15,2),td_marketrate) td_marketrate,";
                        strsql = strsql + "  convert(decimal(15,2),td_brokerage*(td_bqty+td_sqty)*sm_multiplier) Brokerage, convert(decimal(15,2),td_rate)td_rate,";
                        strsql = strsql + " convert(decimal(15,2),case right(sm_prodtype,1) when 'F' then (((td_bqty-td_sqty)*td_rate*sm_multiplier)-((td_bqty-td_sqty)*td_lastclose*";
                        strsql = strsql + " sm_multiplier)) else ((td_bqty-td_sqty)*td_rate) end) drcr, convert(decimal(15,2),((td_bqty-td_sqty) * td_rate*sm_multiplier)) value,";
                        strsql = strsql + " td_trxflag,td_seriesid,td_exchange as exchange,td_Segment as Segment,convert(decimal(15,2),td_servicetax) stax,'' NetValue  ";
                        strsql = strsql + " from trades with(" + StrTradesIndex + "nolock), series_master with (nolock) ";
                        strsql = strsql + " where td_seriesid=sm_seriesid and td_clientcd=@clcd and td_exchange = sm_exchange and td_Segment = sm_Segment and td_dt = @FromDt  and td_exchange=@Exch  and td_Segment=@Seg";

                        strsql = strsql + " union all";
                        strsql = strsql + " select 'Bill For : ' + ltrim(rtrim(convert(char,convert(datetime,ex_dt),103))) as  dt,0,0,case ex_eaflag when 'E' then 'Exercise' else 'Assignment' end ,sm_symbol,  ";
                        strsql = strsql + "left(sm_desc,3)+' '+rtrim(sm_symbol)+' Exp:'+ convert(char(5),convert(char,convert(decimal(15,2), convert(datetime,sm_expirydt),103))+ case sm_strikeprice when 0 then '' else ' '+  ltrim(convert(char(10),sm_strikeprice))+lower(sm_callput) end), ";
                        //strsql = strsql + " left(sm_desc,3)+' '+rtrim(sm_symbol)+' Exp:'+ convert(char(5),convert(decimal(15,2),convert(datetime,sm_expirydt),103)+ case sm_strikeprice when 0 then '' else ' '+ltrim(convert(char(10),sm_strikeprice))+lower(sm_callput) end),";
                        strsql = strsql + "  0  ,ex_aqty as td_bqty,ex_eqty as td_sqty, 0, convert(decimal(15,2),ex_brokerage*(ex_eqty+ex_aqty)), ";
                        strsql = strsql + " convert(decimal(15,2),ex_diffbrokrate )as td_rate,convert(decimal(15,2),(-(ex_eqty+ex_aqty) * ex_diffbrokrate)) drcr, ";
                        strsql = strsql + " convert(decimal(15,2),((ex_eqty+ex_aqty) * ex_diffbrokrate)) as value, '' as td_trxflag, ex_seriesid as td_seriesid,";
                        strsql = strsql + " ex_exchange as exchange,ex_Segment as Segment,0 stax,'' NetValue  ";
                        strsql = strsql + " from exercise with (nolock),series_master with (nolock)  ";
                        strsql = strsql + " where ex_seriesid=sm_seriesid and ex_clientcd=@clcd and ex_exchange = sm_exchange and ex_Segment = sm_Segment and ex_dt = @FromDt  and ex_exchange=@Exch and ex_Segment=@Seg ";

                        strsql = strsql + " Union all  ";
                        strsql = strsql + " Select 'Bill For : ' + ltrim(rtrim(convert(char,convert(datetime,fc_dt),103))) as  dt,0,0,'Charges','zzzyy',fc_desc sm_sname,0 td_lastclose, 0 td_bqty,0 td_sqty,0,0,0 td_rate,";
                        strsql = strsql + " convert(decimal(15,2),convert(decimal(15,2),sum(fc_amount))) drcr,0 value,'C' td_trxflag,0 td_seriesid,fc_exchange as exchange,fc_Segment as Segment,0 stax,'' NetValue  ";
                        strsql = strsql + " from fspecialcharges with (nolock)  ";
                        strsql = strsql + " where fc_clientcd=@clcd and fc_exchange=@Exch and fc_Segment=@Seg and fc_dt = @FromDt";
                        strsql = strsql + " group by fc_dt,fc_dt,fc_desc,fc_exchange,fc_Segment ";

                        strsql = strsql + " Union all";
                        strsql = strsql + " Select  'Bill For : ' + ltrim(rtrim(convert(char,convert(datetime,fb_billdt),103))) as  dt,0,0,'OpnMrg','zzzzz','[Prev. Day Mrgn.]' sm_sname, 0 td_lastclose,0 td_bqty,0 td_sqty,0,0,";
                        strsql = strsql + " 0 td_rate,convert(decimal(15,2),fb_margin1) drcr,0 value,'-2' td_trxflag,0 td_seriesid,fb_exchange  as exchange,fb_Segment  as Segment,0 stax,'' NetValue  ";
                        strsql = strsql + " from Fbills with (nolock)  ";
                        strsql = strsql + " where fb_clientcd=@clcd and fb_exchange=@Exch and fb_Segment=@Seg and fb_billdt = @FromDt  and fb_postmrgyn = 'Y' and fb_margin1 <> 0";

                        strsql = strsql + " Union all  ";
                        strsql = strsql + " Select 'Bill For : ' + ltrim(rtrim(convert(char,convert(datetime,fb_billdt),103))) as  dt,0,0,'CurrMrgn','zzzzz', '[Curr. Day Mrgn.]' sm_sname, 0 td_lastclose,0 td_bqty,0 td_sqty,0,0,";
                        strsql = strsql + " 0 td_rate,convert(decimal(15,2),fb_margin2) drcr,0 value,'-1' td_trxflag,0 td_seriesid,fb_exchange as exchange,fb_Segment as Segment,0 stax,'' NetValue  ";
                        strsql = strsql + " from Fbills with (nolock)  ";
                        strsql = strsql + " where fb_clientcd=@clcd and fb_exchange=@Exch and fb_Segment=@Seg and fb_billdt = @FromDt  and fb_postmrgyn = 'Y' and fb_margin2 <> 0  ";
                        strsql = strsql + " order by dt,sm_symbol, sm_sname,td_trxflag desc, td_seriesid ";

                        strsql = strsql.Replace("@FromDt", "'" + fromDt + "'").Replace("@clcd", "'" + clientId + "'").Replace("@Exch", "'" + exchange + "'").Replace("@Seg", "'" + Segment + "'");

                    }
                    ObjDataSet = objUtility.OpenDataSet(strsql);

                    if (ObjDataSet.Tables[0].Rows.Count == 0)
                    {
                        return ObjDataSet;
                    }
                    else
                    {
                        string strCharges = "";
                        decimal TotalDrCr = 0;
                        for (int i = 0; i <= ObjDataSet.Tables[0].Rows.Count - 1; i++)
                        {
                            if (strCharges == "")
                            {
                                if (ObjDataSet.Tables[0].Rows[i]["Time"].ToString().Trim() == "Charges" || ObjDataSet.Tables[0].Rows[i]["Time"].ToString().Trim() == "OpnMrg" || ObjDataSet.Tables[0].Rows[i]["Time"].ToString().Trim() == "CurrMrgn")
                                {
                                    strCharges = "Charges";
                                    DataRow ObjRow1 = ObjDataSet.Tables[0].NewRow();
                                    ObjRow1["sm_sname"] = "Value Before Adding Charges :";
                                    ObjRow1["dt"] = ObjDataSet.Tables[0].Rows[i]["dt"].ToString().Trim();
                                    ObjRow1["NetValue"] = objUtility.mfnFormatCurrency(TotalDrCr, 2);
                                    ObjDataSet.Tables[0].Rows.InsertAt(ObjRow1, i);
                                    i = i + 1;
                                }
                            }
                            TotalDrCr = TotalDrCr + (decimal)ObjDataSet.Tables[0].Rows[i]["drcr"];
                        }
                    }
                }
            }
            if ((exchangeType == "BSE Cash") || (exchangeType == "NSE Cash") || (exchangeType == "MCX Cash"))  //0-BSE CASH  1-NSE CASH
            {
                string exchange = exchangeType;
                string Stlmnt = string.Empty;
                string strRefDt = "";
                string StrStlmntWhere = string.Empty;

                if (exchangeType == "BSE Cash")
                {
                    Stlmnt = "B" + stlmntType.Trim();
                }
                else if (exchangeType == "NSE Cash")
                {
                    Stlmnt = "N" + stlmntType.Trim();
                }
                else if (exchangeType == "MCX Cash")
                {
                    Stlmnt = "M" + stlmntType.Trim();
                }

                string StrMsg;

                StrMsg = objUtility.fnCheckInterOperability(fromDt, "C");

                if (StrMsg.ToUpper().Trim() == "TRUE")
                {
                    string apiRes6 = objUtility.fnGetInterOpStlmnts(Stlmnt, fromDt, false);
                    blnInterOP = true;
                    arrStlmnt = apiRes6.Split(',').ToList();
                    StrStlmntWhere += "('" + arrStlmnt[0] + "'";
                    for (int j = 1; j < arrStlmnt.Count; j++)
                    {
                        StrStlmntWhere += ",'" + arrStlmnt[j] + "'";
                    }
                    StrStlmntWhere += ")";

                }
                else if (StrMsg.Trim() != "")
                {
                    //ShowMessage(StrMsg.Trim(), MessageType.Info);
                    //return;
                }

                string StrTRXIndex = "";

                string apiRes = objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'Trx' and b.name", "idx_Trx_Clientcd", true);
                if (Convert.ToInt16(apiRes.Trim()) == 1)
                { StrTRXIndex = "index(idx_trx_clientcd),"; }

                if (_configuration["IsTradeWeb"] == "O")//Live
                {
                    if (blnInterOP)
                    {
                        strsql = " select '" + arrStlmnt[0] + "' as td_stlmnt, '" + arrStlmnt[0] + "' +'['+ ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) + ']' Heading ,";
                        strsql = strsql + " td_scripcd,td_orderid as [Order#], td_tradeid [Trade#],";
                        strsql = strsql + " case td_ssrno when -1 then 'b/f' else td_time end Time, ";
                        strsql = strsql + " rtrim(ss_name)+' ('+td_scripcd+')' Security, td_bqty Buy, ";
                        strsql = strsql + " td_sqty Sell, ";
                        strsql = strsql + " convert(decimal (15,2),td_marketrate) as [Market Rate], ";
                        strsql = strsql + " convert(decimal (15,4),td_brokerage*(td_bqty+td_sqty)) [Brokerage], ";
                        strsql = strsql + " convert(decimal (15,4),convert(money, case left(td_stlmnt,1) when 'N' then  case sr_nodelyn when 'Y' then 0 else td_rate end else td_rate end *td_bqty)) [Buy Value], ";
                        strsql = strsql + " convert(decimal (15,4),convert(money, case left(td_stlmnt,1) when 'N' then case sr_nodelyn when 'Y' then 0 else td_rate  end else td_rate end *td_sqty)) [Sell Value] ,";
                        strsql = strsql + " case sr_nodelyn when 'Y' then 'NoDelv' else '' end [_], ";
                        strsql = strsql + " rtrim(ss_name)+case td_ssrno when -1 then 'a' else 'b' end [Ordr] ,td_dt BDate,'' [Net Value]";
                        strsql = strsql + " from trx with(" + StrTRXIndex + "nolock) left outer join std_rates with (nolock) on td_stlmnt = sr_stlmnt and td_scripcd = sr_scripcd and sr_nodelyn='Y' ,securities with (nolock) ";
                        strsql = strsql + " where td_clientcd =@clcd and td_stlmnt=@stlmnt  and  td_Dt between '" + fromDt + "' and '" + fromDt + "'  ";
                        strsql = strsql + " and td_scripcd = ss_cd ";

                        strsql = strsql + " union All  ";
                        strsql = strsql + " select '" + arrStlmnt[0] + "' as td_stlmnt, '" + arrStlmnt[0] + "' +'['+ ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) + ']' Heading ,";
                        strsql = strsql + " td_scripcd,td_orderid as [Order#], td_tradeid [Trade#],";
                        strsql = strsql + " case td_ssrno when -1 then 'b/f' else td_time end Time, ";
                        strsql = strsql + " rtrim(ss_name)+' ('+td_scripcd+')' Security, td_sqty  Buy, ";
                        strsql = strsql + " td_bqty Sell, ";
                        strsql = strsql + " convert(decimal (15,2),td_marketrate) as [Market Rate], ";
                        strsql = strsql + " convert(decimal (15,4),0) [Brokerage], ";
                        strsql = strsql + " convert(decimal (15,4),convert(money, case left(td_stlmnt,1) when 'N' then  case sr_nodelyn when 'Y' then 0 else td_marketrate end else td_marketrate end *td_sqty)) [Buy Value], ";
                        strsql = strsql + " convert(decimal (15,4),convert(money, case left(td_stlmnt,1) when 'N' then case sr_nodelyn when 'Y' then 0 else td_marketrate end else td_marketrate end *td_bqty)) [Sell Value] ,";
                        strsql = strsql + " case sr_nodelyn when 'Y' then 'NoDelv' else '' end [_], ";
                        strsql = strsql + " rtrim(ss_name)+case td_ssrno when -1 then 'a' else 'b' end [Ordr] ,td_dt BDate,'' [Net Value]";
                        strsql = strsql + " from trx with(" + StrTRXIndex + "nolock) left outer join std_rates with (nolock) on td_stlmnt = sr_stlmnt and td_scripcd = sr_scripcd and sr_nodelyn='Y' ,securities with (nolock) ";
                        strsql = strsql + " where td_clientcd =@clcd and td_stlmnt=@stlmnt  and  td_Dt between '" + fromDt + "' and '" + fromDt + "'  ";
                        strsql = strsql + " and td_scripcd = ss_cd and td_marginyn='B'";

                        strsql = strsql + " union All  ";
                        strsql = strsql + " select '" + arrStlmnt[0] + "' as td_stlmnt, '" + arrStlmnt[0] + "' +'['+ ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) + ']' Heading,";
                        strsql = strsql + " '0',0 , 0, 'C/F', rtrim(ss_name)+' ('+td_scripcd+')' Security, ";
                        strsql = strsql + " case sign(sum(td_bqty-td_sqty)) when 1 then 0 else sum(td_bqty-td_sqty) end , ";
                        strsql = strsql + " case sign(sum(td_sqty-td_bqty)) when 1 then 0 else sum(td_bqty-td_sqty) end ,  ";
                        strsql = strsql + " convert(decimal(15,2),max(sr_makingrate)),convert(decimal (15,4),0), ";
                        strsql = strsql + " convert(decimal (15,4),case sign(sum(td_bqty-td_sqty)) when 1 then 0 else sum((td_bqty-td_sqty)*sr_makingrate) end) , ";
                        strsql = strsql + " convert(decimal (15,4),case sign(sum(td_sqty-td_bqty)) when 1 then 0 else  sum((td_bqty-td_sqty)*sr_makingrate) end) ,  ";
                        strsql = strsql + " 'c/f', rtrim(ss_name)+'z' ,td_dt BDate,'' [Net Value] ";
                        strsql = strsql + " from trx with(" + StrTRXIndex + "nolock) ,securities with (nolock),std_rates with (nolock) ";
                        strsql = strsql + " where td_clientcd =@clcd and td_stlmnt=@stlmnt and left(td_stlmnt,1)='B' ";
                        strsql = strsql + " and  td_Dt between '" + fromDt + "' and '" + fromDt + "'  and td_stlmnt = sr_stlmnt and td_scripcd = sr_scripcd  ";
                        strsql = strsql + " and sr_nodelyn='Y' and td_scripcd = ss_cd ";
                        strsql = strsql + " group by ss_name, td_scripcd,td_stlmnt,td_dt ";
                        strsql = strsql + "  having sum(td_bqty-td_sqty)<>0 ";

                        strsql = strsql + " union all";

                        strsql = strsql + " select td_stlmnt,sh_stlmnt, 'Charges',0,0,'Charges',rtrim(sh_desc),0,0,convert(decimal(15,2),0),0, ";
                        strsql = strsql + " convert(decimal (15,4),SUM(sh_amount)),convert(decimal (15,2),0) ,'','zzz', BDate ,'' [Net Value]";
                        strsql = strsql + " From (";

                        strsql = strsql + " select '" + arrStlmnt[0] + "' as td_stlmnt, '" + arrStlmnt[0] + "' +'['+ ltrim(rtrim(convert(char,convert(datetime,se_stdt),103))) + ']' sh_stlmnt, rtrim(sh_desc) sh_desc ,sh_amount, se_stdt BDate";
                        strsql = strsql + " from Specialcharges with (nolock),settlements with (nolock) ";
                        strsql = strsql + " where sh_clientcd=@clcd and sh_stlmnt=@stlmnt and sh_stlmnt = se_Stlmnt ";
                        strsql = strsql + " and se_stdt between '" + fromDt + "' and '" + fromDt + "' ";

                        strsql = strsql + " union all   ";

                        strsql = strsql + " select  '" + arrStlmnt[0] + "' as td_stlmnt,'" + arrStlmnt[0] + "' +'['+ ltrim(rtrim(convert(char,convert(datetime,se_stdt),103))) + ']', rtrim(cg_desc), bc_amount, se_stdt BDate";
                        strsql = strsql + " from Cbilled_charges with (nolock),settlements with (nolock),charges_master with (nolock)  ";
                        strsql = strsql + " where bc_clientcd=@clcd and bc_stlmnt=@stlmnt and bc_stlmnt = se_Stlmnt  ";
                        strsql = strsql + " and bc_companycode = cg_companycode and Left(bc_stlmnt,1) = cg_exchange and  bc_chargecode =cg_cd ";
                        strsql = strsql + " and se_stdt between '" + fromDt + "' and '" + fromDt + "'  ";

                        strsql = strsql + " union all ";

                        strsql = strsql + " select '" + arrStlmnt[0] + "' as td_stlmnt,'" + arrStlmnt[0] + "' +'['+ ltrim(rtrim(convert(char,convert(datetime,se_stdt),103))) + ']',rtrim(cg_desc), sh_servicetax, se_stdt BDate";
                        strsql = strsql + " from Specialcharges with (nolock),settlements with (nolock),charges_master with (nolock)  ";
                        strsql = strsql + " where sh_clientcd=@clcd and sh_stlmnt=@stlmnt and sh_stlmnt = se_Stlmnt  ";
                        strsql = strsql + " and sh_companycode = cg_companycode and Left(sh_stlmnt,1) = cg_exchange and  cg_cd = '01' ";
                        strsql = strsql + " and se_stdt between '" + fromDt + "' and '" + fromDt + "'  and sh_servicetax > 0 ) A";
                        strsql = strsql + " gROUP bY sh_stlmnt,rtrim(sh_desc),BDate,td_stlmnt order by  Heading,ordr, Security,Time";
                        strsql = strsql.Replace("=@stlmnt", " in " + StrStlmntWhere + "").Replace("@clcd", "'" + clientId + "'");
                    }
                    else
                    {
                        strsql = " select td_stlmnt, td_stlmnt +'['+ ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) + ']' Heading ,";
                        strsql = strsql + " td_scripcd,td_orderid as [Order#], td_tradeid [Trade#],";
                        strsql = strsql + " case td_ssrno when -1 then 'b/f' else td_time end Time, ";
                        strsql = strsql + " rtrim(ss_name)+' ('+td_scripcd+')' Security, td_bqty Buy, ";
                        strsql = strsql + " td_sqty Sell, ";
                        strsql = strsql + " convert(decimal (15,2),td_marketrate) as [Market Rate], ";
                        strsql = strsql + " convert(decimal (15,4),td_brokerage*(td_bqty+td_sqty)) [Brokerage], ";
                        strsql = strsql + " convert(decimal (15,4),convert(money, case left(td_stlmnt,1) when 'N' then  case sr_nodelyn when 'Y' then 0 else td_rate end else td_rate end *td_bqty)) [Buy Value], ";
                        strsql = strsql + " convert(decimal (15,4),convert(money, case left(td_stlmnt,1) when 'N' then case sr_nodelyn when 'Y' then 0 else td_rate  end else td_rate end *td_sqty)) [Sell Value] ,";
                        strsql = strsql + " case sr_nodelyn when 'Y' then 'NoDelv' else '' end [_], ";
                        strsql = strsql + " rtrim(ss_name)+case td_ssrno when -1 then 'a' else 'b' end [Ordr] ,td_dt BDate,'' [Net Value]";
                        strsql = strsql + " from trx with(" + StrTRXIndex + "nolock) left outer join std_rates with (nolock) on td_stlmnt = sr_stlmnt and td_scripcd = sr_scripcd and sr_nodelyn='Y' ,securities with (nolock) ";
                        strsql = strsql + " where td_clientcd =@clcd and td_stlmnt=@stlmnt  and  td_Dt between '" + fromDt + "' and '" + fromDt + "'  ";
                        strsql = strsql + " and td_scripcd = ss_cd ";

                        strsql = strsql + " union All  ";
                        strsql = strsql + " select td_stlmnt, td_stlmnt +'['+ ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) + ']' Heading ,";
                        strsql = strsql + " td_scripcd,td_orderid as [Order#], td_tradeid [Trade#],";
                        strsql = strsql + " case td_ssrno when -1 then 'b/f' else td_time end Time, ";
                        strsql = strsql + " rtrim(ss_name)+' ('+td_scripcd+')' Security, td_sqty  Buy, ";
                        strsql = strsql + " td_bqty Sell, ";
                        strsql = strsql + " convert(decimal (15,2),td_marketrate) as [Market Rate], ";
                        strsql = strsql + " convert(decimal (15,4),0) [Brokerage], ";
                        strsql = strsql + " convert(decimal (15,4),convert(money, case left(td_stlmnt,1) when 'N' then  case sr_nodelyn when 'Y' then 0 else td_marketrate end else td_marketrate end *td_sqty)) [Buy Value], ";
                        strsql = strsql + " convert(decimal (15,4),convert(money, case left(td_stlmnt,1) when 'N' then case sr_nodelyn when 'Y' then 0 else td_marketrate end else td_marketrate end *td_bqty)) [Sell Value] ,";
                        strsql = strsql + " case sr_nodelyn when 'Y' then 'NoDelv' else '' end [_], ";
                        strsql = strsql + " rtrim(ss_name)+case td_ssrno when -1 then 'a' else 'b' end [Ordr] ,td_dt BDate,'' [Net Value]";
                        strsql = strsql + " from trx with(" + StrTRXIndex + "nolock) left outer join std_rates with (nolock) on td_stlmnt = sr_stlmnt and td_scripcd = sr_scripcd and sr_nodelyn='Y' ,securities with (nolock) ";
                        strsql = strsql + " where td_clientcd =@clcd and td_stlmnt=@stlmnt  and  td_Dt between '" + fromDt + "' and '" + fromDt + "'  ";
                        strsql = strsql + " and td_scripcd = ss_cd and td_marginyn='B'";

                        strsql = strsql + " union All  ";
                        strsql = strsql + " select td_stlmnt, td_stlmnt +'['+ ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) + ']' Heading,";
                        strsql = strsql + " '0',0 , 0, 'C/F', rtrim(ss_name)+' ('+td_scripcd+')' Security, ";
                        strsql = strsql + " case sign(sum(td_bqty-td_sqty)) when 1 then 0 else sum(td_bqty-td_sqty) end , ";
                        strsql = strsql + " case sign(sum(td_sqty-td_bqty)) when 1 then 0 else sum(td_bqty-td_sqty) end ,  ";
                        strsql = strsql + " convert(decimal(15,2),max(sr_makingrate)),convert(decimal (15,4),0), ";
                        strsql = strsql + " convert(decimal (15,4),case sign(sum(td_bqty-td_sqty)) when 1 then 0 else sum((td_bqty-td_sqty)*sr_makingrate) end) , ";
                        strsql = strsql + " convert(decimal (15,4),case sign(sum(td_sqty-td_bqty)) when 1 then 0 else  sum((td_bqty-td_sqty)*sr_makingrate) end) ,  ";
                        strsql = strsql + " 'c/f', rtrim(ss_name)+'z' ,td_dt BDate,'' [Net Value] ";
                        strsql = strsql + " from trx with(" + StrTRXIndex + "nolock) ,securities with (nolock),std_rates with (nolock) ";
                        strsql = strsql + " where td_clientcd =@clcd and td_stlmnt=@stlmnt and left(td_stlmnt,1)='B' ";
                        strsql = strsql + " and  td_Dt between '" + fromDt + "' and '" + fromDt + "'  and td_stlmnt = sr_stlmnt and td_scripcd = sr_scripcd  ";
                        strsql = strsql + " and sr_nodelyn='Y' and td_scripcd = ss_cd ";
                        strsql = strsql + " group by ss_name, td_scripcd,td_stlmnt,td_dt ";
                        strsql = strsql + "  having sum(td_bqty-td_sqty)<>0 ";

                        strsql = strsql + " union all";

                        strsql = strsql + " select td_stlmnt,sh_stlmnt, 'Charges',0,0,'Charges',rtrim(sh_desc),0,0,convert(decimal(15,2),0),0, ";
                        strsql = strsql + " convert(decimal (15,4),SUM(sh_amount)),convert(decimal (15,2),0) ,'','zzz', BDate ,'' [Net Value]";
                        strsql = strsql + " From (";

                        strsql = strsql + " select sh_stlmnt td_stlmnt, sh_stlmnt +'['+ ltrim(rtrim(convert(char,convert(datetime,se_stdt),103))) + ']' sh_stlmnt, rtrim(sh_desc) sh_desc ,sh_amount, se_stdt BDate";
                        strsql = strsql + " from Specialcharges with (nolock),settlements with (nolock) ";
                        strsql = strsql + " where sh_clientcd=@clcd and sh_stlmnt=@stlmnt and sh_stlmnt = se_Stlmnt ";
                        strsql = strsql + " and se_stdt between '" + fromDt + "' and '" + fromDt + "' ";

                        strsql = strsql + " union all   ";

                        strsql = strsql + " select  bc_stlmnt td_stlmnt,bc_stlmnt +'['+ ltrim(rtrim(convert(char,convert(datetime,se_stdt),103))) + ']', rtrim(cg_desc), bc_amount, se_stdt BDate";
                        strsql = strsql + " from Cbilled_charges with (nolock),settlements with (nolock),charges_master with (nolock)  ";
                        strsql = strsql + " where bc_clientcd=@clcd and bc_stlmnt=@stlmnt and bc_stlmnt = se_Stlmnt  ";
                        strsql = strsql + " and bc_companycode = cg_companycode and Left(bc_stlmnt,1) = cg_exchange and  bc_chargecode =cg_cd ";
                        strsql = strsql + " and se_stdt between '" + fromDt + "' and '" + fromDt + "'  ";

                        strsql = strsql + " union all ";

                        strsql = strsql + " select sh_stlmnt td_stlmnt,sh_stlmnt +'['+ ltrim(rtrim(convert(char,convert(datetime,se_stdt),103))) + ']',rtrim(cg_desc), sh_servicetax, se_stdt BDate";
                        strsql = strsql + " from Specialcharges with (nolock),settlements with (nolock),charges_master with (nolock)  ";
                        strsql = strsql + " where sh_clientcd=@clcd and sh_stlmnt=@stlmnt and sh_stlmnt = se_Stlmnt  ";
                        strsql = strsql + " and sh_companycode = cg_companycode and Left(sh_stlmnt,1) = cg_exchange and  cg_cd = '01' ";
                        strsql = strsql + " and se_stdt between '" + fromDt + "' and '" + fromDt + "'  and sh_servicetax > 0 ) A";
                        strsql = strsql + " gROUP bY sh_stlmnt,rtrim(sh_desc),BDate,td_stlmnt order by  Heading,ordr, Security,Time";
                        strsql = strsql.Replace("=@stlmnt", " like '" + Stlmnt + "%'").Replace("@clcd", "'" + clientId + "'");
                    }
                    ObjDataSet = objUtility.OpenDataSet(strsql);
                    if (ObjDataSet.Tables[0].Rows.Count == 0)
                    {
                        return ObjDataSet;
                    }
                    else
                    {
                        int i = 0;
                        string strstlmnt = string.Empty;
                        string strCharges = "";
                        decimal TotalBval = 0;
                        decimal TotalSval = 0;
                        decimal TotalDiff = 0;
                        string strHeading = string.Empty;

                        string strparmcd = objUtility.GetSysParmSt("ROUNDOFF", "");

                        for (i = 0; i <= ObjDataSet.Tables[0].Rows.Count - 1; i++)
                        {
                            strRefDt = ObjDataSet.Tables[0].Rows[i]["BDate"].ToString().Trim();
                            if (strstlmnt != string.Empty && strstlmnt != ObjDataSet.Tables[0].Rows[i]["td_stlmnt"].ToString().Trim())
                            {
                                strstlmnt = ObjDataSet.Tables[0].Rows[i]["td_stlmnt"].ToString().Trim();
                                DataRow ObjRow1 = ObjDataSet.Tables[0].NewRow();
                                TotalDiff = Math.Round((decimal)(TotalSval - TotalBval), strparmcd == "Y" ? 0 : 2);
                                if (TotalBval < TotalSval)
                                {
                                    ObjRow1["Security"] = "Due To You :";
                                    ObjRow1["Heading"] = strHeading;
                                    ObjDataSet.Tables[0].Rows.InsertAt(ObjRow1, i);
                                    i = i + 1;
                                }
                                else
                                {
                                    ObjRow1["Security"] = "Due To Us :";
                                    ObjRow1["Heading"] = strHeading;
                                    ObjDataSet.Tables[0].Rows.InsertAt(ObjRow1, i);
                                    i = i + 1;
                                }

                                var apiRes2 = objUtility.mfnRoundoffCashbill(strClient, strRefDt, TotalDiff, Strings.Left(strstlmnt, 1), strCompanyCode);

                                ObjRow1["Net Value"] = objUtility.mfnFormatCurrency(Convert.ToDecimal(apiRes2), 2);
                                TotalBval = 0;
                                TotalSval = 0;
                                TotalDiff = 0;
                            }
                            if (strstlmnt == string.Empty)
                            {
                                strstlmnt = ObjDataSet.Tables[0].Rows[i]["td_stlmnt"].ToString().Trim();
                            }
                            if (strstlmnt == ObjDataSet.Tables[0].Rows[i]["td_stlmnt"].ToString().Trim())
                            {
                                TotalBval = TotalBval + (decimal)ObjDataSet.Tables[0].Rows[i]["Buy Value"];
                                TotalSval = TotalSval + (decimal)ObjDataSet.Tables[0].Rows[i]["Sell Value"];
                                TotalSval = System.Math.Abs(TotalSval);
                                TotalBval = System.Math.Abs(TotalBval);
                            }

                            strHeading = ObjDataSet.Tables[0].Rows[i]["Heading"].ToString().Trim();

                            if (strCharges == "")
                            {
                                if (ObjDataSet.Tables[0].Rows[i]["td_scripcd"].ToString().Trim() == "Charges")
                                {
                                    strCharges = "Charges";
                                    DataRow ObjRow1 = ObjDataSet.Tables[0].NewRow();

                                    TotalDiff = Math.Round((decimal)(TotalSval - TotalBval), strparmcd == "Y" ? 0 : 2);

                                    var apiRes2 = objUtility.mfnRoundoffCashbill(strClient, strRefDt, TotalDiff, Strings.Left(strstlmnt, 1), strCompanyCode);

                                    TotalDiff = Convert.ToDecimal(apiRes2);

                                    ObjRow1["Security"] = "Net Item Amount (Sale-Buy) :";
                                    ObjRow1["Net Value"] = objUtility.mfnFormatCurrency(TotalDiff, 2);
                                    ObjRow1["Heading"] = strHeading;
                                    ObjDataSet.Tables[0].Rows.InsertAt(ObjRow1, i);
                                    i = i + 1;
                                }
                            }
                        }
                        DataRow ObjRow2 = ObjDataSet.Tables[0].NewRow();
                        TotalDiff = Math.Round((decimal)(TotalSval - TotalBval), strparmcd == "Y" ? 0 : 2);

                        var apiRes3 = objUtility.mfnRoundoffCashbill(strClient, strRefDt, TotalDiff, Strings.Left(strstlmnt, 1), strCompanyCode);

                        TotalDiff = Convert.ToDecimal(apiRes3);

                        if (TotalBval < TotalSval)
                        {
                            ObjRow2["Security"] = "Due To You :";
                        }
                        else
                        {
                            ObjRow2["Security"] = "Due To Us :";
                        }
                        ObjRow2["Net Value"] = objUtility.mfnFormatCurrency(TotalDiff, 2);
                        ObjRow2["Heading"] = strHeading;
                        ObjDataSet.Tables[0].Rows.InsertAt(ObjRow2, ObjDataSet.Tables[0].Rows.Count);
                        ObjDataSet.AcceptChanges();

                        TotalBval = 0;
                        TotalSval = 0;
                    }
                }
                else
                {
                    if (blnInterOP) //Offline
                    {
                        strsql = "select '1' Odr,'" + arrStlmnt[0] + "' as td_stlmnt,'" + arrStlmnt[0] + "' +' [ '+ ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) + ' ]' Heading ,ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) as td_dt,td_scripcd,max(td_orderid) as [Order#], max(td_tradeid) [Trade#], max(case td_ssrno when -1 then 'b/f' else td_time end) Time, rtrim(td_scripnm)+' ('+td_scripcd+')' Security, ";
                        strsql = strsql + " Case td_bsflag when 'B' then sum(td_bqty) else 0 end as Buy, Case td_bsflag when 'S' then sum(td_sqty) else 0 end as Sell, convert(decimal (15,2),sum((td_bqty+td_sqty)*td_rate)/sum(td_bqty+td_sqty)) as [Market Rate], convert(decimal (15,2),sum(td_brokerage*(td_bqty+td_sqty))) [Brokerage], ";
                        strsql = strsql + " case td_bsflag when 'B' then sum(convert(decimal(15,2), case left(td_stlmnt,1) when 'N' then case td_Nodel when 'Y' then 0 else td_rate end else td_rate end *td_bqty)) else 0 end [Buy Value], ";
                        strsql = strsql + " case td_bsflag when 'S' then sum(convert(decimal(15,2), case left(td_stlmnt,1) when 'N' then case td_Nodel when 'Y' then 0 else td_rate end else td_rate end *td_sqty)) else 0 end [Sell Value] ,";
                        strsql = strsql + " max(case td_nodel when 'Y' then 'NoDelv' else '' end) [_], rtrim(td_scripnm)+ max(case td_ssrno when -1 then 'a' else 'b' end) [Ordr],td_scripnm ";
                        strsql = strsql + " ,0 'Net Qty' ,0 'Net Value' ,0 'Net Rate'";
                        strsql = strsql + " from trx with(" + StrTRXIndex + "nolock) where td_clientcd =@clcd ";
                        strsql = strsql + " and td_Dt between @FromDt and @ToDt ";
                        strsql = strsql + " and  td_stlmnt=@stlmnt";
                        strsql = strsql + " group by td_stlmnt,td_dt,td_scripcd,td_scripnm,td_bsflag ";

                        strsql = strsql + " union All ";
                        strsql = strsql + " select '2' Odr,'" + arrStlmnt[0] + "' as td_stlmnt,'" + arrStlmnt[0] + "' +' [ '+ ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) + ' ]' Heading ,";
                        strsql = strsql + " ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) as td_dt,td_scripcd,";
                        strsql = strsql + " '0' as [Order#], '0' [Trade#], 'T' Time, ";
                        strsql = strsql + " rtrim(td_scripnm)+' ('+td_scripcd+')' Security, ";
                        strsql = strsql + " 0 as Buy, 0 as Sell, 0 [Market Rate], 0 [Brokerage], 0 [Buy Value], 0 [Sell Value] ,";
                        strsql = strsql + " max(case td_nodel when 'Y' then 'NoDelv' else '' end) [_], ";
                        strsql = strsql + " rtrim(td_scripnm)+ max(case td_ssrno when -1 then 'a' else 'b' end) [Ordr],";
                        strsql = strsql + " td_scripnm ,";
                        strsql = strsql + " Sum(td_bqty-td_sqty) 'Net Qty',";
                        strsql = strsql + " Convert(decimal(15,2),Sum((td_bqty-td_sqty)* td_rate)) 'Net Value',";
                        strsql = strsql + " Convert(decimal(15,2),Round(case When Sum(td_bqty-td_sqty) <> 0 Then Sum((td_bqty-td_sqty)* td_rate)/Sum(td_bqty-td_sqty) else 0 end,2)) 'Net Rate'";
                        strsql = strsql + " from trx with(" + StrTRXIndex + "nolock) where td_clientcd =@clcd ";
                        strsql = strsql + " and td_Dt between @FromDt and @ToDt ";
                        strsql = strsql + " and  td_stlmnt=@stlmnt";
                        strsql = strsql + " group by td_stlmnt, td_dt,td_scripcd,td_scripnm  ";
                        strsql = strsql + " having count(distinct td_bsflag) > 1";

                        strsql = strsql + " union All ";
                        strsql = strsql + " select '3' Odr, '" + arrStlmnt[0] + "' as td_stlmnt,'" + arrStlmnt[0] + "' +' [ '+ ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) + ' ]',ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) as td_dt,'0',0 , 0, 'C/F', rtrim(td_scripnm)+' ('+td_scripcd+')' Security, ";
                        strsql = strsql + " case sign(sum(td_bqty-td_sqty)) when 1 then 0 else sum(td_bqty-td_sqty) end , ";
                        strsql = strsql + " case sign(sum(td_sqty-td_bqty)) when 1 then 0 else sum(td_bqty-td_sqty) end , convert(decimal(15,2),max(td_makingrt)), 0, ";
                        strsql = strsql + " case sign(sum(td_bqty-td_sqty)) when 1 then 0 else sum((td_bqty-td_sqty)*td_makingrt) end , ";
                        strsql = strsql + " case sign(sum(td_sqty-td_bqty)) when 1 then 0 else sum((td_bqty-td_sqty)*td_makingrt) end ,  'c/f', rtrim(td_scripnm)+'z', td_scripnm";
                        strsql = strsql + " ,0,0,0";
                        strsql = strsql + " from trx with(" + StrTRXIndex + "nolock) ";
                        strsql = strsql + " where td_clientcd =@clcd ";
                        strsql = strsql + " and td_Dt between @FromDt and @ToDt ";
                        strsql = strsql + " and td_stlmnt=@stlmnt";
                        strsql = strsql + " and td_nodel='Y' and left(td_stlmnt,1)='B' ";
                        strsql = strsql + " group by td_stlmnt,td_dt,td_scripnm, td_scripcd having sum(td_bqty-td_sqty)<>0";//td_stlmnt='" + Stlmnt + "'

                        strsql = strsql + " union all ";
                        strsql = strsql + " select  '4' Odr,'" + arrStlmnt[0] + "' as sh_stlmnt,'" + arrStlmnt[0] + "' +' [ '+ ltrim(rtrim(convert(char,convert(datetime,se_stdt),103))) + ' ]',ltrim(rtrim(convert(char,convert(datetime,se_stdt),103))),'Charges',0,0,'Charges', rtrim(sh_desc), 0,0,convert(decimal(15,2),0),0,convert(decimal(15,2),sum(sh_amount)),0 ,'','zzz','' ";
                        strsql = strsql + " ,0,0,0";
                        strsql = strsql + " from Specialcharges with(nolock),settlements with(nolock)  where sh_clientcd=@clcd and sh_stlmnt = se_Stlmnt ";
                        strsql = strsql + " and se_stdt between  @FromDt and @ToDt";
                        strsql = strsql + " and sh_stlmnt=@stlmnt";
                        strsql = strsql + " group by sh_stlmnt,se_stdt,sh_desc ";
                        strsql = strsql + " order by  Heading,ordr, Security,Odr ";
                        strsql = strsql.Replace("=@stlmnt", " in " + StrStlmntWhere + "").Replace("@clcd", "'" + clientId + "'").Replace("@FromDt", "'" + fromDt + "'").Replace("@ToDt", "'" + fromDt + "'");
                    }
                    else
                    {
                        strsql = "select '1' Odr,td_stlmnt,td_stlmnt +' [ '+ ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) + ' ]' Heading ,ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) as td_dt,td_scripcd,max(td_orderid) as [Order#], max(td_tradeid) [Trade#], max(case td_ssrno when -1 then 'b/f' else td_time end) Time, rtrim(td_scripnm)+' ('+td_scripcd+')' Security, ";
                        strsql = strsql + " Case td_bsflag when 'B' then sum(td_bqty) else 0 end as Buy, Case td_bsflag when 'S' then sum(td_sqty) else 0 end as Sell, convert(decimal (15,2),sum((td_bqty+td_sqty)*td_rate)/sum(td_bqty+td_sqty)) as [Market Rate], convert(decimal (15,2),sum(td_brokerage*(td_bqty+td_sqty))) [Brokerage], ";
                        strsql = strsql + " case td_bsflag when 'B' then sum(convert(decimal(15,2), case left(td_stlmnt,1) when 'N' then case td_Nodel when 'Y' then 0 else td_rate end else td_rate end *td_bqty)) else 0 end [Buy Value], ";
                        strsql = strsql + " case td_bsflag when 'S' then sum(convert(decimal(15,2), case left(td_stlmnt,1) when 'N' then case td_Nodel when 'Y' then 0 else td_rate end else td_rate end *td_sqty)) else 0 end [Sell Value] ,";
                        strsql = strsql + " max(case td_nodel when 'Y' then 'NoDelv' else '' end) [_], rtrim(td_scripnm)+ max(case td_ssrno when -1 then 'a' else 'b' end) [Ordr],td_scripnm ";
                        strsql = strsql + " ,0 'Net Qty' ,0 'Net Value' ,0 'Net Rate'";
                        strsql = strsql + " from trx with(" + StrTRXIndex + "nolock) where td_clientcd =@clcd ";
                        strsql = strsql + " and td_Dt between @FromDt and @ToDt ";
                        strsql = strsql + " and  left(td_stlmnt,2)  = '" + Stlmnt + "'";
                        strsql = strsql + " group by td_stlmnt,td_dt,td_scripcd,td_scripnm,td_bsflag ";

                        strsql = strsql + " union All ";
                        strsql = strsql + " select '2' Odr,td_stlmnt,td_stlmnt +' [ '+ ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) + ' ]' Heading ,";
                        strsql = strsql + " ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) as td_dt,td_scripcd,";
                        strsql = strsql + " '0' as [Order#], '0' [Trade#], 'T' Time, ";
                        strsql = strsql + " rtrim(td_scripnm)+' ('+td_scripcd+')' Security, ";
                        strsql = strsql + " 0 as Buy, 0 as Sell, 0 [Market Rate], 0 [Brokerage], 0 [Buy Value], 0 [Sell Value] ,";
                        strsql = strsql + " max(case td_nodel when 'Y' then 'NoDelv' else '' end) [_], ";
                        strsql = strsql + " rtrim(td_scripnm)+ max(case td_ssrno when -1 then 'a' else 'b' end) [Ordr],";
                        strsql = strsql + " td_scripnm ,";
                        strsql = strsql + " Sum(td_bqty-td_sqty) 'Net Qty',";
                        strsql = strsql + " Convert(decimal(15,2),Sum((td_bqty-td_sqty)* td_rate)) 'Net Value',";
                        strsql = strsql + " Convert(decimal(15,2),Round(case When Sum(td_bqty-td_sqty) <> 0 Then Sum((td_bqty-td_sqty)* td_rate)/Sum(td_bqty-td_sqty) else 0 end,2)) 'Net Rate'";
                        strsql = strsql + " from trx with(" + StrTRXIndex + "nolock) where td_clientcd =@clcd ";
                        strsql = strsql + " and td_Dt between @FromDt and @ToDt ";
                        strsql = strsql + " and  left(td_stlmnt,2)  = '" + Stlmnt + "'";
                        strsql = strsql + " group by td_stlmnt, td_dt,td_scripcd,td_scripnm  ";
                        strsql = strsql + " having count(distinct td_bsflag) > 1";

                        strsql = strsql + " union All ";
                        strsql = strsql + " select '3' Odr, td_stlmnt,td_stlmnt +' [ '+ ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) + ' ]',ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) as td_dt,'0',0 , 0, 'C/F', rtrim(td_scripnm)+' ('+td_scripcd+')' Security, ";
                        strsql = strsql + " case sign(sum(td_bqty-td_sqty)) when 1 then 0 else sum(td_bqty-td_sqty) end , ";
                        strsql = strsql + " case sign(sum(td_sqty-td_bqty)) when 1 then 0 else sum(td_bqty-td_sqty) end , convert(decimal(15,2),max(td_makingrt)), 0, ";
                        strsql = strsql + " case sign(sum(td_bqty-td_sqty)) when 1 then 0 else sum((td_bqty-td_sqty)*td_makingrt) end , ";
                        strsql = strsql + " case sign(sum(td_sqty-td_bqty)) when 1 then 0 else sum((td_bqty-td_sqty)*td_makingrt) end ,  'c/f', rtrim(td_scripnm)+'z', td_scripnm";
                        strsql = strsql + " ,0,0,0";
                        strsql = strsql + " from trx with(" + StrTRXIndex + "nolock) ";
                        strsql = strsql + " where td_clientcd =@clcd ";
                        strsql = strsql + " and td_Dt between @FromDt and @ToDt ";
                        strsql = strsql + " and  left(td_stlmnt,2)  = '" + Stlmnt + "'";

                        strsql = strsql + " and td_nodel='Y' and left(td_stlmnt,1)='B' ";
                        strsql = strsql + " group by td_stlmnt,td_dt,td_scripnm, td_scripcd having sum(td_bqty-td_sqty)<>0";//td_stlmnt='" + Stlmnt + "'

                        strsql = strsql + " union all ";
                        strsql = strsql + " select  '4' Odr,sh_stlmnt,sh_stlmnt +' [ '+ ltrim(rtrim(convert(char,convert(datetime,se_stdt),103))) + ' ]',ltrim(rtrim(convert(char,convert(datetime,se_stdt),103))),'Charges',0,0,'Charges', rtrim(sh_desc), 0,0,convert(decimal(15,2),0),0,convert(decimal(15,2),sum(sh_amount)),0 ,'','zzz','' ";
                        strsql = strsql + " ,0,0,0";
                        strsql = strsql + " from Specialcharges with(nolock),settlements with(nolock)  where sh_clientcd=@clcd and sh_stlmnt = se_Stlmnt ";
                        strsql = strsql + " and se_stdt between  @FromDt and @ToDt";
                        strsql = strsql + " and left(sh_stlmnt,2) = '" + Stlmnt + "'";

                        strsql = strsql + " group by sh_stlmnt,se_stdt,sh_desc ";
                        strsql = strsql + " order by  Heading,ordr, Security,Odr ";
                        strsql = strsql.Replace("@clcd", "'" + clientId + "'").Replace("@FromDt", "'" + fromDt + "'").Replace("@ToDt", "'" + fromDt + "'");

                    }
                    ObjDataSet = objUtility.OpenDataSet(strsql);

                    if (ObjDataSet.Tables[0].Rows.Count > 0)
                    {
                        int i = 0;
                        string strstlmnt = string.Empty;
                        decimal TotalBval = 0;
                        decimal TotalSval = 0;
                        decimal TotalDiff = 0;
                        decimal TotalNet = 0;
                        string strHeading = string.Empty;
                        string strCharges = "";

                        string strparmcd = objUtility.GetSysParmSt("ROUNDOFF", "");

                        for (i = 0; i <= ObjDataSet.Tables[0].Rows.Count - 1; i++)
                        {
                            strRefDt = ObjDataSet.Tables[0].Rows[i]["td_dt"].ToString().Trim();

                            if (strstlmnt != string.Empty && strstlmnt != ObjDataSet.Tables[0].Rows[i]["td_stlmnt"].ToString().Trim())
                            {
                                strstlmnt = ObjDataSet.Tables[0].Rows[i]["td_stlmnt"].ToString().Trim();
                                DataRow ObjRow1 = ObjDataSet.Tables[0].NewRow();
                                TotalDiff = Math.Round((decimal)(TotalSval - TotalBval), strparmcd == "Y" ? 0 : 2);
                                if (TotalBval < TotalSval)
                                {
                                    ObjRow1["Security"] = "Due To You :";
                                    ObjRow1["Heading"] = strHeading;
                                    ObjDataSet.Tables[0].Rows.InsertAt(ObjRow1, i);
                                    i = i + 1;
                                }
                                else
                                {
                                    ObjRow1["Security"] = "Due To Us :";
                                    ObjRow1["Heading"] = strHeading;
                                    ObjDataSet.Tables[0].Rows.InsertAt(ObjRow1, i);
                                    i = i + 1;
                                }

                                var apiRes4 = objUtility.mfnRoundoffCashbill(strClient, strRefDt, TotalDiff, Strings.Left(strstlmnt, 1), strCompanyCode);

                                ObjRow1["Net Value"] = objUtility.mfnFormatCurrency(Convert.ToDecimal(apiRes4), 2);

                                TotalBval = 0;
                                TotalSval = 0;
                                TotalDiff = 0;
                            }
                            if (strstlmnt == string.Empty)
                            {
                                strstlmnt = ObjDataSet.Tables[0].Rows[i]["td_stlmnt"].ToString().Trim();
                            }
                            if (strstlmnt == ObjDataSet.Tables[0].Rows[i]["td_stlmnt"].ToString().Trim())
                            {
                                TotalBval = TotalBval + (decimal)ObjDataSet.Tables[0].Rows[i]["Buy Value"];
                                TotalSval = TotalSval + (decimal)ObjDataSet.Tables[0].Rows[i]["Sell Value"];
                                TotalSval = System.Math.Abs(TotalSval);
                                TotalBval = System.Math.Abs(TotalBval);
                            }
                            strHeading = ObjDataSet.Tables[0].Rows[i]["Heading"].ToString().Trim();

                            if (strCharges == "")
                            {
                                if (Conversion.Val(ObjDataSet.Tables[0].Rows[i]["Odr"].ToString().Trim()) == 4)
                                {
                                    strCharges = "Charges";

                                    DataRow ObjRow1 = ObjDataSet.Tables[0].NewRow();
                                    ObjRow1["Heading"] = strHeading;
                                    ObjRow1["Security"] = "Value Before Adding Charges :";
                                    ObjRow1["Net Value"] = objUtility.mfnFormatCurrency(TotalNet, 2);
                                    ObjDataSet.Tables[0].Rows.InsertAt(ObjRow1, i);
                                    i = i + 1;
                                }
                            }

                            TotalNet = TotalNet + (decimal)ObjDataSet.Tables[0].Rows[i]["Net Value"];
                        }

                        DataRow ObjRow2 = ObjDataSet.Tables[0].NewRow();
                        TotalDiff = Math.Round((decimal)(TotalSval - TotalBval), strparmcd == "Y" ? 0 : 2);

                        var apiRes3 = objUtility.mfnRoundoffCashbill(strClient, strRefDt, TotalDiff, Strings.Left(strstlmnt, 1), strCompanyCode);

                        TotalDiff = Convert.ToDecimal(apiRes3);

                        if (TotalBval < TotalSval)
                        {
                            ObjRow2["Security"] = "Due To You :";
                        }
                        else
                        {
                            ObjRow2["Security"] = "Due To Us :";
                        }

                        ObjRow2["Net Value"] = objUtility.mfnFormatCurrency(TotalDiff, 2);
                        ObjRow2["Heading"] = strHeading;
                        ObjDataSet.Tables[0].Rows.InsertAt(ObjRow2, ObjDataSet.Tables[0].Rows.Count);
                        ObjDataSet.AcceptChanges();

                    }
                    else
                    {
                        return ObjDataSet;
                    }
                }
            }

            return ObjDataSet;
        }
        #endregion
        #endregion

        #region Confirmation Handler Method
        // get dropdown menu cumulative details data
        public dynamic GetCumulativeDetailsHandler(string td_clientcd, string StrOrder, string StrScripCode, string Strbsflag, string StrDate, string StrLookup)
        {
            var query = GetQueryCumulativeDetails(td_clientcd, StrOrder, StrScripCode, Strbsflag, StrDate, StrLookup);
            try
            {
                var ds = CommonRepository.OpenDataSetTmp(query);
                if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                {
                    var json = JsonConvert.SerializeObject(ds.Tables[0], Formatting.Indented);
                    return json;
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        // get dropdown menu confirmation details data
        public dynamic GetConfirmationDetailsHandler(string td_clientcd, string StrOrder, string StrLoopUp)
        {
            var query = GetQueryConfirmationDetails(td_clientcd, StrOrder, StrLoopUp);
            try
            {
                var ds = CommonRepository.OpenDataSetTmp(query);
                if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                {
                    var json = JsonConvert.SerializeObject(ds.Tables[0], Formatting.Indented);
                    return json;
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //get confirmation main data
        public dynamic GetConfirmationMainDataHandler(string userId, int lstConfirmationSelectIndex, string tdDt)
        {
            try
            {
                var ds = GetConfirmationMainData(userId, lstConfirmationSelectIndex, tdDt);
                if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                {
                    var json = JsonConvert.SerializeObject(ds.Tables[0], Formatting.Indented);
                    return json;
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }


        #region Confirmation usefull methods
        //Get confirmation main data
        private DataSet GetConfirmationMainData(string userId, int lstConfirmationSelectIndex, string tdDt)
        {
            DataSet ObjDataSet = new DataSet();
            string StrComTradesIndex = "";
            string StrTradesIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
            { StrTradesIndex = "index(idx_trades_clientcd),"; }

            string StrTRXIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'Trx' and b.name", "idx_Trx_Clientcd", true)) == 1)
            { StrTRXIndex = "index(idx_trx_clientcd),"; }

            if (lstConfirmationSelectIndex == 0)
            {
                string strsql = string.Empty;

                if (_configuration["IsTradeWeb"] == "O")
                {
                    //If IsTradeWeb is false means it will connect to TradePlus Database(Live)
                    strsql = " Select 1 as td_order,'Equity : ' + td_stlmnt as td_type, td_stlmnt,td_scripcd,replace(rtrim(ss_Name)+' ('+td_scripcd+')','&','') td_scripnm, ";
                    strsql = strsql + " sum(td_bqty) 'bqty',sum(convert(decimal(15,2),convert(money,td_bqty * td_rate))) 'bvalue', sum(td_sqty) 'sqty', ";
                    strsql = strsql + " sum(convert(decimal(15,4),convert(money,td_sqty * td_rate))) 'svalue', sum(td_bqty - td_sqty) 'netqty', ";
                    strsql = strsql + " sum(convert(decimal(15,4),convert(money,(td_sqty - td_bqty)*td_rate))) 'netvalue', ";
                    strsql = strsql + " cast(convert(money,case when sum(td_bqty - td_sqty)=0 then 0 else sum((td_bqty-td_sqty)*td_rate)/sum(td_bqty-td_sqty) ";
                    strsql = strsql + " end) as decimal(15,2)) 'average', ";
                    strsql = strsql + " cast(sum(td_brokerage*(td_bqty+td_sqty)) as decimal(15,4)) td_Brokerage, ";
                    strsql = strsql + " replace(td_stlmnt+td_dt+td_scripcd,'&','-') 'td_Lookup' ";
                    strsql = strsql + " from trx with(" + StrTRXIndex + "nolock) , securities with (nolock), Settlements with (nolock) ";
                    strsql = strsql + " where td_clientcd= '" + userId + "'  and td_stlmnt = se_stlmnt and td_scripcd = ss_cd and td_dt='" + tdDt + "' ";
                    strsql = strsql + " group by 'Equity : ' + td_stlmnt ,td_stlmnt,td_scripcd,ss_Name ,td_dt";
                }
                else
                {
                    //If IsTradeWeb is true means it will connect to TradeWeb Database
                    strsql = " Select 1 as td_order,'Equity : ' + td_stlmnt as td_type, td_stlmnt,td_scripcd,replace(rtrim(td_scripnm)+' ('+td_scripcd+')','&','') td_scripnm, ";
                    strsql = strsql + " sum(td_bqty) 'bqty',sum(convert(decimal(15,2),convert(money,td_bqty * td_rate))) 'bvalue', sum(td_sqty) 'sqty', ";
                    strsql = strsql + " sum(convert(decimal(15,4),convert(money,td_sqty * td_rate))) 'svalue', sum(td_bqty - td_sqty) 'netqty', ";
                    strsql = strsql + " sum(convert(decimal(15,4),convert(money,(td_sqty - td_bqty)*td_rate))) 'netvalue', ";
                    strsql = strsql + " cast(convert(money,case when sum(td_bqty - td_sqty)=0 then 0 else sum((td_bqty-td_sqty)*td_rate)/sum(td_bqty-td_sqty) ";
                    strsql = strsql + " end) as decimal(15,2)) 'average', ";
                    strsql = strsql + " cast(sum(td_brokerage*(td_bqty+td_sqty)) as decimal(15,4)) td_Brokerage, ";
                    strsql = strsql + " replace(td_stlmnt+td_dt+td_scripcd,'&','-') 'td_Lookup' ";
                    strsql = strsql + " from trx with(" + StrTRXIndex + "nolock), Settlements with (nolock) ";
                    strsql = strsql + " where td_clientcd= '" + userId + "'  and td_stlmnt = se_stlmnt and td_dt='" + tdDt + "' ";
                    strsql = strsql + " group by td_stlmnt,td_scripcd,td_scripnm ,td_dt";
                }
                strsql = strsql + " Union All";
                strsql = strsql + " select case right(sm_prodtype,1) when 'F' then 2 else 3 end, 'Equity '+ ";
                strsql = strsql + " case right(sm_prodtype,1) when 'F' then 'Future' else 'Option' end td_type, 'Exp: '+";
                strsql = strsql + " convert(char(10),convert(datetime,sm_expirydt),105), case right(sm_prodtype,1) when 'F' then '' else";
                strsql = strsql + " ltrim(convert(char(8),sm_strikeprice))+sm_callput end ,replace(rtrim(sm_symbol)+case right(sm_prodtype,1) ";
                strsql = strsql + " when 'F' then '' else ' ('+ltrim(convert(char(8),sm_strikeprice))+sm_callput+sm_optionstyle+')' end,'&','') ,sum(td_bqty) 'bqty', ";
                strsql = strsql + " sum(convert(decimal(15,2),convert(money,td_bqty * td_rate))) 'bvalue', sum(td_sqty) 'sqty',";
                strsql = strsql + " sum(convert(decimal(15,4),convert(money,td_sqty * td_rate))) 'svalue', sum(td_bqty - td_sqty) 'netqty', ";
                strsql = strsql + " sum(convert(decimal(15,4),convert(money,(td_sqty - td_bqty)*td_rate))) 'netvalue', ";
                strsql = strsql + " cast(convert(money,case when sum(td_bqty - td_sqty)=0 then 0 else sum((td_bqty-td_sqty)*td_rate)/sum(td_bqty-td_sqty)";
                strsql = strsql + " end) as decimal(15,2)) 'average' ,";
                strsql = strsql + " cast(sum(td_brokerage*(td_bqty+td_sqty)) as decimal(15,4)) td_brokerage,";
                strsql = strsql + " replace(td_Exchange+td_Segment+td_dt+sm_expirydt+right(sm_prodtype,1)+sm_symbol+'/'+convert(char(9),sm_strikeprice)+sm_callput,'&','-') ";

                if (_configuration["IsTradeWeb"] == "O")
                    strsql = strsql + " from trades with(" + StrTradesIndex + "nolock), series_master with (nolock) ";
                else
                    strsql = strsql + " from trades with(" + StrTradesIndex + "nolock), series_master with (nolock) ";

                strsql = strsql + " where td_clientcd='" + userId + "' and td_seriesid=sm_seriesid and td_Exchange = sm_exchange and td_Segment = sm_Segment and td_dt ='" + tdDt + "' and td_segment in ('F') ";
                strsql = strsql + " group by sm_prodtype,sm_symbol,sm_desc, sm_expirydt, sm_strikeprice, sm_callput,sm_optionstyle,td_dt,td_Exchange,td_Segment";
                strsql = strsql + " Union All";

                strsql = strsql + " select case ex_eaflag when 'E' then 4 else 5 end ,case ex_eaflag when 'E' then 'Exercise' else 'Assignment' ";
                strsql = strsql + " end Td_Type, 'Exp: ' +convert(char(10),convert(datetime,sm_expirydt),105),";
                strsql = strsql + " ltrim( convert(char(8),sm_strikeprice))+sm_callput, replace(rtrim(sm_symbol)+' ('+";
                strsql = strsql + " ltrim( convert(char(8),sm_strikeprice))+sm_callput+sm_optionstyle+')','&',''), sum(ex_aqty) Bqty,  sum(ex_aqty*ex_diffrate) BAmt,";
                strsql = strsql + " sum(ex_eqty) Sqty, sum(ex_eqty*ex_diffrate) SAmt, sum(ex_aqty-ex_eqty) NQty,  sum((ex_aqty-ex_eqty)*ex_diffrate) NAmt,";
                strsql = strsql + " cast(convert(money,case when sum(ex_aqty-ex_eqty)=0 then 0 else sum((ex_aqty-ex_eqty)*ex_diffrate)/sum(ex_aqty-ex_eqty) end) as decimal(15,2)) 'average',";
                strsql = strsql + " cast(sum(ex_brokerage*(ex_eqty+ex_aqty)) as decimal(15,4)) td_Brokerage,replace(ex_exchange+ex_Segment+ex_dt+sm_expirydt+right(sm_prodtype,1)+sm_symbol,'&','-') ";

                strsql = strsql + " from exercise with (nolock), series_master with (nolock)";
                strsql = strsql + " where ex_clientcd='" + userId + "' and ex_exchange=sm_exchange and ex_Segment=sm_Segment and ex_seriesid=sm_seriesid and ex_dt ='" + tdDt + "'";
                strsql = strsql + " group by ex_eaflag, sm_symbol,sm_desc, sm_expirydt, sm_strikeprice, sm_callput,sm_optionstyle,ex_exchange,ex_Segment,ex_dt,sm_prodtype";
                strsql = strsql + " Union All";

                strsql = strsql + " select case right(sm_prodtype,1) when 'F' then 6 else 7 end, case right(sm_prodtype,1) when 'F' then 'Currency Future'";
                strsql = strsql + " else 'Currency Option' end td_type, 'Exp: ' +convert(char(10),convert(datetime,sm_expirydt),105), case right(sm_prodtype,1)";
                strsql = strsql + " when 'F' then '' else convert(char(8),sm_strikeprice)+sm_callput end,replace(sm_symbol,'&',''), sum(td_bqty) ,";
                strsql = strsql + " sum(round(convert(money,td_bqty * td_rate*sm_multiplier),2)) , sum(td_sqty) ,";
                strsql = strsql + " sum(round(convert(money,td_sqty * td_rate*sm_multiplier),2)) , sum(td_bqty - td_sqty) ,";
                strsql = strsql + " sum(round(convert(money,(td_sqty - td_bqty)*td_rate*sm_multiplier),4)) , ";
                strsql = strsql + " cast(convert(money,case when sum(td_bqty - td_sqty)=0 then 0 else sum((td_sqty - td_bqty)*td_rate*sm_multiplier)/sum(td_bqty-td_sqty) end) as decimal(15,2)) ,";
                strsql = strsql + " cast(sum(td_brokerage*(td_bqty+td_sqty)) as decimal(15,4)) td_brokerage, replace(td_exchange+td_Segment+td_dt+sm_expirydt+right(sm_prodtype,1)+sm_symbol+'/'+convert(char(9),sm_strikeprice)+sm_callput,'&','-')";

                if (_configuration["IsTradeWeb"] == "O")
                    strsql = strsql + " from trades with(" + StrTradesIndex + "nolock), series_master with (nolock)";
                else
                    strsql = strsql + " from trades with(" + StrTradesIndex + "nolock), series_master with (nolock)";

                strsql = strsql + " where td_clientcd='" + userId + "' and td_seriesid=sm_seriesid and td_Exchange = sm_exchange and td_Segment = sm_Segment and td_dt ='" + tdDt + "' and td_Segment in ('K')";
                strsql = strsql + " group by sm_prodtype,sm_symbol,sm_desc, sm_expirydt, sm_callput, sm_strikeprice, td_exchange,td_Segment,td_dt  ";

                if (_configuration["IsTradeWeb"] == "T" || (_configuration["IsTradeWeb"] == "O" && _configuration["Commex"] != string.Empty))
                {
                    strsql = strsql + " Union All";
                    strsql = strsql + " select 8, 'Commodity Futures' as  td_type, 'Exp: ' +convert(char(10),convert(datetime,sm_expirydt),105),'', ";
                    strsql = strsql + " replace(sm_symbol,'&',''),sum(td_bqty) ,sum(round(convert(money,td_bqty * td_rate),4)) , sum(td_sqty) ,  ";
                    strsql = strsql + " sum(round(convert(money,td_sqty * td_rate),4)) , sum(td_bqty - td_sqty) ,  ";
                    strsql = strsql + " sum(round(convert(money,(td_sqty - td_bqty)*td_rate),4)) , ";
                    strsql = strsql + " cast(convert(money,case when sum(td_bqty - td_sqty)=0 then 0 else sum((td_sqty - td_bqty)*td_rate)/sum(td_bqty-td_sqty) end) as decimal(15,4)),";
                    strsql = strsql + " cast(sum(td_brokerage*(td_bqty+td_sqty)*sm_multiplier) as decimal(15,4)), replace(td_exchange+td_dt+sm_expirydt+right(sm_prodtype,1)+sm_symbol,'&','-')";


                    if (_configuration["IsTradeWeb"] == "O")
                    {
                        if (_configuration["Commex"] != null && _configuration["Commex"] != string.Empty)
                        {

                            string StrCommexConn = "";
                            StrCommexConn = objUtility.GetCommexConnection();
                            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb(StrCommexConn + ".sysobjects a, " + StrCommexConn + ".sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
                            { StrComTradesIndex = "index(idx_trades_clientcd),"; }
                            strsql = strsql + " from " + StrCommexConn + ".trades with(" + StrComTradesIndex + "nolock)  ," + StrCommexConn + ".Series_master with (nolock) ";
                        }
                    }
                    else
                    {
                        string StrCTradesIndex = "";
                        if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'Ctrades' and b.name", "idx_Ctrades_clientcd", true)) == 1)
                        { StrCTradesIndex = "index(idx_Ctrades_clientcd),"; }
                        strsql = strsql + " from Ctrades with (" + StrCTradesIndex + "nolock),Cseries_master with (nolock)";
                    }
                    strsql = strsql + " where td_clientcd='" + userId + "' and td_seriesid=sm_seriesid and td_Exchange = sm_exchange ";
                    strsql = strsql + " and td_dt ='" + tdDt + "'";
                    strsql = strsql + " group by sm_prodtype,sm_symbol,sm_desc, sm_expirydt, td_exchange ";
                    strsql = strsql + " ,td_dt";
                }
                strsql = strsql + " Order by td_order,td_stlmnt,td_scripnm ";

                ObjDataSet = objUtility.OpenDataSet(strsql);
            }
            else if (lstConfirmationSelectIndex == 1) //confirmation
            {
                string Strsql = string.Empty;

                if (_configuration["IsTradeWeb"] == "O")//live
                {
                    Strsql = " SELECT 1 as orderid,1 as td_order,'Equity : ' + td_stlmnt as td_type,td_stlmnt, td_scripcd,replace(ss_name,'&','') as td_scripnm, td_bqty as bqty, td_sqty as sqty, cast((td_marketrate) as decimal(15,4)) as rate, cast((td_rate) as decimal(15,4)) as netrate,cast((((td_bqty + td_sqty) * td_rate)) * (Case td_bsflag when 'S' then '-1' else '1' end ) as decimal(15,2)) amount,cast((td_brokerage)as decimal(15,4)) td_brokerage,replace(td_stlmnt+td_dt+td_scripcd,'&','-') as td_lookup,td_bsflag ";
                    Strsql = Strsql + " FROM trx with(" + StrTRXIndex + "nolock), Settlements with (nolock),securities with (nolock) ";
                    Strsql = Strsql + " WHERE td_clientcd='" + userId + "'  and td_stlmnt = se_stlmnt and td_dt='" + tdDt + "' and td_scripcd=ss_cd";
                }
                else
                {
                    Strsql = " SELECT   1 as orderid,1 as td_order,'Equity : ' + td_stlmnt as td_type,td_stlmnt, td_scripcd,  replace(td_scripnm,'&','') td_scripnm, td_bqty as bqty, td_sqty as sqty, cast((td_marketrate) as decimal(15,4)) as rate, cast((td_rate) as decimal(15,4)) as netrate,cast((((td_bqty + td_sqty) * td_rate)) * (Case td_bsflag when 'S' then '-1' else '1' end )  as decimal(15,2)) amount,cast((td_brokerage)as decimal(15,4)) td_brokerage,replace(td_stlmnt+td_dt+td_scripcd,'&','-') as td_lookup,td_bsflag ";
                    Strsql = Strsql + " FROM trx with(" + StrTRXIndex + "nolock), Settlements with (nolock)   ";
                    Strsql = Strsql + " WHERE td_clientcd='" + userId + "'  and td_stlmnt = se_stlmnt and td_dt='" + tdDt + "' "; //and td_scripcd=ss_cd

                    //ADDED at 23-May-2011

                    Strsql = Strsql + " union all ";
                    Strsql = Strsql + " select 2 as orderid,1,'Equity : ' + sh_stlmnt,sh_stlmnt,'', replace(rtrim(sh_desc),'&','') ,0,0 as sqty,0 as rate,0 as netrate,cast((sh_amount) as decimal(15,2))   as amount,";
                    Strsql = Strsql + " 0 as brokerage, '' as lookup,'' ";
                    Strsql = Strsql + " from Specialcharges with (nolock),settlements with (nolock) where sh_clientcd='" + userId + "' ";
                    Strsql = Strsql + " and se_stdt='" + tdDt + "' ";
                    Strsql = Strsql + " and sh_stlmnt = se_Stlmnt   ";
                    Strsql = Strsql + " group by sh_stlmnt ,sh_amount,sh_desc  ";
                    /////------------------------
                }

                Strsql = Strsql + " union all ";
                //Strsql = Strsql + " Select  3,2 as td_order,case right(sm_prodtype,1) when 'F' then 'Future' else 'Option' end td_type,'',sm_symbol,sm_desc,td_bqty as bqty, ";
                Strsql = Strsql + " Select  3,2 as td_order,Case td_segment when 'F' then case TD_EXCHANGE When 'N' Then 'NSE F&O' When 'B' Then 'BSE F&O' end  when 'K' then case TD_EXCHANGE When 'M' Then 'MCX FX' When 'N' Then 'NSE FX' end end td_type,'',sm_symbol,replace(sm_desc,'&',''),td_bqty as bqty, ";
                Strsql = Strsql + " td_sqty as sqty, cast((td_marketrate)as decimal(15,4)) as diffrate,cast((td_rate)as decimal(15,4)) as netrate,convert(decimal(15,2),(td_bqty - td_sqty) * td_rate * sm_multiplier)  as amount,cast((td_brokerage)as decimal(15,4)),replace((td_exchange+td_Segment+td_dt+sm_expirydt+right(sm_prodtype,1)+sm_symbol+'/'+convert(char(9),sm_strikeprice)+sm_callput),'&','-') as td_lookup,td_bsflag from trades with(" + StrTradesIndex + "nolock),series_master with (nolock) where td_seriesid=sm_seriesid and td_Exchange = sm_exchange and td_Segment = sm_Segment and td_clientcd='" + userId + "'  and  ";
                Strsql = Strsql + " td_dt ='" + tdDt + "' and td_trxflag='N' ";

                if (_configuration["IsTradeWeb"] == "T")//
                {
                    //ADDED at 23-May-2011
                    Strsql = Strsql + " union all ";
                    //Strsql = Strsql + " select 4,2, '', '','' as symbol, ";
                    Strsql = Strsql + " select 4,2,";
                    Strsql = Strsql + " Case fc_Segment  ";
                    Strsql = Strsql + "    when 'F' then case fc_exchange When 'N' Then 'NSE F&O' When 'B' Then 'BSE F&O' end  ";
                    Strsql = Strsql + "    when 'K' then case fc_exchange When 'M' Then 'MCX FX' When 'N' Then 'NSE FX' end ";
                    Strsql = Strsql + " end td_type , '','' as symbol, ";
                    Strsql = Strsql + " replace(fc_desc,'&',''),0,0,0,0,cast((sum(fc_amount)) as decimal (15,4)),0,'',''";
                    Strsql = Strsql + " from fspecialcharges with (nolock) where fc_clientcd='" + userId + "' and fc_exchange in ('B','N','M') ";
                    Strsql = Strsql + " and fc_dt='" + tdDt + "' group by fc_dt,fc_dt,fc_desc,fc_exchange,fc_Segment ";
                    /////------------------------
                }

                if (_configuration["IsTradeWeb"] == "O")
                {
                    if (_configuration["Commex"] != null && _configuration["Commex"] != string.Empty)
                    {

                        string StrCommexConn = "";
                        StrCommexConn = objUtility.GetCommexConnection();

                        if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb(StrCommexConn + ".sysobjects a, " + StrCommexConn + ".sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
                        { StrComTradesIndex = "index(idx_trades_clientcd),"; }

                        Strsql = Strsql + " union all ";
                        Strsql = Strsql + " Select 5,3,case ex_eaflag when 'E' then 'Exercise' else 'Assignment' end td_type,'',sm_symbol,replace(sm_desc,'&',''), (ex_eqty * (-1)) as bqty, (ex_aqty * (-1)) as sqty, cast((ex_diffrate) as decimal(15,4)) as diffrate, ";
                        Strsql = Strsql + " cast((ex_mainbrdiffrate) as decimal(15,4)) as netrate,0,cast((ex_brokerage) as decimal(15,4)),replace((ex_exchange+ex_Segment+ex_dt+sm_expirydt+right(sm_prodtype,1)+sm_symbol+'/'+convert(char(9),sm_strikeprice)+sm_callput),'&','-') as td_lookup,'' from exercise with (nolock),series_master with (nolock) where ex_exchange=sm_exchange and sm_Segment=ex_Segment and  sm_seriesid=ex_seriesid and ex_clientcd='" + userId + "'  and ex_dt ='" + tdDt + "' ";
                        Strsql = Strsql + " union all ";
                        Strsql = Strsql + " Select 6,4 as td_order,CASE TD_EXCHANGE When 'N' Then 'NCDX Commodities' When 'M' Then 'MCX Commodities' When 'S' Then 'NSEL Commodities' When 'D' Then 'NSX Commodities' end as td_type,'',sm_symbol,replace(sm_desc,'&',''),td_bqty as bqty, ";
                        Strsql = Strsql + " td_sqty as sqty,cast((td_marketrate) as decimal(15,4)) as diffrate,cast((td_rate)as decimal(15,4)) as netrate,convert(decimal(15,2),td_rate*(td_bqty-td_sqty)*sm_multiplier),cast((td_brokerage)as decimal(15,4)),replace(td_exchange+td_dt+sm_expirydt+right(sm_prodtype,1)+sm_symbol,'&','-') as td_lookup,td_bsflag ";
                        Strsql = Strsql + " from " + StrCommexConn + ".trades with(" + StrComTradesIndex + "nolock) ," + StrCommexConn + ".series_master with (nolock) ";
                        Strsql = Strsql + " where sm_seriesid=td_seriesid and sm_exchange=td_exchange ";
                    }
                }
                else
                {
                    string StrCTradesIndex = "";
                    if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'Ctrades' and b.name", "idx_Ctrades_clientcd", true)) == 1)
                    { StrCTradesIndex = "index(idx_Ctrades_clientcd),"; }

                    Strsql = Strsql + " union all ";
                    Strsql = Strsql + " Select 5,3,case ex_eaflag when 'E' then 'Exercise' else 'Assignment' end td_type,'',sm_symbol,replace(sm_desc,'&',''), (ex_eqty * (-1)) as bqty, (ex_aqty * (-1)) as sqty, cast((ex_diffrate) as decimal(15,4)) as diffrate, ";
                    Strsql = Strsql + " cast((ex_mainbrdiffrate) as decimal(15,4)) as netrate,0,cast((ex_brokerage) as decimal(15,2)),replace((ex_exchange+ex_Segment+ex_dt+sm_expirydt+right(sm_prodtype,1)+sm_symbol+'/'+convert(char(9),sm_strikeprice)+sm_callput),'&','-') as td_lookup,'' from exercise with (nolock),series_master with (nolock) where ex_exchange=sm_exchange and sm_Segment=ex_Segment and  sm_seriesid=ex_seriesid and ex_clientcd='" + userId + "'  and ex_dt ='" + tdDt + "' ";
                    Strsql = Strsql + " union all ";
                    Strsql = Strsql + " Select 6,4 as td_order,CASE TD_EXCHANGE When 'N' Then 'NCDX Commodities' When 'M' Then 'MCX Commodities' When 'S' Then 'NSEL Commodities' When 'D' Then 'NSX Commodities' end as td_type,'',sm_symbol,replace(sm_desc,'&',''),td_bqty as bqty, ";
                    Strsql = Strsql + " td_sqty as sqty,cast((td_marketrate) as decimal(15,4)) as diffrate,cast((td_rate)as decimal(15,4)) as netrate,convert(decimal(15,2),td_rate*(td_bqty-td_sqty)*sm_multiplier),cast((td_brokerage)as decimal(15,4)),replace(td_exchange+td_dt+sm_expirydt+right(sm_prodtype,1)+sm_symbol,'&','-') as td_lookup,td_bsflag ";
                    Strsql = Strsql + " from ctrades with (" + StrCTradesIndex + "nolock),cseries_master with (nolock) where sm_seriesid=td_seriesid and sm_exchange=td_exchange  ";
                }
                // Strsql = Strsql + " from Ctrades with (nolock),Cseries_master with (nolock) ";
                Strsql = Strsql + " and td_clientcd='" + userId + "'  and  ";
                Strsql = Strsql + " td_dt ='" + tdDt + "' and td_trxflag='N' ";

                if (_configuration["IsTradeWeb"] == "T")//
                {
                    //ADDED at 23-May-2011
                    Strsql = Strsql + " union all ";
                    Strsql = Strsql + " select 7,4 as td_order, CASE fc_exchange When 'N' Then 'NCDX Commodities' When 'M' Then 'MCX Commodities' When 'S' Then 'NSEL Commodities' When 'D' Then 'NSX Commodities' end as td_type,'','',replace(fc_desc,'&',''),0,";
                    Strsql = Strsql + " 0,0,0,0,cast((fc_amount) as decimal(15,2)),'','' ";
                    Strsql = Strsql + " from cfspecialcharges with (nolock) where fc_clientcd = '" + userId + "' and fc_dt = '" + tdDt + "' ";
                    Strsql = Strsql + " group by fc_exchange,fc_amount,fc_desc ";
                    /////------------------------
                }

                Strsql = Strsql + " order by td_type,orderid,td_order,td_stlmnt,td_scripnm ";

                ObjDataSet = objUtility.OpenDataSet(Strsql);
            }

            return ObjDataSet;
        }

        // get dropdown menu cumulative details data
        public string GetQueryCumulativeDetails(string td_clientcd, string StrOrder, string StrScripCode, string Strbsflag, string StrDate, string StrLookup)
        {
            string Strsql = "";
            string StrTRXIndex = string.Empty;
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'Trx' and b.name", "idx_Trx_Clientcd", true)) == 1)
            { StrTRXIndex = "index(idx_trx_clientcd),"; }

            string StrTradesIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
            { StrTradesIndex = "index(idx_trades_clientcd),"; }


            if (StrOrder == "1")
            {
                if (_configuration["IsTradeWeb"] == "O")
                {
                    //If IsTradeWeb is false means it will connect to TradePlus Database(Live)
                    Strsql = " Select 1 as td_order,'Equity' as td_type, td_stlmnt,td_scripcd,rtrim(ss_Name)+' ('+td_scripcd+')' td_scripnm, ";
                    Strsql = Strsql + " sum(td_bqty) 'bqty',sum(round(convert(money,td_bqty * td_rate),4)) 'bvalue', sum(td_sqty) 'sqty', ";
                    Strsql = Strsql + " sum(round(convert(money,td_sqty * td_rate),4)) 'svalue', sum(td_bqty - td_sqty) 'netqty', ";
                    Strsql = Strsql + " sum(round(convert(money,(td_sqty - td_bqty)*td_rate),4)) 'netvalue', ";
                    Strsql = Strsql + " cast(convert(money,case when sum(td_bqty - td_sqty)=0 then 0 else sum((td_bqty-td_sqty)*td_rate)/sum(td_bqty-td_sqty) ";
                    Strsql = Strsql + " end) as decimal(15,2)) 'average', ";
                    Strsql = Strsql + " cast(sum(td_brokerage*(td_bqty+td_sqty)) as decimal(15,4)) td_Brokerage, ";
                    Strsql = Strsql + " td_stlmnt+td_dt+td_scripcd 'td_Lookup' ";
                    Strsql = Strsql + " from Trx with (nolock), securities with (nolock), Settlements with (nolock) ";
                    Strsql = Strsql + " where td_clientcd= '" + td_clientcd + "' and td_stlmnt = se_stlmnt and td_scripcd = ss_cd and td_dt='" + StrDate + "' ";
                    if (StrScripCode.Trim() != "")
                    { Strsql += " and td_scripcd = '" + StrScripCode.Trim() + "'"; }
                    if (Strbsflag.Trim() != "")
                    { Strsql += " and td_bsflag = '" + Strbsflag.Trim() + "'"; }
                    Strsql = Strsql + " group by td_stlmnt,td_scripcd,ss_Name ,td_dt";
                }
                else
                {
                    Strsql = "Select 1 as td_order,'Equity' as td_type, td_stlmnt,td_scripcd,rtrim(td_scripnm)+' ('+td_scripcd+')' td_scripnm,";
                    Strsql = Strsql + " sum(td_bqty) 'bqty',sum(round(convert(money,td_bqty * td_rate),4)) 'bvalue', sum(td_sqty) 'sqty', ";
                    Strsql = Strsql + " sum(round(convert(money,td_sqty * td_rate),4)) 'svalue', sum(td_bqty - td_sqty) 'netqty', ";
                    Strsql = Strsql + " sum(round(convert(money,(td_sqty - td_bqty)*td_rate),4)) 'netvalue',  ";
                    Strsql = Strsql + " cast(convert(money,case when sum(td_bqty - td_sqty)=0 then 0 else ";
                    Strsql = Strsql + " sum((td_bqty-td_sqty)*td_rate)/sum(td_bqty-td_sqty)  end) as decimal(15,2)) 'average', ";
                    Strsql = Strsql + " cast(sum(td_brokerage*(td_bqty+td_sqty)) as decimal(15,4)) td_Brokerage,  td_stlmnt+td_dt+td_scripcd 'td_Lookup' ";
                    Strsql = Strsql + " from Trx with(" + StrTRXIndex + "nolock) , Settlements with (nolock)  where td_clientcd= '" + td_clientcd + "'  ";
                    Strsql = Strsql + " and td_stlmnt = se_stlmnt  and td_dt='" + StrDate + "' and td_stlmnt+td_dt+td_scripcd='" + StrLookup + "' ";
                    if (Strbsflag.Trim() != "")
                    { Strsql += " and td_bsflag = '" + Strbsflag.Trim() + "'"; }
                    Strsql = Strsql + "  group by td_stlmnt,td_scripcd,td_scripnm ,td_dt ";
                }
            }
            else if (StrOrder == "2")
            {
                Strsql = "select case right(sm_prodtype,1) when 'F' then 2 else 3 end, 'Equity '+  case right(sm_prodtype,1) ";
                Strsql = Strsql + " when 'F' then 'Future' else 'Option' end td_type, 'Exp: '+ convert(char(10),convert(datetime,sm_expirydt),105) as td_stlmnt, case right(sm_prodtype,1) ";
                Strsql = Strsql + " when 'F' then '' else ltrim(convert(char(8),sm_strikeprice))+sm_callput end ,rtrim(sm_symbol)+case right(sm_prodtype,1)  when 'F' then ''  else ' ('+ltrim(convert(char(8),sm_strikeprice))+sm_callput+')' end as td_Scripnm ,sum(td_bqty) 'bqty',  ";
                Strsql = Strsql + " sum(round(convert(money,td_bqty * td_rate),4)) 'bvalue', sum(td_sqty) 'sqty',";
                Strsql = Strsql + " sum(round(convert(money,td_sqty * td_rate),4)) 'svalue', sum(td_bqty - td_sqty) 'netqty',   sum(round(convert(money,(td_sqty - td_bqty)*td_rate),4)) 'netvalue', ";
                Strsql = Strsql + " cast(convert(money,case when sum(td_bqty - td_sqty)=0 then 0 else  sum((td_bqty-td_sqty)*td_rate)/sum(td_bqty-td_sqty) end) as decimal(15,2)) 'average' , ";
                Strsql = Strsql + " cast(sum(td_brokerage*(td_bqty*td_sqty)) as decimal(15,4)) td_brokerage,  'N'+td_dt+sm_expirydt+right(sm_prodtype,1)+ sm_symbol+'/'+convert(char(9),sm_strikeprice)+sm_callput ";

                if (_configuration["IsTradeWeb"] == "O")
                { Strsql = Strsql + " from trades with (nolock) , series_master with (nolock) "; }
                else
                { Strsql = Strsql + " from  trades with(" + StrTradesIndex + "nolock), series_master with (nolock) "; }

                Strsql = Strsql + " where td_clientcd='" + td_clientcd + "' and  td_exchange+td_segment+td_dt+sm_expirydt+right(sm_prodtype,1)+ sm_symbol+'/'+convert(char(9),sm_strikeprice)+sm_callput='" + StrLookup + "' and td_seriesid=sm_seriesid and td_trxFlag = 'N' and td_dt='" + StrDate + "'";
                Strsql = Strsql + " group by sm_prodtype,sm_symbol,sm_desc, sm_expirydt, sm_strikeprice, sm_callput,td_dt ";
                Strsql = Strsql + " Union All ";

                Strsql = Strsql + " select case ex_eaflag when 'E' then 4 else 5 end ,case ex_eaflag when 'E' then 'Exercise' else 'Assignment'  end Td_Type, 'Exp: ' +convert(char(10), ";
                Strsql = Strsql + " convert(datetime,sm_expirydt),105), ltrim( convert(char(8),sm_strikeprice))+sm_callput,   rtrim(sm_symbol)+' ('+ ltrim( convert(char(8),sm_strikeprice))+sm_callput+')', sum(ex_aqty) Bqty, ";
                Strsql = Strsql + " sum(ex_aqty*ex_diffrate) BAmt, sum(ex_eqty) Sqty, sum(ex_eqty*ex_diffrate) SAmt, sum(ex_aqty-ex_eqty) NQty,   sum((ex_aqty-ex_eqty)*ex_diffrate) NAmt, ";
                Strsql = Strsql + " cast(convert(money,case when sum(ex_aqty-ex_eqty)=0 then 0  else sum((ex_aqty-ex_eqty)*ex_diffrate)/sum(ex_aqty-ex_eqty) end) as decimal(15,2)) 'average', ";
                Strsql = Strsql + " cast(sum(ex_brokerage*(ex_eqty+ex_aqty)) as decimal(15,4)) td_Brokerage, ex_exchange+ex_Segment+ex_dt+sm_expirydt+ right(sm_prodtype,1)+sm_symbol ";
                Strsql = Strsql + " from exercise with (nolock), series_master with (nolock) where ex_clientcd='" + td_clientcd + "' and  ";
                Strsql = Strsql + " ex_exchange=sm_exchange and ex_Segment=sm_Segment and ex_seriesid=sm_seriesid and  ex_exchange+ex_Segment+ex_dt+sm_expirydt+ right(sm_prodtype,1)+sm_symbol='" + StrLookup + "' and ex_dt='" + StrDate + "' group by ex_eaflag, sm_symbol,sm_desc, ";
                Strsql = Strsql + " sm_expirydt, sm_strikeprice, sm_callput,ex_exchange,ex_Segment,ex_dt,sm_prodtype  ";
            }

            else if (StrOrder == "3")
            {
                Strsql = " select case right(sm_prodtype,1) when 'F' then 6 else 7 end,  case right(sm_prodtype,1) when 'F' then 'CurrentFuture' else 'CurrentOption' end td_type,";
                Strsql = Strsql + " 'Exp: ' +convert(char(10),convert(datetime,sm_expirydt),105) as td_stlmnt,  case right(sm_prodtype,1) when 'F' then '' else convert(char(8),sm_strikeprice)+sm_callput end,sm_symbol,  ";
                Strsql = Strsql + " sum(td_bqty) , sum(round(convert(money,td_bqty * td_rate*sm_multiplier),4)) , sum(td_sqty) ,  sum(round(convert(money,td_sqty * td_rate*sm_multiplier),4)) , sum(td_bqty - td_sqty) , ";
                Strsql = Strsql + " sum(round(convert(money,(td_sqty - td_bqty)*td_rate*sm_multiplier),2)) ,   cast(convert(money,case when sum(td_bqty - td_sqty)=0 then 0  ";
                Strsql = Strsql + " else sum((td_sqty - td_bqty)*td_rate*sm_multiplier)/sum(td_bqty-td_sqty) end) as decimal(15,4)) ,  cast(sum(td_brokerage*(td_bqty+td_sqty)) as decimal(15,4)) td_brokerage, ";
                Strsql = Strsql + " (td_exchange+td_Segment+td_dt+sm_expirydt+right(sm_prodtype,1)+sm_symbol+'/'+convert(char(9),sm_strikeprice)+sm_callput)  as td_lookup ";
                if (_configuration["IsTradeWeb"] == "O")
                { Strsql = Strsql + " from trades with (nolock), series_master with (nolock)"; }
                else
                { Strsql = Strsql + " from trades with(" + StrTradesIndex + "nolock), series_master with (nolock) "; }

                Strsql = Strsql + " where td_clientcd='" + td_clientcd + "' and td_dt='" + StrDate + "' and td_exchange+td_Segment+td_dt+sm_expirydt+right(sm_prodtype,1)+sm_symbol+'/'+convert(char(9),sm_strikeprice)+sm_callput='" + StrLookup + "' and td_seriesid=sm_seriesid ";
                Strsql = Strsql + " and td_exchange<>'N' group by sm_prodtype,sm_symbol,sm_desc, sm_expirydt, sm_callput,  sm_strikeprice, td_exchange, td_Segment,td_dt   ";

            }
            else if (StrOrder == "4")
            {
                Strsql = " select 8, 'Commodity Futures' as  td_type, 'Exp: ' +convert(char(10),convert(datetime,sm_expirydt),105) as td_stlmnt,'', ";
                Strsql = Strsql + " sm_symbol as td_Scripnm,sum(td_bqty) AS bqty ,sum(round(convert(money,td_bqty * td_rate),4)) as bvalue , sum(td_sqty) as sqty , ";
                Strsql = Strsql + " sum(round(convert(money,td_sqty * td_rate),4)) AS svalue , sum(td_bqty - td_sqty) as netqty ,  ";
                Strsql = Strsql + " sum(round(convert(money,(td_sqty - td_bqty)*td_rate),4)) as netvalue ,   cast(convert(money,case when sum(td_bqty - td_sqty)=0 then 0  ";
                Strsql = Strsql + " else sum((td_sqty - td_bqty)*td_rate)/sum(td_bqty-td_sqty) end) as decimal(15,2)) as average,  cast(sum(td_brokerage*(td_bqty+td_sqty)*sm_multiplier) as decimal(15,4)) td_brokerage, ";
                Strsql = Strsql + " (td_exchange+td_dt+sm_expirydt+right(sm_prodtype,1)+sm_symbol) as td_lookup ";

                if (_configuration["IsTradeWeb"] == "O")
                {
                    if (objUtility.GetWebParameter("Commex") != null && objUtility.GetWebParameter("Commex") != string.Empty)
                    {

                        string StrCommexConn = "";
                        StrCommexConn = objUtility.GetCommexConnection();
                        Strsql = Strsql + " from " + StrCommexConn + ".Trades," + StrCommexConn + ".Series_master";
                    }
                }
                else
                {
                    Strsql = Strsql + " from Ctrades with (nolock),Cseries_master with (nolock)";
                }
                Strsql = Strsql + " where td_clientcd='" + td_clientcd + "'  ";
                Strsql = Strsql + " and td_seriesid=sm_seriesid and td_dt ='" + StrDate + "' and  td_exchange+td_dt+sm_expirydt+right(sm_prodtype,1)+sm_symbol='" + StrLookup + "' group by sm_prodtype,sm_symbol,sm_desc, ";
                Strsql = Strsql + "  sm_expirydt, td_exchange,td_dt ";
            }

            return Strsql;
        }

        // get dropdown menu confirmation details data
        public string GetQueryConfirmationDetails(string td_clientcd, string StrOrder, string StrLoopUp)
        {
            string Strsql = "";
            string StrTradesIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
            { StrTradesIndex = "index(idx_trades_clientcd),"; }

            string StrTRXIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'Trx' and b.name", "idx_Trx_Clientcd", true)) == 1)
            { StrTRXIndex = "index(idx_trx_clientcd),"; }

            if (StrOrder == "1")
            {
                Strsql = " select td_orderid,  td_tradeid,td_time, cast(convert(money,td_marketrate) as decimal(15,4)) td_marketrate, (td_bqty-td_sqty) td_qty, cast(td_brokerage as decimal(15,4)) td_brokerage, convert(decimal(15,4),td_rate) td_rate, convert(decimal(15,4),td_brokerage*(td_bqty+td_sqty)) td_TtlBrok, convert(decimal (15,4),td_rate*(td_bqty-td_sqty)) NetValue";
                Strsql = Strsql + " from trx with (" + StrTRXIndex + "nolock) where td_clientcd='" + td_clientcd + "' and td_stlmnt+td_dt+td_scripcd='" + StrLoopUp + "'";
            }

            if (StrOrder == "2" || StrOrder == "3" || StrOrder == "6" || StrOrder == "7")
            {
                Strsql = Strsql + " select td_orderid,  td_tradeid,td_time, cast(convert(money,td_marketrate) as decimal(15,4)) td_marketrate, (td_bqty-td_sqty) td_qty, cast(td_brokerage as decimal(15,4)) td_brokerage, td_rate, cast((td_brokerage*(td_bqty+td_sqty))as decimal(15,4)) td_TtlBrok, td_rate*(td_bqty-td_sqty) NetValue";
                Strsql = Strsql + " from trades with (" + StrTradesIndex + "nolock), series_master with (nolock) where sm_seriesid=td_seriesid and sm_exchange=td_exchange and sm_Segment=td_segment and ";
                Strsql = Strsql + " td_clientcd='" + td_clientcd + "' and td_exchange+td_Segment+td_dt+sm_expirydt+right(sm_prodtype,1)+sm_symbol+'/'+convert(char(9),sm_strikeprice)+sm_callput='" + StrLoopUp + "'";
            }

            if (StrOrder == "4" || StrOrder == "5")
            {
                Strsql = Strsql + " select convert(money,ex_mainbrdiffrate) ex_mainbrdiffrate, (ex_aqty-ex_eqty) td_qty, ex_brokerage, ex_diffrate, cast((ex_brokerage*(ex_eqty+ex_aqty))as decimal(15,4)) td_TtlBrok, ex_diffrate*(ex_aqty-ex_eqty) NetValue";
                Strsql = Strsql + " from exercise with (nolock), series_master with (nolock) where sm_seriesid=ex_seriesid and sm_exchange=ex_exchange and sm_Segment=ex_Segment and";
                Strsql = Strsql + " ex_clientcd='" + td_clientcd + "' and ex_dt='20100624' and sm_symbol='NTPC' and sm_expirydt='20100729' and sm_strikeprice=200.00 and sm_callput='C'";
            }
            if (StrOrder == "8")
            {
                Strsql = Strsql + " select td_orderid,  td_tradeid,td_time, convert(money,td_marketrate) td_marketrate, (td_bqty-td_sqty) td_qty, td_brokerage, td_rate,cast((td_brokerage*(td_bqty+td_sqty))as decimal(15,2)) td_TtlBrok, td_rate*(td_bqty-td_sqty) NetValue";

                if (_configuration["IsTradeWeb"] == "O")
                {
                    string StrComTradesIndex = string.Empty;

                    string StrCommexConn = "";
                    StrCommexConn = objUtility.GetCommexConnection();


                    if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb(StrCommexConn + ".sysobjects a, " + StrCommexConn + ".sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
                    { StrComTradesIndex = "index(idx_trades_clientcd),"; }

                    Strsql = Strsql + " from " + StrCommexConn + ".Trades with(" + StrComTradesIndex + "nolock)," + StrCommexConn + ".series_master with (nolock) ";
                    Strsql = Strsql + " where sm_seriesid=td_seriesid and sm_exchange=td_exchange and";
                }
                else
                {

                    string StrCTradesIndex = "";
                    if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'Ctrades' and b.name", "idx_Ctrades_clientcd", true)) == 1)
                    { StrCTradesIndex = "index(idx_Ctrades_clientcd),"; }

                    Strsql = Strsql + " from ctrades with (" + StrCTradesIndex + "nolock),cseries_master with (nolock) where sm_seriesid=td_seriesid and sm_exchange=td_exchange and";
                }

                Strsql = Strsql + " td_clientcd='" + td_clientcd + "' and td_exchange+td_dt+sm_expirydt+right(sm_prodtype,1)+sm_symbol='" + StrLoopUp + "' ";
            }
            return Strsql;
        }
        #endregion
        #endregion

        #region Reset Password method
        public string ResetPassword(string userId)
        {
            string Strsql = "";
            string[] strSMSParamVal = new string[5];
            string strSMSParameter = "SMSUSERID/SMSPWD/SMSSENDER/SMSLENGTH/SMSLINK";
            string strvalue = string.Empty;
            Boolean blnSMS = false;
            string strContrycd = string.Empty;
            string strVal = "";
            if (_configuration["IsTradeWeb"] == "O")
            {
                for (int i = 0; i <= 4; i++)
                {
                    strvalue = strSMSParameter.Split('/')[i];
                    strVal = objUtility.fnFireQueryTradeWeb("sysparameter", "SP_SYSVALUE", "sp_parmcd", strvalue, true);
                    ////  End API Logic
                    strSMSParamVal[i] = strVal;
                }
                if (strSMSParamVal[0] != "" && strSMSParamVal[1] != "" && strSMSParamVal[2] != "" && strSMSParamVal[3] != "" && strSMSParamVal[4] != "")
                {
                    blnSMS = true;
                }
            }
            else
            {
                if (Convert.ToInt32(objUtility.fnFireQueryTradeWeb("webparameter", "count(0)", "sp_parmcd", "SMSSETUP", true)) > 0)
                {
                    strvalue = objUtility.fnFireQueryTradeWeb("webparameter", "SP_SYSVALUE", "sp_parmcd", "SMSSETUP", true);
                    strSMSParamVal[0] = strvalue.Split('~')[1];
                    strSMSParamVal[1] = strvalue.Split('~')[2];
                    strSMSParamVal[2] = strvalue.Split('~')[3];
                    strSMSParamVal[3] = strvalue.Split('~')[4];
                    strSMSParamVal[4] = strvalue.Split('~')[0];
                    strContrycd = strvalue.Split('~')[5];
                    blnSMS = true;
                }
            }

            if (blnSMS == true)
            {
                try
                {
                    string strMobileNo = string.Empty;
                    string strPass = string.Empty;
                    string strMsgTxt = string.Empty;
                    string strUser = string.Empty;
                    string strMobile = string.Empty;
                    string strUserCd = string.Empty;
                    string strFromNo1 = "";
                    string strFromNo2 = "";
                    DataSet ds = new DataSet();

                    Strsql = "Select * from Client_master where cm_cd = '" + userId.Trim() + "'";
                    ds = objUtility.OpenDataSet(Strsql);

                    if (ds.Tables[0].Rows.Count == 0)
                    {
                        return "No Records Found";
                    }
                    if (ds.Tables[0].Rows[0]["cm_mobile"].ToString().Trim() == "")
                    {
                        return "Mobile Number is not available";
                    }
                    strMobileNo = ds.Tables[0].Rows[0]["cm_mobile"].ToString().Trim();
                    strPass = ds.Tables[0].Rows[0]["cm_pwd"].ToString().Trim();
                    if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("Sysparameter", "count(0)", "sp_parmcd", "SMSTWEBPWD", true)) > 0)
                    {
                        strMsgTxt = objUtility.fnFireQueryTradeWeb("sysparameter", "SP_SYSVALUE", "sp_parmcd", "SMSTWEBPWD", true);
                    }
                    else if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("Webparameter", "count(0)", "sp_parmcd", "SMSGETPWD", true)) > 0)
                    {
                        strMsgTxt = objUtility.fnFireQueryTradeWeb("Webparameter", "SP_SYSVALUE", "sp_parmcd", "SMSGETPWD", true);
                    }
                    else
                    {
                        strMsgTxt = "Your password to access TradeWeb is <Password> login ID : <Client>";
                    }
                    strUserCd = ds.Tables[0].Rows[0]["cm_cd"].ToString().Trim();

                    StringBuilder sb = new StringBuilder(strMobileNo);
                    for (int i = 2; i <= strMobileNo.Length - 3; i++)
                    {
                        sb[i] = 'x';
                    }
                    strMobile = sb.ToString();
                    sb = new StringBuilder(userId.Trim().ToUpper());
                    for (int j = 1; j < userId.Trim().Length - 1; j++)
                    {
                        sb[j] = 'x';
                    }
                    strUser = sb.ToString();
                    strMsgTxt = strMsgTxt.Replace("<Client>", strUser);
                    strMsgTxt = strMsgTxt.Replace("<Password>", strPass);

                    strMsgTxt = strMsgTxt.Replace("<&>", "<~>");
                    strMsgTxt = strMsgTxt.Replace("&", "");
                    strMsgTxt = strMsgTxt.Replace("<~>", "&");

                    string strURLLink = strSMSParamVal[4];

                    if (strURLLink.IndexOf("<USERID>") != -1 && strSMSParamVal[0].Trim() != "")
                    {
                        strURLLink = strURLLink.Replace("<USERID>", strSMSParamVal[0].Trim());
                    }
                    if (strURLLink.IndexOf("<PASSWORD>") != -1 && strSMSParamVal[1].Trim() != "")
                    {
                        strURLLink = strURLLink.Replace("<PASSWORD>", strSMSParamVal[1].Trim());
                    }
                    if (strSMSParamVal[2].Trim() == "")
                    {
                        if (strURLLink.IndexOf("<SENDERID>") != -1)
                        {
                            strURLLink = strURLLink.Replace("<SENDERID>", strSMSParamVal[2].Trim());
                        }
                    }
                    else
                    {
                        strFromNo1 = strSMSParamVal[2].Trim();
                        if (strFromNo1.IndexOf("|") != -1)
                        {
                            strFromNo2 = strFromNo1.Split('|')[1];
                            strFromNo1 = strFromNo1.Split('|')[0];
                        }
                        else
                        {
                            strFromNo1 = Strings.Left(strFromNo1.Trim(), 10);
                            strFromNo2 = "";
                        }
                        if (strURLLink.IndexOf("<SENDERID>") != -1)
                        {
                            strURLLink = strURLLink.Replace("<SENDERID>", strSMSParamVal[2].Trim());
                        }
                        else if (strURLLink.IndexOf("<SENDERID1>") != -1 || strURLLink.IndexOf("<SENDERID2>") != -1)
                        {
                            strURLLink = strURLLink.Replace("<SENDERID1>", strFromNo1).Replace("<SENDERID2>", strFromNo2);
                        }
                    }
                    strURLLink = strURLLink.Replace("<MESSAGE>", strMsgTxt);

                    if (strURLLink.IndexOf("/opted.smsapi.org/v1.0.7/") != -1)
                    {
                        strURLLink = strURLLink.Replace("<MESSAGE>", strMsgTxt);
                    }

                    if (strURLLink.IndexOf("/174.143.34.193/") != -1)
                    {
                        if (strMsgTxt.Trim().Length > 160)
                        {
                            strURLLink = strURLLink + "&mt=4";
                        }
                        else
                        {
                            strURLLink = strURLLink + "&mt=0";
                        }
                        strURLLink = strURLLink + "&typeofmessage=1";
                    }

                    string pramval = objUtility.GetSysParmSt(Strsql, Strsql);
                    if (pramval == "Y" || strContrycd.Trim() == "Y")
                    {
                        if (strMobileNo.Trim().Length == 10)
                        {
                            strMobileNo = "91" + strMobileNo;
                        }
                    }
                    else
                    {
                        if (strMobileNo.Trim().Length > 10)
                        {
                            strMobileNo = Strings.Right(strMobileNo.Trim(), 10);
                        }
                    }
                    strURLLink = strURLLink.Replace("<CLIENTMOBILE>", strMobileNo.Trim());

                    if (strURLLink.IndexOf("myvaluefirst.com") != -1)
                    {
                        string strSENDER = "";
                        if (strSMSParamVal[2].Trim().IndexOf("|") != -1)
                        {
                            if (Strings.Left(strMobileNo.Trim(), 2) == "92" || Strings.Left(strMobileNo.Trim(), 2) == "93")
                            {
                                strSENDER = strSMSParamVal[2].Trim().Split('|')[1];
                            }
                            else
                            {
                                strSENDER = strSMSParamVal[2].Trim().Split('|')[0];
                            }
                        }
                        else
                        {
                            strSENDER = Strings.Left(strSMSParamVal[2].Trim(), 10);
                        }
                        strURLLink = strURLLink.Replace("<SENDERID3>", strSENDER);
                    }

                    #region old code 
                    //if (Request.QueryString["ViewSMSLink"] != null)
                    //{
                    //    string strDate = DecodeFrom64(Request.QueryString["ViewSMSLink"]);
                    //    Strsql = "select convert(char,convert(datetime,getdate()),103) as DT";
                    //    DataTable dt = apiObj.OpenDataTableCommon(Strsql);
                    //    if (strDate == dt.Rows[0]["DT"].ToString().Replace("/", "").Trim())
                    //    {
                    //        FormsAuthentication.SetAuthCookie(TxtUserID.Text.Trim(), false);
                    //        Response.Redirect("~/frmsmslink.aspx?SMSLink=" + Server.UrlEncode(strURLLink));
                    //    }
                    //    else
                    //    {
                    //        Response.Redirect(Request.RawUrl.Replace(Request.Url.Query, ""));
                    //    }
                    //    return;
                    //}
                    #endregion
                    if (!string.IsNullOrEmpty(_configuration["SECURITYPROT"]))
                    {
                        if (_configuration["SECURITYPROT"].Trim() == "TLS12")
                        {
                            ServicePointManager.SecurityProtocol = (SecurityProtocolType)192 | (SecurityProtocolType)768 | (SecurityProtocolType)3072 | (SecurityProtocolType)48;
                        }
                    }
                    
                    HttpWebRequest http = (HttpWebRequest)WebRequest.Create(strURLLink);
                    HttpWebResponse response = (HttpWebResponse)http.GetResponse();
                    StreamReader sr = new StreamReader(response.GetResponseStream());
                    string content = sr.ReadToEnd();
                    string strresponse = content;

                    if (response.StatusCode == HttpStatusCode.OK)
                    {
                        strresponse = "SMS Sent Successfully.";
                    }
                    else if (content.IndexOf("<ERROR>") != -1)
                    {
                        if (content.IndexOf("<DESC>") != -1)
                        {
                            strresponse = Strings.Mid(content, Strings.InStr(1, content, "<DESC>", CompareMethod.Text) + 6);
                            strresponse = Strings.Left(content, Strings.InStr(1, content, "</DESC>", CompareMethod.Text) - 1);
                        }
                        else
                        {
                            strresponse = "SMS Sent Successfully.";
                        }
                    }
                    else if (content.IndexOf("\"\"error-status\"\":\"\"Success\"\"") != -1)
                    {
                        strresponse = "SMS Sent Successfully.";
                    }
                    else if (content.IndexOf(strMobileNo.Trim()) != -1)
                    {
                        strresponse = "Message Send Successfully.";
                    }
                    else if (content.IndexOf("<sms>") != -1)
                    {
                        if (content.IndexOf("-1") != -1)
                        {
                            strresponse = "Message Sending Failed";
                        }
                        else if (content.ToUpper().IndexOf("INVALID USERNAME OR PASSWORD") != -1)
                        {
                            strresponse = "Sending Failed. Invalid Username Or Password.";
                        }
                        else
                        {
                            strresponse = "Message Send Successfully.";
                        }
                    }
                    else if (content.IndexOf("Fail") != -1)
                    {
                        strresponse = "Message Sending Failed";
                    }
                    else if (content.ToUpper().IndexOf("INVALID USERNAME OR PASSWORD") != -1 || content.ToUpper().IndexOf("INVALID USERNAME AND PASSWORD") != -1)
                    {
                        strresponse = "Message Sending Failed. Invalid Username Or Password.";
                    }
                    else if (content.ToUpper().IndexOf("1701|") != -1 || content.ToUpper().IndexOf("SUCCESS") != -1)
                    {
                        strresponse = "Message Send Successfully.";
                    }
                    else if (content == "100")
                    {
                        strresponse = "Message Send Successfully.";
                    }
                    else if (content.ToUpper().IndexOf(":") != -1)
                    {
                        if (content.ToUpper().Split(':')[1] == "")
                        {
                            strresponse = content;
                        }
                        else
                        {
                            strresponse = "Message Send Successfully.";
                        }
                    }
                    else if (content.ToUpper().IndexOf("GID") != -1)
                    {
                        strresponse = "Message Send Successfully.";
                    }
                    else
                    {
                        strresponse = content;
                    }
                    Strsql = "Insert into sms_Logs values(";
                    Strsql += "'" + strUserCd + "','" + strMobileNo + "','" + strMsgTxt + "',";
                    Strsql += "'" + strresponse.Replace("'", " ") + "','" + strUserCd + "','" + objUtility.dtos(System.DateTime.Today.Date.ToString()) + "','" + DateTime.Now.ToString("HH:mm:ss") + "')";
                    objUtility.ExecuteSQL(Strsql);

                    if (strresponse.IndexOf("Successfully") != -1)
                    {
                        return "Password sent to your registered mobile " + strMobile;
                    }
                    else
                    {
                        return "Error Sending SMS " + strresponse;
                    }

                }
                catch (Exception e)
                {
                    return "Error Sending SMS ";
                }
            }
            return "Sms sending failed";
        }

        private static string DecodeFrom64(string encodedData)
        {
            try
            {
                string returnValue = null;
                byte[] encodedDataAsBytes = System.Convert.FromBase64String(encodedData);
                returnValue = System.Text.ASCIIEncoding.ASCII.GetString(encodedDataAsBytes);
                return returnValue;
            }
            catch (Exception ex)
            {
                return "";
            }
        }
        #endregion
        #endregion
    }
}
