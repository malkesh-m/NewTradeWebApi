using INVPLService;
using Microsoft.AspNetCore.Http;
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
using System.Text.RegularExpressions;
using System.Web.Helpers;
using TradeWeb.API.Data;
using TradeWeb.API.Models;
using static INVPLService.NVPLSoapClient;

namespace TradeWeb.API.Repository
{
    public interface ITradeWebRepository
    {
        public dynamic UserDetails(string userId, string password);

        public dynamic Login_validate_USER(string userId);

        public dynamic Login_Password_GenerateOTP(string userId, string mode);

        public dynamic Login_Password_Update(string userId, string OTP, string oldPassword, string newPassword);

        public dynamic Login_validate_Password(string userId, string password);

        public dynamic Login_GetPassword(string userId);

        public dynamic GetUserDetais(string userId);

        public dynamic Transaction_Summary(string userId, string type, string FromDate, string ToDate);

        public dynamic Transaction_Accounts(string userId, string type, string fromDate, string toDate);

        public dynamic Transaction_AGTS(string userId, string seg, string fromDate, string toDate);
        public DataTable Ledger_Summary(string userId, string type, string fromdate, string toDate);

        public dynamic Ledger_Year();

        public dynamic Ledger_Detail(string userId, LedgerDetailsModel model, string fromDate, string toDate);

        public dynamic OutStandingPosition(string userId, string AsOnDt);

        public dynamic OutStandingPosition_Detail(string userId, string seriesid, string CESCd);

        public dynamic ProfitLoss_Cash_Summary(string userId, string fromDate, string toDate);

        public dynamic ProfitLoss_Cash_Detail(string userId, string fromDate, string toDate, string scripcd);

        public dynamic ProfitLoss_FO_Summary(string userId, string exchange, string segment, string fromDate, string toDate);

        public dynamic ProfitLoss_Commodity_Summary(string userId, string exchange, string fromDate, string toDate);

        public dynamic Holding_Broker_Current(string userid);

        public dynamic Holding_Broker_Ason(string userid, string AsOnDt);

        public dynamic Holding_MyDematAct_List(string userid);
        public dynamic Holding_MyDemat_Current(string userid, string dematActNo, string strtable);

        public dynamic Holding_MyDematAct_HoldingDates_Execute();

        public dynamic AddUnPledgeRequest(string userId, string unPledge, string dmScripcd, string txtReqQty);
        public dynamic Bills_exchSeg();
        public dynamic Bills_cash_settTypes_list(string syExchange);
        public dynamic Bill_data(string userId, string exchSeg, string settType, string dt);

        public dynamic CommonGetSysParmStHandler(string param1, string param2);


        public dynamic Confirmation(string userId, int type, string dt);

        public dynamic Transaction_Detail(string userId, string exch, string seg, int type, string fromdate, string todate, string scripcode);


        public dynamic GetMarginMainData(string cm_cd, string strCompanyCode);

        public dynamic MarginMainData(string cm_cd, string strCompanyCode, string date);

        public dynamic GetDropdownListData(string cm_cd, string strCompanyCode);

        public dynamic GetMarginPledgeData(string cm_cd, string UserId, string strCompanyCode, string CmbDPID_Value);

        public dynamic GetCurrentPledgeRequest(string UserId);

        public dynamic AddPledgeRequest(string UserId, string CmbDPID_Value, bool blnIdentityOn, string intcnt, string lblScripcd, string txtQty);

        public dynamic Get_Page_Load_Data(string cm_cd);

        public dynamic Get_Buttons_Data(string BtnClick, string SelectedCLCode);

        public dynamic Get_Transaction_Btn_Data(string BtnClick, string SelectedCLCode, string SelectedValue, string FromDate, string ToDate);

        public dynamic Get_Transaction_Btn_RPJ_Detailed_Data(string Client, string Type, string FromDate, string ToDate);

        public dynamic GetINVPLDivListing(string userId, string FromDate, string ToDate);

        public dynamic GetINVPLGainLoss(string userId, string FromDate, string ToDate, Boolean chkjobing, Boolean chkdelivery, Boolean chkIgnoreSection, string TrxType);

        public dynamic GetINVPLTradeListing(string userId, string FromDate, string ToDate);

        public dynamic GetINVPLGainLossDetails(string userId, string FromDate, string ToDate, string reportFor, string ignore112A, string scripCd);

        public dynamic GeTINVPLTradeListingDetails(string userId, string fromDate, string toDate, string sccdPostBack);

        public dynamic GetINVPLTradeListingDelete(string userId, string srNo);

        public dynamic GetINVPLTradeListingSave(string userId, string date, string settelment, string bsFlag, string tradeType, double quantity, double netRate, double serviceTax, double STT, double otherCharge1, double otherCharge2, string sccdPostBack);

        public dynamic UpdateFundAndSharesRequest(bool isPostBack);

        public dynamic RdButtonSharesChecked(string cm_cd);

        public dynamic GetRmsRequest(string cm_cd);

        public dynamic ExecuteRequestReportPageLoad(bool isPostBack);

        public dynamic InsertRequestValues(string userId, string strLstSeg, string cmbRequest, string strFromDt, string strToDt);

        public dynamic GetTradesData(string cm_cd, string FromDate, string ToDate, string SelectedIndex);

        public dynamic GetTempRMSSummaryData(string cm_cd, string strCompanyCode);

        public dynamic GetStatusFundData(string cm_cd, string strCompanyCode);

        public dynamic GetStatusCollateralData(string cm_cd, string strCompanyCode);

        public dynamic GetprSecurityListRptHandler(Boolean blnBSE, Boolean blnNSE);

        public dynamic GetShortFallMainGridData(string cm_cd, int IntNtxtDays);

        public dynamic GetDdlExchangeList(string cmbProductValue, string cmbDocumentTypeValue);

        public dynamic GetDigitalDocumentData(string userId, string cmbProductValue, string cmbDocumentTypeValue, string cmbExchangeValue, string fromDate, string toDate);

        public dynamic GetDigitalDocumentDownload(string userId, string cmbProductValue, string cmbDocumentTypeValue, string cmbExchangeValue, string fromDate);

        public dynamic AddDdlProductListItem();

        public dynamic Family_List(string userId);

        public dynamic Family_Add(string userId, string password, string UCC_Code);

        public dynamic Family_Remove(string UCC_Code);

        public dynamic Family_Balance(List<string> UCC_Codes);

        public dynamic Family_RetainedStoke(List<string> UCC_Codes);

        public dynamic Family_Holding(List<string> UCC_Codes);

        public dynamic Family_Position(List<string> UCC_Codes);

        public dynamic Family_Transaction(FamilyTransactionModel model);

        public dynamic Family_Transaction_Details(string client, string type, string fromDate, string toDate);

        public dynamic Family_RetainedStokeJson(List<string> UCC_Codes);

        public dynamic Family_HoldingJson(List<string> UCC_Codes);

        public dynamic Family_TransactionJson(FamilyTransactionModel model);

        public dynamic Family_TransactionDetailJson(string Client, string Type, string FromDate, string ToDate);
    }

    public class TradeWebRepository : ITradeWebRepository
    {
        #region Class level declarations.
        private readonly IConfiguration _configuration;
        private readonly UtilityCommon objUtility;
        private string strsql = "";
        private string strConnecton = "";
        NVPLSoapClient nVPLSoapClient = new NVPLSoapClient(EndpointConfiguration.INVPLSoap);
        IHttpContextAccessor _httpContextAccessor;
        #endregion

        #region Constructor
        public TradeWebRepository(IConfiguration configuration, UtilityCommon objUtility, IHttpContextAccessor httpContextAccessor)
        {
            _configuration = configuration;
            this.objUtility = objUtility;
            _httpContextAccessor = httpContextAccessor;
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
                    if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
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
                string qury = "select cm_cd ClientCode, cm_Name ClientName,cm_mobile Mobile,cm_email Email from Client_master with (nolock) where cm_cd='" + userId + "'";
                var ds = CommonRepository.FillDataset(qury);
                if (ds != null)
                {
                    if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
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
                string result = Login_GetPassword_Execute(userId); //"select cm_mobile from Client_master with (nolock) where cm_cd='" + userId + "'  and cm_pwd='" + password + "'";
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

        public dynamic Login_Password_GenerateOTP(string userId, string mode)
        {
            try
            {
                string result = Login_Password_GenerateOTP_SMS_Email(userId, mode); //"select cm_mobile from Client_master with (nolock) where cm_cd='" + userId + "'  and cm_pwd='" + password + "'";
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

        public dynamic Login_Password_Update(string userId, string OTP, string oldPassword, string newPassword)
        {
            try
            {
                Boolean result = Login_Password_Update_execute(userId, OTP, oldPassword, newPassword); //"select cm_mobile from Client_master with (nolock) where cm_cd='" + userId + "'  and cm_pwd='" + password + "'";

                return JsonConvert.SerializeObject(result);
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
        public DataTable Ledger_Summary(string userId, string type, string fromdate, string toDate)
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
                    return data;
                    //return JsonConvert.SerializeObject(data, Formatting.None);
                }
                return new DataTable(); // List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /*public dynamic Ledger_Summary(string userId, string type, string fromdate, string toDate)
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
        }*/

        public dynamic Ledger_Detail(string userId, LedgerDetailsModel model, string fromDate, string toDate)
        {

            string strTable = " Ledger ";
            string strsql = "";
            string strCommTable = string.Empty;
            string strCommClientMaster = string.Empty;
            string type_cesCd = "";

            if (_configuration["Commex"] != null && _configuration["Commex"] != string.Empty)
            {
                string StrCommexConn = "";
                StrCommexConn = objUtility.GetCommexConnectionNew(_configuration["Commex"]);
                strCommTable = StrCommexConn + ".ledger";
                strCommClientMaster = StrCommexConn + ".Client_master";
            }

            foreach (var segmentModel in model.type_exchseg)
            {
                foreach (var exSeg in segmentModel.exchseg)
                {
                    if (!string.IsNullOrEmpty(type_cesCd))
                    {
                        type_cesCd = type_cesCd + "|" + segmentModel.type + "," + exSeg;
                    }
                    else
                    {
                        type_cesCd = type_cesCd + segmentModel.type + "," + exSeg;
                    }

                }

            }

            strsql = string.Empty;

            for (int i = 0; i < type_cesCd.Split("|").Length; i++)
            {
                if (strsql.Length > 0 && type_cesCd.Split("|")[i] != "")
                {
                    strsql = strsql + " union all ";
                }

                if (type_cesCd.Split("|")[i].Split(",")[0].ToUpper() == "TRADING")
                {
                    strsql = strsql + " select 'Trading' as [Type],ld_clientcd,convert(char,convert(datetime,'" + fromDate + "'),103) ld_dt,Rtrim(CES_Exchange) + '-' + CES_Segment [ExchSeg],";
                    strsql = strsql + " cast(sum(case sign(datediff(d,'" + fromDate + "',ld_dt)) when -1 then ld_amount else 0 end)as decimal(15,2)) as ld_amount,'Opening Balance' ld_particular, case sign(sum(ld_amount)) when 1 then 'D' else 'C' end as ";
                    strsql = strsql + " ld_debitflag ,'' ld_chequeno, 'O' ld_documenttype,''ld_common ,convert(datetime,'" + fromDate + "') Ldate,ld_dpid,'' LookUp";
                    strsql = strsql + " from " + strTable + " with (nolock) , Companyexchangesegments ";
                    strsql = strsql + " where LD_DPID = CES_Cd and ld_clientcd = '" + userId + "'";
                    strsql = strsql + " and ld_dt < '" + fromDate + "' and ld_dpid = '" + type_cesCd.Split("|")[i].Split(",")[1] + "'";
                    strsql = strsql + " group by ld_clientcd,ld_dpid,CES_Exchange,CES_Segment having sum(ld_amount)<> 0 ";
                    strsql = strsql + " union all ";
                    strsql = strsql + " select 'Trading' as [Type],ld_clientcd, ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) as ld_dt,Rtrim(CES_Exchange) + '-' + CES_Segment [ExchSeg],";
                    strsql = strsql + " cast(ld_amount as decimal(15,2)),ld_particular,ld_debitflag,ld_chequeno,ld_documenttype,ld_common,convert(datetime,ld_dt) Ldate,ld_dpid,case When ld_documentType = 'B' then substring(LD_DPID,2,1)+'/'+substring(LD_DPID,3,1)+'/'+ld_common+'/'+ld_commondt else '' end LookUp ";
                    strsql = strsql + " from " + strTable + " with (nolock), Companyexchangesegments ";
                    strsql = strsql + " where LD_DPID = CES_Cd and  ld_clientcd = '" + userId + "'";
                    strsql = strsql + "  and ld_dt between '" + fromDate + "' and '" + toDate + "' and ld_dpid = '" + type_cesCd.Split("|")[i].Split(",")[1] + "'";
                }

                if (type_cesCd.Split("|")[i].Split(",")[0].ToUpper() == "TRADING-MARGIN")
                {
                    strsql = strsql + " select 'Trading-Margin' as [Type],ld_clientcd,convert(char,convert(datetime,'" + fromDate + "'),103) ld_dt,Rtrim(CES_Exchange) + '-' + CES_Segment [ExchSeg],";
                    strsql = strsql + " cast(sum(case sign(datediff(d,'" + fromDate + "',ld_dt)) when -1 then ld_amount else 0 end)as decimal(15,2)) as ld_amount,'Opening Balance' ld_particular, case sign(sum(ld_amount)) when 1 then 'D' else 'C' end as ";
                    strsql = strsql + " ld_debitflag ,'' ld_chequeno, 'O' ld_documenttype,''ld_common ,convert(datetime,'" + fromDate + "') Ldate,ld_dpid,'' LookUp";
                    strsql = strsql + " from " + strTable + " with (nolock) , Companyexchangesegments ";
                    strsql = strsql + " where LD_DPID = CES_Cd and ld_clientcd in (select distinct cm_brkggroup from client_master where cm_cd='" + userId + "' and cm_brkggroup <> '' ) ";
                    strsql = strsql + " and ld_dt < '" + fromDate + "' and ld_dpid = '" + type_cesCd.Split("|")[i].Split(",")[1] + "'";
                    strsql = strsql + " group by ld_clientcd,ld_dpid,CES_Exchange,CES_Segment having sum(ld_amount)<> 0 ";
                    strsql = strsql + " union all ";
                    strsql = strsql + " select 'Trading-Margin' as [Type],ld_clientcd, ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) as ld_dt,Rtrim(CES_Exchange) + '-' + CES_Segment [ExchSeg],";
                    strsql = strsql + " cast(ld_amount as decimal(15,2)),ld_particular,ld_debitflag,ld_chequeno,ld_documenttype,ld_common,convert(datetime,ld_dt) Ldate,ld_dpid,'' LookUp ";
                    strsql = strsql + " from " + strTable + " with (nolock), Companyexchangesegments ";
                    strsql = strsql + " where LD_DPID = CES_Cd and ld_clientcd in (select distinct cm_brkggroup from client_master where cm_cd='" + userId + "' and cm_brkggroup <> '' ) ";
                    strsql = strsql + "  and ld_dt between '" + fromDate + "' and '" + toDate + "' and ld_dpid = '" + type_cesCd.Split("|")[i].Split(",")[1] + "'";
                }


                if (type_cesCd.Split("|")[i].Split(",")[0].ToUpper() == "COMMODITY")
                {
                    strsql = strsql + " select 'Trading' as [Type],ld_clientcd,convert(char,convert(datetime,'" + fromDate + "'),103) ld_dt,Rtrim(CES_Exchange) + '-Comm' [ExchSeg],";
                    strsql = strsql + " cast(sum(case sign(datediff(d,'" + fromDate + "',ld_dt)) when -1 then ld_amount else 0 end)as decimal(15,2)) as ld_amount,'Opening Balance' ld_particular, case sign(sum(ld_amount)) when 1 then 'D' else 'C' end as ";
                    strsql = strsql + " ld_debitflag ,'' ld_chequeno, 'O' ld_documenttype,''ld_common ,convert(datetime,'" + fromDate + "') Ldate,ld_dpid,'' LookUp";
                    strsql = strsql + " from " + strCommTable + " with (nolock) , Companyexchangesegments ";
                    strsql = strsql + " where LD_DPID = CES_Cd and ld_clientcd = '" + userId + "'";
                    strsql = strsql + " and ld_dt < '" + fromDate + "' and ld_dpid = '" + type_cesCd.Split("|")[i].Split(",")[1] + "'";
                    strsql = strsql + " group by ld_clientcd,ld_dpid,CES_Exchange,CES_Segment having sum(ld_amount)<> 0 ";
                    strsql = strsql + " union all ";
                    strsql = strsql + " select 'Trading' as [Type],ld_clientcd, ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) as ld_dt,Rtrim(CES_Exchange) + '-Comm' [ExchSeg],";
                    strsql = strsql + " cast(ld_amount as decimal(15,2)),ld_particular,ld_debitflag,ld_chequeno,ld_documenttype,ld_common,convert(datetime,ld_dt) Ldate,ld_dpid, case when ld_documentType = 'B' then substring(LD_DPID,2,1)+'/'+substring(LD_DPID,3,1)+'/'+ld_common+'/'+ld_commondt else 0 end LookUp ";
                    strsql = strsql + " from " + strCommTable + " with (nolock), Companyexchangesegments ";
                    strsql = strsql + " where LD_DPID = CES_Cd and  ld_clientcd = '" + userId + "'";
                    strsql = strsql + "  and ld_dt between '" + fromDate + "' and '" + toDate + "' and ld_dpid = '" + type_cesCd.Split("|")[i].Split(",")[1] + "'";
                }

                if (type_cesCd.Split("|")[i].Split(",")[0].ToUpper() == "COMMODITY-MARGIN")
                {
                    strsql = strsql + " select 'Commodity-Margin' as [Type],ld_clientcd,convert(char,convert(datetime,'" + fromDate + "'),103) ld_dt,Rtrim(CES_Exchange) + '-Comm' [ExchSeg],";
                    strsql = strsql + " cast(sum(case sign(datediff(d,'" + fromDate + "',ld_dt)) when -1 then ld_amount else 0 end)as decimal(15,2)) as ld_amount,'Opening Balance' ld_particular, case sign(sum(ld_amount)) when 1 then 'D' else 'C' end as ";
                    strsql = strsql + " ld_debitflag ,'' ld_chequeno, 'O' ld_documenttype,''ld_common ,convert(datetime,'" + fromDate + "') Ldate,ld_dpid,'' LookUp";
                    strsql = strsql + " from " + strCommTable + " with (nolock) , Companyexchangesegments ";
                    strsql = strsql + " where LD_DPID = CES_Cd and ld_clientcd in (select distinct cm_brkggroup from client_master where cm_cd='" + userId + "' and cm_brkggroup <> '' ) ";
                    strsql = strsql + " and ld_dt < '" + fromDate + "' and ld_dpid = '" + type_cesCd.Split("|")[i].Split(",")[1] + "'";
                    strsql = strsql + " group by ld_clientcd,ld_dpid,CES_Exchange,CES_Segment having sum(ld_amount)<> 0 ";
                    strsql = strsql + " union all ";
                    strsql = strsql + " select 'Commodity-Margin' as [Type],ld_clientcd, ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) as ld_dt,Rtrim(CES_Exchange) + '-Comm' [ExchSeg],";
                    strsql = strsql + " cast(ld_amount as decimal(15,2)),ld_particular,ld_debitflag,ld_chequeno,ld_documenttype,ld_common,convert(datetime,ld_dt) Ldate,ld_dpid,'' LookUp ";
                    strsql = strsql + " from " + strCommTable + " with (nolock), Companyexchangesegments ";
                    strsql = strsql + " where LD_DPID = CES_Cd and ld_clientcd in (select distinct cm_brkggroup from client_master where cm_cd='" + userId + "' and cm_brkggroup <> '' ) ";
                    strsql = strsql + "  and ld_dt between '" + fromDate + "' and '" + toDate + "' and ld_dpid = '" + type_cesCd.Split("|")[i].Split(",")[1] + "'";
                }

                if (type_cesCd.Split("|")[i].Split(",")[0].ToUpper() == "MTF")
                {
                    strsql = strsql + " select 'MTF' as [Type],ld_clientcd,convert(char,convert(datetime,'" + fromDate + "'),103) ld_dt,Rtrim(CES_Exchange) + '-MTF' [ExchSeg],";
                    strsql = strsql + " cast(sum(case sign(datediff(d,'" + fromDate + "',ld_dt)) when -1 then ld_amount else 0 end)as decimal(15,2)) as ld_amount,'Opening Balance' ld_particular, case sign(sum(ld_amount)) when 1 then 'D' else 'C' end as ";
                    strsql = strsql + " ld_debitflag ,'' ld_chequeno, 'O' ld_documenttype,''ld_common ,convert(datetime,'" + fromDate + "') Ldate,ld_dpid,'' LookUp";
                    strsql = strsql + " from " + strTable + " with (nolock) , Companyexchangesegments ";
                    strsql = strsql + " where LD_DPID = CES_Cd and ld_clientcd in (select distinct MTFC_FillerB from MrgTdgFin_Clients where MTFC_CMCD ='" + userId + "') ";
                    strsql = strsql + " and ld_dt < '" + fromDate + "' and ld_dpid = '" + type_cesCd.Split("|")[i].Split(",")[1] + "'";
                    strsql = strsql + " group by ld_clientcd,ld_dpid,CES_Exchange,CES_Segment having sum(ld_amount)<> 0 ";
                    strsql = strsql + " union all ";
                    strsql = strsql + " select 'MTF' as [Type],ld_clientcd, ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) as ld_dt,Rtrim(CES_Exchange) + '-MTF' [ExchSeg],";
                    strsql = strsql + " cast(ld_amount as decimal(15,2)),ld_particular,ld_debitflag,ld_chequeno,ld_documenttype,ld_common,convert(datetime,ld_dt) Ldate,ld_dpid,'' LookUp ";
                    strsql = strsql + " from " + strTable + " with (nolock), Companyexchangesegments ";
                    strsql = strsql + " where LD_DPID = CES_Cd and ld_clientcd in (select distinct cm_brkggroup from client_master where cm_cd='" + userId + "' and cm_brkggroup <> '' ) ";
                    strsql = strsql + "  and ld_dt between '" + fromDate + "' and '" + toDate + "' and ld_dpid = '" + type_cesCd.Split("|")[i].Split(",")[1] + "'";

                }

                if (type_cesCd.Split("|")[i].Split(",")[0].ToUpper() == "NBFC")
                {
                    strsql = strsql + " select 'Trading-Margin' as [Type],ld_clientcd,convert(char,convert(datetime,'" + fromDate + "'),103) ld_dt,'NBFC' [ExchSeg],";
                    strsql = strsql + " cast(sum(case sign(datediff(d,'" + fromDate + "',ld_dt)) when -1 then ld_amount else 0 end)as decimal(15,2)) as ld_amount,'Opening Balance' ld_particular, case sign(sum(ld_amount)) when 1 then 'D' else 'C' end as ";
                    strsql = strsql + " ld_debitflag ,'' ld_chequeno, 'O' ld_documenttype,''ld_common ,convert(datetime,'" + fromDate + "') Ldate,ld_dpid,'' LookUp";
                    strsql = strsql + " from NBFC_Ledger with (nolock) ";
                    strsql = strsql + " where ld_clientcd = '" + userId + "'";
                    strsql = strsql + " and ld_dt < '" + fromDate + "'";
                    strsql = strsql + " group by ld_clientcd,ld_dpid having sum(ld_amount)<> 0 ";
                    strsql = strsql + " union all ";
                    strsql = strsql + " select 'MTF' as [Type],ld_clientcd, ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) as ld_dt,'NBFC' [ExchSeg],";
                    strsql = strsql + " cast(ld_amount as decimal(15,2)),ld_particular,ld_debitflag,ld_chequeno,ld_documenttype,ld_common,convert(datetime,ld_dt) Ldate,ld_dpid,'' LookUp ";
                    strsql = strsql + " from NBFC_Ledger with (nolock) ";
                    strsql = strsql + " where ld_clientcd = '" + userId + "'";
                    strsql = strsql + " and ld_dt between '" + fromDate + "' and '" + toDate + "' and ld_dpid = '" + type_cesCd.Split("|")[i].Split(",")[1] + "'";
                }

            }
            strsql = " select Type [Type],ld_clientcd [ClientCode],ld_dt [Date],ExchSeg,ld_amount Amount,ld_particular Particular,ld_debitflag Debitflag,ld_chequeno Chequeno,ld_documenttype Documenttype,ld_common Common,Ldate,ld_dpid CESCD,LookUp from (" + strsql + ") a ";
            strsql = strsql + " order by Type,Ldate";
            try
            {
                var ds = CommonRepository.FillDataset(strsql);
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

        public dynamic Ledger_Year()
        {
            strsql = strsql + "select distinct ld_AccYear AccYear from Ledger";
            try
            {
                var ds = CommonRepository.FillDataset(strsql);
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
        public dynamic OutStandingPosition_Detail(string userId, string seriesid, string CESCd)
        {
            try
            {
                string StrTradesIndex = "";

                string Strsql = string.Empty;

                if (CESCd.Substring(2, 1) == "X")
                {
                    Strsql = " select td_dt tradedt, sum(td_bqty) Buy, convert(decimal(15,2),sum(td_bqty*td_rate*sm_multiplier)) BuyAmount, sum(td_sqty) Sell, ";
                    Strsql = Strsql + " convert(decimal(15,2),sum(td_sqty*td_rate* sm_multiplier)) SellAmount, sum(td_bqty-td_sqty) Net, ";
                    Strsql = Strsql + "  convert(decimal(15,2), (sum((td_bqty-td_sqty)*td_rate* sm_multiplier)))  NetAmount ";
                    Strsql = Strsql + " from trades with (" + StrTradesIndex + "nolock), series_master with (nolock) where td_clientcd='" + userId + "' ";
                    Strsql = Strsql + " and td_exchange=sm_exchange and td_seriesid=sm_seriesid ";
                    Strsql = Strsql + " and td_seriesid='" + seriesid + "' and td_exchange='" + CESCd.Substring(1, 1) + "' ";
                    Strsql = Strsql + " and td_companycode='" + CESCd.Substring(0, 1) + "' ";
                    Strsql = Strsql + " group by td_dt";
                }
                else
                {

                    if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
                    { StrTradesIndex = "index(idx_trades_clientcd),"; }

                    Strsql = " select td_dt tradedt, sum(td_bqty) Buy, convert(decimal(15,2),sum(td_bqty*td_rate* sm_multiplier)) BuyAmount, sum(td_sqty) Sell, ";
                    Strsql = Strsql + " convert(decimal(15,2),sum(td_sqty*td_rate* sm_multiplier)) SellAmount, sum(td_bqty-td_sqty) Net, ";
                    Strsql = Strsql + "  convert(decimal(15,2), (sum((td_bqty-td_sqty)*td_rate*sm_multiplier)))  NetAmount ";
                    Strsql = Strsql + " from trades with (" + StrTradesIndex + "nolock), series_master with (nolock) where td_clientcd='" + userId + "' ";
                    Strsql = Strsql + " and td_exchange=sm_exchange and td_Segment=sm_Segment and td_seriesid=sm_seriesid ";
                    Strsql = Strsql + " and td_seriesid='" + seriesid + "' and td_exchange='" + CESCd.Substring(1, 1) + "' ";
                    Strsql = Strsql + " and td_Segment='" + CESCd.Substring(2, 1) + "' ";
                    Strsql = Strsql + " and td_companycode='" + CESCd.Substring(0, 1) + "' ";
                    Strsql = Strsql + " group by td_dt";
                }
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

        public dynamic Holding_MyDematAct_List(string userid)
        {
            string strsql = string.Empty;

            if (_configuration["Cross"] != "")
            {
                string[] ArrCross = _configuration["Cross"].Split("/");
                strsql = "select cm_blsavingcd ClientCode ,cm_cd DematActNo from ";
                strsql = strsql + " " + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".Client_master c ";
                strsql = strsql + " where cm_blsavingcd = '" + userid + "'";
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


        public dynamic Holding_MyDemat_Current(string userid, string dematActNo, string strtable)
        {
            string strsql = string.Empty;

            if (_configuration["Cross"] != "")
            {
                string[] ArrCross = _configuration["Cross"].Split("/");
                strsql = "select cm_blsavingcd ClientCode ,hld_ac_code DematAc, a.hld_isin_code ISIN,b.sc_company_name CompanyName,b.sc_isinname ISINName,cast((a.hld_ac_pos) as decimal(15,3)) Quantity, ";
                strsql = strsql + " d.bt_description BalanceType ,cast((sc_rate) as decimal(15,2)) as Rate, ";
                strsql = strsql + " cast(( ( a.hld_ac_pos * sc_Rate)) as decimal(15,2))  as Value ";
                strsql = strsql + " from " + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + "." + strtable + " a,";
                strsql = strsql + " " + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".Security b ,";
                strsql = strsql + " " + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".Client_master c ,";
                strsql = strsql + "" + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".Beneficiary_type d ";
                strsql = strsql + " where a.hld_ac_code = cm_cd and cm_blsavingcd = '" + userid + "' and a.hld_ac_code = '" + dematActNo + "' and a.hld_isin_code = b.sc_isincode ";
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
        public dynamic Holding_MyDematAct_HoldingDates_Execute()
        {
            string strX = string.Empty;

            char[] ArrSeparators = new char[1];
            ArrSeparators[0] = '/';
            if (objUtility.GetWebParameter("Cross") != "") // strBoid  LEft 2 <>IN
            {
                string[] ArrCross = objUtility.GetWebParameter("Cross").Split(ArrSeparators);
                strX = " select replace(name,'Holding_','') name from [" + ArrCross[0].Trim() + "].[" + ArrCross[1].Trim() + "].[" + ArrCross[2].Trim() + "].sysobjects where xtype = 'U' and name like ('Holding_%') and ISDATE(RIGHT(name,8))=1 ";
            }
            if (objUtility.GetWebParameter("Estro") != "")
            {
                if (strX != "")
                { strX += "  Union All "; }
                string[] ArrEstro = objUtility.GetWebParameter("Estro").Split(ArrSeparators);
                strX += " select replace(name,'Holding_','') name from [" + ArrEstro[0].Trim() + "].[" + ArrEstro[1].Trim() + "].[" + ArrEstro[2].Trim() + "].sysobjects where xtype = 'U' and name like ('Holding_%') and ISDATE(RIGHT(name,8))=1 ";
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

        public dynamic Bills_exchSeg()
        {
            var query = "select CES_Cd CESCd ,CES_Exchange exchange,CES_Segment segment from CompanyExchangeSegments";
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

        // get settlement type for dropdown
        public dynamic Bills_cash_settTypes_list(string exch)
        {
            var query = "select sy_type type,sy_desc description from Settlement_type with (nolock) where sy_exchange='" + exch + "' and sy_Status = 'A' Order by case sy_maptype  when 'N' Then 1 When 'C' Then 2 else 3 end,sy_desc";
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
        public dynamic Bill_data(string userId, string exchSeg, string settType, string dt)
        {
            try
            {
                var ds = Bill_Process(userId, exchSeg, settType, dt);
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
        public DataSet Bill_Process(string userId, string exchSeg, string stlmntType, string dt)
        {
            Boolean blnInterOP = new Boolean();
            List<string> arrexchange = new List<string>();
            List<string> arrStlmnt = new List<string>();

            DataSet ObjDataSet = new DataSet();

            if (exchSeg.Substring(1, 1) == "C")
            {
                string StrStlmntWhere = string.Empty;
                string Stlmnt = objUtility.fnFireQuery("settlements", "se_stlmnt", "se_stdt = '" + dt + "' and se_exchange+se_type", exchSeg.Substring(0, 1) + stlmntType, true);

                string StrMsg;
                StrMsg = objUtility.fnCheckInterOperability(dt, "C");

                if (StrMsg.ToUpper().Trim() == "TRUE")
                {
                    string apiRes6 = objUtility.fnGetInterOpStlmnts(Stlmnt, dt, false);
                    blnInterOP = true;
                    arrStlmnt = apiRes6.Split(',').ToList();
                    StrStlmntWhere += "('" + arrStlmnt[0] + "'";
                    for (int j = 1; j < arrStlmnt.Count; j++)
                    {
                        StrStlmntWhere += ",'" + arrStlmnt[j] + "'";
                    }
                    StrStlmntWhere += ")";
                }
                else
                {
                    arrStlmnt.Add(Stlmnt);
                    StrStlmntWhere += "'" + Stlmnt + "'";
                }

                string StrTRXIndex = "";

                string apiRes = objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'Trx' and b.name", "idx_Trx_Clientcd", true);
                if (Convert.ToInt16(apiRes.Trim()) == 1)
                { StrTRXIndex = "index(idx_trx_clientcd),"; }

                if (_configuration["IsTradeWeb"] == "O")//Live
                {
                    if (blnInterOP)
                    {
                        strsql = " select '" + arrStlmnt[0] + "' as Stlmnt, '" + arrStlmnt[0] + "' +'['+ ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) + ']' Heading ,";
                        strsql = strsql + " td_scripcd ScripCode,td_orderid as [OrderID], td_tradeid [TradeID],";
                        strsql = strsql + " case td_ssrno when -1 then 'b/f' else td_time end TradeTime, ";
                        strsql = strsql + " rtrim(ss_name) ScripName, td_bqty Buy, ";
                        strsql = strsql + " td_sqty Sell, ";
                        strsql = strsql + " convert(decimal (15,2),td_marketrate) as [MarketRate], ";
                        strsql = strsql + " convert(decimal (15,4),td_brokerage*(td_bqty+td_sqty)) [Brokerage], ";
                        strsql = strsql + " convert(decimal (15,4),convert(money, case left(td_stlmnt,1) when 'N' then  case sr_nodelyn when 'Y' then 0 else td_rate end else td_rate end *td_bqty)) [BuyValue], ";
                        strsql = strsql + " convert(decimal (15,4),convert(money, case left(td_stlmnt,1) when 'N' then case sr_nodelyn when 'Y' then 0 else td_rate  end else td_rate end *td_sqty)) [SellValue] ,";
                        strsql = strsql + " case sr_nodelyn when 'Y' then 'NoDelv' else '' end [NoDelv], ";
                        strsql = strsql + " rtrim(ss_name)+case td_ssrno when -1 then 'a' else 'b' end [Ordr] ,td_dt tdDt,'' [NetValue]";
                        strsql = strsql + " from trx with(" + StrTRXIndex + "nolock) left outer join std_rates with (nolock) on td_stlmnt = sr_stlmnt and td_scripcd = sr_scripcd and sr_nodelyn='Y' ,securities with (nolock) ";
                        strsql = strsql + " where td_clientcd =@clcd and td_stlmnt=@stlmnt  and  td_Dt between '" + dt + "' and '" + dt + "'  ";
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
                        strsql = strsql + " where td_clientcd =@clcd and td_stlmnt=@stlmnt  and  td_Dt between '" + dt + "' and '" + dt + "'  ";
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
                        strsql = strsql + " and  td_Dt between '" + dt + "' and '" + dt + "'  and td_stlmnt = sr_stlmnt and td_scripcd = sr_scripcd  ";
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
                        strsql = strsql + " and se_stdt between '" + dt + "' and '" + dt + "' ";

                        strsql = strsql + " union all   ";

                        strsql = strsql + " select  '" + arrStlmnt[0] + "' as td_stlmnt,'" + arrStlmnt[0] + "' +'['+ ltrim(rtrim(convert(char,convert(datetime,se_stdt),103))) + ']', rtrim(cg_desc), bc_amount, se_stdt BDate";
                        strsql = strsql + " from Cbilled_charges with (nolock),settlements with (nolock),charges_master with (nolock)  ";
                        strsql = strsql + " where bc_clientcd=@clcd and bc_stlmnt=@stlmnt and bc_stlmnt = se_Stlmnt  ";
                        strsql = strsql + " and bc_companycode = cg_companycode and Left(bc_stlmnt,1) = cg_exchange and  bc_chargecode =cg_cd ";
                        strsql = strsql + " and se_stdt between '" + dt + "' and '" + dt + "'  ";

                        strsql = strsql + " union all ";

                        strsql = strsql + " select '" + arrStlmnt[0] + "' as td_stlmnt,'" + arrStlmnt[0] + "' +'['+ ltrim(rtrim(convert(char,convert(datetime,se_stdt),103))) + ']',rtrim(cg_desc), sh_servicetax, se_stdt BDate";
                        strsql = strsql + " from Specialcharges with (nolock),settlements with (nolock),charges_master with (nolock)  ";
                        strsql = strsql + " where sh_clientcd=@clcd and sh_stlmnt=@stlmnt and sh_stlmnt = se_Stlmnt  ";
                        strsql = strsql + " and sh_companycode = cg_companycode and Left(sh_stlmnt,1) = cg_exchange and  cg_cd = '01' ";
                        strsql = strsql + " and se_stdt between '" + dt + "' and '" + dt + "'  and sh_servicetax > 0 ) A";
                        strsql = strsql + " gROUP bY sh_stlmnt,rtrim(sh_desc),BDate,td_stlmnt ";
                        strsql = strsql.Replace("=@stlmnt", " in " + StrStlmntWhere + "").Replace("@clcd", "'" + userId + "'");
                    }
                    else
                    {
                        strsql = " select td_stlmnt stlmnt, td_stlmnt +'['+ ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) + ']' Heading ,";
                        strsql = strsql + " td_scripcd ScripCode,td_orderid as [OrderID], td_tradeid [TradeID],";
                        strsql = strsql + " case td_ssrno when -1 then 'b/f' else td_time end TradeTime, ";
                        strsql = strsql + " rtrim(ss_name) ScripName, td_bqty Buy, ";
                        strsql = strsql + " td_sqty Sell, ";
                        strsql = strsql + " convert(decimal (15,2),td_marketrate) as MarketRate , ";
                        strsql = strsql + " convert(decimal (15,4),td_brokerage*(td_bqty+td_sqty)) [Brokerage], ";
                        strsql = strsql + " convert(decimal (15,4),convert(money, case left(td_stlmnt,1) when 'N' then  case sr_nodelyn when 'Y' then 0 else td_rate end else td_rate end *td_bqty)) BuyValue, ";
                        strsql = strsql + " convert(decimal (15,4),convert(money, case left(td_stlmnt,1) when 'N' then case sr_nodelyn when 'Y' then 0 else td_rate  end else td_rate end *td_sqty)) SellValue ,";
                        strsql = strsql + " case sr_nodelyn when 'Y' then 'NoDelv' else '' end [NoDelv], ";
                        strsql = strsql + " rtrim(ss_name)+case td_ssrno when -1 then 'a' else 'b' end [Ordr] ,td_dt tdDt,'' [NetValue]";
                        strsql = strsql + " from trx with(" + StrTRXIndex + "nolock) left outer join std_rates with (nolock) on td_stlmnt = sr_stlmnt and td_scripcd = sr_scripcd and sr_nodelyn='Y' ,securities with (nolock) ";
                        strsql = strsql + " where td_clientcd =@clcd and td_stlmnt=@stlmnt  and  td_Dt between '" + dt + "' and '" + dt + "'  ";
                        strsql = strsql + " and td_scripcd = ss_cd ";

                        strsql = strsql + " union All  ";
                        strsql = strsql + " select td_stlmnt, td_stlmnt +'['+ ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) + ']' Heading ,";
                        strsql = strsql + " td_scripcd,td_orderid as [Order#], td_tradeid [Trade#],";
                        strsql = strsql + " case td_ssrno when -1 then 'b/f' else td_time end Time, ";
                        strsql = strsql + " ss_name Security, td_sqty  Buy, ";
                        strsql = strsql + " td_bqty Sell, ";
                        strsql = strsql + " convert(decimal (15,2),td_marketrate) as [Market Rate], ";
                        strsql = strsql + " convert(decimal (15,4),0) [Brokerage], ";
                        strsql = strsql + " convert(decimal (15,4),convert(money, case left(td_stlmnt,1) when 'N' then  case sr_nodelyn when 'Y' then 0 else td_marketrate end else td_marketrate end *td_sqty)) [Buy Value], ";
                        strsql = strsql + " convert(decimal (15,4),convert(money, case left(td_stlmnt,1) when 'N' then case sr_nodelyn when 'Y' then 0 else td_marketrate end else td_marketrate end *td_bqty)) [Sell Value] ,";
                        strsql = strsql + " case sr_nodelyn when 'Y' then 'NoDelv' else '' end [_], ";
                        strsql = strsql + " rtrim(ss_name)+case td_ssrno when -1 then 'a' else 'b' end [Ordr] ,td_dt BDate,'' [Net Value]";
                        strsql = strsql + " from trx with(" + StrTRXIndex + "nolock) left outer join std_rates with (nolock) on td_stlmnt = sr_stlmnt and td_scripcd = sr_scripcd and sr_nodelyn='Y' ,securities with (nolock) ";
                        strsql = strsql + " where td_clientcd =@clcd and td_stlmnt=@stlmnt  and  td_Dt between '" + dt + "' and '" + dt + "'  ";
                        strsql = strsql + " and td_scripcd = ss_cd and td_marginyn='B'";

                        strsql = strsql + " union All  ";
                        strsql = strsql + " select td_stlmnt, td_stlmnt +'['+ ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) + ']' Heading,";
                        strsql = strsql + " '0',0 , 0, 'C/F', rtrim(ss_name) Security, ";
                        strsql = strsql + " case sign(sum(td_bqty-td_sqty)) when 1 then 0 else sum(td_bqty-td_sqty) end , ";
                        strsql = strsql + " case sign(sum(td_sqty-td_bqty)) when 1 then 0 else sum(td_bqty-td_sqty) end ,  ";
                        strsql = strsql + " convert(decimal(15,2),max(sr_makingrate)),convert(decimal (15,4),0), ";
                        strsql = strsql + " convert(decimal (15,4),case sign(sum(td_bqty-td_sqty)) when 1 then 0 else sum((td_bqty-td_sqty)*sr_makingrate) end) , ";
                        strsql = strsql + " convert(decimal (15,4),case sign(sum(td_sqty-td_bqty)) when 1 then 0 else  sum((td_bqty-td_sqty)*sr_makingrate) end) ,  ";
                        strsql = strsql + " 'c/f', rtrim(ss_name)+'z' ,td_dt BDate,'' [Net Value] ";
                        strsql = strsql + " from trx with(" + StrTRXIndex + "nolock) ,securities with (nolock),std_rates with (nolock) ";
                        strsql = strsql + " where td_clientcd =@clcd and td_stlmnt=@stlmnt and left(td_stlmnt,1)='B' ";
                        strsql = strsql + " and  td_Dt between '" + dt + "' and '" + dt + "'  and td_stlmnt = sr_stlmnt and td_scripcd = sr_scripcd  ";
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
                        strsql = strsql + " and se_stdt between '" + dt + "' and '" + dt + "' ";

                        strsql = strsql + " union all   ";

                        strsql = strsql + " select  bc_stlmnt td_stlmnt,bc_stlmnt +'['+ ltrim(rtrim(convert(char,convert(datetime,se_stdt),103))) + ']', rtrim(cg_desc), bc_amount, se_stdt BDate";
                        strsql = strsql + " from Cbilled_charges with (nolock),settlements with (nolock),charges_master with (nolock)  ";
                        strsql = strsql + " where bc_clientcd=@clcd and bc_stlmnt=@stlmnt and bc_stlmnt = se_Stlmnt  ";
                        strsql = strsql + " and bc_companycode = cg_companycode and Left(bc_stlmnt,1) = cg_exchange and  bc_chargecode =cg_cd ";
                        strsql = strsql + " and se_stdt between '" + dt + "' and '" + dt + "'  ";

                        strsql = strsql + " union all ";

                        strsql = strsql + " select sh_stlmnt td_stlmnt,sh_stlmnt +'['+ ltrim(rtrim(convert(char,convert(datetime,se_stdt),103))) + ']',rtrim(cg_desc), sh_servicetax, se_stdt BDate";
                        strsql = strsql + " from Specialcharges with (nolock),settlements with (nolock),charges_master with (nolock)  ";
                        strsql = strsql + " where sh_clientcd=@clcd and sh_stlmnt=@stlmnt and sh_stlmnt = se_Stlmnt  ";
                        strsql = strsql + " and sh_companycode = cg_companycode and Left(sh_stlmnt,1) = cg_exchange and  cg_cd = '01' ";
                        strsql = strsql + " and se_stdt between '" + dt + "' and '" + dt + "'  and sh_servicetax > 0 ) A";
                        strsql = strsql + " gROUP bY sh_stlmnt,rtrim(sh_desc),BDate,td_stlmnt";
                        strsql = strsql.Replace("=@stlmnt", " like '" + Stlmnt + "%'").Replace("@clcd", "'" + userId + "'");
                    }
                    strsql = " select Stlmnt,ScripCode,OrderID,TradeID,TradeTime,ScripName,Buy,Sell,MarketRate,Brokerage,BuyValue,SellValue,Ordr,tdDt,NetValue from (" + strsql + ") a ";
                    strsql = strsql + " Order by Ordr,ScripName,TradeTime ";
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
                            if (strstlmnt != string.Empty && strstlmnt != ObjDataSet.Tables[0].Rows[i]["Stlmnt"].ToString().Trim())
                            {
                                strstlmnt = ObjDataSet.Tables[0].Rows[i]["Stlmnt"].ToString().Trim();
                                DataRow ObjRow1 = ObjDataSet.Tables[0].NewRow();
                                TotalDiff = Math.Round((decimal)(TotalSval - TotalBval), strparmcd == "Y" ? 0 : 2);
                                if (TotalBval < TotalSval)
                                {
                                    ObjRow1["ScripName"] = "Due To You :";
                                    ObjDataSet.Tables[0].Rows.InsertAt(ObjRow1, i);
                                    i = i + 1;
                                }
                                else
                                {
                                    ObjRow1["ScripName"] = "Due To Us :";
                                    ObjDataSet.Tables[0].Rows.InsertAt(ObjRow1, i);
                                    i = i + 1;
                                }

                                var apiRes2 = objUtility.mfnRoundoffCashbill(userId, dt, TotalDiff, Strings.Left(strstlmnt, 1));

                                ObjRow1["Net Value"] = objUtility.mfnFormatCurrency(Convert.ToDecimal(apiRes2), 2);
                                TotalBval = 0;
                                TotalSval = 0;
                                TotalDiff = 0;
                            }
                            if (strstlmnt == string.Empty)
                            {
                                strstlmnt = ObjDataSet.Tables[0].Rows[i]["Stlmnt"].ToString().Trim();
                            }
                            if (strstlmnt == ObjDataSet.Tables[0].Rows[i]["Stlmnt"].ToString().Trim())
                            {
                                TotalBval = TotalBval + (decimal)ObjDataSet.Tables[0].Rows[i]["BuyValue"];
                                TotalSval = TotalSval + (decimal)ObjDataSet.Tables[0].Rows[i]["SellValue"];
                                TotalSval = System.Math.Abs(TotalSval);
                                TotalBval = System.Math.Abs(TotalBval);
                            }

                            if (strCharges == "")
                            {
                                if (ObjDataSet.Tables[0].Rows[i]["ScripCode"].ToString().Trim() == "Charges")
                                {
                                    strCharges = "Charges";
                                    DataRow ObjRow1 = ObjDataSet.Tables[0].NewRow();

                                    TotalDiff = Math.Round((decimal)(TotalSval - TotalBval), strparmcd == "Y" ? 0 : 2);

                                    var apiRes2 = objUtility.mfnRoundoffCashbill(userId, dt, TotalDiff, Strings.Left(strstlmnt, 1));

                                    TotalDiff = Convert.ToDecimal(apiRes2);

                                    ObjRow1["ScripName"] = "Net Item Amount (Sale-Buy) :";
                                    ObjRow1["NetValue"] = objUtility.mfnFormatCurrency(TotalDiff, 2);
                                    ObjDataSet.Tables[0].Rows.InsertAt(ObjRow1, i);
                                    i = i + 1;
                                }
                            }
                        }
                        DataRow ObjRow2 = ObjDataSet.Tables[0].NewRow();
                        TotalDiff = Math.Round((decimal)(TotalSval - TotalBval), strparmcd == "Y" ? 0 : 2);

                        var apiRes3 = objUtility.mfnRoundoffCashbill(userId, dt, TotalDiff, Strings.Left(strstlmnt, 1));

                        TotalDiff = Convert.ToDecimal(apiRes3);

                        if (TotalBval < TotalSval)
                        {
                            ObjRow2["ScripName"] = "Due To You :";
                        }
                        else
                        {
                            ObjRow2["ScripName"] = "Due To Us :";
                        }
                        ObjRow2["NetValue"] = objUtility.mfnFormatCurrency(TotalDiff, 2);
                        ObjDataSet.Tables[0].Rows.InsertAt(ObjRow2, ObjDataSet.Tables[0].Rows.Count);
                        ObjDataSet.AcceptChanges();

                        TotalBval = 0;
                        TotalSval = 0;
                    }
                }
            }

            if (exchSeg.Substring(1, 1) == "F" || exchSeg.Substring(1, 1) == "K")
            {
                string StrTradesIndex = "";
                var result = objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true);
                if (Convert.ToInt16(result.Trim()) == 1)
                { StrTradesIndex = "index(idx_trades_clientcd),"; }


                string StrMsg;
                StrMsg = objUtility.fnCheckInterOperability(dt, exchSeg.Substring(1, 1));

                if (StrMsg.ToUpper().Trim() == "TRUE")
                {
                    exchSeg = "X" + exchSeg.Substring(1, 1);
                }


                if (_configuration["IsTradeWeb"] == "O")//live database
                {

                    string StrConn = _configuration.GetConnectionString("DefaultConnection");
                    using (SqlConnection ObjConnectionTmp = new SqlConnection(StrConn))
                    {
                        ObjConnectionTmp.Open();
                        ObjDataSet = objUtility.fnForBill(userId, exchSeg, dt, ObjConnectionTmp);
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
                                if (Conversion.Val(ObjDataSet.Tables[0].Rows[i]["SortOrder"].ToString().Trim()) >= 10)
                                {
                                    strCharges = "Charges";

                                    DataRow ObjRow1 = ObjDataSet.Tables[0].NewRow();
                                    ObjRow1["seriesname"] = "Value Before Adding Charges :";
                                    ObjRow1["tddt"] = ObjDataSet.Tables[0].Rows[i]["tddt"].ToString().Trim();
                                    ObjRow1["NetValue"] = objUtility.mfnFormatCurrency(TotalDrCr, 2);
                                    ObjDataSet.Tables[0].Rows.InsertAt(ObjRow1, i);
                                    i = i + 1;
                                }
                            }
                            TotalDrCr = TotalDrCr + (decimal)ObjDataSet.Tables[0].Rows[i]["drcr"];
                        }
                        DataRow ObjRow2 = ObjDataSet.Tables[0].NewRow();

                        ObjRow2["tddt"] = ObjDataSet.Tables[0].Rows[ObjDataSet.Tables[0].Rows.Count - 1]["tddt"].ToString().Trim();
                        ObjRow2["NetValue"] = objUtility.mfnFormatCurrency(TotalDrCr, 2);
                        ObjDataSet.Tables[0].Rows.InsertAt(ObjRow2, ObjDataSet.Tables[0].Rows.Count);
                        ObjDataSet.AcceptChanges();
                    }
                }
            }

            if (exchSeg.Substring(1, 1) == "X")
            {
                if (_configuration["IsTradeWeb"] == "O")//live database
                {
                    string StrConn = _configuration.GetConnectionString("DefaultConnection");
                    using (SqlConnection ObjConnectionTmp = new SqlConnection(StrConn))
                    {
                        ObjConnectionTmp.Open();
                        ObjDataSet = objUtility.fnForBill(userId, exchSeg, dt, ObjConnectionTmp);
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
                                if (Conversion.Val(ObjDataSet.Tables[0].Rows[i]["SortOrder"].ToString().Trim()) >= 10)
                                {
                                    strCharges = "Charges";
                                    DataRow ObjRow1 = ObjDataSet.Tables[0].NewRow();
                                    ObjRow1["seriesname"] = "Value Before Adding Charges :";
                                    ObjRow1["NetValue"] = objUtility.mfnFormatCurrency(TotalDrCr, 2);
                                    ObjDataSet.Tables[0].Rows.InsertAt(ObjRow1, i);
                                    i = i + 1;
                                }
                            }
                            TotalDrCr = TotalDrCr + (decimal)ObjDataSet.Tables[0].Rows[i]["drcr"];
                        }
                        DataRow ObjRow2 = ObjDataSet.Tables[0].NewRow();

                        ObjRow2["NetValue"] = objUtility.mfnFormatCurrency(TotalDrCr, 2);
                        ObjDataSet.Tables[0].Rows.InsertAt(ObjRow2, ObjDataSet.Tables[0].Rows.Count);
                        ObjDataSet.AcceptChanges();
                    }
                }
            }

            return ObjDataSet;
        }
        #endregion
        #endregion

        #region Confirmation Handler Method
        // get dropdown menu cumulative details data
        public dynamic Transaction_Detail(string userId, string exch, string seg, int type, string fromdate, string todate, string scripcode)
        {
            var query = Transaction_Detail_Query(userId, exch, seg, type, fromdate, todate, scripcode);
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
        public dynamic Confirmation(string userId, int type, string tdDt)
        {
            try
            {
                var ds = Confirmation_Process(userId, type, tdDt);
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
        private DataSet Confirmation_Process(string userId, int type, string dt)
        {
            DataSet ObjDataSet = new DataSet();
            string StrComTradesIndex = "";
            string StrTradesIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
            { StrTradesIndex = "index(idx_trades_clientcd),"; }

            string StrTRXIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'Trx' and b.name", "idx_Trx_Clientcd", true)) == 1)
            { StrTRXIndex = "index(idx_trx_clientcd),"; }

            if (type == 0)
            {
                string strsql = string.Empty;

                strsql = " Select 1 as SortOrder,'Equity : ' + td_stlmnt as type, td_stlmnt stlmnt,td_scripcd scripcode,replace(rtrim(ss_Name)+' ('+td_scripcd+')','&','') scripname, ";
                strsql = strsql + " sum(td_bqty) 'Buy',sum(convert(decimal(15,2),convert(money,td_bqty * td_rate))) 'BuyAmount', sum(td_sqty) 'Sell', ";
                strsql = strsql + " sum(convert(decimal(15,4),convert(money,td_sqty * td_rate))) 'SellAmount', sum(td_bqty - td_sqty) 'Net', ";
                strsql = strsql + " sum(convert(decimal(15,4),convert(money,(td_sqty - td_bqty)*td_rate))) 'NetAmount', ";
                strsql = strsql + " cast(convert(money,case when sum(td_bqty - td_sqty)=0 then 0 else sum((td_bqty-td_sqty)*td_rate)/sum(td_bqty-td_sqty) ";
                strsql = strsql + " end) as decimal(15,2)) 'AvgRate', ";
                strsql = strsql + " cast(sum(td_brokerage*(td_bqty+td_sqty)) as decimal(15,4)) Brokerage, ";
                strsql = strsql + " left(td_stlmnt,1)+'/C/'+td_scripcd 'Lookup' ";
                strsql = strsql + " from trx with(" + StrTRXIndex + "nolock) , securities with (nolock), Settlements with (nolock) ";
                strsql = strsql + " where td_clientcd= '" + userId + "'  and td_stlmnt = se_stlmnt and td_scripcd = ss_cd and td_dt='" + dt + "' ";
                strsql = strsql + " group by 'Equity : ' + td_stlmnt ,td_stlmnt,td_scripcd,ss_Name ,td_dt";
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
                strsql = strsql + " td_Exchange+'/'+td_Segment+'/'+Ltrim(convert(char,td_seriesid)) ";
                strsql = strsql + " from trades with(" + StrTradesIndex + "nolock), series_master with (nolock) ";
                strsql = strsql + " where td_clientcd='" + userId + "' and td_seriesid=sm_seriesid and td_Exchange = sm_exchange and td_Segment = sm_Segment and td_dt ='" + dt + "' and td_segment in ('F') ";
                strsql = strsql + " group by sm_prodtype,sm_symbol,sm_desc, sm_expirydt, sm_strikeprice, sm_callput,sm_optionstyle,td_dt,td_Exchange,td_Segment,td_seriesid";
                strsql = strsql + " Union All";

                strsql = strsql + " select case ex_eaflag when 'E' then 4 else 5 end ,case ex_eaflag when 'E' then 'Exercise' else 'Assignment' ";
                strsql = strsql + " end Td_Type, 'Exp: ' +convert(char(10),convert(datetime,sm_expirydt),105),";
                strsql = strsql + " ltrim( convert(char(8),sm_strikeprice))+sm_callput, replace(rtrim(sm_symbol)+' ('+";
                strsql = strsql + " ltrim( convert(char(8),sm_strikeprice))+sm_callput+sm_optionstyle+')','&',''), sum(ex_aqty) Bqty,  sum(ex_aqty*ex_diffrate) BAmt,";
                strsql = strsql + " sum(ex_eqty) Sqty, sum(ex_eqty*ex_diffrate) SAmt, sum(ex_aqty-ex_eqty) NQty,  sum((ex_aqty-ex_eqty)*ex_diffrate) NAmt,";
                strsql = strsql + " cast(convert(money,case when sum(ex_aqty-ex_eqty)=0 then 0 else sum((ex_aqty-ex_eqty)*ex_diffrate)/sum(ex_aqty-ex_eqty) end) as decimal(15,2)) 'average',";
                strsql = strsql + " cast(sum(ex_brokerage*(ex_eqty+ex_aqty)) as decimal(15,4)) td_Brokerage,ex_exchange+'/'+ex_Segment+'/'+Ltrim(convert(char,ex_seriesid)) ";

                strsql = strsql + " from exercise with (nolock), series_master with (nolock)";
                strsql = strsql + " where ex_clientcd='" + userId + "' and ex_exchange=sm_exchange and ex_Segment=sm_Segment and ex_seriesid=sm_seriesid and ex_dt ='" + dt + "'";
                strsql = strsql + " group by ex_eaflag, sm_symbol,sm_desc, sm_expirydt, sm_strikeprice, sm_callput,sm_optionstyle,ex_exchange,ex_Segment,ex_dt,sm_prodtype,ex_seriesid";
                strsql = strsql + " Union All";

                strsql = strsql + " select case right(sm_prodtype,1) when 'F' then 6 else 7 end, case right(sm_prodtype,1) when 'F' then 'Currency Future'";
                strsql = strsql + " else 'Currency Option' end td_type, 'Exp: ' +convert(char(10),convert(datetime,sm_expirydt),105), case right(sm_prodtype,1)";
                strsql = strsql + " when 'F' then '' else convert(char(8),sm_strikeprice)+sm_callput end,replace(sm_symbol,'&',''), sum(td_bqty) ,";
                strsql = strsql + " sum(round(convert(money,td_bqty * td_rate*sm_multiplier),2)) , sum(td_sqty) ,";
                strsql = strsql + " sum(round(convert(money,td_sqty * td_rate*sm_multiplier),2)) , sum(td_bqty - td_sqty) ,";
                strsql = strsql + " sum(round(convert(money,(td_sqty - td_bqty)*td_rate*sm_multiplier),4)) , ";
                strsql = strsql + " cast(convert(money,case when sum(td_bqty - td_sqty)=0 then 0 else sum((td_sqty - td_bqty)*td_rate*sm_multiplier)/sum(td_bqty-td_sqty) end) as decimal(15,2)) ,";
                strsql = strsql + " cast(sum(td_brokerage*(td_bqty+td_sqty)) as decimal(15,4)) td_brokerage, td_Exchange+'/'+td_Segment+'/'+Ltrim(convert(char,td_seriesid)) ";
                strsql = strsql + " from trades with(" + StrTradesIndex + "nolock), series_master with (nolock)";
                strsql = strsql + " where td_clientcd='" + userId + "' and td_seriesid=sm_seriesid and td_Exchange = sm_exchange and td_Segment = sm_Segment and td_dt ='" + dt + "' and td_Segment in ('K')";
                strsql = strsql + " group by sm_prodtype,sm_symbol,sm_desc, sm_expirydt, sm_callput, sm_strikeprice, td_exchange,td_Segment,td_dt,td_seriesid ";

                if (_configuration["Commex"] != string.Empty)
                {

                    string StrCommexConn = "";
                    StrCommexConn = objUtility.GetCommexConnection();
                    if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb(StrCommexConn + ".sysobjects a, " + StrCommexConn + ".sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
                    { StrComTradesIndex = "index(idx_trades_clientcd),"; }

                    strsql = strsql + " Union All";

                    strsql = strsql + " select case right(sm_prodtype,1) when 'F' then 8 else 9 end, case right(sm_prodtype,1) when 'F' then 'Commodity Future'";
                    strsql = strsql + " else 'Commodity Option' end td_type, 'Exp: ' +convert(char(10),convert(datetime,sm_expirydt),105), case right(sm_prodtype,1)";
                    strsql = strsql + " when 'F' then '' else convert(char(8),sm_strikeprice)+sm_callput end,replace(sm_symbol,'&',''), sum(td_bqty) ,";
                    strsql = strsql + " sum(round(convert(money,td_bqty * td_rate),4)) , sum(td_sqty) ,  ";
                    strsql = strsql + " sum(round(convert(money,td_sqty * td_rate),4)) , sum(td_bqty - td_sqty) ,  ";
                    strsql = strsql + " sum(round(convert(money,(td_sqty - td_bqty)*td_rate),4)) , ";
                    strsql = strsql + " cast(convert(money,case when sum(td_bqty - td_sqty)=0 then 0 else sum((td_sqty - td_bqty)*td_rate)/sum(td_bqty-td_sqty) end) as decimal(15,4)),";
                    strsql = strsql + " cast(sum(td_brokerage*(td_bqty+td_sqty)*sm_multiplier) as decimal(15,4)),td_Exchange+'/'+'X'+'/'+Ltrim(convert(char,td_seriesid)) ";
                    strsql = strsql + " from " + StrCommexConn + ".trades with(" + StrComTradesIndex + "nolock)  ," + StrCommexConn + ".Series_master with (nolock) ";
                    strsql = strsql + " where td_clientcd='" + userId + "' and td_seriesid=sm_seriesid and td_Exchange = sm_exchange ";
                    strsql = strsql + " and td_dt ='" + dt + "'";
                    strsql = strsql + " group by sm_prodtype,sm_symbol,sm_desc, sm_expirydt, sm_callput, sm_strikeprice, td_exchange,td_dt,td_seriesid";
                }
                strsql = " select * from (" + strsql + ") a Order by SortORder,stlmnt,scripname ";

                ObjDataSet = objUtility.OpenDataSet(strsql);
            }
            else if (type == 1) //confirmation
            {
                string Strsql = string.Empty;

                Strsql = " SELECT 1 as SorORder,'Equity : ' + td_stlmnt as type,td_stlmnt stlmnt, td_scripcd scripcode,replace(ss_name,'&','') as scripname, td_bqty as buy, td_sqty as sell, cast((td_marketrate) as decimal(15,4)) as marketrate, cast((td_rate) as decimal(15,4)) as netrate,cast((((td_bqty + td_sqty) * td_rate)) * (Case td_bsflag when 'S' then '-1' else '1' end ) as decimal(15,2)) netamount,cast((td_brokerage)as decimal(15,4)) brokerage,td_stlmnt+'/'+td_scripcd as lookup ";
                Strsql = Strsql + " FROM trx with(" + StrTRXIndex + "nolock), Settlements with (nolock),securities with (nolock) ";
                Strsql = Strsql + " WHERE td_clientcd='" + userId + "'  and td_stlmnt = se_stlmnt and td_dt='" + dt + "' and td_scripcd=ss_cd";
                Strsql = Strsql + " union all ";
                Strsql = Strsql + " Select 2 as td_order,Case td_segment when 'F' then case TD_EXCHANGE When 'N' Then 'NSE F&O' When 'B' Then 'BSE F&O' end  when 'K' then case TD_EXCHANGE When 'M' Then 'MCX FX' When 'N' Then 'NSE FX' end end td_type,'',sm_symbol,replace(sm_desc,'&',''),td_bqty as bqty, ";
                Strsql = Strsql + " td_sqty as sqty, cast((td_marketrate)as decimal(15,4)) as diffrate,cast((td_rate)as decimal(15,4)) as netrate,convert(decimal(15,2),(td_bqty - td_sqty) * td_rate * sm_multiplier)  as amount,cast((td_brokerage)as decimal(15,4)),td_exchange+'/'+td_Segment+'/'+ convert(char,td_seriesid)  as td_lookup from trades with(" + StrTradesIndex + "nolock),series_master with (nolock) where td_seriesid=sm_seriesid and td_Exchange = sm_exchange and td_Segment = sm_Segment and td_clientcd='" + userId + "'  and  ";
                Strsql = Strsql + " td_dt ='" + dt + "' and td_trxflag='N' ";

                if (_configuration["Commex"] != null && _configuration["Commex"] != string.Empty)
                {

                    string StrCommexConn = "";
                    StrCommexConn = objUtility.GetCommexConnection();

                    if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb(StrCommexConn + ".sysobjects a, " + StrCommexConn + ".sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
                    { StrComTradesIndex = "index(idx_trades_clientcd),"; }

                    Strsql = Strsql + " union all ";
                    Strsql = Strsql + " Select 3,case ex_eaflag when 'E' then 'Exercise' else 'Assignment' end td_type,'',sm_symbol,replace(sm_desc,'&',''), (ex_eqty * (-1)) as bqty, (ex_aqty * (-1)) as sqty, cast((ex_diffrate) as decimal(15,4)) as diffrate, ";
                    Strsql = Strsql + " cast((ex_mainbrdiffrate) as decimal(15,4)) as netrate,0,cast((ex_brokerage) as decimal(15,4)),ex_exchange+'/'+ex_Segment+ Ltrim(convert(char,ex_seriesid)) as td_lookup from exercise with (nolock),series_master with (nolock) where ex_exchange=sm_exchange and sm_Segment=ex_Segment and  sm_seriesid=ex_seriesid and ex_clientcd='" + userId + "'  and ex_dt ='" + dt + "' ";
                    Strsql = Strsql + " union all ";
                    Strsql = Strsql + " Select 4 as td_order,CASE TD_EXCHANGE When 'N' Then 'NCDX Commodities' When 'M' Then 'MCX Commodities' When 'S' Then 'NSEL Commodities' When 'D' Then 'NSX Commodities' end as td_type,'',sm_symbol,replace(sm_desc,'&',''),td_bqty as bqty, ";
                    Strsql = Strsql + " td_sqty as sqty,cast((td_marketrate) as decimal(15,4)) as diffrate,cast((td_rate)as decimal(15,4)) as netrate,convert(decimal(15,2),td_rate*(td_bqty-td_sqty)*sm_multiplier),cast((td_brokerage)as decimal(15,4)),td_exchange+'/X/'+ convert(char,td_seriesid) as td_lookup ";
                    Strsql = Strsql + " from " + StrCommexConn + ".trades with(" + StrComTradesIndex + "nolock) ," + StrCommexConn + ".series_master with (nolock) ";
                    Strsql = Strsql + " where sm_seriesid=td_seriesid and sm_exchange=td_exchange ";
                }

                Strsql = Strsql + " and td_clientcd='" + userId + "'  and  ";
                Strsql = Strsql + " td_dt ='" + dt + "' and td_trxflag='N' ";

                Strsql = "select * from (" + Strsql + ") a order by SorORder,type,stlmnt,scripname ";

                ObjDataSet = objUtility.OpenDataSet(Strsql);
            }

            return ObjDataSet;
        }

        // get dropdown menu cumulative details data
        public string Transaction_Detail_Query(string userId, string exch, string seg, int type, string fromdate, string todate, string scripcode)
        {
            string Strsql = "";
            string StrTRXIndex = string.Empty;
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'Trx' and b.name", "idx_Trx_Clientcd", true)) == 1)
            { StrTRXIndex = "index(idx_trx_clientcd),"; }

            string StrTradesIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
            { StrTradesIndex = "index(idx_trades_clientcd),"; }


            if (seg == "C")
            {
                if (type == 0 || type == 1)
                {
                    Strsql = " Select 1 as SortOrder,'Equity' as type, td_stlmnt as stlmnt,td_scripcd scripcd ,rtrim(ss_Name)+' ('+td_scripcd+')' scripname, ";
                    Strsql = Strsql + " sum(td_bqty) 'Buy',sum(round(convert(money,td_bqty * td_rate),4)) 'BuyAmount', sum(td_sqty) 'Sell', ";
                    Strsql = Strsql + " sum(round(convert(money,td_sqty * td_rate),4)) 'Sellamount', sum(td_bqty - td_sqty) 'Net', ";
                    Strsql = Strsql + " sum(round(convert(money,(td_sqty - td_bqty)*td_rate),4)) 'NetAmount', ";
                    Strsql = Strsql + " cast(convert(money,case when sum(td_bqty - td_sqty)=0 then 0 else sum((td_bqty-td_sqty)*td_rate)/sum(td_bqty-td_sqty) ";
                    Strsql = Strsql + " end) as decimal(15,2)) 'AvgRate', ";
                    Strsql = Strsql + " cast(sum(td_brokerage*(td_bqty+td_sqty)) as decimal(15,4)) Brokerage,td_orderid orderid,td_tradeid tradeid,td_time trdtime,";
                    Strsql = Strsql + " case when sum(td_bqty) > 0 Then round(sum(td_bqty * td_rate)/sum(td_bqty),4) else 0 end BuyRate,";
                    Strsql = Strsql + " case when sum(td_sqty) > 0 Then round(sum(td_sqty * td_rate)/sum(td_sqty),4) else 0 end SellRate";
                    Strsql = Strsql + " from Trx with (nolock), securities with (nolock), Settlements with (nolock) ";
                    Strsql = Strsql + " where td_clientcd= '" + userId + "' and td_stlmnt = se_stlmnt and td_scripcd = ss_cd ";
                    if (exch != "" && exch != null) { Strsql = Strsql + " and left(td_stlmnt,1) = '" + exch + "'"; }
                    Strsql = Strsql + " and td_dt between '" + fromdate + "' and '" + todate + "'";
                    if (scripcode != "" && scripcode != null) { Strsql += " and td_scripcd = '" + scripcode.Trim() + "'"; }
                    Strsql = Strsql + " group by td_stlmnt,td_scripcd,ss_Name ,td_dt,td_tradeid,td_time";
                }
            }
            else if (seg == "F" || seg == "K")
            {
                if (type == 0 || type == 1)
                {
                    if (Strsql.Length > 0) { Strsql = Strsql + " union all "; }
                    Strsql += "select case right(sm_prodtype,1) when 'F' then 2 else 3 end SortOrder, case td_Segment When 'F' Then 'Equity ' else 'Currency ' end + case right(sm_prodtype,1) ";
                    Strsql = Strsql + " when 'F' then 'Future' else 'Option' end type, 'Exp: '+ convert(char(10),convert(datetime,sm_expirydt),105) as stlmnt, case right(sm_prodtype,1) ";
                    Strsql = Strsql + " when 'F' then '' else ltrim(convert(char(8),sm_strikeprice))+sm_callput end scripcode,rtrim(sm_symbol)+case right(sm_prodtype,1)  when 'F' then ''  else ' ('+ltrim(convert(char(8),sm_strikeprice))+sm_callput+')' end as scripname,sum(td_bqty) 'buy',  ";
                    Strsql = Strsql + " sum(round(convert(money,td_bqty * td_rate * sm_multiplier ),4)) 'BuyAmount', sum(td_sqty) 'Sell',";
                    Strsql = Strsql + " sum(round(convert(money,td_sqty * td_rate * sm_multiplier ),4)) 'SellAmount', sum(td_bqty - td_sqty) 'Net',   sum(round(convert(money,(td_sqty - td_bqty)*td_rate*sm_multiplier),4)) 'NetAmount', ";
                    Strsql = Strsql + " cast(convert(money,case when sum(td_bqty - td_sqty)=0 then 0 else  sum((td_bqty-td_sqty)*td_rate)/sum(td_bqty-td_sqty) end) as decimal(15,2)) 'AvgRate' , ";
                    Strsql = Strsql + " cast(sum(td_brokerage*(td_bqty*td_sqty)) as decimal(15,4)) Brokerage,td_orderid orderid,td_tradeid tradeid,td_time trdtime,";
                    Strsql = Strsql + " case when sum(td_bqty) > 0 Then round(sum(td_bqty * td_rate)/sum(td_bqty),4) else 0 end BuyRate,";
                    Strsql = Strsql + " case when sum(td_sqty) > 0 Then round(sum(td_sqty * td_rate)/sum(td_sqty),4) else 0 end SellRate";
                    Strsql = Strsql + " from trades with (nolock) , series_master with (nolock) ";
                    Strsql = Strsql + " where td_clientcd='" + userId + "' and td_segment = '" + seg + "'";
                    if (exch != "" && exch != null) { Strsql = Strsql + " and  td_exchange='" + exch + "'"; }
                    Strsql = Strsql + " and td_seriesid=sm_seriesid and td_trxFlag = 'N' ";
                    Strsql = Strsql + " and td_dt between '" + fromdate + "' and '" + todate + "'";
                    if (scripcode != "" && scripcode != null) { Strsql = Strsql + "and td_seriesid = " + scripcode + ""; }
                    Strsql = Strsql + " group by td_Segment,sm_prodtype,sm_symbol,sm_desc, sm_expirydt, sm_strikeprice, sm_callput,td_dt,td_orderid,td_tradeid,td_time";
                }
                if (type == 0 || type == 2)
                {
                    if (Strsql.Length > 0) { Strsql = Strsql + " union all "; }
                    Strsql += " select case ex_eaflag when 'E' then 4 else 5 end SortOrder ,case ex_Segment When 'F' Then 'Equity ' else 'Currency ' end +  case ex_eaflag when 'E' then 'Exercise' else 'Assignment'  end type, 'Exp: ' +convert(char(10), ";
                    Strsql = Strsql + " convert(datetime,sm_expirydt),105) stlmnt, ltrim( convert(char(8),sm_strikeprice))+sm_callput scripcode,   rtrim(sm_symbol)+' ('+ ltrim( convert(char(8),sm_strikeprice))+sm_callput+')' scripname, sum(ex_aqty) Buy, ";
                    Strsql = Strsql + " sum(ex_aqty*ex_diffrate) BuyAmount , sum(ex_eqty) Sell, sum(ex_eqty*ex_diffrate) SellAmount, sum(ex_aqty-ex_eqty) Net,   sum((ex_aqty-ex_eqty)*ex_diffrate) NetAmount, ";
                    Strsql = Strsql + " cast(convert(money,case when sum(ex_aqty-ex_eqty)=0 then 0  else sum((ex_aqty-ex_eqty)*ex_diffrate)/sum(ex_aqty-ex_eqty) end) as decimal(15,2)) 'AvgRate', ";
                    Strsql = Strsql + " cast(sum(ex_brokerage*(ex_eqty+ex_aqty)) as decimal(15,4)) Brokerage,";
                    Strsql = Strsql + " 0 orderid, 0 tradeid,'' trdtime,";
                    Strsql = Strsql + " case when sum(ex_aqty) > 0 Then round(sum(ex_aqty*ex_diffrate)/sum(ex_aqty),4) else 0 end BuyRate,";
                    Strsql = Strsql + " case when sum(ex_eqty) > 0 Then round(sum(ex_eqty*ex_diffrate)/sum(ex_eqty),4) else 0 end SellRate";
                    Strsql = Strsql + " from exercise with (nolock), series_master with (nolock) where ex_clientcd='" + userId + "' and  ";
                    Strsql = Strsql + " ex_exchange=sm_exchange and ex_Segment=sm_Segment and ex_seriesid=sm_seriesid ";
                    if (exch != "" && exch != null) { Strsql = Strsql + " and ex_exchange='" + exch + "'"; }
                    Strsql = Strsql + " and ex_Segment = '" + seg + "' and ex_dt='" + fromdate + "'";
                    if (scripcode != "" && scripcode != null) Strsql = Strsql + " and ex_seriesid = " + scripcode;
                    Strsql = Strsql + " group by ex_Segment,ex_eaflag, sm_symbol,sm_desc, sm_expirydt, sm_strikeprice, sm_callput,ex_exchange,ex_Segment,ex_dt,sm_prodtype";
                }
            }
            else if (seg == "X")
            {
                if (type == 0 || type == 1)
                {
                    if (Strsql.Length > 0) { Strsql = Strsql + " union all "; }
                    Strsql += "select case right(sm_prodtype,1) when 'F' then 2 else 3 end SortOrder, 'Commodity '+  case right(sm_prodtype,1) ";
                    Strsql = Strsql + " when 'F' then 'Future' else 'Option' end type, 'Exp: '+ convert(char(10),convert(datetime,sm_expirydt),105) as stlmnt, case right(sm_prodtype,1) ";
                    Strsql = Strsql + " when 'F' then '' else ltrim(convert(char(8),sm_strikeprice))+sm_callput end scripcode,rtrim(sm_symbol)+case right(sm_prodtype,1)  when 'F' then ''  else ' ('+ltrim(convert(char(8),sm_strikeprice))+sm_callput+')' end as scripname,sum(td_bqty) 'buy',  ";
                    Strsql = Strsql + " sum(round(convert(money,td_bqty * td_rate * sm_multiplier ),4)) 'BuyAmount', sum(td_sqty) 'Sell',";
                    Strsql = Strsql + " sum(round(convert(money,td_sqty * td_rate * sm_multiplier ),4)) 'SellAmount', sum(td_bqty - td_sqty) 'Net',   sum(round(convert(money,(td_sqty - td_bqty)*td_rate* sm_multiplier ),4)) 'NetAmount', ";
                    Strsql = Strsql + " cast(convert(money,case when sum(td_bqty - td_sqty)=0 then 0 else  sum((td_bqty-td_sqty)*td_rate)/sum(td_bqty-td_sqty) end) as decimal(15,2)) 'AvgRate' , ";
                    Strsql = Strsql + " cast(sum(td_brokerage*(td_bqty*td_sqty)) as decimal(15,4)) Brokerage,td_orderid orderid,td_tradeid tradeid,td_time trdtime,";
                    Strsql = Strsql + " case when sum(td_bqty) > 0 Then round(sum(td_bqty * td_rate)/sum(td_bqty),4) else 0 end BuyRate,";
                    Strsql = Strsql + " case when sum(td_sqty) > 0 Then round(sum(td_sqty * td_rate)/sum(td_sqty),4) else 0 end SellRate";
                    if (objUtility.GetWebParameter("Commex") != null && objUtility.GetWebParameter("Commex") != string.Empty)
                    {
                        string StrCommexConn = "";
                        StrCommexConn = objUtility.GetCommexConnection();
                        Strsql = Strsql + " from " + StrCommexConn + ".Trades," + StrCommexConn + ".Series_master";
                    }
                    Strsql = Strsql + " where td_clientcd='" + userId + "'  ";
                    if (exch != "" && exch != null) { Strsql = Strsql + " and  td_exchange='" + exch + "'"; }
                    Strsql = Strsql + " and td_seriesid=sm_seriesid and td_trxFlag = 'N' ";
                    Strsql = Strsql + " and td_dt between '" + fromdate + "' and '" + todate + "'";
                    if (scripcode != "" && scripcode != null) { Strsql = Strsql + " and td_seriesid = " + scripcode + ""; }
                    Strsql = Strsql + " group by sm_prodtype,sm_symbol,sm_desc, sm_expirydt, sm_strikeprice, sm_callput,td_dt,td_orderid,td_tradeid,td_time";
                }
            }

            return Strsql;
        }

        #endregion
        #endregion

        #region Login method

        public string Login_Password_GenerateOTP_SMS_Email(string userId, string mode)
        {
            List<OtpUserIdSession> sessionOtp = new List<OtpUserIdSession>();
            string Strsql = "";
            Boolean blnMobile = false;
            Boolean blnEmail = false;
            mode = mode.ToUpper();
            if (mode == "BOTH")
            {
                blnMobile = true;
                blnEmail = true;
            }
            else if (mode == "MOBILE")
            {
                blnMobile = true;
                blnEmail = false;
            }
            else if (mode == "EMAIL")
            {
                blnMobile = false;
                blnEmail = true;
            }
            else
                return "Invalid Mode Value";

            DataSet ds = new DataSet();
            ds = objUtility.OpenDataSet("select cm_cd, cm_mobile, cm_email from client_master where cm_cd='" + userId + "';");

            if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
            {
            }
            else
            {
                return "user not found";
            }

            string chars1 = "1234567890";
            char[] stringChars1 = new char[4];

            Random random1 = new Random();

            for (int i = 0; i < stringChars1.Length; i++)
            {
                stringChars1[i] = chars1[random1.Next(chars1.Length)];
            }

            string strSotp = new String(stringChars1);
            string strMsgTxt = string.Empty;
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("Sysparameter", "count(0)", "sp_parmcd", "SMSCLPW", true)) > 0)
            {
                strMsgTxt = objUtility.fnFireQueryTradeWeb("sysparameter", "SP_SYSVALUE", "sp_parmcd", "SMSCLPW", true);
            }
            else
            {
                strMsgTxt = "Your OTP to change password request for <Client> is <OTP>";
            }
            //strMsgTxt = "Your OTP to initiate change request for <Client> is <OTP>";
            strMsgTxt = strMsgTxt.Replace("<Client>", userId.ToUpper());
            strMsgTxt = strMsgTxt.Replace("<OTP>", strSotp);

            strMsgTxt = strMsgTxt.Replace("<&>", "<~>");
            strMsgTxt = strMsgTxt.Replace("&", "");
            strMsgTxt = strMsgTxt.Replace("<~>", "&");


            if (blnMobile == true)
            {
                string strMobileNo = ds.Tables[0].Rows[0]["cm_mobile"].ToString();
                if (string.IsNullOrEmpty(strMobileNo))
                {
                    return "Mobile no not found";
                }
                strMobileNo = strMobileNo.Trim();
                string[] strSMSParamVal = new string[5];
                string strSMSParameter = "SMSUSERID/SMSPWD/SMSSENDER/SMSLENGTH/SMSLINK";
                string strvalue = string.Empty;
                string strFromNo1 = "";
                string strFromNo2 = "";

                for (int i = 0; i <= 4; i++)
                {
                    strvalue = strSMSParameter.Split('/')[i];
                    strSMSParamVal[i] = objUtility.fnFireQueryTradeWeb("sysparameter", "SP_SYSVALUE", "sp_parmcd", strvalue, true);
                }

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
                //////  Calling API here
                //nm.Clear();
                //nm.Add("param1", "SMSCOUNTRYCD");
                //nm.Add("param2", "");
                //string strv = objAPI.Call_API_Get_string(nm, "ChangeDetail/GetSysParmStChangeDetail");
                //strv = JsonConvert.DeserializeObject<string>(strv);

                string strv = objUtility.GetSysParmSt("SMSCOUNTRYCD", "");
                //// End API Calling 
                if (strv == "Y")
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
                sr.Close();
                //response.Close();

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
                else
                {
                    strresponse = content;
                }

                Strsql = "Insert into sms_Logs values(";
                Strsql += "'" + userId.ToUpper() + "','" + strMobileNo + "','" + strMsgTxt + "',";
                Strsql += "'" + strresponse.Replace("'", " ") + "','" + userId.ToUpper() + "','" + objUtility.dtos(System.DateTime.Today.Date.ToString()) + "','" + DateTime.Now.ToString("HH:mm:ss") + "')";
                objUtility.ExecuteSQL(Strsql);
            }

            if (blnEmail == true)
            {
                string strEsendto = ds.Tables[0].Rows[0]["cm_email"].ToString();

                if (string.IsNullOrEmpty(strEsendto))
                {
                    return "Email not found";
                }

                if (_configuration["SMTPSERVER"].Trim() != "" && strEsendto != "")
                {
                    try
                    {
                        string strHost = string.Empty;
                        int intPort;
                        string strUserID = string.Empty;
                        string strPWD = string.Empty;
                        string strEmail = string.Empty;
                        string strSSL = string.Empty;
                        string strSMTPParamVal = _configuration["SMTPSERVER"];
                        string strEmailID = strEsendto;

                        strHost = strSMTPParamVal.Split('/')[0];
                        intPort = Convert.ToInt32(strSMTPParamVal.Split('/')[1]);
                        strUserID = strSMTPParamVal.Split('/')[2];
                        strPWD = strSMTPParamVal.Split('/')[3];
                        strEmail = strSMTPParamVal.Split('/')[4];
                        strSSL = strSMTPParamVal.Split('/')[5];

                        MailMessage Msg = new MailMessage();

                        Msg.From = new MailAddress(strEmail);
                        Msg.To.Add(strEmailID);
                        Msg.Subject = "Change Request OTP For Client " + userId.ToUpper();
                        Msg.Body = strMsgTxt;
                        Msg.IsBodyHtml = true;
                        SmtpClient smtp = new SmtpClient();
                        smtp.Host = strHost;
                        smtp.Port = intPort;
                        smtp.Credentials = new System.Net.NetworkCredential(strUserID, strPWD);
                        smtp.EnableSsl = strSSL == "N" ? false : true;
                        System.Net.ServicePointManager.ServerCertificateValidationCallback = delegate (object s,
                        System.Security.Cryptography.X509Certificates.X509Certificate certificate,
                        System.Security.Cryptography.X509Certificates.X509Chain chain,
                        System.Net.Security.SslPolicyErrors sslPolicyErrors)
                        {
                            return true;
                        };
                        smtp.Send(Msg);
                    }
                    catch (Exception)
                    {
                        return "Unable to send Email";
                    }
                }
            }

            List<OtpUserIdSession> responseSessionOtp = _httpContextAccessor.HttpContext.Session.GetObject<List<OtpUserIdSession>>("userOtp");

            if (responseSessionOtp == null)
            {
                responseSessionOtp = new List<OtpUserIdSession>
                {
                    new OtpUserIdSession { UserId = "xyzabc", OTP= "1234" }
                };
            }
            var otpUser = responseSessionOtp.Where(a => a.UserId == userId).FirstOrDefault();
            if (otpUser != null)
            {
                responseSessionOtp.Remove(otpUser);
            }

            responseSessionOtp.Add(new OtpUserIdSession { UserId = userId, OTP = strSotp });

            _httpContextAccessor.HttpContext.Session.SetObject("userOtp", responseSessionOtp);

            _httpContextAccessor.HttpContext.Session.SetString("testOtp", strSotp);

            string strMessage = string.Empty;
            if (mode == "ALL")
            {
                strMessage = "OTP sent to Mobile & Email";
            }
            else if (mode == "MOBILE" || mode == "NMOBILE")
            {
                strMessage = "OTP sent to " + (mode == "MOBILE" ? "Mobile" : "new Mobile");
            }
            else if (mode == "EMAIL" || mode == "NEMAIL")
            {
                strMessage = "OTP sent to " + (mode == "EMAIL" ? "Email" : "new Email");
            }

            return strSotp;
        }

        public Boolean Login_Password_Update_execute(string userId, string OTP, string oldPassword, string newPassword)
        {
            try
            {
                string Strsql = "";

                if (string.IsNullOrEmpty(newPassword))
                {
                    return false;
                }

                if (!string.IsNullOrEmpty(OTP) && !string.IsNullOrEmpty(oldPassword))
                {
                    return false;
                }
                else if (!string.IsNullOrEmpty(OTP))
                {
                    var responseSessionOtp = _httpContextAccessor.HttpContext.Session.GetObject<List<OtpUserIdSession>>("userOtp");
                    var responseOtp = responseSessionOtp.Where(x => x.UserId == userId && x.OTP == OTP).FirstOrDefault();

                    if (responseOtp != null)
                    {
                        responseSessionOtp.Remove(responseOtp);
                        _httpContextAccessor.HttpContext.Session.SetObject("userOtp", responseSessionOtp);

                        Strsql = "select cm_cd,cm_pwd from client_master where cm_cd='" + userId + "'";
                        DataSet ObjDsPwd = new DataSet();
                        ObjDsPwd = objUtility.OpenDataSet(Strsql);
                        if (ObjDsPwd.Tables[0].Rows.Count > 0)
                        {
                            string strLastLoginDt = objUtility.fnFireQueryTradeWeb("Client_master", "cm_lastlogindt", "cm_cd", userId, true);

                            Strsql = " update Client_master set cm_pwd='" + newPassword + "' where cm_cd='" + userId + "'";
                            objUtility.ExecuteSQL(Strsql);

                            return true;
                        }

                        return false;
                    }
                    else
                    {
                        return false;
                    }

                }
                else if (!string.IsNullOrEmpty(oldPassword))
                {
                    Strsql = "select cm_cd,cm_pwd from client_master where cm_cd='" + userId + "'";
                    DataSet ObjDsPwd = new DataSet();
                    ObjDsPwd = objUtility.OpenDataSet(Strsql);
                    if (ObjDsPwd.Tables[0].Rows.Count > 0)
                    {
                        string strLastLoginDt = objUtility.fnFireQueryTradeWeb("Client_master", "cm_lastlogindt", "cm_cd", userId, true);
                        if (ObjDsPwd.Tables[0].Rows[0]["cm_pwd"].ToString().Trim() == oldPassword)
                        {
                            Strsql = " update Client_master set cm_pwd='" + newPassword + "' where cm_cd='" + userId + "'";
                            Strsql = Strsql + " and cm_pwd='" + oldPassword + "'";
                            objUtility.ExecuteSQL(Strsql);

                            return true;
                        }
                        else
                        {
                            return false;
                        }
                    }

                    return false;
                }
                return false;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region Reset Password method
        public string Login_GetPassword_Execute(string userId)
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

        #region Margin Handler method
        // For getting margin data
        public dynamic GetMarginMainData(string cm_cd, string strCompanyCode)
        {
            try
            {
                var ds = GetQueryMainData(cm_cd, strCompanyCode);
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

        // For getting dropdownlist data
        public dynamic GetDropdownListData(string cm_cd, string strCompanyCode)
        {
            var query = GetQueryDropdownData(cm_cd, strCompanyCode);
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

        // For getting Margin Pledge data
        public dynamic GetMarginPledgeData(string cm_cd, string UserId, string strCompanyCode, string CmbDPID_Value)
        {
            try
            {
                var ds = GetQueryMarginPledgeData(cm_cd, UserId, strCompanyCode, CmbDPID_Value);
                //var ds = CommonRepository.OpenDataSetTmp(query);
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

        // For getting current pledge request
        public dynamic GetCurrentPledgeRequest(string UserId)
        {
            try
            {
                var ds = GetQueryCurrentPledgeRequest(UserId);

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

        // For insert margin pledge request
        public dynamic AddPledgeRequest(string UserId, string CmbDPID_Value, bool blnIdentityOn, string intcnt, string lblScripcd, string txtQty)
        {
            try
            {
                var ds = AddQueryPledgeRequest(UserId, CmbDPID_Value, blnIdentityOn, intcnt, lblScripcd, txtQty);
                var json = JsonConvert.SerializeObject(ds);
                return json;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #region Margin usefull method

        // get data for margin main grid
        private DataSet GetQueryMainData(string cm_cd, string strCompanyCode)
        {
            DataSet objdataset = new DataSet();
            if (_configuration["IsTradeWeb"] == "O")//Live DB {
            {
                objdataset = fnGetRptSQL(true, cm_cd, strCompanyCode);
            }
            else
            {
                strsql = " Select case right(fm_companycode,2) When 'BF' Then 'BSE F&O' When 'NF' Then 'NSE F&O' When 'MF' Then 'MCX F&O' else '' end ExchSeg,";
                strsql += " fm_clientcd,cast(fm_spanmargin as decimal(15,2)) as fm_spanmargin,cast(fm_exposurevalue as decimal(15,2)) as fm_exposurevalue, cast(fm_buypremmargin as decimal(15,2)) as fm_buypremmargin,";
                strsql += " cast(fm_initialmargin as decimal(15,2)) as fm_initialmargin,cast(fm_additionalmargin as decimal(15,2)) as fm_additionalmargin, cast(fm_collected as decimal(15,2)) as fm_collected,cast(isnull(fm_spanmargin,0 )+ isnull(fm_exposurevalue,0) as decimal(15,2)) 'margin',";
                strsql += " cast(case when (fm_initialmargin + fm_exposurevalue- case when fm_collected > 0 then fm_collected else 0 end) >0 then (fm_initialmargin + fm_exposurevalue- case when fm_collected > 0 then fm_collected else 0 end) else 0 end as decimal(15,2)) ShortFall";
                strsql += " ,convert(char,convert(datetime, fm_dt),103) as DisplayDate from fmargins where right(fm_companycode,1) = 'F' and fm_dt = (select max(fm_Dt) from fmargins Where right(fm_companycode,1) = 'F' ) and fm_clientcd='" + cm_cd + "'";
                strsql += " union all";
                strsql += " Select case right(fm_companycode,2) When 'BK' Then 'BSE FX' When 'NK' Then 'NSE FX' When 'MK' Then 'MCX FX' else '' end ExchSeg,";
                strsql += " fm_clientcd,cast(fm_spanmargin as decimal(15,2)) as fm_spanmargin,cast(fm_exposurevalue as decimal(15,2)) as fm_exposurevalue, cast(fm_buypremmargin as decimal(15,2)) as fm_buypremmargin,";
                strsql += " cast(fm_initialmargin as decimal(15,2)) as fm_initialmargin,cast(fm_additionalmargin as decimal(15,2)) as fm_additionalmargin, cast(fm_collected as decimal(15,2)) as fm_collected,cast(isnull(fm_spanmargin,0 )+ isnull(fm_exposurevalue,0) as decimal(15,2)) 'margin',";
                strsql += " cast(case when (fm_initialmargin + fm_exposurevalue- case when fm_collected > 0 then fm_collected else 0 end) >0 then (fm_initialmargin + fm_exposurevalue- case when fm_collected > 0 then fm_collected else 0 end) else 0 end as decimal(15,2)) ShortFall";
                strsql += " ,convert(char,convert(datetime, fm_dt),103) as DisplayDate from fmargins where right(fm_companycode,1) = 'K' and fm_dt = (select max(fm_Dt) from fmargins Where right(fm_companycode,1) = 'K' ) and fm_clientcd='" + cm_cd + "'";
                strsql += " union all";
                strsql += " Select case right(fm_companycode,2) When 'MX' Then 'MCX Commodity' When 'NX' Then 'NCDEX Commodity' else '' end ExchSeg,fm_clientcd,cast(fm_spanmargin as decimal(15,2)) as fm_spanmargin,";
                strsql += " cast(fm_exposurevalue as decimal(15,2)) as fm_exposurevalue, cast(fm_buypremmargin as decimal(15,2)) as fm_buypremmargin,cast(fm_initialmargin as decimal(15,2)) as fm_initialmargin,cast(fm_additionalmargin as decimal(15,2)) as fm_additionalmargin, cast(fm_collected as decimal(15,2)) as fm_collected,";
                strsql += " cast(isnull(fm_spanmargin,0 )+ isnull(fm_exposurevalue,0) as decimal(15,2)) 'margin',cast(case when (fm_initialmargin + fm_exposurevalue- case when fm_collected > 0 then fm_collected else 0 end) >0 then (fm_initialmargin + fm_exposurevalue- case when fm_collected > 0 then fm_collected else 0 end) else 0 end as decimal(15,2)) ShortFall,convert(char,convert(datetime, fm_dt),103) as  DisplayDate ";
                strsql += " from fmargins  where right(fm_companycode,1) = 'X' and fm_dt = (select max(fm_Dt)  from fmargins Where right(fm_companycode,1) = 'X' ) and fm_clientcd='" + cm_cd + "'";
                objdataset = objUtility.OpenDataSet(strsql);

            }
            return objdataset;
        }

        // used for getting margin main data
        public DataSet fnGetRptSQL(bool blnisLetter, string cm_cd, string strCompanyCode)
        {
            string StrConn = _configuration.GetConnectionString("DefaultConnection");
            string strCase = "";
            string strFild = "";
            string strExchSegme = "";
            string strSegCode = "";
            DataSet rsExcgSeg;

            string strTotalShortFallX = "";
            string strTotalCollectedX = "";
            string strClientWhere = "";

            using (SqlConnection ObjConnectionTmp = new SqlConnection(StrConn))
            {
                string StrCommexConn = "";
                if (objUtility.GetWebParameter("Commex") != null && objUtility.GetWebParameter("Commex") != string.Empty)
                {
                    StrCommexConn = objUtility.GetCommexConnection();
                }

                string strSql = "";
                ObjConnectionTmp.Open();
                objUtility.ExecuteSQLTmp("if OBJECT_ID('tempdb..#TmpPeakColl') is not null Drop Table #TmpPeakColl", ObjConnectionTmp);
                objUtility.ExecuteSQLTmp("Create Table #TmpPeakColl (Tmp_Clientcd VarChaR(8),Tmp_CompanyCode VarChaR(3),Tmp_PeakMargin Money,Tmp_PeakColl Money,Tmp_Shortfall Money, Tmp_exchange char(1), Tmp_segment char(1) , Tmp_Nfiller4 money, Tmp_Nfiller5 money )", ObjConnectionTmp);

                string strdate = "";
                strdate = objUtility.fnFireQueryTradeWeb("Fmargins", "max(fm_Dt)", "1", "1", true).ToString().Trim();
                if (Conversion.Val(objUtility.fnFireQueryTradeWeb("Fmargin_PeakMargin", "Count(0)", "fc_companycode='" + strCompanyCode + "' and fc_dt ", "(select max(fc_dt) from Fmargin_PeakMargin)", true).ToString()) > 0)
                {

                    strSql = " insert into #TmpPeakColl ";
                    strSql += " select fm_clientcd,'" + strCompanyCode + "'+fm_exchange+fm_Segment ,isNull(fm_NFiller4,0), isNull(fm_NFiller5,0) , 0,  fm_exchange, fm_Segment ,isNull(fm_NFiller4,0), isNull(fm_NFiller5,0) ";
                    strSql += " from Fmargins, Client_master ";
                    strSql += " Where fm_clientcd=cm_cd and fm_Companycode ='" + strCompanyCode + "' and fm_dt = (select max(fm_Dt) from fmargins) ";
                    if (objUtility.GetWebParameter("Commex") != null && objUtility.GetWebParameter("Commex") != string.Empty)
                    {
                        strSql += " union ";
                        strSql += " select fm_clientcd,'" + strCompanyCode + "'+fm_exchange+'X',isNull(fm_PeakMargin,0)+isNull(fm_Filler2,0),isNull(fm_Filler1,0), 0 ,  fm_exchange, 'X' fm_Segment ,isNull(fm_PeakMargin,0)+isNull(fm_Filler2,0),isNull(fm_Filler1,0)";
                        strSql += " from " + StrCommexConn + ".Fmargins, Client_master ";
                        strSql += " Where fm_clientcd=cm_cd and fm_Companycode ='" + strCompanyCode + "' and fm_dt = (select max(fm_Dt) from fmargins) and fm_clientcd ='" + cm_cd + "'";
                    }
                    objUtility.ExecuteSQLTmp(strSql, ObjConnectionTmp);

                    strSql = " Update #TmpPeakColl set Tmp_PeakMargin = Round(Tmp_PeakMargin * " + objUtility.fnPeakFactor(strdate) + "/100,2) Where Right(Tmp_CompanyCode,2) <> 'MX' ";
                    objUtility.ExecuteSQLTmp(strSql, ObjConnectionTmp);
                }
                else
                {
                    strSql = " insert into #TmpPeakColl ";
                    strSql += " select fm_clientcd,fm_exchange+fm_Segment,isNull(fm_NFiller4,0), 0 , 0 , fm_exchange, fm_Segment ,isNull(fm_NFiller4,0), isNull(fm_NFiller5,0)";
                    strSql += " from Fmargins, Client_master ";
                    strSql += " Where fm_clientcd=cm_cd and fm_Companycode ='" + strCompanyCode + "' and fm_dt = (select max(fm_Dt) from fmargins) ";
                    if (objUtility.GetWebParameter("Commex") != null && objUtility.GetWebParameter("Commex") != string.Empty)
                    {
                        strSql += " union ";
                        strSql += " select fm_clientcd,fm_exchange+'X',isNull(fm_PeakMargin,0)+isNull(fm_Filler2,0), 0 , 0 , fm_exchange, 'X' fm_Segment, isNull(fm_PeakMargin,0)+isNull(fm_Filler2,0),isNull(fm_Filler1,0) ";
                        strSql += " from " + StrCommexConn + ".Fmargins, Client_master ";
                        strSql += " Where fm_clientcd=cm_cd and fm_Companycode ='" + strCompanyCode + "' and fm_dt = (select max(fm_Dt) from fmargins) ";
                    }
                    objUtility.ExecuteSQLTmp(strSql, ObjConnectionTmp);

                    strSql = " Update #TmpPeakColl set Tmp_PeakMargin = Round(Tmp_PeakMargin * " + objUtility.fnPeakFactor(strdate) + "/100,2) Where Right(Tmp_CompanyCode,2) <> 'MX'";
                    objUtility.ExecuteSQLTmp(strSql, ObjConnectionTmp);

                    strSql = " Update #TmpPeakColl set Tmp_PeakColl = case When isNull(fc_FillerN9,0) > 0 then isNull(fc_FillerN9,0) else 0 end ";
                    strSql += " from Fmargin_clients ";
                    strSql += " Where fc_clientcd=Tmp_clientcd and fc_Companycode ='" + strCompanyCode + "' and fc_Exchange = '' and fc_dt = (select max(fm_Dt) from fmargins) ";
                    objUtility.ExecuteSQLTmp(strSql, ObjConnectionTmp);
                }
                strSql = " Update #TmpPeakColl set Tmp_PeakMargin=0,Tmp_PeakColl=0 Where Tmp_PeakColl>=Tmp_PeakMargin";
                objUtility.ExecuteSQLTmp(strSql, ObjConnectionTmp);


                if (blnisLetter)
                {
                    strSql = " Update #TmpPeakColl set Tmp_ShortFall = case When (Tmp_PeakMargin-Tmp_PeakColl) > 0 then (Tmp_PeakMargin-Tmp_PeakColl) else 0 end";
                    objUtility.ExecuteSQLTmp(strSql, ObjConnectionTmp);
                }

                strClientWhere += " and cm_cd = '" + cm_cd + "'";
                strClientWhere += " and cm_type <> 'I'";


                strCase = "";
                strFild = "";

                if (objUtility.GetWebParameter("Commex") != null && objUtility.GetWebParameter("Commex") != string.Empty)
                {
                    try
                    {
                        strSql = "Drop table #FmarginsRpt";
                        objUtility.ExecuteSQLTmp(strSql, ObjConnectionTmp);
                    }
                    catch (Exception ex)
                    {
                    }
                    finally
                    {
                        strSql = " CREATE TABLE [#FmarginsRpt]( ";
                        strSql += " [fm_companycode] [char](1) NOT NULL,[fm_exchange] [char](1) NOT NULL,[fm_dt] [char](8) NOT NULL, ";
                        strSql += " [fm_clientcd] [char](8) NOT NULL,[fm_spanmargin] [money] NOT NULL,[fm_buypremmargin] [money] NOT NULL, ";
                        strSql += " [fm_initialmargin] [money] NOT NULL,[fm_exposurevalue] [money] NOT NULL,[fm_clienttype] [char](1) NOT NULL, ";
                        strSql += " [fm_additionalmargin] [money] NOT NULL,[fm_collected] [money] NOT NULL,[fm_mainbrcd] [char](8) NOT NULL, ";
                        strSql += " [mkrid] [char](8) NOT NULL,[mkrdt] [char](8) NOT NULL,[fm_Regmargin] [money] NULL,[fm_Tndmargin] [money] NULL, ";
                        strSql += " [fm_Dlvmargin] [money] NULL,[fm_SpreadBen] [money] NULL,[fm_SplMargin] [money] NULL,[fm_collectedT2] [money] NOT NULL, ";
                        strSql += " [fm_InitShort] [money] NOT NULL,[fm_MTMAddShort] [money] NOT NULL,[fm_OthShort] [money] NOT NULL,[fm_ConcMargin] [money] NOT NULL, ";
                        strSql += " [fm_DelvPMargin] [money] NOT NULL,[fm_MTMLoss] [money] NOT NULL) ";
                        objUtility.ExecuteSQLTmp(strSql, ObjConnectionTmp);
                    }


                    strSql = " Insert into #FmarginsRpt select fm_companycode,fm_exchange,fm_dt,fm_clientcd,Sum(fm_spanmargin),Sum(fm_buypremmargin),Sum(fm_initialmargin),Sum(fm_exposurevalue),''fm_clienttype,";
                    strSql += " Sum(fm_additionalmargin),Sum(fm_collected),'' fm_mainbrcd,'' mkrid,'' mkrdt,Sum(fm_Regmargin),Sum(fm_Tndmargin),Sum(fm_Dlvmargin),Sum(fm_SpreadBen),Sum(fm_SplMargin),Sum(fm_collectedT2),Sum(fm_InitShort),";
                    strSql += " Sum(fm_MTMAddShort),Sum(fm_OthShort),Sum(fm_ConcMargin),Sum(fm_DelvPMargin),Sum(fm_MTMLoss) from ( ";
                    strSql += " select fm_companycode,fm_exchange,fm_dt,fm_clientcd,fm_spanmargin,fm_buypremmargin,fm_initialmargin,fm_exposurevalue,''fm_clienttype,fm_additionalmargin,fm_collected,";
                    strSql += " ''fm_mainbrcd,''mkrid,''mkrdt,fm_Regmargin,fm_Tndmargin,fm_Dlvmargin,fm_SpreadBen,fm_SplMargin,fm_collectedT2,fm_InitShort,fm_MTMAddShort,fm_OthShort,fm_ConcMargin,fm_DelvPMargin,0 fm_MTMLoss ";
                    strSql += " from  " + StrCommexConn + ".Fmargins, " + StrCommexConn + ".Client_master ";
                    strSql += " Where fm_clientcd = cm_cd and fm_Companycode ='" + strCompanyCode + "' and fm_dt =  (select max(fm_Dt) from fmargins) " + strClientWhere;
                    strSql += " union all ";
                    strSql += " select po_companycode,po_exchange,po_dt,po_clientcd,0 fm_spanmargin,0 fm_buypremmargin,0 fm_initialmargin,0 fm_exposurevalue,0 fm_clienttype,0 fm_additionalmargin,";
                    strSql += " 0 fm_collected,'' fm_mainbrcd,'' mkrid,'' mkrdt,0 fm_Regmargin,0 fm_Tndmargin,0 fm_Dlvmargin,0 fm_SpreadBen,0 fm_SplMargin,0 fm_collectedT2,0 fm_InitShort,";
                    strSql += " 0 fm_MTMAddShort,0 fm_OthShort,0 fm_ConcMargin,0 fm_DelvPMargin,case When -sum(po_futvalue) > 0 Then -sum(po_futvalue) else 0 end MarginReq ";
                    strSql += " from  " + StrCommexConn + ".Fpositions,  " + StrCommexConn + ".Client_master";
                    strSql += " Where po_companycode='" + strCompanyCode + "' and po_clientcd = cm_cd ";
                    strSql += " and po_dt = (select max(fm_Dt) from fmargins) " + strClientWhere;
                    strSql += " Group by po_clientcd,po_companycode,po_exchange,po_dt";
                    strSql += " Having case When -sum(po_futvalue) > 0 Then -sum(po_futvalue) else 0 end > 0 ";
                    strSql += " ) a Group by fm_companycode,fm_exchange,fm_dt,fm_clientcd ";
                    objUtility.ExecuteSQLTmp(strSql, ObjConnectionTmp);

                    strSql = " Update #FmarginsRpt set fm_collected = fc_collected , fm_collectedT2  = fc_Collected1 ";
                    strSql += " From Fmargin_Clients  ";
                    strSql += " Where fc_companycode = '" + strCompanyCode + "' and fc_exchange = fm_Exchange and fc_Segment = 'X' and fc_dt = (select max(fm_Dt) from fmargins) and fm_clientcd = fc_clientcd ";
                    strSql += " and not exists ( Select fm_clientcd from " + StrCommexConn + ".Fmargins Where fm_companycode = '" + strCompanyCode + "' and fm_dt = (select max(fm_Dt) from fmargins) and fc_clientcd  = fm_clientcd and fm_Exchange = fc_Exchange ) ";
                    objUtility.ExecuteSQLTmp(strSql, ObjConnectionTmp);

                }

                strSql = "select  distinct  '0' 'Product' ,(fm_exchange+fm_segment) fm_ExchSeg,fm_segment Seg from Fmargins, Client_master ";
                strSql += " Where fm_clientcd=cm_cd and fm_Companycode ='" + strCompanyCode + "' and fm_dt = (select max(fm_Dt) from fmargins) ";
                if (objUtility.GetWebParameter("Commex") != null && objUtility.GetWebParameter("Commex") != string.Empty)
                {
                    strSql += " union ";
                    strSql += " select distinct '1' 'Product' ,(fm_exchange+'X') fm_ExchSeg,'X' Seg from #FmarginsRpt ";
                    strSql += " Where fm_Companycode ='" + strCompanyCode + "' and fm_dt = (select max(fm_Dt) from fmargins) ";
                }
                strSql += " Order by Product,Seg,fm_ExchSeg ";
                rsExcgSeg = objUtility.OpenDataSetTmp(strSql, ObjConnectionTmp);
                string strTemp = "";
                strFild = "";
                foreach (DataRow objrow in rsExcgSeg.Tables[0].Rows)
                {
                    strExchSegme = objrow["fm_ExchSeg"].ToString();
                    if (Strings.Right(strExchSegme, 1) == "C")
                    { strSegCode = "Cash"; }
                    else if (Strings.Right(strExchSegme, 1) == "F")
                    { strSegCode = "Fo"; }
                    else if (Strings.Right(strExchSegme, 1) == "K")
                    { strSegCode = "Fx"; }
                    else if (Strings.Right(strExchSegme, 1) == "X")
                    { strSegCode = "Cx"; }

                    strCase = strCase + " case fm_exchange+fm_Segment when '" + strExchSegme + "' then case When (fm_TotalMrgn-(fm_collected+fm_collected1)) > 0 Then (fm_TotalMrgn-(fm_collected+fm_collected1)) else 0 end else 0 end TotalShort" + strSegCode + strExchSegme + ",";
                    strTotalShortFallX += "sum(TotalShort" + strSegCode + strExchSegme + ")+";

                    strCase = strCase + " case fm_exchange+fm_Segment when '" + strExchSegme + "' then (fm_collected+fm_collected1) else 0 end Collected" + strSegCode + strExchSegme + ",";
                    strTotalCollectedX += "sum(Collected" + strSegCode + strExchSegme + ")+";

                    strCase = strCase + " case fm_exchange+fm_Segment when '" + strExchSegme + "' then fm_TotalMrgn else 0 end TotalMrgn" + strSegCode + strExchSegme + ",";
                    strTemp = strTemp + "sum(TotalMrgn" + strSegCode + strExchSegme + ") TotalMrgn" + strSegCode + strExchSegme + ",";
                }

                if (strCase.Trim() != "")
                //{ Return "";}
                {
                    if (blnisLetter)
                    {
                        strCase = Strings.Left(strCase.Trim(), Strings.Len(strCase.Trim()) - 1);
                        strFild = "";
                        strFild = " fm_clientcd,fm_exchange,fm_segment,isNull(cm_Name,'Not Found') cm_Name,cm_email,bm_email,cm_brboffcode,bm_branchname,bm_add1 ,cm_add1,bm_add2 ,cm_add2,bm_add3 ,cm_add3,";
                        strFild += " Tmp_Shortfall PeakShort, Tmp_NFiller4, Tmp_NFiller5 , Tmp_PeakColl , Tmp_PeakMargin ,";

                        if (Strings.Right(strTotalShortFallX.Trim(), 1) == "+")
                        {
                            strTotalShortFallX = Strings.Left(strTotalShortFallX, Strings.Len(strTotalShortFallX) - 1) + " TotalShort";
                            strFild += strTotalShortFallX + ", ";
                        }
                        if (Strings.Right(strTotalCollectedX.Trim(), 1) == "+")
                        {
                            strTotalCollectedX = Strings.Left(strTotalCollectedX, Strings.Len(strTotalCollectedX) - 1) + " Collected";
                            strFild += strTotalCollectedX + ", ";
                        }
                        strFild += strTemp;
                        strFild = Strings.Left(strFild.Trim(), Strings.Len(strFild.Trim()) - 1);


                        strCase += " , fm_TotalMrgn ";
                        strFild += " , Sum(fm_TotalMrgn) fm_TotalMrgn ";

                        strSql = " Select case fm_exchange when 'B' then 'BSE-' when 'N' then 'NSE-' when 'M' then 'MCX-' when 'F' then 'NCDEX-' else '' end + case fm_segment when 'C' then 'CASH'  when 'F' then 'FO'  when 'K' then 'FX' when 'M' then 'MF' when 'X' then 'COMM' else '' end ExchSeg,cast(fm_TotalMrgn as decimal(15,2)) fm_TotalMrgn,cast(Collected as decimal(15,2)) Collected,cast(TotalShort as decimal(15,2)) TotalShort,";
                        strSql += " cast(case when fm_TotalMrgn > 0 then ((TotalShort * 100)/ fm_TotalMrgn) else 0 end as decimal(15,2)) TotalShortPER,cast(Tmp_NFiller4 as decimal(15,2)) Tmp_NFiller4,cast(Tmp_PeakMargin as decimal(15,2)) Tmp_PeakMargin,";
                        strSql += " cast(Tmp_NFiller5 as decimal(15,2)) Tmp_NFiller5,cast(PeakShort as decimal(15,2)) PeakShort,cast(Case When PeakShort > TotalShort then PeakShort else TotalShort end as decimal(15,2)) 'Tmp_HighestShortFall' ";

                        if (Conversion.Val(objUtility.fnFireQueryTradeWeb("Sysparameter", "Count(0)", "sp_parmcd", "MGPENALTY", true)) > 0)
                        {
                            strSql += ",";
                            strSql += " Convert(Decimal(15,2), Round(( ((Case When PeakShort > TotalShort then PeakShort else TotalShort end)) *  ";
                            strSql += " (Case When ((Case When PeakShort > TotalShort then PeakShort else TotalShort end)) < 100000  ";
                            strSql += " and Case When  fm_TotalMrgn = 0 then 0 else ((((Case When PeakShort > TotalShort then PeakShort else TotalShort end))*100) / (fm_TotalMrgn)) end < 10 Then 0.5 Else 1 End/100)),2)  ";
                            strSql += " ) ShortPenalty ";
                        }
                        strSql += "  from ( ";
                        strSql += "  select " + strFild + " From (";
                        strSql += " Select fm_clientcd,fm_exchange,fm_segment," + strCase;
                        strSql += " from ( select fm_clientcd,fm_exchange,fm_Segment,Sum(fm_TotalMrgn) fm_TotalMrgn,Sum(fm_collected) fm_collected,Sum(fm_collected1) fm_collected1 from ( ";
                        strSql += " select fm_clientcd,fm_exchange,fm_Segment,fm_TotalMrgn,fm_collected,fm_collected1  from Fmargins, Client_master Where cm_cd=fm_clientcd and fm_Companycode ='" + strCompanyCode + "' and fm_dt = (select max(fm_Dt) from fmargins)";
                        if (objUtility.GetSysParmSt("FMRGCombined", "").Trim() == "F")
                        {
                            strSql += " union all ";
                            strSql += " select fc_Filler1,fc_exchange,fc_Segment,0 fm_TotalMrgn,fc_collected,fc_collected1  from Fmargin_Clients, Client_master Where cm_cd=fc_clientcd and fc_exchange <> '' and fc_Companycode ='" + strCompanyCode + "' and fc_dt = (select max(fm_Dt) from fmargins)";
                        }
                        strSql += " ) a Group By fm_clientcd,fm_exchange,fm_Segment ";
                        strSql += " ) z , Client_master,branch_master ";
                        strSql += " Where fm_clientcd = cm_cd and cm_brboffcode = bm_branchcd " + strClientWhere;
                        if (objUtility.GetWebParameter("Commex") != null && objUtility.GetWebParameter("Commex") != string.Empty)
                        {
                            strSql += " union all ";
                            string strTotalMargin = "";
                            strTotalMargin = " (case fm_exchange When 'M' Then fm_Regmargin + fm_exposurevalue + fm_buypremmargin else fm_initialmargin + fm_exposurevalue end) + case fm_Exchange When 'M' Then fm_additionalmargin + fm_Tndmargin + fm_Dlvmargin - fm_SpreadBen + fm_ConcMargin + fm_DelvPMargin else fm_additionalmargin + fm_SplMargin end + fm_MTMLoss ";
                            strCase = strCase.Replace("fm_Segment", "'X'");
                            strCase = strCase.Replace("fm_TotalMrgn", strTotalMargin);
                            strCase = strCase.Replace("(fm_collected+fm_collected1)", "(fm_collected+fm_collectedt2)");
                            strSql += " Select fm_clientcd,fm_exchange,'X' fm_Segment," + strCase;
                            strSql += " from #FmarginsRpt," + StrCommexConn + ".Client_master," + StrCommexConn + ".branch_master Where fm_clientcd = cm_cd and cm_brboffcode = bm_branchcd and fm_Companycode ='" + strCompanyCode + "' and fm_dt = (select max(fm_Dt) from fmargins) ";
                        }
                        strSql += " ) a , Client_master,branch_master , #TmpPeakColl p ";
                        strSql += " Where fm_clientcd = cm_cd and fm_exchange=tmp_exchange and fm_segment=tmp_segment and cm_brboffcode = bm_branchcd and fm_clientcd = Tmp_Clientcd ";
                        strSql += " Group by fm_clientcd,fm_exchange,fm_Segment,cm_Name,cm_email,bm_email,cm_brboffcode,bm_branchname,bm_add1 ,cm_add1,bm_add2 ,cm_add2,bm_add3 ,cm_add3,Tmp_Shortfall,Tmp_NFiller4, Tmp_Nfiller5, Tmp_PeakColl, Tmp_PeakMargin ";
                        if (Conversion.Val(objUtility.fnFireQueryTradeWeb("Sysparameter", "Count(0)", "sp_parmcd", "MGPENALTY", true)) > 0)
                        {
                            strSql += " Having Sum(fm_TotalMrgn) > 0  ";
                        }
                        strSql += " ) b ";

                        //strSql += " Where PeakShort > 0 Or TotalShort > 0 ";

                        strSql += " Order By fm_clientcd ";
                    }
                    return objUtility.OpenDataSetTmp(strSql, ObjConnectionTmp);

                }
                return null;

            }
        }

        // get data for dropdown
        public string GetQueryDropdownData(string cm_cd, string strCompanyCode)
        {
            DataSet DsReqP = new DataSet();
            DsReqP = objUtility.OpenDataSet("select * from SysObjects where name= 'PledgeRequest'");
            if (DsReqP.Tables[0].Rows.Count == 0)
            {
                prCreate();
            }
            string strServer = "";
            char[] ArrSeparators = new char[1];
            ArrSeparators[0] = '/';
            strsql = "select  case left(da_dpid,2) when 'IN' then  rtrim(da_dpid)+rtrim(da_actno) else da_actno end as BOId,da_name as Name, da_clientcd  from dematact with (nolock) where DA_STATUS = 'A' and da_clientcd = '" + cm_cd + "' ";
            if (objUtility.GetWebParameter("Cross") != "" && objUtility.GetWebParameter("Cross") != null)
            {
                string[] ArrCross = objUtility.GetWebParameter("Cross").Split(ArrSeparators);
                strServer = "[" + ArrCross[0].Trim() + "].[" + ArrCross[1].Trim() + "].[" + ArrCross[2].Trim() + "].";
                strsql += " and exists (select cud_boid from " + strServer.Trim() + "Client_UCC_Details where cud_boid=da_actno and cud_UCC = '" + cm_cd + "' and cud_tmid in (" + mfnGetTMID(strCompanyCode) + ") ) ";
            }
            if (objUtility.GetWebParameter("Estro") != "" && objUtility.GetWebParameter("Estro") != null)
            {
                string[] ArrEstro = objUtility.GetWebParameter("Estro").Split(ArrSeparators);
                strServer = "[" + ArrEstro[0].Trim() + "]" + "." + "[" + ArrEstro[1].Trim() + "]" + "." + "[" + ArrEstro[2].Trim() + "]" + ".";
                strsql += " and exists (select cud_clientID from " + strServer.Trim() + "Client_UCC_Details where cud_clientID=da_actno and cud_UCC = '" + cm_cd + "' and cud_tmid in (" + mfnGetTMID(strCompanyCode) + ") ) ";
            }
            return strsql;
        }

        // used for dropdown method
        public void prCreate()
        {
            strsql = "Create table PledgeRequest  ( ";
            strsql += " Rq_SrNo Numeric Identity(1,1) not null,";
            strsql += " Rq_Clientcd varchar(8) not null,";
            strsql += " Rq_DematActNo char(16) not null,";
            strsql += " Rq_Scripcd varchar(6) not null,";
            strsql += " Rq_Qty numeric not null,";
            strsql += " Rq_IpAddress varchar(50) not null,";
            strsql += " Rq_Date char(8) not null,";
            strsql += " Rq_Time char(8) not null,";
            strsql += " Rq_Status1 char(1) not null,";
            strsql += " Rq_Status2 char(1) not null,";
            strsql += " Rq_Status3 char(1) not null,";
            strsql += " Rq_Status4 char(8) not null,";
            strsql += " Rq_Note varchar(50) NOT NULL, ";
            strsql += " CONSTRAINT [Pk_PledgeRequest] PRIMARY KEY CLUSTERED ";
            strsql += "([Rq_SrNo] ASC)";
            strsql += ")";
            objUtility.ExecuteSQL(strsql);
        }

        //used for dropdown method
        private string mfnGetTMID(string srtCompanyCode)
        {
            string strValue = "";
            string strExchange = "";
            DataSet ds = new DataSet();
            strsql = " select distinct ces_exchange from CompanyExchangeSegments ";
            ds = objUtility.OpenDataSet(strsql);
            if (ds.Tables[0].Rows.Count > 0)
            {
                strsql = "";
                for (int p = 0; p <= ds.Tables[0].Rows.Count - 1; p++)
                {
                    switch (ds.Tables[0].Rows[p]["ces_exchange"].ToString().Trim())
                    {
                        case "BSE":
                            if (strsql != "")
                                strsql += "Union All ";
                            strsql += "select 'BSE' Exch, em_bclearingno clearingno  from Entity_master where em_cd = '" + srtCompanyCode + "' ";
                            break;
                        case "NSE":
                            if (strsql != "")
                                strsql += "Union All ";
                            strsql += "select 'NSE' Exch, em_nclearingno clearingno  from Entity_master where em_cd = '" + srtCompanyCode + "' ";
                            break;
                    }
                }
            }

            ds = new DataSet();
            ds = objUtility.OpenDataSet(strsql);
            for (int p = 0; p <= ds.Tables[0].Rows.Count - 1; p++)
            {
                if (ds.Tables[0].Rows[p]["Exch"].ToString().Trim() == "BSE")
                    strValue += "'" + Conversion.Val(ds.Tables[0].Rows[p]["clearingno"].ToString().Trim()) + "',";
                else
                    strValue += "'" + ds.Tables[0].Rows[p]["clearingno"].ToString().Trim().PadLeft(5, '0') + "',";
            }
            return Strings.Left(strValue, strValue.Length - 1);
        }

        // get data for margin pledge
        public DataSet GetQueryMarginPledgeData(string cm_cd, string UserId, string strCompanyCode, string CmbDPID_Value)
        {
            string StrConn = _configuration.GetConnectionString("DefaultConnection");
            using (SqlConnection ObjConnectionTmp = new SqlConnection(StrConn))
            {
                ObjConnectionTmp.Open();
                try
                {
                    objUtility.ExecuteSQLTmp("Drop Table #TblHolding", ObjConnectionTmp);
                }
                catch (Exception ex)
                { }
                finally
                {
                    strsql = "Create Table #TblHolding ( ";
                    strsql += " th_Identity Numeric Identity(1,1) , ";
                    strsql += " th_cmCd VarChar(8), ";
                    strsql += " th_DematActNo Char(16), ";
                    strsql += " th_Scripcd Char(8), ";
                    strsql += " th_ISIN Char(12), ";
                    strsql += " th_Qty Numeric , ";
                    strsql += " th_Rate Money  , ";
                    strsql += " th_MrgShortFall money,";
                    strsql += " th_PorjectedRisk money,";
                    strsql += " th_Haircut money,";
                    strsql += " th_NetRate money,";
                    strsql += " th_netValue money, ";
                    strsql += " th_Retain money, ";
                    strsql += " th_Approved varchar(12) , ";
                    strsql += " th_RegForFO char(1), ";
                    strsql += " th_Value money";
                    strsql += " ) ";
                    objUtility.ExecuteSQLTmp(strsql, ObjConnectionTmp);
                }

                string strServer = "";

                if (_configuration["IsTradeWeb"] == "O")
                {//-----------------------------------------------------------Live----------------------------------------------------------------------------------------

                    char[] ArrSeparators = new char[1];
                    ArrSeparators[0] = '/';

                    if (objUtility.GetWebParameter("Cross") != "" && Microsoft.VisualBasic.Strings.Mid(CmbDPID_Value.Trim(), 1, 2) != "IN") // strBoid  LEft 2 <>IN           
                    {
                        string[] ArrCross = objUtility.GetWebParameter("Cross").Split(ArrSeparators);

                        strServer = "[" + ArrCross[0].Trim() + "].[" + ArrCross[1].Trim() + "].[" + ArrCross[2].Trim() + "].";
                        strsql = "Insert into #TblHolding ";
                        strsql += " select '',hld_ac_code,'',hld_isin_code,hld_ac_pos,0,0,0,0,0,0,0,'','',0 ";
                        strsql += " from " + strServer.Trim() + "Holding , ";
                        strsql += " " + strServer.Trim() + "Client_master ";
                        strsql += " where cm_cd = hld_ac_code and hld_ac_type = '11' and cm_active = '01'";
                        strsql += " and exists (select da_actno from Dematact,Client_master Where cm_cd = da_clientcd and da_actno = hld_ac_code and DA_STATUS = 'A' and left(da_dpid,2) <> 'IN' and ltrim(rtrim(cm_cd)) = '" + cm_cd.Trim() + "')";
                        strsql += " and exists (select cud_boid from " + strServer.Trim() + "Client_UCC_Details where cud_boid=hld_ac_code and cud_UCC = '" + cm_cd.Trim() + "' and cud_tmid in (" + mfnGetTMID(strCompanyCode) + ")) ";
                        strsql += " and hld_ac_code = '" + CmbDPID_Value.Trim() + "'";
                        objUtility.ExecuteSQLTmp(strsql, ObjConnectionTmp);

                        strsql = "Update #TblHolding set th_cmCd = cud_UCC from " + strServer.Trim() + "Client_UCC_Details where cud_boid=th_DematActNo and cud_tmid in (" + mfnGetTMID(strCompanyCode) + ")";
                        objUtility.ExecuteSQLTmp(strsql, ObjConnectionTmp);
                    }

                    if (objUtility.GetWebParameter("Estro") != "" && Microsoft.VisualBasic.Strings.Mid(CmbDPID_Value.Trim(), 1, 2) == "IN")
                    {
                        string[] ArrEstro = objUtility.GetWebParameter("Estro").Split(ArrSeparators);

                        strServer = "[" + ArrEstro[0].Trim() + "].[" + ArrEstro[1].Trim() + "].[" + ArrEstro[2].Trim() + "].";
                        DataSet Dsx = new DataSet();
                        strsql = "Select sp_sysvalue from " + strServer + "Sysparameter where sp_parmcd = 'DPID'";
                        Dsx = objUtility.OpenDataSet(strsql);
                        string strDpid = Dsx.Tables[0].Rows[0][0].ToString().Trim();

                        strsql = "Insert into #TblHolding ";
                        strsql += " select '','" + strDpid + "'+hld_ac_code,'',hld_isin_code,hld_ac_pos,0,0,0,0,0,0,0,'','',0 ";
                        strsql += " from " + strServer.Trim() + "Holding , " + strServer.Trim() + "Client_master ";
                        strsql += " where cm_cd = hld_ac_code and hld_ac_type = '22' and cm_active = '01'";
                        strsql += " and ltrim(rtrim(cm_blsavingcd)) <> '' ";
                        strsql += " and '" + strDpid + "'+hld_ac_code in (select da_dpid+da_actno from Dematact,Client_master Where cm_cd=da_clientcd and DA_STATUS = 'A' and left(da_dpid,2) = 'IN' and ltrim(rtrim(cm_cd)) = '" + cm_cd.Trim() + "'";
                        strsql += " ) ";
                        objUtility.ExecuteSQLTmp(strsql, ObjConnectionTmp);

                        strsql = "Update #TblHolding set th_cmCd = da_clientcd from Dematact Where DA_STATUS = 'A' and case When left(da_dpid,2) = 'IN' Then da_dpid+da_actno else da_actno end = th_DematActNo ";
                        objUtility.ExecuteSQLTmp(strsql, ObjConnectionTmp);
                    }

                    strsql = "Update #TblHolding set th_Scripcd =  im_scripcd From ISIN Where im_isin = th_ISIN and im_active = 'Y' and im_scripcd not Between '600000' and '699999' ";
                    objUtility.ExecuteSQLTmp(strsql, ObjConnectionTmp);

                    if (objUtility.GetSysParmSt("PRMISECURITY", "").Trim() == "Y")
                    {
                        strsql = " Delete From #TblHolding ";
                        strsql += " from securities Where th_scripcd =ss_cd and ss_Permscm <> 'Y' ";
                        objUtility.ExecuteSQLTmp(strsql, ObjConnectionTmp);
                    }

                    strsql = "Update #TblHolding set th_haircut = 100 ";
                    objUtility.ExecuteSQLTmp(strsql, ObjConnectionTmp);

                    string strRMSVALATLTRT = objUtility.GetSysParmSt("RMSVALATLTRT", "").ToUpper().Trim();
                    double gHAIRCUT = Conversion.Val(objUtility.GetSysParmSt("HAIRCUTVAL", "").ToUpper().Trim());
                    double gAddHairCut = Conversion.Val(objUtility.GetSysParmSt("FMRADDHRCUT", "").ToUpper().Trim());
                    strsql = " update #TblHolding set th_haircut = case vm_exchange When 'B' then vm_margin_rate  else vm_applicable_var end from VarMargin ";
                    strsql += " where vm_exchange = 'B' and vm_scripcd = th_scripcd ";
                    strsql += " and vm_dt =(select max(vm_dt) from VarMargin where vm_exchange = 'B' and vm_scripcd = th_scripcd ";
                    strsql += " and vm_dt " + (gHAIRCUT == 0 ? "<=" : "<") + " '" + DateTime.Today.ToString("yyyyMMdd") + "')";
                    objUtility.ExecuteSQLTmp(strsql, ObjConnectionTmp);

                    if (Conversion.Val(strRMSVALATLTRT) > 0)
                    {
                        strsql = " update #TblHolding set th_haircut = case vm_exchange When 'B' then vm_margin_rate  else vm_applicable_var end from VarMargin,Securities ";
                        strsql += " where vm_exchange = 'B' and vm_scripcd = th_scripcd And vm_scripcd = ss_cd And ss_group = 'F' ";
                        strsql += " and vm_dt =(select max(vm_dt) from VarMargin where vm_exchange = 'B' and vm_scripcd = th_scripcd ";
                        strsql += " and vm_dt " + (gHAIRCUT == 0 ? "<=" : "<") + " '" + DateTime.Today.ToString("yyyyMMdd") + "')";
                        objUtility.ExecuteSQLTmp(strsql, ObjConnectionTmp);
                    }

                    strsql = " update #TblHolding set th_haircut = case vm_exchange When 'B' then vm_margin_rate  else vm_applicable_var end from VarMargin ";
                    strsql += " where vm_exchange = 'N' and vm_scripcd = th_scripcd ";
                    strsql += " and vm_dt =(select max(vm_dt) from VarMargin where vm_exchange = 'N' and vm_scripcd = th_scripcd ";
                    strsql += " and vm_dt " + (gHAIRCUT == 0 ? "<=" : "<") + " '" + DateTime.Today.ToString("yyyyMMdd") + "')";
                    objUtility.ExecuteSQLTmp(strsql, ObjConnectionTmp);

                    if (Conversion.Val(strRMSVALATLTRT) > 0)
                    {
                        strsql = " update #TblHolding set th_haircut = case vm_exchange When 'B' then vm_margin_rate  else vm_applicable_var end from VarMargin,Securities ";
                        strsql += " where vm_exchange = 'N' and vm_scripcd = th_scripcd And vm_scripcd = ss_cd And ss_group = 'F'";
                        strsql += " and vm_dt =(select max(vm_dt) from VarMargin where vm_exchange = 'N' and vm_scripcd = th_scripcd ";
                        strsql += " and vm_dt " + (gHAIRCUT == 0 ? "<=" : "<") + " '" + DateTime.Today.ToString("yyyyMMdd") + "')";
                        objUtility.ExecuteSQLTmp(strsql, ObjConnectionTmp);
                    }

                    if (gAddHairCut > 0)
                    {
                        strsql = " update #TblHolding set th_haircut = th_haircut + " + gAddHairCut + " Where th_haircut <= 100 - " + gAddHairCut;
                        objUtility.ExecuteSQLTmp(strsql, ObjConnectionTmp); ;
                    }
                    strsql = " update #TblHolding set th_Rate = mk_closerate from Market_Rates ";
                    strsql += " where mk_exchange = 'B' and mk_scripcd = th_scripcd ";
                    strsql += " and mk_dt =(select max(mk_dt) from Market_Rates  where mk_exchange = 'B'";
                    strsql += " and mk_scripcd = th_scripcd ";
                    strsql += " and mk_dt " + (gHAIRCUT == 0 ? "<=" : "<") + "'" + DateTime.Today.ToString("yyyyMMdd") + "')";
                    objUtility.ExecuteSQLTmp(strsql, ObjConnectionTmp);

                    if (Conversion.Val(strRMSVALATLTRT) > 0)
                    {
                        strsql = " update #TblHolding set th_Rate = mk_closerate from Market_Rates ,Securities";
                        strsql += " where mk_exchange = 'B' and mk_scripcd = th_scripcd And mk_scripcd = ss_cd And ss_group = 'F'";
                        strsql += " and mk_dt =(select max(mk_dt) from Market_Rates  where mk_exchange = 'B'";
                        strsql += " and mk_scripcd = th_scripcd ";
                        strsql += " and mk_dt " + (gHAIRCUT == 0 ? "<=" : "<") + "'" + DateTime.Today.ToString("yyyyMMdd") + "')";
                        objUtility.ExecuteSQLTmp(strsql, ObjConnectionTmp);
                    }
                    strsql = " update #TblHolding set th_Rate = mk_closerate from Market_Rates ";
                    strsql += " where mk_exchange = 'N' and mk_scripcd = th_scripcd ";
                    strsql += " and mk_dt =(select max(mk_dt) from Market_Rates  where mk_exchange = 'N'";
                    strsql += " and mk_scripcd = th_scripcd ";
                    strsql += " and mk_dt " + (gHAIRCUT == 0 ? "<=" : "<") + "'" + DateTime.Today.ToString("yyyyMMdd") + "')";
                    objUtility.ExecuteSQLTmp(strsql, ObjConnectionTmp);

                    if (Conversion.Val(strRMSVALATLTRT) > 0)
                    {
                        strsql = " update #TblHolding set th_Rate = mk_closerate from Market_Rates ,Securities";
                        strsql += " where mk_exchange = 'N' and mk_scripcd = th_scripcd And mk_scripcd = ss_cd And ss_group = 'F'";
                        strsql += " and mk_dt =(select max(mk_dt) from Market_Rates  where mk_exchange = 'N'";
                        strsql += " and mk_scripcd = th_scripcd ";
                        strsql += " and mk_dt " + (gHAIRCUT == 0 ? "<=" : "<") + "'" + DateTime.Today.ToString("yyyyMMdd") + "')";
                        objUtility.ExecuteSQLTmp(strsql, ObjConnectionTmp);
                    }

                    objUtility.ExecuteSQLTmp("Delete from #TblHolding where th_Rate <= 0 ", ObjConnectionTmp);
                    objUtility.ExecuteSQLTmp("Delete from #TblHolding where th_Qty <= 0 ", ObjConnectionTmp);

                    strsql = "update #TblHolding set th_NetRate =  th_Rate - (Round(th_Rate * ((th_Haircut) / 100), 2)) ";
                    objUtility.ExecuteSQLTmp(strsql, ObjConnectionTmp);
                    objUtility.ExecuteSQLTmp("Delete from #TblHolding where th_NetRate <= 0 ", ObjConnectionTmp);

                    objUtility.ExecuteSQLTmp("Update #TblHolding  set th_netValue= Round(th_Qty*th_NetRate,2) ", ObjConnectionTmp);

                    strsql = "update #TblHolding set th_Value = Round(th_Qty*th_rate,2) ";
                    objUtility.ExecuteSQLTmp(strsql, ObjConnectionTmp);

                    DataSet dsOpen = new DataSet();
                    strsql = " select * from PledgeRequest where rq_clientcd='" + UserId + "' and Rq_Status1='P' ";
                    dsOpen = objUtility.OpenDataSet(strsql);
                    if (dsOpen.Tables[0].Rows.Count > 0)
                    {
                        strsql = "update #TblHolding set th_retain = isNull((select sum(Rq_Qty) From PledgeRequest Where Rq_Clientcd = '" + UserId + "' and Rq_Scripcd=th_scripcd and Rq_Status1 = 'P'),0) ";
                        objUtility.ExecuteSQLTmp(strsql, ObjConnectionTmp);
                    }
                    strsql = "select th_cmcd,cm_Name,th_MrgShortFall,";
                    strsql += " th_scripcd,ss_Name,cast(th_qty as decimal(15,0)) as th_qty,cast(th_rate as decimal(15,2)) th_rate,cast(th_HairCut as decimal(15,2)) th_HairCut,th_ISIN,";
                    strsql += " cast(th_netValue as decimal(15,2)) th_netValue,cast(th_retain as decimal(15,0)) Retain,th_DematActNo,th_ISIN,cast(th_Value as decimal(15,2)) th_Value,th_NetRate,";
                    strsql += " cast(((th_netValue/th_qty)* th_qty ) as decimal(15,2)) th_ReqValue from #TblHolding,Securities,Client_master ";
                    strsql += " where th_scripcd = ss_cd and th_cmcd = cm_cd ";
                    strsql += " order by th_retain desc,th_netValue desc,ss_Name";

                    return objUtility.OpenDataSetTmp(strsql, ObjConnectionTmp);
                }
            }

            return null;
        }

        //get current pledge request
        public DataSet GetQueryCurrentPledgeRequest(string UserId)
        {
            strsql = "delete from PledgeRequest where Rq_Clientcd='" + UserId + "' and Rq_Status1='P'";
            objUtility.ExecuteSQL(strsql);

            DataSet Dstemp = new DataSet();
            Dstemp = objUtility.OpenDataSet("SELECT isnull (IDENT_CURRENT('PledgeRequest'),0)");

            if (Convert.ToInt64(Dstemp.Tables[0].Rows[0][0]) > 0)
            {
                DataSet DsReqId = new DataSet();
                DsReqId = objUtility.OpenDataSet("SELECT IDENT_CURRENT('PledgeRequest')");
                return DsReqId;
            }

            return Dstemp;
        }

        // insert margin pledge request
        public string AddQueryPledgeRequest(string UserId, string CmbDPID_Value, bool blnIdentityOn, string intcnt, string lblScripcd, string txtQty)
        {
            string gstrToday = DateTime.Today.ToString("yyyyMMdd");
            string strHostAdd = Dns.GetHostName();
            if (blnIdentityOn)
                strsql = "insert into PledgeRequest values ( ";
            else
                strsql = "insert into PledgeRequest values ( " + Convert.ToInt64(intcnt) + ",";

            strsql += " '" + UserId + "','" + CmbDPID_Value.Trim() + "','" + lblScripcd.Trim() + "','" + Conversion.Val(txtQty) + "','" + strHostAdd + "',";
            strsql += " '" + gstrToday + "',";
            strsql += " convert(char(8),getdate(),108),";
            strsql += " 'P','P','P','" + objUtility.Encrypt((gstrToday).ToString().Trim()) + "','')";
            objUtility.ExecuteSQL(strsql);

            return "Success";
        }

        #endregion
        #endregion

        #region Family Handler method
        //TODO : For Getting Page_Load Data
        public dynamic Get_Page_Load_Data(string cm_cd)
        {
            var ds = Get_QueryFor_Page_Load_Data(cm_cd);
            try
            {
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

        //TODO : For Getting Buttons Data
        public dynamic Get_Buttons_Data(string BtnClick, string SelectedCLCode)
        {
            var ds = Get_QueryFor_Buttons_Data(BtnClick, SelectedCLCode);
            try
            {
                //var ds = CommonRepository.FillDataset(qury);
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

        //TODO : For Getting Transaction Button Data
        public dynamic Get_Transaction_Btn_Data(string BtnClick, string SelectedCLCode, string SelectedValue, string FromDate, string ToDate)
        {
            var ds = Get_QueryFor_Transaction_Btn_Data(BtnClick, SelectedCLCode, SelectedValue, FromDate, ToDate);
            try
            {
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

        //TODO : For Getting Transaction Button RPJ Detailed Data
        public dynamic Get_Transaction_Btn_RPJ_Detailed_Data(string Client, string Type, string FromDate, string ToDate)
        {
            var ds = Get_QueryFor_Transaction_Btn_RPJ_Detailed_Data(Client, Type, FromDate, ToDate);
            try
            {
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

        #region Family usefull method

        public DataSet Get_QueryFor_Page_Load_Data(string cm_cd)
        {
            DataSet ds = new DataSet();
            ds = ShowMasterGrid(cm_cd);
            return ds;
        }
        public DataSet ShowMasterGrid(string cm_cd)
        {
            strsql = "select upper(case when CF_CD=CF_FamilyCd then 'MAIN' else '' end) MainCd,upper(cf_cd) cf_cd ,cm_name";
            strsql += " from Client_Family,Client_master where cm_cd=CF_CD and CF_FamilyCd='" + cm_cd + "'  order by MainCd desc ";
            DataSet ObjDataSet = new DataSet();
            ObjDataSet = objUtility.OpenDataSet(strsql);
            return ObjDataSet;
        }

        public DataSet Get_QueryFor_Buttons_Data(string BtnClick, string SelectedCLCode)
        {
            DataSet ds = new DataSet();
            ds = BindGrid(BtnClick, SelectedCLCode);
            return ds;
        }
        public DataSet BindGrid(string BtnClick, string SelectedCLCode)
        {
            DataSet ds = new DataSet();
            if (BtnClick == "L")
                ds = ShowLedger(BtnClick, SelectedCLCode);
            else if (BtnClick == "H")
                ds = ShowHolding(BtnClick, SelectedCLCode);
            else if (BtnClick == "DP")
                ds = ShowDPHolding(BtnClick, SelectedCLCode);
            else if (BtnClick == "O")
                ds = ShowOutstanding(BtnClick, SelectedCLCode);
            return ds;
        }
        public DataSet ShowLedger(string BtnClick, string SelectedCLCode)
        {
            int CurrentYear = DateTime.Now.Year;
            if (DateTime.Today.Month < 3)
            {
                CurrentYear = CurrentYear - 1;
            }
            string Fromdt = objUtility.dtos(new DateTime(CurrentYear, 4, 1).ToString());
            string Todt = objUtility.dtos(new DateTime(CurrentYear + 1, 3, 31).ToString());
            string StrDataFields = "";
            string StrHeaderTitles = "";
            string StrSubTotalsFor = "";
            string StrTextAlign = "";
            string StrTextLength = "";
            string Strcolhide = "";
            string StrCDC = "";
            string StrAccC = "";
            string StrCDM = "";
            string StrAccM = "";
            string StrCDL = "";
            string StrAccL = "";
            char[] ArrSeparters = new char[1];
            ArrSeparters[0] = '/';
            string[] StrClCd;
            StrClCd = Convert.ToString(SelectedCLCode).Split(ArrSeparters);
            int i = 0;
            for (i = 0; i < StrClCd.Length - 1; i++)
            {
                string StrName = Strings.Left(StrClCd[i].Trim().Split('~')[1].Trim(), 15);

                StrAccC += " cast(sum(Case When ld_clientcd = '" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then ld_amount else 0 end) as decimal(15,2)) as  '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_" + StrName.Trim() + "',";
                StrCDC += "'" + StrClCd[i].Trim().Split('~')[0].Trim() + "',";
                StrAccL += " cast(sum(Case When ld_clientcd = '" + StrClCd[i].Trim().Split('~')[0].Trim() + objUtility.GetSysParmSt("MTFP_SUFFIX", "") + "' then ld_amount else 0 end) as decimal(15,2)) as  '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_" + StrName.Trim() + "',";
                StrCDL += "'" + StrClCd[i].Trim().Split('~')[0].Trim() + objUtility.GetSysParmSt("MTFP_SUFFIX", "") + "',";

                string StrMrg = objUtility.fnFireQueryTradeWeb("client_master", "distinct cm_brkggroup", "cm_cd", StrClCd[i].Trim().Split('~')[0].Trim(), true).Trim();
                StrAccM += (StrMrg.Trim() == "" ? "0" : "cast(sum(Case When ld_clientcd = '" + StrMrg.Trim() + "' then ld_amount else 0 end) as decimal(15,2))") + " as  '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_" + StrName.Trim() + "',";
                StrCDM += "'" + StrMrg.Trim() + "',";
                StrDataFields += StrClCd[i].Trim().Split('~')[0].Trim() + "_" + StrName.Trim() + ",";
                StrHeaderTitles += StrClCd[i].Trim().Split('~')[0].Trim() + "_" + StrName.Trim() + ",";
                StrSubTotalsFor += StrClCd[i].Trim().Split('~')[0].Trim() + "_" + StrName.Trim() + ",";
                StrTextAlign += "R,";
                StrTextLength += "15,";
                Strcolhide += i + 1 + ",";
            }
            Strcolhide += i + 1;
            //StrAccC = Strings.Left(StrAccC, StrAccC.Length - 1);
            StrCDC = Strings.Left(StrCDC, StrCDC.Length - 1);
            //StrAccL = Strings.Left(StrAccL, StrAccL.Length - 1);
            StrCDL = Strings.Left(StrCDL, StrCDL.Length - 1);
            StrCDM = Strings.Left(StrCDM, StrCDM.Length - 1);
            //StrDataFields = Strings.Left(StrDataFields, StrDataFields.Length - 1);
            //StrHeaderTitles = Strings.Left(StrHeaderTitles, StrHeaderTitles.Length - 1);
            //StrSubTotalsFor = Strings.Left(StrSubTotalsFor, StrSubTotalsFor.Length - 1);
            //StrTextAlign = Strings.Left(StrTextAlign, StrTextAlign.Length - 1);
            //StrTextLength = Strings.Left(StrTextLength, StrTextLength.Length - 1);
            //Strcolhide = Strings.Left(Strcolhide, Strcolhide.Length - 1);


            strsql = "select 'C' as account, ";
            strsql += "case substring(ld_dpid,2,1)   when 'B' then 'BSE-' when 'N' then 'NSE-' when 'M' then 'MCX-' when 'F' then 'NCDEX-' else '' end +  ";
            strsql += " case substring(ld_dpid,3,1)   when 'C' then 'CASH'  when 'F' then 'DERIVATIVE'  when 'K' then 'FX' when 'M' then 'MF' when 'X' then 'COMM' else '' end  as heading,";
            strsql += StrAccC;
            strsql += " cast(sum(ld_amount) as decimal(15,2)) as  'Total' ";
            strsql += " from Ledger ";
            strsql += " Where ld_clientcd in (" + StrCDC + " )";
            strsql += " Group By ld_dpid";

            strsql += " Union all select 'M' as account, ";
            strsql += " case substring(ld_dpid,2,1)   when 'B' then 'BSE-' when 'N' then 'NSE-' when 'M' then 'MCX-' when 'F' then 'NCDEX-' else '' end + ";
            strsql += " case substring(ld_dpid,3,1)   when 'C' then 'CASH'  when 'F' then 'DERIVATIVE'  when 'K' then 'FX'  when 'X' then 'COMM' else '' end + '(M)',";
            strsql += StrAccM;
            strsql += " cast(sum(ld_amount) as decimal(15,2)) as  'Total' ";
            strsql += " from Ledger ";
            strsql += " Where ld_clientcd in (" + StrCDM + " )";
            strsql += " Group By ld_dpid ";

            //MTF ledger
            if (Convert.ToInt32(objUtility.fnFireQueryTradeWeb("sysobjects", "count(*)", "name", "MrgTdgFin_TRX", true)) > 0)
            {
                strsql += " union all select 'L' as account, case substring(ld_dpid,2,1)   when 'B' then 'BSE-' when 'N' then 'NSE-' when 'M' then 'MCX-' else '' end + 'MTF' ,";
                strsql += StrAccL;
                strsql += " cast(sum(ld_amount) as decimal(15,2)) as  'Total' ";
                strsql += " from ledger with (nolock),MrgTdgFin_Clients ";
                strsql += " where ld_clientcd in (" + StrCDL + " ) and ld_dt <= '20210331' ";
                strsql += "  and ld_clientcd =  Rtrim(MTFC_CMCD) + '" + objUtility.GetSysParmSt("MTFP_SUFFIX", "") + "' group by ld_dpid";
            }

            // NBFC
            if (Convert.ToInt32(objUtility.fnFireQueryTradeWeb("sysobjects", "count(*)", "name", "nbfc_clients", true)) > 0)
            {
                strsql += " union all ";
                strsql += "select 'N' as account, 'NBFC',";
                strsql += StrAccC;
                strsql += " cast(sum(ld_amount) as decimal(15,2)) as  'Total' ";
                strsql += " from NBFC_Ledger with (nolock) where ld_clientcd in (" + StrCDC + " ) and ld_dt <= '" + Todt + "' group by ld_dpid ";
            }
            if (_configuration["Commex"] != null && _configuration["Commex"] != string.Empty)
            {
                string StrCommexConn = "";
                StrCommexConn = objUtility.GetCommexConnection();
                strsql += " union all select 'CM' as account, ";
                strsql += " case substring(ld_dpid,2,1) when 'M' then 'MCX-COMM' when 'N' then 'NCDEX-COMM' when 'S' then 'NSEL-COMM' when 'D' then 'NSX-COMM' end as heading ,";
                strsql += StrAccC;
                strsql += " cast(sum(ld_amount) as decimal(15,2)) as  'Total' ";
                strsql += " from   " + StrCommexConn + ".ledger";
                strsql += " where ld_clientcd in (" + StrCDC + " ) and ld_dt <= '" + Todt + "' ";
                strsql += " group by ld_dpid ";

                strsql += " order by account";
            }
            DataSet ObjDataSet = new DataSet();
            ObjDataSet = objUtility.OpenDataSet(strsql);
            return ObjDataSet;
        }
        public DataSet ShowHolding(string BtnClick, string SelectedCLCode)
        {
            string Strsql = string.Empty;
            DataSet ObjDataSet = new DataSet();

            string strdate = DateTime.Today.Date.ToString("yyyyMMdd");
            int i = 0;
            string strstat = string.Empty;
            Strsql = "select st_sysvalue from stationary where st_parmcd='DMTCOLLATDP' and st_exchange = 'B' ";
            ObjDataSet = objUtility.OpenDataSet(Strsql);
            if (ObjDataSet.Tables[0].Rows.Count != 0)
                strstat = ObjDataSet.Tables[0].Rows[0][0].ToString().Trim();
            char[] ArrSeprator = new char[1];
            ArrSeprator[0] = ',';
            string[] arrstat = strstat.Split(ArrSeprator);
            string strcollat = "( ";
            for (i = 0; i <= arrstat.Length - 1; i++)
            {
                strcollat = strcollat + "'" + arrstat[i] + "',";
            }
            strcollat = strcollat + ")";
            strcollat = strcollat.Replace(",)", ")");

            string StrSelect = "";
            string StrCDC = "";
            char[] ArrSeparters = new char[1];
            ArrSeparters[0] = '/';
            string[] StrClCd;
            string StrDataFields = "";
            string StrSubTotalFields = "";
            string StrHeaderTitles = "";
            //string StrSubTotalsFor = "";
            string StrTextAlign = "";
            string StrTextLength = "";
            string Strcolhide = "";

            StrClCd = Convert.ToString(SelectedCLCode).Split(ArrSeparters);

            for (i = 0; i < StrClCd.Length - 1; i++)
            {
                StrSelect += " cast(sum(Case When dm_clientcd = '" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then qty else 0 end) as decimal(15,0)) as  '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_Qty',cast(sum(Case When dm_clientcd = '" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then (ss_bserate*qty) else 0 end) as decimal(15,2)) as  '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_Valuation',";
                StrCDC += "'" + StrClCd[i].Trim().Split('~')[0].Trim() + "',";
                StrDataFields += StrClCd[i].Trim().Split('~')[0].Trim() + "_Qty," + StrClCd[i].Trim().Split('~')[0].Trim() + "_Valuation,";
                StrSubTotalFields += StrClCd[i].Trim().Split('~')[0].Trim() + "_Valuation,";
                StrHeaderTitles += "Qty,Valuation,";
                StrTextAlign += "R,R,";
                StrTextLength += "15,15,";
                //Strcolhide += i + 1 + "," + i + 2 + ",";
            }

            StrCDC = Strings.Left(StrCDC, StrCDC.Length - 1);
            SqlConnection con;
            using (var db = new DataContext())
            {
                con = new SqlConnection((db.Database.GetDbConnection()).ConnectionString);
                con.Open();
                objUtility.GetHairCut(con);
                strsql = "select dm_isin,ss_name,bh_type, " + StrSelect;
                strsql += " cast(sum(qty)  as decimal(15,0)) as  'TotalQty', ";
                strsql += " cast(sum((ss_bserate*qty)) as decimal(15,2)) as  'TotalVal' ";
                strsql += " from ( ";
                strsql += objUtility.GetSqlTradeHolding(con.ConnectionString, StrCDC, strdate, strcollat, con);
                strsql += " ) a group by dm_isin,ss_name,bh_type having abs(sum(qty)) > 0 ";

                ObjDataSet = new DataSet();
                ObjDataSet = objUtility.OpenDataSetTmp(strsql, con);
            }
            return ObjDataSet;
        }
        public DataSet ShowDPHolding(string BtnClick, string SelectedCLCode)
        {
            string StrSelect = "";
            string StrCDC = "";
            char[] ArrSeparters = new char[1];
            ArrSeparters[0] = '/';
            string[] StrClCd;
            string StrDataFields = "";
            string StrSubTotalFields = "";
            string StrHeaderTitles = "";
            //string StrSubTotalsFor = "";
            string StrTextAlign = "";
            string StrTextLength = "";
            string Strcolhide = "";
            StrClCd = Convert.ToString(SelectedCLCode).Split(ArrSeparters);
            int i = 0;
            for (i = 0; i < StrClCd.Length - 1; i++)
            {
                StrSelect += " cast(sum(Case When CM.cm_blsavingcd = '" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then a.hld_ac_pos else 0 end) as decimal(15,0)) as  '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_Qty',cast(sum(Case When CM.cm_blsavingcd = '" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then (a.hld_ac_pos * sc_Rate) else 0 end) as decimal(15,2)) as  '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_Valuation',";
                StrCDC += "'" + StrClCd[i].Trim().Split('~')[0].Trim() + "',";
                StrDataFields += StrClCd[i].Trim().Split('~')[0].Trim() + "_Qty," + StrClCd[i].Trim().Split('~')[0].Trim() + "_Valuation,";
                StrSubTotalFields += StrClCd[i].Trim().Split('~')[0].Trim() + "_Valuation,";
                StrHeaderTitles += "Qty,Valuation,";
                StrTextAlign += "R,R,";
                StrTextLength += "15,15,";
                //Strcolhide += i + 1 + "," + i + 2 + ",";
            }
            StrCDC = Strings.Left(StrCDC, StrCDC.Length - 1);

            if (_configuration["IsTradeWeb"] == "O")
            {//-----------------------------------------------------------Live----------------------------------------------------------------------------------------
                char[] ArrSeparators = new char[1];
                ArrSeparators[0] = '/';
                if (_configuration["Cross"] != "") // strBoid  LEft 2 <>IN
                {
                    string[] ArrCross = _configuration["Cross"].Split(ArrSeparators);
                    strsql = "select a.hld_isin_code,b.sc_isinname,bt_description as BType, " + StrSelect + " cast(sum(a.hld_ac_pos) as decimal(15,0)) as  'TotalQty', cast(sum(a.hld_ac_pos * sc_Rate) as decimal(15,2)) as  'TotalVal' ";
                    strsql += " from " + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".Holding a,";
                    strsql += ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".Security b ," + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".client_master CM, ";
                    strsql += ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".Beneficiary_type d where a.hld_isin_code = b.sc_isincode ";
                    strsql += " and d.bt_code = a.hld_ac_type and CM.cm_cd = a.hld_ac_code and CM.cm_blsavingcd in (" + StrCDC + ") group by a.hld_isin_code,b.sc_isinname,bt_description ";
                }
                else
                    if (_configuration["Estro"] != "")
                {
                    string[] ArrEstro = _configuration["Estro"].Split(ArrSeparators);
                    strsql = "select a.hld_isin_code,b.sc_company_name,bt_description as BType, " + StrSelect + " cast(sum(a.hld_ac_pos) as decimal(15,0)) as  'TotalQty', cast(sum(a.hld_ac_pos * sc_Rate) as decimal(15,2)) as  'TotalVal' ";
                    strsql += " from " + ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".Holding a,";
                    strsql += ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".Security b ,";
                    strsql += ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".Beneficiary_type d, ";
                    strsql += ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".sysParameter, " + ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".client_master CM  ";
                    strsql += " where a.hld_isin_code = b.sc_isincode and sp_parmcd = 'DPID' and CM.cm_blsavingcd in (" + StrCDC + ") ";
                    strsql += " and d.bt_code = a.hld_ac_type and CM.cm_cd = a.hld_ac_code group by a.hld_isin_code,b.sc_company_name,bt_description ";
                }
            }
            DataSet ObjDataSet = new DataSet();
            ObjDataSet = objUtility.OpenDataSet(strsql);
            return ObjDataSet;

        }
        public DataSet ShowOutstanding(string BtnClick, string SelectedCLCode)
        {
            string StrSelect = "";
            string StrCDC = "";
            char[] ArrSeparters = new char[1];
            ArrSeparters[0] = '/';
            string[] StrClCd;
            string StrDataFields = "";
            string StrHeaderTitles = "";
            //string StrSubTotalsFor = "";
            string StrTextAlign = "";
            string StrTextLength = "";
            string Strcolhide = "";
            StrClCd = Convert.ToString(SelectedCLCode).Split(ArrSeparters);
            int i = 0;
            for (i = 0; i < StrClCd.Length - 1; i++)
            {
                StrSelect += " sum(case when td_clientcd ='" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then (td_bqty-td_sqty) else 0 end ) as '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_Net',";
                StrSelect += " convert(decimal(15,2), case sum(case when td_clientcd ='" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then (td_bqty -td_sqty) else 0 end ) when 0 then 0 ";
                StrSelect += " else abs(sum(case when td_clientcd ='" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then ((td_bqty -td_sqty)*td_rate) else 0 end )/sum(case when td_clientcd='" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then (td_bqty-td_sqty) else 0 end ))end) '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_Avgrate', ";
                StrCDC += "'" + StrClCd[i].Trim().Split('~')[0].Trim() + "',";
                StrDataFields += StrClCd[i].Trim().Split('~')[0].Trim() + "_Net," + StrClCd[i].Trim().Split('~')[0].Trim() + "_Avgrate,";
                StrHeaderTitles += "Net,AvgRate,";
                StrTextAlign += "R,R,";
                StrTextLength += "15,15,";
                //Strcolhide += i + 1 + "," + i + 2 + ",";
            }
            StrCDC = Strings.Left(StrCDC, StrCDC.Length - 1);

            string StrTradesIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
            { StrTradesIndex = "index(idx_trades_clientcd),"; }

            if (_configuration["IsTradeWeb"] == "O")//Connect to Live DataBase
            {
                string StrCommexConn = "";
                StrCommexConn = objUtility.GetCommexConnection();

                //Query To Fecth Record From TradePlus DataBase
                strsql = "Select ltrim(rtrim(sm_desc)) sm_desc," + StrSelect;
                strsql += " convert(decimal(15,2), (isnull((select ms_lastprice from Market_summary with (nolock) where ms_exchange = td_exchange and ms_Segment = td_Segment and ms_seriesid = td_seriesid and ms_dt = (select max(ms_dt) from Market_summary with (nolock) where ms_exchange = td_exchange and ms_Segment = td_Segment and ms_seriesid = td_seriesid and  ms_dt <= '" + DateTime.Today.Date.ToString("yyyyMMdd") + "')),0) + case  when right(sm_prodtype,1) <> 'F' then  sm_strikeprice  else 0 end) ) ot_closeprice,  ";
                strsql += " convert(decimal(15,2), (isnull((select ms_lastprice from Market_summary with (nolock) where ms_exchange = td_exchange and ms_Segment = td_Segment and ms_seriesid = td_seriesid  and ms_dt = (select max(ms_dt) from Market_summary with (nolock) where ms_exchange = td_exchange and ms_Segment = td_Segment and ms_seriesid = td_seriesid and  ms_dt <= '" + DateTime.Today.Date.ToString("yyyyMMdd") + "')),0)   + case when right(sm_prodtype,1) <> 'F' then sm_strikeprice  else 0 end)	 *sum(td_bqty-td_sqty) * sm_multiplier ) Closing,  ";
                strsql += " case sm_prodtype when 'IF' then 1 when 'EF' then 2 when 'IO' then 3 when 'EO' then 4 else 5 end listorder, ";
                strsql += " case td_Segment when 'K' then case td_exchange when 'N' then 'NSEFX' when 'M' then 'MCXFX' when 'B' then 'BSEFX' end  when 'F' then Case td_exchange when 'B' then 'BSE' when 'N' then 'NSE' when 'M' then 'MCX' when 'X' then  Case td_exchange when 'B' then 'BSE' when 'N' then 'NSE' when 'M' then 'MCX' end  end end strExchange, ";
                strsql += " case right(sm_prodtype,1) when 'F' then 'Future' else 'Option' end+ case td_segment when 'X' then '(Commodities)' else (case sm_prodtype when 'CF' then ' (Currency)'  else ''end ) end  as tdType ";
                strsql += " from Trades  with (" + StrTradesIndex + "nolock), Series_master with (nolock)   ";
                strsql += " where td_seriesid=sm_seriesid and td_exchange = sm_exchange and td_Segment = sm_Segment  ";
                strsql += " and td_clientcd in (" + StrCDC + ") and td_dt <= '" + DateTime.Today.Date.ToString("yyyyMMdd") + "' and sm_expirydt >= '" + DateTime.Today.Date.ToString("yyyyMMdd") + "'  ";
                strsql += " group by sm_desc,td_exchange,td_Segment,td_seriesid,sm_prodtype,sm_strikeprice,sm_multiplier ";
                strsql += " having sum(case when td_clientcd in (" + StrCDC + ")  then (td_bqty-td_sqty) else 0 end )  <> 0";

                string StrComTradesIndex = "";
                //Query To Fecth Record From Commex DataBase
                if (_configuration["Commex"] != null && _configuration["Commex"] != string.Empty)
                {
                    if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb(StrCommexConn + ".sysobjects a, " + StrCommexConn + ".sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
                    { StrComTradesIndex = "index(idx_trades_clientcd),"; }

                    strsql += " Union all ";
                    strsql += " Select ltrim(rtrim(sm_desc)) sm_desc, " + StrSelect;
                    strsql += " convert(decimal(15,2), (isnull((select ms_lastprice from " + StrCommexConn + ".Market_summary with (nolock) where ms_exchange = td_exchange and ms_seriesid = td_seriesid and ms_dt = (select max(ms_dt) from " + StrCommexConn + ".Market_summary with (nolock) where ms_exchange = td_exchange and ms_seriesid = td_seriesid and  ms_dt <= '" + DateTime.Today.Date.ToString("yyyyMMdd") + "')),0)   + case  when right(sm_prodtype,1) <> 'F' then  sm_strikeprice  else 0 end) ) ot_closeprice, ";
                    strsql += " convert(decimal(15,2), (isnull((select ms_lastprice from " + StrCommexConn + ".Market_summary with (nolock) where ms_exchange = td_exchange and ms_seriesid = td_seriesid  and ms_dt = (select max(ms_dt) from " + StrCommexConn + ".Market_summary with (nolock) where ms_exchange = td_exchange and ms_seriesid = td_seriesid and  ms_dt <= '" + DateTime.Today.Date.ToString("yyyyMMdd") + "')),0)   + case  when right(sm_prodtype,1) <> 'F' then sm_strikeprice  else 0 end)	  *sum(td_bqty-td_sqty) * sm_multiplier ) Closing, ";
                    strsql += " case sm_prodtype when 'CF'then 11 else 12 end listorder, ";
                    strsql += " case td_exchange when 'M' then 'MCX' when 'N' then 'NCDEX' when 'S' then 'NSEL' when 'F' Then 'NCDEX' end strExchange ,";
                    strsql += " case right(sm_prodtype,1)when 'F' then 'Future (Commodities)' else 'Option (Commodities)' end as tdType ";
                    strsql += " from " + StrCommexConn + ".Trades with(nolock), " + StrCommexConn + ".Series_master with (nolock) ";
                    strsql += " where td_seriesid=sm_seriesid and td_exchange = sm_exchange and td_clientcd  in (" + StrCDC + ") ";
                    strsql += " and td_dt <= '" + DateTime.Today.Date.ToString("yyyyMMdd") + "' and sm_expirydt > '" + DateTime.Today.Date.ToString("yyyyMMdd") + "' ";
                    strsql += " group by sm_desc,td_exchange,td_seriesid,sm_prodtype,sm_strikeprice,sm_multiplier ";
                    strsql += " having sum(case when td_clientcd in (" + StrCDC + ")  then (td_bqty-td_sqty) else 0 end )  <> 0";
                    strsql += " order by listorder,sm_desc";
                }
            }
            DataSet ObjDataSet = new DataSet();
            ObjDataSet = objUtility.OpenDataSet(strsql);
            return ObjDataSet;
        }

        public DataSet Get_QueryFor_Transaction_Btn_Data(string BtnClick, String SelectedCLCode, string SelectedValue, string FromDate, string ToDate)
        {
            DataSet ds = new DataSet();
            if (BtnClick == "T")
            {
                if (SelectedValue == "Trades")
                {
                    ds = ShowTransaction(BtnClick, SelectedCLCode, SelectedValue, FromDate, ToDate);
                }

                if (SelectedValue == "Journals" || SelectedValue == "Receipts/Payments")
                {
                    ds = ShowTrxReceipts(BtnClick, SelectedCLCode, SelectedValue, FromDate, ToDate);
                }
            }
            return ds;
        }
        public DataSet ShowTransaction(string BtnClick, String SelectedCLCode, string SelectedValue, string FromDate, string ToDate)
        {
            string StrSelect = "";
            string StrCDC = "";
            char[] ArrSeparters = new char[1];
            ArrSeparters[0] = '/';
            string[] StrClCd;
            string StrDataFields = "";
            string StrHeaderTitles = "";
            //string StrSubTotalsFor = "";
            string StrTextAlign = "";
            string StrTextLength = "";
            string Strcolhide = "";
            string StrColWidth = "";
            StrClCd = Convert.ToString(SelectedCLCode).Split(ArrSeparters);
            int i = 0;
            for (i = 0; i < StrClCd.Length - 1; i++)
            {
                StrSelect += " SUM(Case when td_clientcd = '" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then X.NQty else 0 end) as '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_Qty',";
                StrSelect += " SUM(Case when td_clientcd = '" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then case  When NQty = 0 then 0 else Convert(decimal(15,2),(X.NAmt / X.NQty )) end  else 0 end) as '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_Rate',";
                //StrSelect += " SUM(Case when td_clientcd = '" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then Case When X.NQty = 0 then 0 else X.NAmt end else 0 end) as '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_Amount',";

                StrCDC += "'" + StrClCd[i].Trim().Split('~')[0].Trim() + "',";
                StrDataFields += StrClCd[i].Trim().Split('~')[0].Trim() + "_Qty," + StrClCd[i].Trim().Split('~')[0].Trim() + "_Rate,";
                StrHeaderTitles += "Qty,Rate,";
                StrTextAlign += "R,R,";
                StrTextLength += "15,15,";
                StrColWidth += "8,15";
                Strcolhide += 3 + i + "," + 3 + (i + 1) + ",";
            }
            StrCDC = Strings.Left(StrCDC, StrCDC.Length - 1);
            Strcolhide = Strings.Left(Strcolhide, Strcolhide.Length - 1);
            string StrComTradesIndex = string.Empty;
            string StrTradesIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
            { StrTradesIndex = "index(idx_trades_clientcd),"; }

            string StrTRXIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'Trx' and b.name", "idx_Trx_Clientcd", true)) == 1)
            { StrTRXIndex = "index(idx_trx_clientcd),"; }

            DataSet ObjDataSet = new DataSet();

            strsql = " select td_scripnm, snm,Td_Type, " + StrSelect;
            strsql += " SUM(X.NQty) as 'TotalQty',SUM(case  When NQty = 0 then 0 else Convert(decimal(15,2),(X.NAmt / X.NQty )) end) as 'TotalRate'";
            strsql += " From ( ";

            if (_configuration["IsTradeWeb"] == "O")//Live DB
            {
                strsql += " select td_clientcd,1 Td_order,'' as td_ac_type,'' as td_trxdate,'' as td_isin_code,'' as sc_company_name,";
                strsql += " cast((case when sum(td_bqty-td_sqty)=0 then 0 else sum((td_bqty-td_sqty)*td_rate)/sum(td_bqty-td_sqty) end)as";
                strsql += " decimal(15,2) )   as rate,'Equity ' Td_Type,'' as FScripNm,'' as FExDt,";
                strsql += " rtrim(td_scripcd)td_scripnm , rtrim(ss_name) snm,";
                strsql += " sum(td_bqty) Bqty, convert(decimal(15,2), sum(td_bqty*td_rate)) BAmt, ";
                strsql += " sum(td_sqty) Sqty, convert(decimal(15,2), sum(td_sqty*td_rate)) SAmt, sum(td_bqty-td_sqty) NQty, convert(decimal(15,2), sum((td_bqty-td_sqty)*td_rate)) NAmt, ";
                strsql += " '' as td_debit_credit,0 as sm_strikeprice,'' as sm_callput,'Equity|'+td_scripcd LinkCode ";
                strsql += " from Trx with (" + StrTRXIndex + "nolock) , securities with(nolock)";
                strsql += " where td_clientcd in (" + StrCDC + ") and td_dt between '" + FromDate + "' and '" + ToDate + "' ";
                strsql += " and td_Scripcd = ss_cd";
                strsql += " group by td_scripcd, ss_name ,'Equity|'+td_scripcd,td_clientcd  ";
            }

            strsql += " union all ";
            strsql += " select td_clientcd,case left(sm_productcd,1) when 'F' then 2 else 3 end,'', '','' as td_isin_code,'' as sc_company_name, ";
            strsql += " cast((case when  sum(td_bqty-td_sqty)=0 then 0 else sum((td_bqty-td_sqty)*td_rate)/sum(td_bqty-td_sqty) end)as decimal(15,2) ) as rate,";
            strsql += " Case When td_segment='K' then 'Currency ' else 'Equity ' end + ";
            strsql += " Case left(sm_productcd,1) when 'F' Then 'Future ' else 'Option ' end Td_Type,rtrim(sm_symbol), sm_expirydt,rtrim(sm_symbol), case left(sm_productcd,1) when 'F' then 'Fut ' else 'Opt ' end+ rtrim(sm_symbol)+'  Exp: '+ ltrim(rtrim(convert(char,convert(datetime,sm_expirydt),103))) + case left(sm_productcd,1) when 'F' then '' else ''+rtrim(convert(char(9),sm_strikeprice))+sm_callput+sm_optionstyle end, sum(td_bqty) Bqty, convert(decimal(15,2),sum(td_bqty*td_rate*sm_multiplier)) BAmt,  ";
            strsql += " sum(td_sqty) Sqty, convert(decimal(15,2),sum(td_sqty*td_rate*sm_multiplier)) SAmt, sum(td_bqty-td_sqty) NQty,  ";
            strsql += " convert(decimal(15,2),sum((td_bqty-td_sqty)*td_rate *sm_multiplier)) NAmt,'' as td_debit_credit ,sm_strikeprice, sm_callput,";
            strsql += " Case When td_segment='K' then 'Currency' else 'Equity' end + ";
            strsql += " Case left(sm_productcd,1) when 'F' Then 'Future' else 'Option' end + '|' + td_exchange + '|' + replace(sm_symbol,'&','-')  + '|' + left(sm_productcd,1) + '|' + sm_expirydt + '|' + Rtrim(Ltrim(Convert(char,sm_strikeprice))) + '|' +  sm_callput + '|' +  sm_optionstyle + '|' +  td_Segment LinkCode";
            strsql += " from trades with (" + StrTradesIndex + "nolock), series_master with (nolock) ";
            strsql += " where td_clientcd in (" + StrCDC + ") and sm_exchange=td_exchange and sm_Segment=td_Segment and td_seriesid=sm_seriesid ";
            strsql += " and td_dt between '" + FromDate + "' and '" + ToDate + "' and td_trxflag <> 'O'  ";
            strsql += " group by sm_symbol, sm_productcd,td_exchange,td_Segment, sm_expirydt, sm_strikeprice, sm_callput ,td_exchange,sm_optionstyle,td_clientcd";
            strsql += " union all ";
            strsql += " select ex_clientcd,4 ,'','' as td_trxdate,'' as td_isin_code,'' as sc_company_name,cast((case when  sum(ex_aqty-ex_eqty)=0 then 0 else sum((ex_aqty-ex_eqty)*ex_diffrate *case ex_eaflag When 'A' Then -1 else 1 end)/sum(ex_aqty-ex_eqty) end)as decimal(15,2) ) as rate , ";
            strsql += " Case When ex_Segment='K' then 'Currency ' else 'Equity ' end + case ex_eaflag when 'E' then 'Exercise ' else 'Assignment ' end Td_Type, '','', rtrim(sm_symbol), case left(sm_productcd,1) when 'F' then 'Fut ' else 'Opt ' end+ rtrim(sm_symbol)+'  Exp: '+ ltrim(rtrim(convert(char,convert(datetime,sm_expirydt),103))) + case left(sm_productcd,1) when 'F' then '' else ''+rtrim(convert(char(9),sm_strikeprice))+sm_callput+sm_optionstyle end, sum(ex_aqty) Bqty, ";
            strsql += " convert(decimal(15,2),sum(ex_aqty*ex_diffrate *case ex_eaflag When 'A' Then -1 else 1 end *sm_multiplier)) BAmt, sum(ex_eqty) Sqty, convert(decimal(15,2),sum(ex_eqty*ex_diffrate *case ex_eaflag When 'A' Then -1 else 1 end  *sm_multiplier)) SAmt, ";
            strsql += " sum(ex_aqty-ex_eqty) NQty, convert(decimal(15,2),sum((ex_aqty-ex_eqty)*ex_diffrate *case ex_eaflag When 'A' Then -1 else 1 end    *sm_multiplier)) NAmt,'' as td_debit_credit,0,'', ";
            strsql += " Case When ex_segment='K' then 'Currency' else 'Equity' end + ";
            strsql += " case ex_eaflag when 'E' then 'Exercise' else 'Assignment' end + '|' + ex_exchange + '|' + replace(sm_symbol,'&','-')  + '|' + left(sm_productcd,1) + '|' + sm_expirydt + '|' + Rtrim(Ltrim(Convert(char,sm_strikeprice))) + '|' +  sm_callput + '|' +  sm_optionstyle + '|' +  ex_Segment LinkCode";
            strsql += " from exercise with (nolock), series_master with (nolock) where ex_clientcd in (" + StrCDC + ")";
            strsql += " and ex_exchange=sm_exchange and ex_Segment=sm_Segment and ex_seriesid=sm_seriesid ";
            strsql += " and ex_dt between '" + FromDate + "' and '" + ToDate + "' group by ex_eaflag, sm_symbol,ex_exchange,ex_Segment,sm_productcd,sm_expirydt, sm_strikeprice, sm_callput ,sm_optionstyle,ex_clientcd ";


            if (_configuration["IsTradeWeb"] == "O")//Live
            {
                if (_configuration["Commex"] != null && _configuration["Commex"] != string.Empty)
                {

                    string StrCommexConn = "";
                    StrCommexConn = objUtility.GetCommexConnection();
                    if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb(StrCommexConn + ".sysobjects a, " + StrCommexConn + ".sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
                    { StrComTradesIndex = "index(idx_trades_clientcd),"; }

                    strsql += " union all ";
                    strsql += " select td_clientcd,case left(sm_productcd,1) when 'F' then 5 else 6 end,'', '','' as td_isin_code,";
                    strsql += " '' as sc_company_name,   cast((case when  sum(td_bqty-td_sqty)=0 then 0 else ";
                    strsql += " sum((td_bqty-td_sqty)*td_rate)/sum(td_bqty-td_sqty) end)as decimal(15,2) ) as rate,";
                    strsql += " case left(sm_productcd,1) when 'F' then ";
                    strsql += " 'Future (Commodities)' else 'Option (Commodities)' end Td_Type,rtrim(sm_symbol), sm_expirydt,rtrim(sm_symbol), case left(sm_productcd,1) ";
                    strsql += " when 'F' then 'Fut ' else 'Opt ' end+ rtrim(sm_symbol)+'  Exp: '+ ";
                    strsql += " ltrim(rtrim(convert(char,convert(datetime,sm_expirydt),103))) + ";
                    strsql += " case left(sm_productcd,1) when 'F' then '' else ''+rtrim(convert(char(9),sm_strikeprice))+sm_callput end, ";
                    strsql += " sum(td_bqty) Bqty, convert(decimal(15,2), sum(td_bqty*td_rate *sm_multiplier)) BAmt,  sum(td_sqty) Sqty, convert(decimal(15,2), sum(td_sqty*td_rate*sm_multiplier)) SAmt, ";
                    strsql += " sum(td_bqty-td_sqty) NQty, convert(decimal(15,2),sum((td_bqty-td_sqty)*td_rate*sm_multiplier)) NAmt,'' as td_debit_credit ,sm_strikeprice, sm_callput,'Commodities' + '|' + td_exchange + '|' + replace(sm_symbol,'&','-')  + '|' + sm_expirydt LinkCode ";
                    strsql += " from " + StrCommexConn + ".trades with(" + StrComTradesIndex + "nolock), " + StrCommexConn + ".series_master with (nolock) ";
                    strsql += " where td_clientcd in (" + StrCDC + ") and sm_exchange=td_exchange and td_seriesid=sm_seriesid and td_dt ";
                    strsql += " between '" + FromDate + "' and '" + ToDate + "' and td_trxflag <> 'O'  ";
                    strsql += " group by sm_symbol, sm_productcd,td_exchange, sm_expirydt, sm_strikeprice, sm_callput,td_clientcd  ";
                }
            }

            strsql += "  ) X ";
            strsql += " Group BY Td_Type,td_scripnm,snm";
            strsql += " Order by Td_Type,snm,td_scripnm";

            ObjDataSet = new DataSet();
            ObjDataSet = objUtility.OpenDataSet(strsql);
            return ObjDataSet;
        }
        public DataSet ShowTrxReceipts(string BtnClick, String SelectedCLCode, string SelectedValue, string FromDate, string ToDate)
        {
            string StrCDC = "";
            char[] ArrSeparters = new char[1];
            ArrSeparters[0] = '/';
            string[] StrClCd;
            StrClCd = Convert.ToString(SelectedCLCode).Split(ArrSeparters);
            int i = 0;
            for (i = 0; i < StrClCd.Length - 1; i++)
            {
                StrCDC += "'" + StrClCd[i].Trim().Split('~')[0].Trim() + "',";
            }
            StrCDC = Strings.Left(StrCDC, StrCDC.Length - 1);

            if (SelectedValue == "Receipts/Payments")
            {
                strsql = " select ltrim(rtrim(ld_clientcd)) ld_clientcd,cm_name,";
                strsql += " convert(decimal(15,2),sum(case ld_documenttype When 'R' Then Abs(ld_amount) else 0 end))  RAmt, ";
                strsql += " convert(decimal(15,2),sum(case ld_documenttype When 'P' Then (ld_amount) else 0 end))  PAmt ";
                strsql += " from ledger with (nolock),Client_master with (nolock)";
                strsql += " where cm_cd = ld_clientcd and ld_documenttype in ('R','P')";
                strsql += " and ld_clientcd in (" + StrCDC + ") and ld_dt between '" + FromDate + "' and '" + ToDate + "'";
                strsql += " group by  ld_clientcd,cm_name ";
                strsql += " order by cm_name ";

                DataSet ObjDataSet = objUtility.OpenDataSet(strsql);
                return ObjDataSet;
            }
            else
            {
                strsql = " select ld_clientcd,cm_name, ";
                strsql += " sum(case ld_debitflag when 'D' then convert(decimal(15,2),ld_amount)else 0 end)  Dr, ";
                strsql += " sum(case ld_debitflag when 'D' then 0 else convert(decimal(15,2),-ld_amount) end ) Cr ";
                strsql += " from ledger with (nolock),Client_master with (nolock) where cm_cd = ld_clientcd and ld_documenttype='J' ";
                strsql += " and ld_clientcd in (" + StrCDC + ")";
                strsql += "  and ld_dt between '" + FromDate + "' and '" + ToDate + "'";
                strsql += " group by cm_name,ld_clientcd order by cm_name";

                DataSet ObjDataSet = objUtility.OpenDataSet(strsql);
                return ObjDataSet;
            }
        }

        public DataSet Get_QueryFor_Transaction_Btn_RPJ_Detailed_Data(string Client, string Type, string FromDate, string ToDate)
        {
            //Un-Comment below two lines when running API Seperately
            #region
            //FromDate = objUtility.dtos(FromDate.ToString());
            //ToDate = objUtility.dtos(ToDate.ToString());
            #endregion
            string StrNm = objUtility.fnFireQueryTradeWeb(" Client_master ", "cm_name", "cm_cd", Client.Trim(), true);
            if (Type.Trim() == "R" || Type.Trim() == "P")
            {
                strsql = " select 'DP Transaction' Td_Type,ld_documentno , ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) Date , ld_Particular , ld_Chequeno,";
                strsql += "convert(decimal(15,2),case ld_documenttype When 'R' Then (-1) else 1 end*ld_amount)  Amount from ledger with (nolock)";
                strsql += "where ld_documenttype = '" + Type.Trim() + "'";
                strsql += "and ld_clientcd='" + Client.Trim() + "' and ld_dt between '" + FromDate + "' and '" + ToDate + "'";
                strsql += "order by ld_dt desc ";
            }
            else
            {
                strsql = "select 'DP Transaction' Td_Type,ld_documentno , ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) Date , ";
                strsql = strsql + " ld_Particular  , case ld_debitflag when 'D' then convert(decimal(15,2),ld_amount) else 0 end  Debit,";
                strsql = strsql + " case ld_debitflag when 'D' then 0 else convert(decimal(15,2),-ld_amount) end  Credit";
                strsql = strsql + " from ledger with (nolock) where ld_documenttype= 'J'";
                strsql = strsql + " and ld_clientcd='" + Client.Trim() + "'";
                strsql = strsql + " and ld_dt between '" + FromDate + "' and '" + ToDate + "'";
                strsql = strsql + " order by ld_dt desc";
            }
            DataSet ObjDataSet = new DataSet();
            ObjDataSet = objUtility.OpenDataSet(strsql);
            return ObjDataSet;
        }

        #endregion

        #endregion

        #region NVPL Handler method

        public dynamic GetINVPLDivListing(string userId, string FromDate, string ToDate)
        {
            try
            {
                //_nVPLSoapClient.Timeout = 300000;
                string strurl = objUtility.GetWebParameter("TNetInvplUrl");
                DataSet ds = new DataSet();

                if (strurl != "" || strurl != null)
                {
                    if (objUtility.WebRequestTest(strurl) == false)
                    {
                        return "Service not available at the moment try after some time.";
                    }
                }
                //ObjInvpl.Url = strurl;

                string jsondata = nVPLSoapClient.DividendAsync(userId, FromDate, ToDate).Result;
                string strdecimalcol = "SQty,SAmount,BQty,BAmount,Trading,ShortTerm,LongTerm,UnRealGainShort,UnRealGainLong,UnRealGain,NetQty,StockAtCost,MarketRate,StockAtMkt";
                if (!jsondata.Contains("No Record Found") && jsondata != "")
                {
                    ds = objUtility.ConvertJsonToDatatable(jsondata, strdecimalcol);
                }
                else
                {
                    return "No Data";
                }

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

        public dynamic GetINVPLGainLoss(string userId, string FromDate, string ToDate, Boolean chkjobing, Boolean chkdelivery, Boolean chkIgnoreSection, string TrxType)
        {
            try
            {
                DataSet ds = new DataSet();
                string strwhere = "";
                string jsondata = string.Empty;
                //ObjInvpl.Timeout = 300000;
                string strurl = objUtility.GetWebParameter("TNetInvplUrl");
                bool blnIncSttDel;
                bool blnIncSttTrd;
                blnIncSttDel = (objUtility.fnFireQueryTradeWeb("Sysparameter", "sp_sysvalue", "sp_parmcd", "GAINLOSSTTDL", true) == "Y");
                blnIncSttTrd = (objUtility.fnFireQueryTradeWeb("Sysparameter", "sp_sysvalue", "sp_parmcd", "GAINLOSSTTTR", true) == "Y");

                if (strurl != "" || strurl != null)
                {
                    if (objUtility.WebRequestTest(strurl) == false)
                    {
                        return "Service not available at the moment try after some time.";
                    }
                }
                //ObjInvpl.Url = strurl;
                try
                { string strtemp = nVPLSoapClient.ITACT112AAsync().Result; }
                catch
                {
                    return "Using older version of Tradenet";
                }

                string strdecimalcol = "SQty,SAmount,BQty,BAmount,Trading,ShortTerm,LongTerm,UnRealGainShort,UnRealGainLong,UnRealGain,NetQty,StockAtCost,MarketRate,StockAtMkt,STT";
                bool blnISIN = false;
                if (TrxType == "B")
                {
                    if (chkjobing == true && chkdelivery == true)
                    {
                        jsondata = nVPLSoapClient.ActualPLSummaryAsync(userId, FromDate, ToDate, "B", chkIgnoreSection == true ? "Y" : "N").Result;
                        blnISIN = GetISINColumn(jsondata, ds, strdecimalcol);

                        strwhere = " Tmp_Flag in ('T','X') and Tmp_SDt between '" + FromDate + "' and '" + ToDate + "'";
                    }
                    else if (chkjobing == true)
                    {
                        jsondata = nVPLSoapClient.ActualPLSummaryAsync(userId, FromDate, ToDate, "T", chkIgnoreSection == true ? "Y" : "N").Result;
                        blnISIN = GetISINColumn(jsondata, ds, strdecimalcol);

                        strwhere = " Tmp_Flag in ('T') and Tmp_SDt between '" + FromDate + "' and '" + ToDate + "' ";
                    }
                    else if (chkdelivery == true)
                    {
                        jsondata = nVPLSoapClient.ActualPLSummaryAsync(userId, FromDate, ToDate, "D", chkIgnoreSection == true ? "Y" : "N").Result;
                        blnISIN = GetISINColumn(jsondata, ds, strdecimalcol);

                        strwhere = " Tmp_Flag in ('X') and Tmp_SDt between '" + FromDate + "' and '" + ToDate + "' ";
                    }
                }
                else
                {
                    FromDate = objUtility.dtos(DateTime.Today.ToString("dd/MM/yyyy"));
                    jsondata = nVPLSoapClient.NotionalSummaryAsync(userId, FromDate, chkIgnoreSection == true ? "Y" : "N").Result;
                    blnISIN = GetISINColumn(jsondata, ds, strdecimalcol);

                    strwhere = " Tmp_Flag in ('B','S')";
                }

                if (!jsondata.Contains("No Record Found") && jsondata != "")
                {
                    ds = objUtility.ConvertJsonToDatatable(jsondata, strdecimalcol);
                }
                else
                {
                    return "No Records Found";
                }

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

        public dynamic GetINVPLTradeListing(string userId, string FromDate, string ToDate)
        {
            DataSet ds = new DataSet();
            string jsondata = string.Empty;
            //ObjInvpl.Timeout = 300000;
            string strurl = objUtility.GetWebParameter("TNetInvplUrl");
            if (strurl != "" || strurl != null)
            {
                if (objUtility.WebRequestTest(strurl) == false)
                {
                    return "alert('Service not available at the moment try after some time.";
                }
            }
            //ObjInvpl.Url = strurl;
            jsondata = nVPLSoapClient.TradeListingSummaryAsync(userId, FromDate, ToDate).Result;
            string strdecimalcol = "Bqty,BRate,BAmt,sqty,SRate,SAmt,NetQty,NAmt";
            if (!jsondata.Contains("No Record Found") && jsondata != "")
            {
                ds = objUtility.ConvertJsonToDatatable(jsondata, strdecimalcol);
            }
            else
            {
                return "No Records Found";
            }

            if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
            {
                var json = JsonConvert.SerializeObject(ds.Tables[0], Formatting.Indented);
                return json;
            }
            return new List<string>();
        }

        public dynamic GetINVPLGainLossDetails(string userId, string FromDate, string ToDate, string reportFor, string ignore112A, string scripCd)
        {
            try
            {

                DataSet ObjDataSetDetail = new DataSet();
                string strwhere = "";
                //ObjInvpl.Timeout = 300000;
                string strurl = objUtility.GetWebParameter("TNetInvplUrl");

                if (strurl == "" || strurl == null)
                {
                    if (objUtility.WebRequestTest(strurl) == false)
                    {
                        return "Service not available at the moment try after some time.";
                    }
                }

                //ObjInvpl.Url = strurl;
                string jsondata = string.Empty;

                strwhere += "'" + reportFor[0] + "'";
                if (reportFor == "TX")
                {
                    jsondata = nVPLSoapClient.ActualPLDetailAsync(userId, FromDate, ToDate, "B", scripCd, ignore112A).Result;
                }
                else if (reportFor == "T")
                {
                    jsondata = nVPLSoapClient.ActualPLDetailAsync(userId, FromDate, ToDate, "T", scripCd, ignore112A).Result;
                }
                else if (reportFor == "X")
                {
                    jsondata = nVPLSoapClient.ActualPLDetailAsync(userId, FromDate, ToDate, "D", scripCd, ignore112A).Result;
                }
                else if (reportFor == "BS")
                {
                    jsondata = nVPLSoapClient.NotionalDetailAsync(userId, FromDate, scripCd, ignore112A).Result;
                }

                jsondata = jsondata.Replace("{\"Data\" : ", "");
                string strdecimalcol = "Tmp_Qty,Trading,LongTerm,ShortTerm,StockAtCost,StockAtMkt,UnRealGainShort,UnRealGainLong";
                if (!jsondata.Contains("No Record Found") && jsondata != "")
                {
                    ObjDataSetDetail = objUtility.ConvertJsonToDatatable(jsondata, strdecimalcol);
                    int i = 0;
                    if (ObjDataSetDetail.Tables[0].Rows.Count > 0)
                    {
                        for (i = 0; i < ObjDataSetDetail.Tables[0].Rows.Count; i++)
                        {
                            if (ObjDataSetDetail.Tables[0].Rows[i]["Tmp_LTCG"].ToString().Trim() == "*")
                            {
                                if (reportFor == "BS")
                                    ObjDataSetDetail.Tables[0].Rows[i]["Avgrate"] = Convert.ToString(ObjDataSetDetail.Tables[0].Rows[i]["Avgrate"]) + Convert.ToString(ObjDataSetDetail.Tables[0].Rows[i]["Tmp_LTCG"]);
                                else
                                    ObjDataSetDetail.Tables[0].Rows[i]["Tmp_BRate"] = Convert.ToString(ObjDataSetDetail.Tables[0].Rows[i]["Tmp_BRate"]) + Convert.ToString(ObjDataSetDetail.Tables[0].Rows[i]["Tmp_LTCG"]);
                            }
                        }
                    }
                }
                else
                {
                    //lbl.Text = "No Records Found";
                    return "No Records Found";
                }
                if (ObjDataSetDetail?.Tables?.Count > 0 && ObjDataSetDetail?.Tables[0]?.Rows?.Count > 0)
                {
                    var json = JsonConvert.SerializeObject(ObjDataSetDetail.Tables[0], Formatting.Indented);
                    return json;
                }
                return new List<string>();
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        public dynamic GeTINVPLTradeListingDetails(string userId, string fromDate, string toDate, string sccdPostBack)
        {
            try
            {
                string strurl = objUtility.GetWebParameter("TNetInvplUrl");
                string jsondata = string.Empty;
                DataSet ObjDataSet = new DataSet();
                // ObjInvpl.Timeout = 300000;
                if (strurl != "" || strurl != null)
                {
                    if (objUtility.WebRequestTest(strurl) == false)
                    {
                        return "Service not available at the moment try after some time.";
                    }
                }
                //ObjInvpl.Url = strurl;
                if (objUtility.stod(Convert.ToString(fromDate)) > objUtility.stod(fromDate))
                { jsondata = nVPLSoapClient.TradeListingDetailAsync(userId, fromDate, toDate, sccdPostBack).Result; }
                else
                { jsondata = nVPLSoapClient.TradeListingDetailAsync(userId, fromDate, toDate, sccdPostBack).Result; }

                string strdecimalcol = "Qty,td_Rate,Value,td_ServiceTax,td_STT,td_OtherChrgs1,td_OtherChrgs2";
                if (!jsondata.Contains("No Record Found") && jsondata != "")
                {
                    ObjDataSet = objUtility.ConvertJsonToDatatable(jsondata, strdecimalcol);
                }
                else
                {
                    return "No Record Found";
                }

                if (ObjDataSet?.Tables?.Count > 0 && ObjDataSet?.Tables[0]?.Rows?.Count > 0)
                {
                    var json = JsonConvert.SerializeObject(ObjDataSet.Tables[0], Formatting.Indented);
                    return json;
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public dynamic GetINVPLTradeListingDelete(string userId, string srNo)
        {
            try
            {
                DataSet ObjDataSet = new DataSet();
                string strmsg = string.Empty;
                //ObjInvpl.Timeout = 300000;
                string strurl = objUtility.GetWebParameter("TNetInvplUrl");
                if (strurl != "" || strurl != null)
                {
                    if (objUtility.WebRequestTest(strurl) == false)
                    {
                        return "Service not available at the moment try after some time.";
                    }
                }
                //ObjInvpl.Url = strurl;
                strmsg = nVPLSoapClient.TradeDeleteAsync(userId, srNo).Result;

                ObjDataSet = objUtility.ConvertJsonToDatatable(strmsg, "");
                if (ObjDataSet.Tables[0].Rows.Count > 0)
                {
                    if (ObjDataSet.Tables[0].Rows[0]["Status"].ToString().Trim() == "Y")
                    {
                        return "Deleted";

                    }
                    else
                    { return ObjDataSet.Tables[0].Rows[0]["Remarks"].ToString(); }

                }
                return null;
            }
            catch (Exception Ex)
            {
                throw Ex;
            }
        }

        public dynamic GetINVPLTradeListingSave(string userId, string date, string settelment, string bsFlag, string tradeType, double quantity, double netRate, double serviceTax, double STT, double otherCharge1, double otherCharge2, string sccdPostBack)
        {
            try
            {
                DataSet ObjDataSet = new DataSet();
                double MarketRate = netRate;
                date = objUtility.stod(date).ToString("dd/MM/yyyy").Replace('-', '/');
                if (date.ToString() != "")
                {
                    if (InvalidDateCheck(date.ToString()) == false)
                    {
                        return "Invalid Date";
                    }
                }
                else
                {
                    return "Date Cannot Be Blank";
                }
                if (quantity.ToString() == "" || Conversion.Val(quantity.ToString()) <= 0)
                {
                    return "Quantity Cannot Be Blank !";
                }

                double dblservicetax, dblstt, dblchrg1, dblchrg2;
                dblservicetax = Math.Round((serviceTax / quantity), 4);
                dblstt = Math.Round((STT / quantity), 4);
                dblchrg1 = Math.Round((otherCharge1 / quantity), 4);
                dblchrg2 = Math.Round((otherCharge2 / quantity), 4);

                StringBuilder SbInsert = new StringBuilder();
                SbInsert.Append("<Trade>");
                SbInsert.Append("<srno>0</srno>");
                SbInsert.Append("<Trxflag>M</Trxflag>");
                SbInsert.Append("<Date>" + objUtility.dtos(date) + "</Date>");
                SbInsert.Append("<stlmnt> " + settelment + " </stlmnt>");
                SbInsert.Append("<TRDType>" + tradeType + "</TRDType>");
                SbInsert.Append("<bsflag> " + bsFlag.ToString() + "</bsflag>");
                SbInsert.Append("<qty>  " + Conversion.Val(quantity) + "</qty>");
                SbInsert.Append("<Rate>" + netRate + "</Rate>");
                SbInsert.Append("<ServiceTax>" + dblservicetax + "</ServiceTax>");
                SbInsert.Append("<STT> " + dblstt + "</STT>");
                SbInsert.Append("<OtherChrgs1>" + dblchrg1 + "</OtherChrgs1>");
                SbInsert.Append("<OtherChrgs2>" + dblchrg2 + "</OtherChrgs2>");
                SbInsert.Append("<mkrid>" + userId + "</mkrid>");
                SbInsert.Append("<mkrdt>" + DateTime.Today.Date.ToString("yyyyMMdd") + "</mkrdt>");
                SbInsert.Append("</Trade>");

                string strmsg = string.Empty;
                //ObjInvpl.Timeout = 300000;
                string strurl = objUtility.GetWebParameter("TNetInvplUrl");
                if (strurl != "" || strurl != null)
                {
                    if (objUtility.WebRequestTest(strurl) == false)
                    {
                        return "Service not available at the moment try after some time.";
                    }
                }
                //ObjInvpl.Url = strurl;
                strmsg = nVPLSoapClient.TradeInsertAsync(userId, sccdPostBack.Trim(), SbInsert.ToString()).Result;

                ObjDataSet = objUtility.ConvertJsonToDatatable(strmsg, "");
                if (ObjDataSet.Tables[0].Rows.Count > 0)
                {
                    if (ObjDataSet.Tables[0].Rows[0]["Status"].ToString().Trim() == "Y")
                    {
                        return "Saved";
                    }
                    else
                    { return ObjDataSet.Tables[0].Rows[0]["Remarks"].ToString(); }
                }
                return null;
            }
            catch (Exception ex)
            {
                return ex;
            }
        }
        #region NVPL Usefull method

        private bool GetISINColumn(string jsondata, DataSet ds, string strdecimalcol)
        {
            bool blnISIN = false;
            if (!jsondata.Contains("No Record Found"))
            {
                ds = objUtility.ConvertJsonToDatatable(jsondata, strdecimalcol);
                if (ds.Tables[0].Rows.Count > 0)
                {
                    for (int i = 0; i < ds.Tables[0].Columns.Count; i++)
                    {
                        blnISIN = (ds.Tables[0].Columns[i].ColumnName == "Tmp_ISIN" ? true : false);
                    }
                }
            }
            return blnISIN;
        }

        public static bool InvalidDateCheck(string obj)
        {
            if (Regex.IsMatch(obj.ToString(), (@"(((0[1-9]|[12][0-9]|3[01])([/])(0[13578]|10|12)([/])(\d{4}))|(([0][1-9]|[12][0-9]|30)([/])(0[469]|11)([/])(\d{4}))|((0[1-9]|1[0-9]|2[0-8])([/])(02)([/])(\d{4}))|((29)(\.|-|\/)(02)([/])([02468][048]00))|((29)([/])(02)([/])([13579][26]00))|((29)([/])(02)([/])([0-9][0-9][0][48]))|((29)([/])(02)([/])([0-9][0-9][2468][048]))|((29)([/])(02)([/])([0-9][0-9][13579][26])))")))
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        #endregion

        #endregion

        #region Request Handler method

        //// update fund request or share requrest
        public dynamic UpdateFundAndSharesRequest(bool isPostBack)
        {
            try
            {
                UpdateFundAndSharesRequestQuery(isPostBack);
                var json = JsonConvert.SerializeObject("success");
                return json;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //// Radio button shares checked
        public dynamic RdButtonSharesChecked(string cm_cd)
        {
            try
            {
                var ds = RdButtonSharesCheckedQuery(cm_cd);
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

        //// Get rms request
        public dynamic GetRmsRequest(string cm_cd)
        {
            try
            {
                var ds = GetRmsRequestQuery(cm_cd);
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

        //// Execute page request report page load query
        public dynamic ExecuteRequestReportPageLoad(bool isPostBack)
        {
            try
            {
                ExecuteRequestReportPageLoadQuery(isPostBack);
                var json = JsonConvert.SerializeObject("success");
                return json;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //// Insert Request value after button click
        public dynamic InsertRequestValues(string userId, string strLstSeg, string cmbRequest, string strFromDt, string strToDt)
        {
            try
            {
                var result = InsertRequestValuesQuery(userId, strLstSeg, cmbRequest, strFromDt, strToDt);
                var json = JsonConvert.SerializeObject(result);
                return json;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #region Request usefull method

        //// create table for fund request or share requrest.
        private void CreateTableForRequest()
        {
            DataSet DsReqF = new DataSet();
            DsReqF = objUtility.OpenDataSet("select * from SysObjects where name= 'FundsRequest'");
            if (DsReqF.Tables[0].Rows.Count == 0)
            {
                prCreateRequestTable();
            }
            DataSet DsReqS = new DataSet();
            DsReqS = objUtility.OpenDataSet("select * from SysObjects where name= 'SharesRequest'");
            if (DsReqS.Tables[0].Rows.Count == 0)
            {
                prCreateRequest();
            }
        }

        private void prCreateRequest()
        {
            strsql = "Create table SharesRequest  ( ";
            strsql += " Rq_SrNo Numeric Identity(1,1) not null,";
            strsql += " Rq_Clientcd varchar(8) not null,";
            strsql += " Rq_Scripcd varchar(6) not null,";
            strsql += " Rq_Qty numeric not null,";
            strsql += " Rq_IpAddress varchar(50) not null,";
            strsql += " Rq_Date char(8) not null,";
            strsql += " Rq_Time char(8) not null,";
            strsql += " Rq_Satus1 char(1) not null,";
            strsql += " Rq_Satus2 char(1) not null,";
            strsql += " Rq_Satus3 char(1) not null,";
            strsql += " Rq_Satus4 char(1) not null,";
            strsql += " Rq_Note varchar(50) not null)";
            objUtility.ExecuteSQL(strsql);

        }
        private void prCreateRequestTable()
        {
            strsql = "Create table FundsRequest  ( ";
            strsql += " Rq_SrNo Numeric Identity(1,1) not null,";
            strsql += " Rq_Clientcd varchar(8) not null,";
            strsql += " Rq_Type Char(1) not null,";
            strsql += " Rq_Amount Money not null,";
            strsql += " Rq_IpAddress varchar(50) not null,";
            strsql += " Rq_Date char(8) not null,";
            strsql += " Rq_Time char(8) not null,";
            strsql += " Rq_Satus1 char(1) not null,";
            strsql += " Rq_Satus2 char(1) not null,";
            strsql += " Rq_Satus3 char(1) not null,";
            strsql += " Rq_Satus4 char(1) not null,";
            strsql += " Rq_Note varchar(50) not null)";
            objUtility.ExecuteSQL(strsql);
        }

        //// update fund request or share requrest
        private void UpdateFundAndSharesRequestQuery(bool isPostBack)
        {
            if (isPostBack == false)
            {
                CreateTableForRequest();
            }

            if (Convert.ToInt32(objUtility.fnFireQueryTradeWeb("sysobjects a , syscolumns b", " b.length ", "a.id = b.id  and a.name = 'FundsRequest' and b.name", "Rq_Satus4", true)) < 8)
            {
                strsql = "alter table FundsRequest alter column Rq_Satus4 Varchar(8) not null";
                objUtility.ExecuteSQL(strsql);

                DataSet dsStatus = objUtility.OpenDataSet("Select * from FundsRequest where Rq_Satus1 = 'P'");
                for (int i = 0; i <= dsStatus.Tables[0].Rows.Count - 1; i++)
                {
                    strsql = "update FundsRequest Set Rq_Satus4 = '" + objUtility.Encrypt(dsStatus.Tables[0].Rows[i]["Rq_Date"].ToString()) + "' where rq_srno = " + dsStatus.Tables[0].Rows[i]["rq_srno"] + "";
                    objUtility.ExecuteSQL(strsql);
                }
            }

            if (Convert.ToInt32(objUtility.fnFireQueryTradeWeb("sysobjects a , syscolumns b", " b.length ", "a.id = b.id  and a.name = 'SharesRequest' and b.name", "Rq_Satus4", true)) < 8)
            {
                strsql = "alter table SharesRequest alter column Rq_Satus4 Varchar(8) not null";
                objUtility.ExecuteSQL(strsql);

                DataSet dsStatus = objUtility.OpenDataSet("Select * from SharesRequest where Rq_Satus1 = 'P'");
                for (int i = 0; i <= dsStatus.Tables[0].Rows.Count - 1; i++)
                {
                    strsql = "update SharesRequest Set Rq_Satus4 = '" + objUtility.Encrypt(dsStatus.Tables[0].Rows[i]["Rq_Date"].ToString()) + "' where rq_srno = " + dsStatus.Tables[0].Rows[i]["rq_srno"] + "";
                    objUtility.ExecuteSQL(strsql);
                }
            }
        }

        //// Radio button shares checked
        private DataSet RdButtonSharesCheckedQuery(string cm_cd)
        {
            if (_configuration["IsTradeWeb"] == "O")//live
            {
                strsql = "select dm_scripcd bh_scripcd,isNull(IM_ISIN,'') bh_isin,ss_Name bh_Scripname,sum(-dm_qty) bh_qty,";
                strsql += "  CAST(sum(-dm_qty *(case when ss_bseratedt > ss_nseratedt then ss_bserate else ss_nserate end)) as decimal(15,2)) bh_valuation,";
                strsql += " isNull((select sum(Rq_Qty) From SharesRequest Where Rq_Clientcd = '" + cm_cd + "' and Rq_Scripcd=dm_scripcd and Rq_Satus1 = 'P'),0) ReqQty, ";
                strsql += " (case when ss_bseratedt > ss_nseratedt then ss_bserate else ss_nserate end) Rate";
                strsql += " From Demat left outer join ISIN on dm_scripcd = im_scripcd  ,settlements,ourdps,SECURITIES";
                strsql += " Where dm_type = 'BC' and dm_locked = 'N' and dm_transfered = 'N'and dm_stlmnt = se_stlmnt and dm_ourdp = od_cd and dm_Scripcd = ss_cd";
                strsql += " and dm_clientcd = '" + cm_cd + "' and im_active = 'Y' ";
                strsql += " and im_priority =(select min(im_priority) from ISIN  Where im_active = 'Y'  and im_scripcd = dm_Scripcd)";
                strsql += " and od_acttype  in ('B','M') and se_shpayoutdt <= CONVERT(CHAR,getdaTE(),112)Group by dm_scripcd ,ss_Name,IM_ISIN,ss_bseratedt,ss_nseratedt,ss_bserate,ss_nserate  having (sum(-dm_qty)) >0order by ss_Name ";
            }
            else
            {
                strsql = " select bh_scripcd,bh_isin,bh_Scripname,(bh_qty*-1)bh_qty, cast((bh_valuation*-1 )as decimal(15,0))bh_valuation ,";
                strsql += " isNull((select sum(Rq_Qty) From SharesRequest Where Rq_Clientcd = bh_clientcd and Rq_Scripcd=bh_scripcd and Rq_Satus1 = 'P'),0) ReqQty, 0 Rate ";
                strsql += " from benholding where bh_clientcd='" + cm_cd + "' and bh_Type not in ('UNDEL')";
            }

            return objUtility.OpenDataSet(strsql);
        }

        //// Get rms request
        private DataSet GetRmsRequestQuery(string cm_cd)
        {
            StringBuilder strsql = new StringBuilder();
            strsql.Append("select  (ltrim(rtrim(ces_exchange)) + '/' + ltrim(rtrim(ces_segment)))Exch ,cm_cd ,cm_name,ld_dpid,  cast((-1*sum(ld_amount)) as decimal(15,2)) amt, ");
            strsql.Append(" isNull((select CAST(Sum(Rq_Amount) as decimal(15,2)) from FundsRequest Where rq_clientcd = ld_clientcd and Rq_Note = ld_dpid and Rq_Satus1 = 'P' ),0) rq_amt ");
            strsql.Append(" from  Client_Master, companyexchangesegments, Ledger ");
            strsql.Append(" where ld_clientcd = cm_cd and CES_Cd=ld_dpid  and ld_clientcd = '" + cm_cd + "'  ");
            strsql.Append("group by  ld_clientcd ,cm_brkggroup,cm_cd,cm_name,ld_dpid,ltrim(rtrim(ces_exchange)) + '/' + ltrim(rtrim(ces_segment)),ld_dpid,ld_clientcd having abs(sum(ld_amount)) > 0");

            return objUtility.OpenDataSet(strsql.ToString());

        }

        #region Request Report module

        //// Execute page request report page load query.
        private void ExecuteRequestReportPageLoadQuery(bool isPostBack)
        {
            if (isPostBack == false)
            {
                CreateTableRequestReport();
            }
            SqlCommand ObjCommand = new SqlCommand();
            SqlConnection ObjConnection;
            using (var db = new DataContext())
            {
                ObjConnection = new SqlConnection((db.Database.GetDbConnection()).ConnectionString);
                ObjConnection.Open();
                if (Convert.ToInt32(objUtility.fnFireQueryTradeWeb("sysobjects a , syscolumns b", " b.length ", "a.id = b.id  and a.name = 'requests' and b.name", "Rq_Satus4", true)) < 8)
                {
                    strsql = "alter table Requests alter column Rq_Satus4 Varchar(8) not null";
                    ObjCommand.CommandText = strsql;
                    ObjCommand.Connection = ObjConnection;
                    ObjCommand.ExecuteNonQuery();

                    DataSet dsStatus = objUtility.OpenDataSet("Select * from Requests where Rq_Satus1 = 'P'");
                    for (int i = 0; i <= dsStatus.Tables[0].Rows.Count - 1; i++)
                    {
                        strsql = "update Requests Set Rq_Satus4 = '" + objUtility.Encrypt(dsStatus.Tables[0].Rows[i]["Rq_Date"].ToString()) + "' where rq_srno = " + dsStatus.Tables[0].Rows[i]["rq_srno"] + "";
                        ObjCommand.CommandText = strsql;
                        ObjCommand.Connection = ObjConnection;
                        ObjCommand.ExecuteNonQuery();
                    }
                }
            }
        }

        //// Create table for Request report.
        private void CreateTableRequestReport()
        {
            DataSet DsReq = new DataSet();
            DsReq = objUtility.OpenDataSet("select * from SysObjects where name= 'Requests'");
            if (DsReq.Tables[0].Rows.Count == 0)
            {
                strsql = "Create table Requests  (";
                strsql += " Rq_SrNo Numeric Identity(1,1) not null,";
                strsql += " Rq_Clientcd varchar(8) not null,";
                strsql += " Rq_Type varchar(35) not null,";
                strsql += " Rq_DateFrom char(8) not null,";
                strsql += " Rq_DateTo char(9) not null,";
                strsql += " Rq_IpAddress varchar(50) not null,";
                strsql += " Rq_Date char(8) not null,";
                strsql += " Rq_Time char(8) not null,";
                strsql += " Rq_Satus1 char(1) not null,";
                strsql += " Rq_Satus2 char(1) not null,";
                strsql += " Rq_Satus3 char(1) not null,";
                strsql += " Rq_Satus4 char(1) not null,";
                strsql += " Rq_Note varchar(50) not null)";
                objUtility.ExecuteSQL(strsql);
            }

            DsReq = new DataSet();
            DsReq = objUtility.OpenDataSet("select b.Name from sysobjects a, syscolumns b with (nolock) where a.id=b.id and a.name='Requests' and b.name = 'Rq_Segment'");
            if (DsReq.Tables[0].Rows.Count == 0)
            {
                strsql = "Alter table Requests add Rq_Segment Char(10)";
                objUtility.ExecuteSQL(strsql);

                strsql = "Update Requests set Rq_Segment= ''";
                objUtility.ExecuteSQL(strsql);

                strsql = "Alter table Requests alter Column Rq_Segment Char(10) not null";
                objUtility.ExecuteSQL(strsql);
            }
        }

        //// Insert Request value after button click
        private string InsertRequestValuesQuery(string userId, string strLstSeg, string cmbRequest, string strFromDt, string strToDt)
        {
            string gstrToday = DateTime.Today.ToString("yyyyMMdd");
            string strHostAdd = Dns.GetHostName();
            bool blnIdentityOn = false;
            DataSet Dstemp = new DataSet();
            Dstemp = objUtility.OpenDataSet("SELECT isnull (IDENT_CURRENT('Requests'),0)");
            long intcnt = 0;
            if (Convert.ToInt64(Dstemp.Tables[0].Rows[0][0]) > 0)
            {
                blnIdentityOn = true;
                DataSet DsReqId = new DataSet();
                DsReqId = objUtility.OpenDataSet("SELECT IDENT_CURRENT('Requests')");
                intcnt = Convert.ToInt64(DsReqId.Tables[0].Rows[0][0]);
            }
            else
            {
                blnIdentityOn = false;
                DataSet Dsmax = new DataSet();
                Dsmax = objUtility.OpenDataSet("SELECT isNull(Max(Rq_srNo),0) from Requests");
                intcnt = Convert.ToInt64(Dsmax.Tables[0].Rows[0][0]) + 1;
            }

            if (blnIdentityOn)
                strsql = "insert into Requests values ( ";
            else
                strsql = "insert into Requests values ( " + intcnt + ",";

            strsql += " '" + userId + "','" + cmbRequest + "','" + strFromDt + "','" + strToDt + "','" + strHostAdd + "',";
            strsql += " '" + gstrToday + "',";
            strsql += " convert(char(8),getdate(),108),";
            strsql += " 'P','P','P','" + objUtility.Encrypt((gstrToday).ToString().Trim()) + "','',";
            strsql += " '" + strLstSeg + "')";
            objUtility.ExecuteSQL(strsql);

            return intcnt.ToString();
        }

        #endregion
        #endregion
        #endregion

        #region Digital Document Handler Method
        // get dropdown Exchange list
        public dynamic GetDdlExchangeList(string cmbProductValue, string cmbDocumentTypeValue)
        {
            try
            {
                var ds = GetQueryDdlExchangeList(cmbProductValue, cmbDocumentTypeValue);

                if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                {
                    var json = JsonConvert.SerializeObject(ds, Formatting.Indented);
                    return json;
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        // get digital document main data
        public dynamic GetDigitalDocumentData(string userId, string cmbProductValue, string cmbDocumentTypeValue, string cmbExchangeValue, string fromDate, string toDate)
        {
            try
            {
                var ds = GetQueryDigitalDocumentData(userId, cmbProductValue, cmbDocumentTypeValue, cmbExchangeValue, fromDate, toDate);
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

        // Add new item comodity in dropdown product list
        public dynamic AddDdlProductListItem()
        {
            try
            {
                var ds = AddQueryDdlProductListItem();
                if (ds != null && ds != "")
                {
                    var json = JsonConvert.SerializeObject(ds);
                    return json;
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        // get digital document main data
        public dynamic GetDigitalDocumentDownload(string userId, string cmbProductValue, string cmbDocumentTypeValue, string cmbExchangeValue, string fromDate)
        {
            try
            {
                var ds = GetQueryDigitalDocumentDownload(userId, cmbProductValue, cmbDocumentTypeValue, cmbExchangeValue, fromDate);
                if (ds != null)
                {
                    if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                    {
                        var response = DownloadDocument(ds.Tables[0].Rows[0]["doctype"].ToString().Trim(), fromDate, ds.Tables[0].Rows[0]["dd_srno"].ToString().Trim());
                        return response;
                    }
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #region DigitalDocument usefull method

        public DataSet GetQueryDdlExchangeList(string cmbProductValue, string cmbDocumentTypeValue)
        {
            string strsql = "";
            DataSet CommonDataSet = new DataSet();
            if (cmbProductValue == "X")
            {
                strsql = "select rtrim(ltrim(CES_Exchange)) exch ";
                strsql += "from " + objUtility.GetCommexConnection() + ".CompanyExchangeSegments where left(ces_cd,1) = (select min(em_cd) from Entity_master Where Len(Ltrim(Rtrim(em_cd))) = 1) ";
            }
            else
            {
                strsql = "select rtrim(ltrim(CES_Exchange)) +  case substring(rtrim(ltrim(ces_cd)),3,1) when 'F' then 'DERV' when 'K' then rtrim(ltrim(CES_Segment)) else + ' ' + rtrim(ltrim(CES_Segment))  end exch ";
                strsql += "from CompanyExchangeSegments where RIGHT(ces_cd,1) <> 'X'  and left(ces_cd,1) = (select min(em_cd) from Entity_master Where Len(Ltrim(Rtrim(em_cd))) = 1) ";
            }

            CommonDataSet.Tables.Add(objUtility.OpenNewDataTable(strsql));

            if (cmbDocumentTypeValue == "Combine Contract" || cmbDocumentTypeValue == "Combine Margin Statement")
            {
                if (cmbProductValue == "X")
                {
                    strsql = "select rtrim(ltrim(CES_Exchange)) + case when substring(rtrim(ltrim(ces_cd)),2,1) in ('B','N') then + ' ' + rtrim(ltrim(CES_Segment)) else '' end exch ";
                    strsql += "from CompanyExchangeSegments where RIGHT(ces_cd,1) = 'X' and left(ces_cd,1) = (select min(em_cd) from Entity_master Where Len(Ltrim(Rtrim(em_cd))) = 1) ";

                    CommonDataSet.Tables.Add(objUtility.OpenNewDataTable(strsql));
                }
            }
            return CommonDataSet;
        }


        public DataSet GetQueryDigitalDocumentData(string userId, string cmbProductValue, string cmbDocumentTypeValue, string cmbExchangeValue, string fromDate, string toDate)
        {
            Boolean IsCommexES = false;
            string strsql = "";
            if (cmbProductValue == "X")
            {
                if (_configuration["CommexES"] != null && _configuration["CommexES"] != string.Empty)
                {
                    IsCommexES = true;
                    strsql = "select 'Commex' doctype,'DownLoad'as download,dd_srno,do_desc,ltrim(rtrim(convert(char,convert(datetime,dd_dt),103))) as dd_dt,dd_stlmnt,dd_contractno,'' DT ";
                    char[] ArrSeparators = new char[1];
                    ArrSeparators[0] = '/';
                    string[] ArrCommex = _configuration["CommexES"].Split(ArrSeparators);
                    strsql = strsql + " from   " + ArrCommex[0].Trim() + "." + ArrCommex[1].Trim() + "." + ArrCommex[2].Trim() + ".digital_details,";
                    strsql = strsql + ArrCommex[0].Trim() + "." + ArrCommex[1].Trim() + "." + ArrCommex[2].Trim() + ".DocumentType_master";
                    strsql = strsql + " where dd_clientcd = '" + userId + "'";
                    if (cmbDocumentTypeValue == "Combine Contract" || cmbDocumentTypeValue == "Periodic Settlement")
                    {
                        strsql = strsql + " and dd_dt between '" + fromDate + "' and '" + toDate + "'"; ;
                    }
                    else
                    {
                        strsql = strsql + " and dd_dt='" + fromDate + "'";
                    }
                    strsql = strsql + "and dd_filetype = do_cd ";
                    strsql = strsql + " and dd_filetype='" + getdoc(cmbDocumentTypeValue, cmbExchangeValue) + "' ";
                    strsql = strsql + " order by dd_dt desc ";
                }
            }
            else
            {
                strsql = "select 'Tplus' doctype,'Download'as download,dd_srno,do_desc,ltrim(rtrim(convert(char,convert(datetime,dd_dt),103))) as dd_dt,dd_stlmnt,dd_contractno,'" + fromDate + "' DT ";
                strsql = strsql + "from digital_details ,DocumentType_master ";
                strsql = strsql + "where dd_clientcd = '" + userId + "'";
                if (cmbDocumentTypeValue == "Combine Contract" || cmbDocumentTypeValue == "Periodic Settlement" || cmbDocumentTypeValue == "Combine Margin Statement")
                {
                    strsql = strsql + " and dd_dt between '" + fromDate + "' and '" + toDate + "'"; ;
                }
                else
                {
                    strsql = strsql + " and dd_dt='" + fromDate + "'";
                }
                strsql = strsql + " and dd_filetype = do_cd ";
                strsql = strsql + " and dd_filetype='" + getdoc(cmbDocumentTypeValue, cmbExchangeValue) + "' ";
                strsql = strsql + " order by dd_dt desc ";
            }

            DataSet ObjDataSet1 = new DataSet();
            if (strsql != null && strsql != "")
            {
                SqlConnection ObjConnection = new SqlConnection(objUtility.EsignConnectionString(IsCommexES, fromDate));
                if (ObjConnection.State == ConnectionState.Closed)
                { ObjConnection.Open(); }

                SqlDataAdapter ObjAdpater = new SqlDataAdapter();
                SqlCommand sqlCommand = new SqlCommand(strsql, ObjConnection);
                ObjAdpater.SelectCommand = sqlCommand;
                ObjAdpater.Fill(ObjDataSet1);
                ObjAdpater.Dispose();
            }
            return ObjDataSet1;
        }

        public DataSet GetQueryDigitalDocumentDownload(string userId, string cmbProductValue, string cmbDocumentTypeValue, string cmbExchangeValue, string fromDate)
        {
            Boolean IsCommexES = false;
            string strsql = "";
            if (cmbProductValue == "X")
            {
                if (_configuration["CommexES"] != null && _configuration["CommexES"] != string.Empty)
                {
                    IsCommexES = true;
                    strsql = "select 'Commex' doctype,'DownLoad'as download,dd_srno,do_desc,ltrim(rtrim(convert(char,convert(datetime,dd_dt),103))) as dd_dt,dd_stlmnt,dd_contractno,'' DT ";
                    char[] ArrSeparators = new char[1];
                    ArrSeparators[0] = '/';
                    string[] ArrCommex = _configuration["CommexES"].Split(ArrSeparators);
                    strsql = strsql + " from   " + ArrCommex[0].Trim() + "." + ArrCommex[1].Trim() + "." + ArrCommex[2].Trim() + ".digital_details,";
                    strsql = strsql + ArrCommex[0].Trim() + "." + ArrCommex[1].Trim() + "." + ArrCommex[2].Trim() + ".DocumentType_master";
                    strsql = strsql + " where dd_clientcd = '" + userId + "'";
                    if (cmbDocumentTypeValue == "Combine Contract" || cmbDocumentTypeValue == "Periodic Settlement")
                    {
                        strsql = strsql + " and dd_dt between '" + fromDate + "' and '" + fromDate + "'"; ;
                    }
                    else
                    {
                        strsql = strsql + " and dd_dt='" + fromDate + "'";
                    }
                    strsql = strsql + "and dd_filetype = do_cd ";
                    strsql = strsql + " and dd_filetype='" + getdoc(cmbDocumentTypeValue, cmbExchangeValue) + "' ";
                    strsql = strsql + " order by dd_dt desc ";
                }
            }
            else
            {
                strsql = "select 'Tplus' doctype,'Download'as download,dd_srno,do_desc,ltrim(rtrim(convert(char,convert(datetime,dd_dt),103))) as dd_dt,dd_stlmnt,dd_contractno,'" + fromDate + "' DT ";
                strsql = strsql + "from digital_details ,DocumentType_master ";
                strsql = strsql + "where dd_clientcd = '" + userId + "'";
                if (cmbDocumentTypeValue == "Combine Contract" || cmbDocumentTypeValue == "Periodic Settlement" || cmbDocumentTypeValue == "Combine Margin Statement")
                {
                    strsql = strsql + " and dd_dt between '" + fromDate + "' and '" + fromDate + "'"; ;
                }
                else
                {
                    strsql = strsql + " and dd_dt='" + fromDate + "'";
                }
                strsql = strsql + " and dd_filetype = do_cd ";
                strsql = strsql + " and dd_filetype='" + getdoc(cmbDocumentTypeValue, cmbExchangeValue) + "' ";
                strsql = strsql + " order by dd_dt desc ";
            }

            DataSet ObjDataSet1 = new DataSet();
            if (strsql != null && strsql != "")
            {
                SqlConnection ObjConnection = new SqlConnection(objUtility.EsignConnectionString(IsCommexES, fromDate));
                if (ObjConnection.State == ConnectionState.Closed)
                { ObjConnection.Open(); }

                SqlDataAdapter ObjAdpater = new SqlDataAdapter();
                SqlCommand sqlCommand = new SqlCommand(strsql, ObjConnection);
                ObjAdpater.SelectCommand = sqlCommand;
                ObjAdpater.Fill(ObjDataSet1);
                ObjAdpater.Dispose();
            }

            return ObjDataSet1;

            //if (ObjDataSet1?.Tables?.Count > 0 && ObjDataSet1?.Tables[0]?.Rows?.Count > 0)
            //{
            //    var json = JsonConvert.SerializeObject(ObjDataSet1.Tables[0], Formatting.Indented);
            //    return json;
            //}
        }

        public DigitalDocumentModel DownloadDocument(string docType, string date, string srNo)
        {
            Boolean IsCommex = false;
            strsql = "select dd_filename from ";
            if (docType == "Tplus")
            {
                strsql += " digital_details ";
            }
            else if (docType == "Commex")
            {
                IsCommex = true;
                char[] ArrSeparators = new char[1];
                ArrSeparators[0] = '/';
                string[] ArrCommex = _configuration["CommexES"].Split(ArrSeparators);
                strsql = strsql + ArrCommex[0].Trim() + "." + ArrCommex[1].Trim() + "." + ArrCommex[2].Trim() + ".digital_details";
            }
            strsql += " where dd_srno = '" + srNo + "'";
            DataSet DataSet = new DataSet();
            SqlConnection ObjConnection = new SqlConnection(objUtility.EsignConnectionString(IsCommex, date.Trim()));
            SqlCommand cmd1 = new SqlCommand(strsql, ObjConnection);
            SqlDataAdapter adp = new SqlDataAdapter();
            adp.SelectCommand = cmd1;
            adp.Fill(DataSet);
            if (DataSet.Tables[0].Rows.Count > 0)
            {
                strsql = "select dd_document from ";
                if (docType == "Tplus")
                {
                    strsql += " digital_details ";
                }
                else if (docType == "Commex")
                {
                    char[] ArrSeparators = new char[1];
                    ArrSeparators[0] = '/';
                    string[] ArrCommex = _configuration["CommexES"].Split(ArrSeparators);
                    strsql = strsql + ArrCommex[0].Trim() + "." + ArrCommex[1].Trim() + "." + ArrCommex[2].Trim() + ".digital_details";
                }
                strsql += " where dd_srno = '" + srNo + "'";
                if (ObjConnection.State == ConnectionState.Closed)
                {
                    ObjConnection.Open();
                }
                SqlCommand cmd = new SqlCommand(strsql, ObjConnection);

                SqlDataReader ObjReader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                if (ObjReader.HasRows == true)
                {
                    ObjReader.Read();
                    var bytes = (byte[])ObjReader["dd_document"];
                    var result = Convert.ToBase64String(bytes, 0, bytes.Length);

                    return new DigitalDocumentModel { FileName = DataSet.Tables[0].Rows[0]["dd_filename"].ToString().Trim(), FileData = result };
                }
                ObjReader.Close();
            }
            return null;
        }

        // Add new item comodity in dropdown product list
        public string AddQueryDdlProductListItem()
        {
            var result = (Convert.ToInt16(objUtility.fnFireQueryTradeWeb(objUtility.GetCommexConnection() + ".CompanyExchangeSegments ", "count(0)", "left(ces_cd,1) = (select min(em_cd) from Entity_master Where Len(Ltrim(Rtrim(em_cd))) = 1) and 1", "1", true)) > 0) && _configuration["Commex"] != null && _configuration["Commex"] != string.Empty;

            return result.ToString();
        }

        public string getdoc(string cmbDocumentTypeValue, string cmbExchangeValue)
        {
            string getdoctype = string.Empty;
            if (cmbDocumentTypeValue == "Digital Ledger")
            {
                getdoctype = "LGR";
            }
            else if (cmbDocumentTypeValue == "Digital Security Ledger")
            {
                getdoctype = "SECLED";
            }
            else if (cmbDocumentTypeValue == "Digital Collateral Ledger")
            {
                getdoctype = "COLLED";
            }
            else if (cmbDocumentTypeValue == "Combine Contract")
            {
                getdoctype = cmbExchangeValue == "MCX" ? "CNOTE" : "CNOTE" + objUtility.GetCommExch(cmbExchangeValue);
                if (_configuration["IsTradeWeb"] == "O")
                {
                    if (cmbExchangeValue == "NCDEX")
                    {
                        getdoctype = (objUtility.GetSysParmStComm("CHGNCDEXCD", "") == "Y" ? "CNOTEF" : "CNOTEN");
                    }
                }
                if (cmbExchangeValue == "BSE/NSE Comm" || cmbExchangeValue == "BSE Comm" || cmbExchangeValue == "NSE Comm")
                {
                    getdoctype = "XNOTE";
                }
            }
            else if (cmbDocumentTypeValue == "Periodic Settlement")
            {
                getdoctype = "QTR";
            }
            else if (cmbDocumentTypeValue == "Combine Margin Statement")
            {
                getdoctype = "CMRG";
            }
            else
            {
                if (cmbExchangeValue == "BSE Cash")
                {
                    if (cmbDocumentTypeValue == "Contract")
                    {
                        getdoctype = "CBSE";
                    }
                    else if (cmbDocumentTypeValue == "Margin")
                    {
                        getdoctype = "MBC";
                    }
                    else if (cmbDocumentTypeValue == "STT")
                    {
                        getdoctype = "SCB";
                    }
                }
                else if (cmbExchangeValue == "NSE Cash")
                {
                    if (cmbDocumentTypeValue == "Contract")
                    {
                        getdoctype = "CNSE";
                    }
                    else if (cmbDocumentTypeValue == "Margin")
                    {
                        getdoctype = "MNC";
                    }
                    else if (cmbDocumentTypeValue == "STT")
                    {
                        getdoctype = "SCN";
                    }
                }
                else if (cmbExchangeValue == "MCX Cash")
                {
                    if (cmbDocumentTypeValue == "Contract")
                    {
                        getdoctype = "CMCX";
                    }
                    else if (cmbDocumentTypeValue == "Margin")
                    {
                        getdoctype = "MMC";
                    }
                }
                else if (cmbExchangeValue == "NSEDERV")
                {
                    if (cmbDocumentTypeValue == "Contract")
                    {
                        getdoctype = "FNSE";
                    }
                    else if (cmbDocumentTypeValue == "Margin")
                    {
                        getdoctype = "MNF";
                    }
                    else if (cmbDocumentTypeValue == "STT")
                    {
                        getdoctype = "SFN";
                    }
                    else if (cmbDocumentTypeValue == "Bill")
                    {
                        getdoctype = "FNSEB";
                    }
                }
                else if (cmbExchangeValue == "BSEDERV")
                {
                    if (cmbDocumentTypeValue == "Contract")
                    {
                        getdoctype = "FBSE";
                    }
                    else if (cmbDocumentTypeValue == "Margin")
                    {
                        getdoctype = "MBF";
                    }
                    else if (cmbDocumentTypeValue == "STT")
                    {
                        getdoctype = "SFB";
                    }
                    else if (cmbDocumentTypeValue == "Bill")
                    {
                        getdoctype = "FBSEB";
                    }
                }
                else if (cmbExchangeValue == "MCXDERV")
                {
                    if (cmbDocumentTypeValue == "Contract")
                    {
                        getdoctype = "FMCX";
                    }
                    else if (cmbDocumentTypeValue == "Margin")
                    {
                        getdoctype = "MMF";
                    }
                    else if (cmbDocumentTypeValue == "Bill")
                    {
                        getdoctype = "FMCXB";
                    }
                }
                else if (cmbExchangeValue == "BSEFX")
                {
                    if (cmbDocumentTypeValue == "Contract")
                    {
                        getdoctype = "BSEFX";
                    }
                    else if (cmbDocumentTypeValue == "Margin")
                    {
                        getdoctype = "MBFX";
                    }
                    else if (cmbDocumentTypeValue == "Bill")
                    {
                        getdoctype = "BSEFXB";
                    }
                }
                else if (cmbExchangeValue == "NSEFX")
                {
                    if (cmbDocumentTypeValue == "Contract")
                    {
                        getdoctype = "NSEFX";
                    }
                    else if (cmbDocumentTypeValue == "Margin")
                    {
                        getdoctype = "MNFX";
                    }
                    else if (cmbDocumentTypeValue == "STT")
                    {
                        getdoctype = "SFN";
                    }
                    else if (cmbDocumentTypeValue == "Bill")
                    {
                        getdoctype = "NSEFXB";
                    }
                }
                else if (cmbExchangeValue == "MCXFX")
                {
                    if (cmbDocumentTypeValue == "Contract")
                    {
                        getdoctype = "MCXFX";
                    }
                    else if (cmbDocumentTypeValue == "Margin")
                    {
                        getdoctype = "MMFX";
                    }
                    else if (cmbDocumentTypeValue == "STT")
                    {
                        getdoctype = "SFN";
                    }
                    else if (cmbDocumentTypeValue == "Bill")
                    {
                        getdoctype = "MCXFXB";
                    }
                }
                else if (cmbExchangeValue == "MCX")
                {
                    if (cmbDocumentTypeValue == "Contract")
                    {
                        getdoctype = "MCX";
                    }
                    else if (cmbDocumentTypeValue == "Margin")
                    {
                        getdoctype = "MARMCX";
                    }
                }
                else if (cmbExchangeValue == "NCDEX")
                {
                    if (cmbDocumentTypeValue == "Contract")
                    {
                        getdoctype = "NCDEX";
                    }
                    else if (cmbDocumentTypeValue == "Margin")
                    {
                        getdoctype = "MARNCX";
                    }
                }
                else if (cmbExchangeValue == "ICEX")
                {
                    if (cmbDocumentTypeValue == "Contract")
                    {
                        getdoctype = "MCX";
                    }
                    else if (cmbDocumentTypeValue == "Margin")
                    {
                        getdoctype = "MARMCX";
                    }
                }
                else if (cmbExchangeValue == "NCME")
                {
                    if (cmbDocumentTypeValue == "Contract")
                    {
                        getdoctype = "MCX";
                    }
                    else if (cmbDocumentTypeValue == "Margin")
                    {
                        getdoctype = "MARMCX";
                    }
                }
                else if (cmbExchangeValue == "NSEL")
                {
                    if (cmbDocumentTypeValue == "Contract")
                    {
                        getdoctype = "NSEL";
                    }
                    else if (cmbDocumentTypeValue == "Margin")
                    {
                        getdoctype = "MARNSEL";
                    }
                }
            }
            return getdoctype;
        }

        #endregion

        #endregion

        #region Marging Finance Handler method

        // Get Trades Data
        public dynamic GetTradesData(string cm_cd, string FromDate, string ToDate, string SelectedIndex)
        {
            var ds = GetQueryTradesData(cm_cd, FromDate, ToDate, SelectedIndex);
            try
            {
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
        // get Temp table RmsSummary Data for status module.
        public dynamic GetTempRMSSummaryData(string cm_cd, string strCompanyCode)
        {
            try
            {
                var ds = GetQueryTempRMSSummaryData(cm_cd, strCompanyCode);
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
        // Get fund data of status module
        public dynamic GetStatusFundData(string cm_cd, string strCompanyCode)
        {
            try
            {
                var ds = GetQueryStatusFundData(cm_cd, strCompanyCode);
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
        // Get collateral data of status module
        public dynamic GetStatusCollateralData(string cm_cd, string strCompanyCode)
        {
            try
            {
                var ds = GetQueryStatusCollateralData(cm_cd, strCompanyCode);
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
        //TODO : Get prSecurityListRpt Data
        public dynamic GetprSecurityListRptHandler(Boolean blnBSE, Boolean blnNSE)
        {
            var ds = GetQueryprSecurityListRptData(blnBSE, blnNSE);
            try
            {
                if (ds != null)
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

        public dynamic GetShortFallMainGridData(string cm_cd, int IntNtxtDays)
        {
            try
            {
                var ds = GetQueryShortFallMainGridData(cm_cd, IntNtxtDays);
                if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                {
                    var json = JsonConvert.SerializeObject(ds, Formatting.Indented);
                    return json;
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #region Margin Finance usefull method

        // Get Trades Data
        public DataSet GetQueryTradesData(string cm_cd, string FromDate, string ToDate, string SelectedIndex)
        {
            DataSet ds = new DataSet();
            if (Conversion.Val(FromDate) <= Conversion.Val(ToDate))
            {
                ds = BindGrid(cm_cd, FromDate, ToDate, SelectedIndex);
            }
            return ds;
        }
        public DataSet BindGrid(string cm_cd, string FromDate, string ToDate, string SelectedIndex)
        {
            DataSet ds = new DataSet();
            if (SelectedIndex == "0")
            {
                ds = ShowMasterGridItemWise(cm_cd, FromDate, ToDate);
            }
            else
            {
                ds = ShowMasterGridDateWise(cm_cd, FromDate, ToDate);
            }
            return ds;
        }
        // Get ItemWise Data
        public DataSet ShowMasterGridItemWise(string cm_cd, string FromDate, string ToDate)
        {
            string StrMTFTRXIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'MrgTdgFin_TRX' and b.name", "idx_MrgTdgFin_TRX_Clientcd", true)) == 1)
            { StrMTFTRXIndex = "index(idx_MrgTdgFin_TRX_clientcd),"; }

            DataSet ObjDataSet = new DataSet();
            if (_configuration["IsTradeWeb"] == "O")//Live DB
            {
                strsql = " select 1 Td_order,'' as td_ac_type,'' as td_trxdate,'' as td_isin_code,'' as sc_company_name, cast((case when sum(MTtd_bqty-MTtd_sqty)=0 then 0  ";
                strsql = strsql + " else sum((MTtd_bqty-Mttd_sqty)*MTtd_rate)/sum(MTtd_bqty-MTtd_sqty) end)as decimal(15,2) )   as rate,'MTF' Td_Type,'' as FScripNm,'' as FExDt,";
                strsql = strsql + " rtrim(MTtd_scripcd)td_scripnm , rtrim(ss_name) snm, sum(MTtd_bqty) Bqty, convert(decimal(15,2), sum(MTtd_bqty*MTtd_rate)) BAmt,  sum(MTtd_sqty) ";
                strsql = strsql + " Sqty, convert(decimal(15,2), sum(MTtd_sqty*MTtd_rate)) SAmt, sum(MTtd_bqty-MTtd_sqty) NQty, convert(decimal(15,2), sum((MTtd_bqty-MTtd_sqty)*";
                strsql = strsql + "  MTtd_rate)) NAmt,  '' as td_debit_credit,0 as sm_strikeprice,'' as sm_callput, 'MTF|' + MTtd_scripcd LinkCode ";
                strsql = strsql + "  from MrgTdgFin_TRX(" + StrMTFTRXIndex + "nolock), securities with(nolock) ";
                strsql = strsql + " where MTtd_clientcd='" + cm_cd + "' and MTtd_dt between '" + FromDate + "' and '" + ToDate + "' ";
                strsql = strsql + " and MTtd_Scripcd = ss_cd";
                strsql = strsql + " group by MTtd_scripcd, ss_name ";
            }
            else//TradeWeb-pragya
            {
                strsql = " select 1 Td_order,'' as td_ac_type,'' as td_trxdate,'' as td_isin_code,'' as sc_company_name, cast((case when sum(MTtd_bqty-MTtd_sqty)=0 then 0  ";
                strsql = strsql + " else sum((MTtd_bqty-Mttd_sqty)*MTtd_rate)/sum(MTtd_bqty-MTtd_sqty) end)as decimal(15,2) )   as rate,'MTF' Td_Type,'' as FScripNm,'' as FExDt,";
                strsql = strsql + " rtrim(MTtd_scripcd)td_scripnm , rtrim(ss_name) snm, sum(MTtd_bqty) Bqty, convert(decimal(15,2), sum(MTtd_bqty*MTtd_rate)) BAmt,  sum(MTtd_sqty) ";
                strsql = strsql + " Sqty, convert(decimal(15,2), sum(MTtd_sqty*MTtd_rate)) SAmt, sum(MTtd_bqty-MTtd_sqty) NQty, convert(decimal(15,2), sum((MTtd_bqty-MTtd_sqty)*";
                strsql = strsql + "  MTtd_rate)) NAmt,  '' as td_debit_credit,0 as sm_strikeprice,'' as sm_callput, 'MTF|' + MTtd_scripcd LinkCode ";
                strsql = strsql + "  from MrgTdgFin_TRX(" + StrMTFTRXIndex + "nolock), TPSecurities with(nolock) ";
                strsql = strsql + " where MTtd_clientcd='" + cm_cd + "' and MTtd_dt between '" + FromDate + "' and '" + ToDate + "' ";
                strsql = strsql + " and MTtd_Scripcd = ss_cd";
                strsql = strsql + " group by MTtd_scripcd, ss_name ";
            }

            ObjDataSet = objUtility.OpenDataSet(strsql);
            return ObjDataSet;
        }
        // Get DateWise Data
        public DataSet ShowMasterGridDateWise(string cm_cd, string FromDate, string ToDate)
        {
            string StrMTFTRXIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'MrgTdgFin_TRX' and b.name", "idx_MrgTdgFin_TRX_Clientcd", true)) == 1)
            { StrMTFTRXIndex = "index(idx_MrgTdgFin_TRX_clientcd),"; }

            char[] ArrSeparators = new char[1];
            ArrSeparators[0] = '/';
            DataSet ObjDataSet = new DataSet();
            strsql = "select 1 Td_order,'MTF' Td_Type,ltrim(rtrim(convert(char,convert(datetime,MTtd_dt),103))) Dt, rtrim(MTtd_Stlmnt) MTtd_Stlmnt, ";
            strsql = strsql + "sum(MTtd_bqty) Bqty, convert(decimal(15,2),sum(MTtd_bqty*MTtd_rate)) BAmt, sum(MTtd_sqty) Sqty, convert(decimal(15,2),sum(MTtd_sqty*MTtd_rate)) SAmt, ";
            strsql = strsql + "sum(MTtd_bqty-MTtd_sqty) NQty, ";
            strsql = strsql + "convert(decimal(15,2),sum((MTtd_bqty-MTtd_sqty)*MTtd_rate)) NAmt, MTtd_dt Dt1, 'MTF|'+ MTtd_Stlmnt  LinkCode from MrgTdgFin_TRX with (" + StrMTFTRXIndex + "nolock) where MTtd_clientcd ='" + cm_cd + "' and ";
            strsql = strsql + "MTtd_dt between '" + FromDate + "' and '" + ToDate + "' group by  MTtd_Stlmnt,MTtd_dt  ";


            ObjDataSet = objUtility.OpenDataSet(strsql);
            return ObjDataSet;
        }

        // Get TempRMSS Summary Data
        public DataSet GetQueryTempRMSSummaryData(string cm_cd, string strCompanyCode)
        {
            DataSet DsRMS = new DataSet();
            string strSql = "";

            SqlConnection ObjConnectionTmp;
            using (var db = new DataContext())
            {
                ObjConnectionTmp = new SqlConnection((db.Database.GetDbConnection()).ConnectionString);
                ObjConnectionTmp.Open();

                CreateTempTable(cm_cd, ObjConnectionTmp, strCompanyCode);

                strSql = "select  " + UtilityCommon.strTempRMSSummary + ".*,cm_name from " + UtilityCommon.strTempRMSSummary + ", Client_master WHERE cm_cd= TMP_CLIENTCD and TMP_CLIENTCD='" + cm_cd + "'";
                strSql += " order by  cm_name";

                DsRMS = objUtility.OpenDataSetTmp(strSql, ObjConnectionTmp);
            }
            return DsRMS;
        }
        //Create temp table for process data
        private void CreateTempTable(string cm_cd, SqlConnection con, string strCompanyCode)
        {
            string StrAsOnDt = objUtility.dtos(DateTime.Today.ToString("dd/MM/yyyy"));
            if (Conversion.Val("20201101") < Conversion.Val(objUtility.dtos(DateTime.Today.ToString("dd/MM/yyyy"))))
                objUtility.prPledgeProcess(StrAsOnDt, "", " and cm_cd = '" + cm_cd + "'", false, false, false, con, strCompanyCode);
            else
                objUtility.prProcess(StrAsOnDt, "", " and cm_cd = '" + cm_cd + "'", false, false, false, con);
        }

        // Get fund data of status module
        public DataSet GetQueryStatusFundData(string cm_cd, string strCompanyCode)
        {
            string strSql = "";
            DataSet DsFunded = new DataSet();
            SqlConnection ObjConnectionTmp;
            using (var db = new DataContext())
            {
                ObjConnectionTmp = new SqlConnection((db.Database.GetDbConnection()).ConnectionString);
                ObjConnectionTmp.Open();

                CreateTempTable(cm_cd, ObjConnectionTmp, strCompanyCode);

                strSql = "select Tmp_Scripcd,ss_name, SUM(Tmp_Qty) Qty,";
                strSql += " cast(SUM(Tmp_Value) as decimal(15,2))  ActualCost,cast(SUM(Tmp_Qty * Tmp_MarketRate ) as decimal(15,2)) ClosePrice,cast(((SUM(Tmp_Qty * Tmp_MarketRate ))-(SUM(Tmp_Value))) as decimal(15,2)) Value, ";
                strSql += " cast(SUM(Tmp_Qty * Tmp_MarketRate ) - SUM(Tmp_Value) as decimal(15,2)) MTM,cast(SUM(Tmp_NetValue) as decimal(15,2)) MarginReq, cast(Tmp_MrgHairCut as decimal(15,2)) Tmp_MrgHairCut ,";
                strSql += " case when Tmp_Exchange='B' then 'BSE' when Tmp_Exchange='N' then 'NSE' else '' end Exch from " + UtilityCommon.strTempRMSDetail + ",Client_master,securities ";
                strSql += " where cm_cd=Tmp_Clientcd and Tmp_Scripcd=ss_cd and Tmp_Type='M' ";
                strSql += " and Tmp_Clientcd= '" + cm_cd + "'";
                strSql += " Group By  Tmp_Exchange,Tmp_Clientcd,cm_name,Tmp_MrgHairCut,Tmp_Scripcd,ss_name,Tmp_MrgHairCut";
                strSql += " Order By ss_name,Tmp_Clientcd";
                DsFunded = objUtility.OpenDataSetTmp(strSql, ObjConnectionTmp);
            }
            return DsFunded;
        }

        // Get collateral data of status module
        public DataSet GetQueryStatusCollateralData(string cm_cd, string strCompanyCode)
        {
            string strSql = "";
            DataSet DsCollateral = new DataSet();
            SqlConnection ObjConnectionTmp;
            using (var db = new DataContext())
            {
                ObjConnectionTmp = new SqlConnection((db.Database.GetDbConnection()).ConnectionString);
                ObjConnectionTmp.Open();

                CreateTempTable(cm_cd, ObjConnectionTmp, strCompanyCode);

                strSql = "select Tmp_Scripcd,ss_name, SUM(Tmp_Qty) Qty,";
                strSql += " Tmp_MarketRate as Rate, cast(sum(Tmp_Value) as decimal(15,2))  Value, cast(Tmp_MrgHairCut as decimal(15,2)) HairCut,";
                strSql += "  cast(sum(Tmp_NetValue) as decimal(15,2)) NetValue,case when Tmp_Exchange='B' then 'BSE' when Tmp_Exchange='N' then 'NSE' else '' end Exch ";
                strSql += " from " + UtilityCommon.strTempRMSDetail + ",Client_master,securities ";
                strSql += " where cm_cd=Tmp_Clientcd and Tmp_Scripcd=ss_cd and Tmp_Type='C' ";
                strSql += " and Tmp_Clientcd= '" + cm_cd + "'";
                strSql += " Group By  Tmp_Exchange,Tmp_Clientcd,cm_name,Tmp_MrgHairCut,Tmp_MarketRate,Tmp_Scripcd,ss_name";
                strSql += " Order By ss_name,Tmp_Clientcd";

                DsCollateral = objUtility.OpenDataSetTmp(strSql, ObjConnectionTmp);
            }
            return DsCollateral;
        }

        // Get Status Approved Security Data
        public DataSet GetQueryprSecurityListRptData(Boolean blnBSE, Boolean blnNSE)
        {
            return objUtility.prSecurityListRpt(DateTime.Today.ToString("yyyyMMdd"), blnBSE, blnNSE);
        }

        //Get margin trading finance shortfall main grid data
        public DataSet GetQueryShortFallMainGridData(string cm_cd, int IntNtxtDays)
        {
            DataSet dsMainDataSet = new DataSet();
            string strdate = DateTime.Today.ToString("dd/MM/yyyy").Replace("-", "/");
            string strSql = "";
            SqlConnection con;
            using (var db = new DataContext())
            {
                con = new SqlConnection((db.Database.GetDbConnection()).ConnectionString);
                con.Open();
                DataSet dsDates = new DataSet();
                Temptable(con);

                for (int i = 0; i < IntNtxtDays; i++)
                {
                AgainT2:
                    strSql = "select * from Tholiday_master where  ";

                    if (objUtility.GetSysParmSt("MTFP_LICBSE", "") == "Y")
                        strSql += "  hm_exchange in ('B')";
                    else if (objUtility.GetSysParmSt("MTFP_LICNSE", "") == "Y")
                        strSql += "  hm_exchange in ('N')";
                    else if (objUtility.GetSysParmSt("MTFP_LICBSE", "") == "Y" && objUtility.GetSysParmSt("MTFP_LICNSE", "") == "Y")
                        strSql += " hm_exchange in ('B','N')";

                    strSql += " and hm_Segment = 'C' and hm_dt = '" + objUtility.dtos(strdate) + "'";
                    dsDates = objUtility.OpenDataSet(strSql);
                    //var abc = objUtility.OpenDataSet(strSql).Tables[0];

                    if (dsDates.Tables[0].Rows.Count > 0)
                    {
                        strdate = ConvertDT(objUtility.dtos(strdate)).AddDays(-1).ToString();
                        goto AgainT2;
                    }
                    else
                    {
                        objUtility.prProcess(objUtility.dtos(strdate), "", " and cm_cd = '" + cm_cd + "'", false, false, false, con);
                        strSql = " insert into #TmpDebitClient ";
                        strSql += "select Tmp_Clientcd,'" + DateTime.Today.ToString("dd/MM/yyyy") + "',Tmp_Limit,Tmp_TplusBal,Tmp_LoanBal,Tmp_FundedAmount,Tmp_FundedMrgReq,Tmp_CollateralFund,Tmp_CollateralValue,Tmp_ShortFallExcess,Tmp_TradeValue,Tmp_M2MLoss";
                        strSql += " From " + UtilityCommon.strTempRMSSummary + ", Client_master WHERE cm_cd= TMP_CLIENTCD order by cm_name";
                        objUtility.ExecuteSQLTmp(strSql, con);
                        if (strdate == DateTime.Today.ToString("dd/MM/yyyy").Replace("-", "/"))
                        {
                            strSql = " insert into #TmpDebitClntDetail ";
                            strSql += " select Tmp_Type,'" + objUtility.dtos(strdate) + "',Tmp_Exchange,Tmp_Clientcd,Tmp_Scripcd,Tmp_RegForFO,Tmp_Qty,Tmp_Rate,Tmp_MarketRate,Tmp_Value,Tmp_MrgHairCut,Tmp_NetValue";
                            strSql += " From " + UtilityCommon.strTempRMSDetail + ", Client_master WHERE cm_cd= Tmp_Clientcd and Tmp_Type = 'M' order by cm_name";
                            objUtility.ExecuteSQLTmp(strSql, con);
                        }
                    }
                    strdate = ConvertDT(objUtility.dtos(strdate)).AddDays(-1).ToString();


                    strSql = " delete from #TmpDebitClient where Td_Clientcd in ( Select Td_Clientcd from #TmpDebitClient where Td_ShortFallExcess >= 0  )  ";
                    objUtility.ExecuteSQLTmp(strSql, con);
                    strSql = " delete from #TmpDebitClntDetail where Tcd_Clientcd not in ( Select Distinct Td_Clientcd from #TmpDebitClient )  ";
                    objUtility.ExecuteSQLTmp(strSql, con);

                    strSql = "select #TmpDebitClient.*,cm_name,bm_email,cm_email,bm_branchcd,cm_panno,cm_pincode, cm_dob";
                    strSql += " from #TmpDebitClient, Client_master ,Branch_master";
                    strSql += " WHERE cm_cd= Td_Clientcd and cm_brboffcode = bm_branchcd and Td_dt = '" + DateTime.Today.ToString("dd/MM/yyyy") + "'";

                    //DataSet DsDrClient = new DataSet();
                    //DataSet dsDetails = new DataSet();
                    //DsDrClient = objUtility.OpenDataSetTmp(strSql, con);

                    //var abc = DsDrClient.Tables[0];
                    dsMainDataSet.Tables.Add(objUtility.OpenNewDataTableTmp(strSql, con));

                    strSql = "select (case when Tcd_Exchange='D' then 'BSE' when Tcd_Exchange='F' then'NSE' when Tcd_Exchange='M' then'MCX' else Tcd_Exchange +'SE' end) Tcd_Exchange,Tcd_Scripcd,ss_name, SUM(Tcd_Qty) Qty,  cast(SUM(Tcd_Value) as decimal(15,2))  ActualCost,";
                    strSql += "  cast(SUM(Tcd_Qty * Tcd_MarketRate)  as decimal(15,2)) ClosePrice, cast(((SUM(Tcd_Qty * Tcd_MarketRate))-(SUM(Tcd_Value))) as decimal(15,2)) MTM, cast(SUM(Tcd_NetValue) as decimal(15,2)) MarginReq,cast(Tcd_MrgHairCut as decimal(15,2)) Tcd_MrgHairCut ,0.00 MTM  ,Tcd_Clientcd";
                    strSql += " from #TmpDebitClntDetail,Client_master,securities";
                    strSql += " where cm_cd=Tcd_Clientcd and Tcd_Scripcd=ss_cd ";
                    strSql += " Group By Tcd_Exchange,Tcd_Clientcd,cm_name,Tcd_MrgHairCut,Tcd_Scripcd,ss_name ";
                    strSql += " Order By ss_name,Tcd_Clientcd";
                    //dsDetails = objUtility.OpenDataSetTmp(strSql, con);
                    dsMainDataSet.Tables.Add(objUtility.OpenNewDataTableTmp(strSql, con));
                }
            }
            return dsMainDataSet;
        }


        //Method for convert date
        public DateTime ConvertDT(string Date)
        {
            int Year = int.Parse(Date.Substring(0, 4));
            int Month = int.Parse(Date.Substring(4, 2));
            int Day = int.Parse(Date.Substring(6, 2));
            return new DateTime(Year, Month, Day);
        }

        //Create temp table for ShortFall Webform
        protected void Temptable(SqlConnection con)
        {
            string strSql = "";
            try
            {
                strSql = " Drop Table #TmpDebitClient";
                objUtility.ExecuteSQLTmp(strSql, con);

                strSql = "Create Table #TmpDebitClient (";
                strSql += " Td_Clientcd VarChar(8),";
                strSql += " Td_dt VarChar(10),";
                strSql += " Td_Limit Money,";
                strSql += " Td_TplusBal Money,";
                strSql += " Td_LoanBal Money,";
                strSql += " Td_FundedAmount Money,";
                strSql += " Td_FundedMrgReq Money,";
                strSql += " Td_CollateralFund Money,";
                strSql += " Td_CollateralValue Money,";
                strSql += " Td_ShortFallExcess Money,";
                strSql += " Td_TradeValue Money,";
                strSql += " Td_M2MLoss Money)";
                objUtility.ExecuteSQLTmp(strSql, con);
            }
            catch (Exception ex)
            {
                strSql = "Create Table #TmpDebitClient (";
                strSql += " Td_Clientcd VarChar(8),";
                strSql += " Td_dt VarChar(10),";
                strSql += " Td_Limit Money,";
                strSql += " Td_TplusBal Money,";
                strSql += " Td_LoanBal Money,";
                strSql += " Td_FundedAmount Money,";
                strSql += " Td_FundedMrgReq Money,";
                strSql += " Td_CollateralFund Money,";
                strSql += " Td_CollateralValue Money,";
                strSql += " Td_ShortFallExcess Money,";
                strSql += " Td_TradeValue Money,";
                strSql += " Td_M2MLoss Money)";
                objUtility.ExecuteSQLTmp(strSql, con);
            }
            try
            {
                strSql = "Drop Table #TmpDebitClntDetail";
                objUtility.ExecuteSQLTmp(strSql, con);

                strSql = "Create Table #TmpDebitClntDetail (";
                strSql += " Tcd_Type Char(1),";
                strSql += " Tcd_dt VarChar(10),";
                strSql += " Tcd_Exchange Char(1),";
                strSql += " Tcd_Clientcd VarChar(8),";
                strSql += " Tcd_Scripcd VarChar(6),";
                strSql += " Tcd_RegForFO VarChar(1),";
                strSql += " Tcd_Qty Numeric,";
                strSql += " Tcd_Rate Money,";
                strSql += " Tcd_MarketRate Money,";
                strSql += " Tcd_Value Money,";
                strSql += " Tcd_MrgHairCut Money,";
                strSql += " Tcd_NetValue Money)";
                objUtility.ExecuteSQLTmp(strSql, con);
            }
            catch (Exception ex)
            {
                strSql = "Create Table #TmpDebitClntDetail (";
                strSql += " Tcd_Type Char(1),";
                strSql += " Tcd_dt VarChar(10),";
                strSql += " Tcd_Exchange Char(1),";
                strSql += " Tcd_Clientcd VarChar(8),";
                strSql += " Tcd_Scripcd VarChar(6),";
                strSql += " Tcd_RegForFO VarChar(1),";
                strSql += " Tcd_Qty Numeric,";
                strSql += " Tcd_Rate Money,";
                strSql += " Tcd_MarketRate Money,";
                strSql += " Tcd_Value Money,";
                strSql += " Tcd_MrgHairCut Money,";
                strSql += " Tcd_NetValue Money)";
                objUtility.ExecuteSQLTmp(strSql, con);
            }
        }
        #endregion
        #endregion

        #endregion

        #region new family handler

        //For getting Outstanding details data
        public dynamic Family_List(string userId)
        {
            try
            {
                string strSql = "select upper(case when CF_CD=CF_FamilyCd then 'MAIN' else '' end) MainCd,upper(cf_cd) cf_cd ,cm_name";
                strSql += " from Client_Family,Client_master where cm_cd=CF_CD and CF_FamilyCd='" + userId + "'  order by MainCd desc ";

                var ds = CommonRepository.FillDataset(strSql);
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

        public dynamic Family_Add(string userId, string password, string UCC_Code)
        {
            string strHostAdd = Dns.GetHostName();

            string apiRes = objUtility.fnFireQueryTradeWeb("Client_Family", "count(*)", "Cf_cd", UCC_Code, true);

            if (Conversion.Val(apiRes) > 0)
            {
                return "( " + UCC_Code.Trim() + " ) Already Assign To Family";
            }

            if (objUtility.GetWebParameter("PANASPASSWORD") == "Y")
                strsql = " select cm_pwd,cm_Panno from Client_master with (nolock) where cm_cd='" + UCC_Code.Trim() + "'";
            else
                strsql = " select cm_pwd,'' cm_Panno from Client_master with (nolock) where cm_cd='" + UCC_Code.Trim() + "'";

            DataSet ObjPwd = objUtility.OpenDataSet(strsql);//apiObj.CommonOpenDataSet(strsql);

            if (ObjPwd.Tables[0].Rows.Count > 0)
            {
                if (ObjPwd.Tables[0].Rows[0]["cm_pwd"].ToString().Trim() != password.Trim())
                {
                    if (ObjPwd.Tables[0].Rows[0]["cm_Panno"].ToString().Trim() == "")
                    {
                        return "'Invalid Password!'";
                    }
                    if (ObjPwd.Tables[0].Rows[0]["cm_Panno"].ToString().Trim() != password.Trim())
                    {
                        return "'Invalid Password!'";
                    }
                }
            }

            string apiRes1 = objUtility.fnFireQueryTradeWeb("Client_Family", "count(*)", "Cf_cd", userId, true);

            if (Conversion.Val(apiRes1) == 0)
            {
                strsql = "insert into Client_Family values ('" + userId + "','" + userId + "',convert(char(8),getdate(),112),convert(char(8),getdate(),108),'" + strHostAdd + "')";
                objUtility.ExecuteSQL(strsql);
            }
            DataSet dsStatus = objUtility.OpenDataSet("select cf_familyCd from Client_Family where cf_cd='" + userId + "'");

            string StrClientCd = userId;
            if (UCC_Code.ToUpper().Trim() != StrClientCd.ToUpper().Trim())
            {
                strsql = "insert into Client_Family values ('" + UCC_Code.Trim() + "','" + dsStatus.Tables[0].Rows[0]["cf_familyCd"].ToString() + "',convert(char(8),getdate(),112),convert(char(8),getdate(),108),'" + strHostAdd + "')";
                objUtility.ExecuteSQL(strsql);
            }
            return "Success";
        }

        public dynamic Family_Remove(string UCC_Code)
        {
            try
            {
                var result = FamilyRemoveQuery(UCC_Code);
                return JsonConvert.SerializeObject(result); ;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public dynamic Family_Balance(List<string> UCC_Codes)
        {
            try
            {
                var ds = FamilyBalanceQuery(UCC_Codes);
                if (ds != null)
                {
                    return ds;
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public dynamic Family_RetainedStoke(List<string> UCC_Codes)
        {
            try
            {
                var ds = FamilyRetainedStokeQuery(UCC_Codes);
                if (ds != null)
                {
                    if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                    {
                        return JsonConvert.SerializeObject(ds.Tables[0], Formatting.Indented);
                    }
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public dynamic Family_Holding(List<string> UCC_Codes)
        {
            try
            {
                var ds = FamilyHoldingQuery(UCC_Codes);
                if (ds != null)
                {
                    if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                    {
                        return JsonConvert.SerializeObject(ds.Tables[0], Formatting.Indented);
                    }
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public dynamic Family_Position(List<string> UCC_Codes)
        {
            try
            {
                var ds = FamilyPositionQuery(UCC_Codes);
                if (ds != null)
                {
                    if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                    {
                        return JsonConvert.SerializeObject(ds.Tables[0], Formatting.Indented);
                    }
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public dynamic Family_Transaction(FamilyTransactionModel model)
        {
            try
            {
                var ds = FamilyTransactionQuery(model);
                if (ds != null)
                {
                    if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                    {
                        return JsonConvert.SerializeObject(ds.Tables[0], Formatting.Indented);
                    }
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public dynamic Family_Transaction_Details(string client, string type, string fromDate, string toDate)
        {
            try
            {
                var ds = FamilyTransactionDetailQuery(client, type, fromDate, toDate);
                if (ds != null)
                {
                    if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                    {
                        return JsonConvert.SerializeObject(ds.Tables[0], Formatting.Indented);
                    }
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public dynamic Family_RetainedStokeJson(List<string> UCC_Codes)
        {
            try
            {
                var result = FamilyRetainedStokeJson(UCC_Codes);
                if (result != null)
                {
                    return result;
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public dynamic Family_HoldingJson(List<string> UCC_Codes)
        {
            try
            {
                var result = FamilyHoldingJson(UCC_Codes);
                if (result != null)
                {
                    return result;
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public dynamic Family_TransactionJson(FamilyTransactionModel model)
        {
            try
            {
                var result = FamilyTransactionJson(model);
                if (result != null)
                {
                    return result;
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public dynamic Family_TransactionDetailJson(string Client, string Type, string FromDate, string ToDate)
        {
            try
            {
                var result = FamilyTransactionDetailJson(Client, Type, FromDate, ToDate);
                if (result != null)
                {
                    return result;
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #region family query

        public string FamilyRemoveQuery(string UCC_Code)
        {
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("Client_Family", "COUNT(0)", " CF_CD=CF_FamilyCd and CF_CD ", UCC_Code, true)) == 1)
                strsql = "delete from Client_Family where CF_FamilyCd ='" + UCC_Code + "' ";
            else
                strsql = "delete from Client_Family where CF_CD ='" + UCC_Code + "' ";

            objUtility.ExecuteSQL(strsql);
            return "Success";
        }

        public List<FamilyBalanceResponse> FamilyBalanceQuery(List<string> UCC_Codes)
        {
            string SelectedCLCode = "";
            string uccName = "";
            foreach (var uccCode in UCC_Codes)
            {
                uccName = objUtility.fnFireQueryTradeWeb("client_master", "cm_name", "cm_cd", uccCode, true);

                if (!String.IsNullOrEmpty(uccName))
                {
                    SelectedCLCode = SelectedCLCode + uccCode.Trim().ToUpper() + "~" + uccName.Trim() + "/";
                }
            }

            int CurrentYear = DateTime.Now.Year;
            if (DateTime.Today.Month < 3)
            {
                CurrentYear = CurrentYear - 1;
            }
            string Fromdt = objUtility.dtos(new DateTime(CurrentYear, 4, 1).ToString());
            string Todt = objUtility.dtos(new DateTime(CurrentYear + 1, 3, 31).ToString());
            string StrDataFields = "";
            string StrHeaderTitles = "";
            string StrSubTotalsFor = "";
            string StrTextAlign = "";
            string StrTextLength = "";
            string Strcolhide = "";
            string StrCDC = "";
            string StrAccC = "";
            string StrCDM = "";
            string StrAccM = "";
            string StrCDL = "";
            string StrAccL = "";
            char[] ArrSeparters = new char[1];
            ArrSeparters[0] = '/';
            string[] StrClCd;
            StrClCd = Convert.ToString(SelectedCLCode).Split(ArrSeparters);
            int i = 0;
            for (i = 0; i < StrClCd.Length - 1; i++)
            {
                string StrName = Strings.Left(StrClCd[i].Trim().Split('~')[1].Trim(), 15);

                StrAccC += " cast(sum(Case When ld_clientcd = '" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then ld_amount else 0 end) as decimal(15,2)) as  '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_" + StrName.Trim() + "',";
                StrCDC += "'" + StrClCd[i].Trim().Split('~')[0].Trim() + "',";
                StrAccL += " cast(sum(Case When ld_clientcd = '" + StrClCd[i].Trim().Split('~')[0].Trim() + objUtility.GetSysParmSt("MTFP_SUFFIX", "") + "' then ld_amount else 0 end) as decimal(15,2)) as  '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_" + StrName.Trim() + "',";
                StrCDL += "'" + StrClCd[i].Trim().Split('~')[0].Trim() + objUtility.GetSysParmSt("MTFP_SUFFIX", "") + "',";

                string StrMrg = objUtility.fnFireQueryTradeWeb("client_master", "distinct cm_brkggroup", "cm_cd", StrClCd[i].Trim().Split('~')[0].Trim(), true).Trim();
                StrAccM += (StrMrg.Trim() == "" ? "0" : "cast(sum(Case When ld_clientcd = '" + StrMrg.Trim() + "' then ld_amount else 0 end) as decimal(15,2))") + " as  '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_" + StrName.Trim() + "',";
                StrCDM += "'" + StrMrg.Trim() + "',";
                StrDataFields += StrClCd[i].Trim().Split('~')[0].Trim() + "_" + StrName.Trim() + ",";
                StrHeaderTitles += StrClCd[i].Trim().Split('~')[0].Trim() + "_" + StrName.Trim() + ",";
                StrSubTotalsFor += StrClCd[i].Trim().Split('~')[0].Trim() + "_" + StrName.Trim() + ",";
                StrTextAlign += "R,";
                StrTextLength += "15,";
                Strcolhide += i + 1 + ",";
            }
            Strcolhide += i + 1;
            StrCDC = Strings.Left(StrCDC, StrCDC.Length - 1);
            StrCDL = Strings.Left(StrCDL, StrCDL.Length - 1);
            StrCDM = Strings.Left(StrCDM, StrCDM.Length - 1);


            strsql = "select 'C' as account, ";
            strsql += "case substring(ld_dpid,2,1)   when 'B' then 'BSE-' when 'N' then 'NSE-' when 'M' then 'MCX-' when 'F' then 'NCDEX-' else '' end +  ";
            strsql += " case substring(ld_dpid,3,1)   when 'C' then 'CASH'  when 'F' then 'DERIVATIVE'  when 'K' then 'FX' when 'M' then 'MF' when 'X' then 'COMM' else '' end  as heading,";
            strsql += StrAccC;
            strsql += " cast(sum(ld_amount) as decimal(15,2)) as  'Total' ";
            strsql += " from Ledger ";
            strsql += " Where ld_clientcd in (" + StrCDC + " )";
            strsql += " Group By ld_dpid";

            strsql += " Union all select 'M' as account, ";
            strsql += " case substring(ld_dpid,2,1)   when 'B' then 'BSE-' when 'N' then 'NSE-' when 'M' then 'MCX-' when 'F' then 'NCDEX-' else '' end + ";
            strsql += " case substring(ld_dpid,3,1)   when 'C' then 'CASH'  when 'F' then 'DERIVATIVE'  when 'K' then 'FX'  when 'X' then 'COMM' else '' end + '(M)',";
            strsql += StrAccM;
            strsql += " cast(sum(ld_amount) as decimal(15,2)) as  'Total' ";
            strsql += " from Ledger ";
            strsql += " Where ld_clientcd in (" + StrCDM + " )";
            strsql += " Group By ld_dpid ";

            //MTF ledger
            if (Convert.ToInt32(objUtility.fnFireQueryTradeWeb("sysobjects", "count(*)", "name", "MrgTdgFin_TRX", true)) > 0)
            {
                strsql += " union all select 'L' as account, case substring(ld_dpid,2,1)   when 'B' then 'BSE-' when 'N' then 'NSE-' when 'M' then 'MCX-' else '' end + 'MTF' ,";
                strsql += StrAccL;
                strsql += " cast(sum(ld_amount) as decimal(15,2)) as  'Total' ";
                strsql += " from ledger with (nolock),MrgTdgFin_Clients ";
                strsql += " where ld_clientcd in (" + StrCDL + " ) and ld_dt <= '20210331' ";
                strsql += "  and ld_clientcd =  Rtrim(MTFC_CMCD) + '" + objUtility.GetSysParmSt("MTFP_SUFFIX", "") + "' group by ld_dpid";
            }

            // NBFC
            if (Convert.ToInt32(objUtility.fnFireQueryTradeWeb("sysobjects", "count(*)", "name", "nbfc_clients", true)) > 0)
            {
                strsql += " union all ";
                strsql += "select 'N' as account, 'NBFC',";
                strsql += StrAccC;
                strsql += " cast(sum(ld_amount) as decimal(15,2)) as  'Total' ";
                strsql += " from NBFC_Ledger with (nolock) where ld_clientcd in (" + StrCDC + " ) and ld_dt <= '" + Todt + "' group by ld_dpid ";
            }
            if (_configuration["Commex"] != null && _configuration["Commex"] != string.Empty)
            {
                string StrCommexConn = "";
                StrCommexConn = objUtility.GetCommexConnection();
                strsql += " union all select 'CM' as account, ";
                strsql += " case substring(ld_dpid,2,1) when 'M' then 'MCX-COMM' when 'N' then 'NCDEX-COMM' when 'S' then 'NSEL-COMM' when 'D' then 'NSX-COMM' end as heading ,";
                strsql += StrAccC;
                strsql += " cast(sum(ld_amount) as decimal(15,2)) as  'Total' ";
                strsql += " from   " + StrCommexConn + ".ledger";
                strsql += " where ld_clientcd in (" + StrCDC + " ) and ld_dt <= '" + Todt + "' ";
                strsql += " group by ld_dpid ";

                strsql += " order by account";
            }
            DataSet ObjDataSet = new DataSet();
            ObjDataSet = objUtility.OpenDataSet(strsql);

            List<FamilyBalanceResponse> familyBalanceResponses = new List<FamilyBalanceResponse>();

            ArrSeparters[0] = '_';
            string[] code;

            foreach (DataColumn column in ObjDataSet.Tables[0].Columns)
            {

                if (column.ColumnName != "account" && column.ColumnName != "heading" && column.ColumnName != "Total")
                {
                    FamilyBalanceResponse familyBalance = new FamilyBalanceResponse();
                    familyBalance.Name = column.ColumnName;
                    code = column.ColumnName.Split(ArrSeparters);
                    familyBalance.Code = code[0];
                    familyBalance.ExchSegBalances = new List<ExchSegBalance>();
                    for (i = 0; i < ObjDataSet.Tables[0].Rows.Count; i++)
                    {
                        if (familyBalance.ExchSegBalances != null)
                        {

                            //var abc = dr.ItemArray[].ToString();
                            familyBalance.ExchSegBalances.Add(new ExchSegBalance
                            {

                                ExchSeg = ObjDataSet.Tables[0].Rows[i]["heading"].ToString(),
                                Balance = ObjDataSet.Tables[0].Rows[i][column.ColumnName].ToString(),
                            });
                        }
                        else
                        {
                            familyBalance.ExchSegBalances.Add(new ExchSegBalance
                            {
                                ExchSeg = ObjDataSet.Tables[0].Rows[i]["heading"].ToString(),
                                Balance = ObjDataSet.Tables[0].Rows[i][column.ColumnName].ToString(),
                            });
                        }
                    }

                    familyBalanceResponses.Add(familyBalance);
                }
            }

            return familyBalanceResponses;
        }

        #region transaction
        public DataSet FamilyTransactionQuery(FamilyTransactionModel model)
        {
            string SelectedCLCode = "";
            string uccName = "";
            foreach (var uccCode in model.UCC_Codes)
            {
                uccName = objUtility.fnFireQueryTradeWeb("client_master", "cm_name", "cm_cd", uccCode, true);

                if (!String.IsNullOrEmpty(uccName))
                {
                    SelectedCLCode = SelectedCLCode + uccCode.Trim().ToUpper() + "~" + uccName.Trim() + "/";
                }
            }

            if (model.SelectedValue == "Trades")
            {
                return ShowFamilyTransaction(SelectedCLCode, model.FromDate, model.ToDate);
            }

            if (model.SelectedValue == "Journals" || model.SelectedValue == "Receipts/Payments")
            {
                return ShowFamilyTrxReceipts(SelectedCLCode, model.SelectedValue, model.FromDate, model.ToDate);
            }

            return null;
        }

        public DataSet ShowFamilyTransaction(string SelectedCLCode, string FromDate, string ToDate)
        {
            string StrSelect = "";
            string StrCDC = "";
            char[] ArrSeparters = new char[1];
            ArrSeparters[0] = '/';
            string[] StrClCd;
            string StrDataFields = "";
            string StrHeaderTitles = "";
            //string StrSubTotalsFor = "";
            string StrTextAlign = "";
            string StrTextLength = "";
            string Strcolhide = "";
            string StrColWidth = "";
            StrClCd = Convert.ToString(SelectedCLCode).Split(ArrSeparters);
            int i = 0;
            for (i = 0; i < StrClCd.Length - 1; i++)
            {
                StrSelect += " SUM(Case when td_clientcd = '" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then X.NQty else 0 end) as '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_Qty',";
                StrSelect += " SUM(Case when td_clientcd = '" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then case  When NQty = 0 then 0 else Convert(decimal(15,2),(X.NAmt / X.NQty )) end  else 0 end) as '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_Rate',";
                //StrSelect += " SUM(Case when td_clientcd = '" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then Case When X.NQty = 0 then 0 else X.NAmt end else 0 end) as '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_Amount',";

                StrCDC += "'" + StrClCd[i].Trim().Split('~')[0].Trim() + "',";
                StrDataFields += StrClCd[i].Trim().Split('~')[0].Trim() + "_Qty," + StrClCd[i].Trim().Split('~')[0].Trim() + "_Rate,";
                StrHeaderTitles += "Qty,Rate,";
                StrTextAlign += "R,R,";
                StrTextLength += "15,15,";
                StrColWidth += "8,15";
                Strcolhide += 3 + i + "," + 3 + (i + 1) + ",";
            }
            StrCDC = Strings.Left(StrCDC, StrCDC.Length - 1);
            Strcolhide = Strings.Left(Strcolhide, Strcolhide.Length - 1);
            string StrComTradesIndex = string.Empty;
            string StrTradesIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
            { StrTradesIndex = "index(idx_trades_clientcd),"; }

            string StrTRXIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'Trx' and b.name", "idx_Trx_Clientcd", true)) == 1)
            { StrTRXIndex = "index(idx_trx_clientcd),"; }

            DataSet ObjDataSet = new DataSet();

            strsql = " select td_scripnm, snm,Td_Type, " + StrSelect;
            strsql += " SUM(X.NQty) as 'TotalQty',SUM(case  When NQty = 0 then 0 else Convert(decimal(15,2),(X.NAmt / X.NQty )) end) as 'TotalRate'";
            strsql += " From ( ";

            if (_configuration["IsTradeWeb"] == "O")//Live DB
            {
                strsql += " select td_clientcd,1 Td_order,'' as td_ac_type,'' as td_trxdate,'' as td_isin_code,'' as sc_company_name,";
                strsql += " cast((case when sum(td_bqty-td_sqty)=0 then 0 else sum((td_bqty-td_sqty)*td_rate)/sum(td_bqty-td_sqty) end)as";
                strsql += " decimal(15,2) )   as rate,'Equity ' Td_Type,'' as FScripNm,'' as FExDt,";
                strsql += " rtrim(td_scripcd)td_scripnm , rtrim(ss_name) snm,";
                strsql += " sum(td_bqty) Bqty, convert(decimal(15,2), sum(td_bqty*td_rate)) BAmt, ";
                strsql += " sum(td_sqty) Sqty, convert(decimal(15,2), sum(td_sqty*td_rate)) SAmt, sum(td_bqty-td_sqty) NQty, convert(decimal(15,2), sum((td_bqty-td_sqty)*td_rate)) NAmt, ";
                strsql += " '' as td_debit_credit,0 as sm_strikeprice,'' as sm_callput,'Equity|'+td_scripcd LinkCode ";
                strsql += " from Trx with (" + StrTRXIndex + "nolock) , securities with(nolock)";
                strsql += " where td_clientcd in (" + StrCDC + ") and td_dt between '" + FromDate + "' and '" + ToDate + "' ";
                strsql += " and td_Scripcd = ss_cd";
                strsql += " group by td_scripcd, ss_name ,'Equity|'+td_scripcd,td_clientcd  ";
            }

            strsql += " union all ";
            strsql += " select td_clientcd,case left(sm_productcd,1) when 'F' then 2 else 3 end,'', '','' as td_isin_code,'' as sc_company_name, ";
            strsql += " cast((case when  sum(td_bqty-td_sqty)=0 then 0 else sum((td_bqty-td_sqty)*td_rate)/sum(td_bqty-td_sqty) end)as decimal(15,2) ) as rate,";
            strsql += " Case When td_segment='K' then 'Currency ' else 'Equity ' end + ";
            strsql += " Case left(sm_productcd,1) when 'F' Then 'Future ' else 'Option ' end Td_Type,rtrim(sm_symbol), sm_expirydt,rtrim(sm_symbol), case left(sm_productcd,1) when 'F' then 'Fut ' else 'Opt ' end+ rtrim(sm_symbol)+'  Exp: '+ ltrim(rtrim(convert(char,convert(datetime,sm_expirydt),103))) + case left(sm_productcd,1) when 'F' then '' else ''+rtrim(convert(char(9),sm_strikeprice))+sm_callput+sm_optionstyle end, sum(td_bqty) Bqty, convert(decimal(15,2),sum(td_bqty*td_rate*sm_multiplier)) BAmt,  ";
            strsql += " sum(td_sqty) Sqty, convert(decimal(15,2),sum(td_sqty*td_rate*sm_multiplier)) SAmt, sum(td_bqty-td_sqty) NQty,  ";
            strsql += " convert(decimal(15,2),sum((td_bqty-td_sqty)*td_rate *sm_multiplier)) NAmt,'' as td_debit_credit ,sm_strikeprice, sm_callput,";
            strsql += " Case When td_segment='K' then 'Currency' else 'Equity' end + ";
            strsql += " Case left(sm_productcd,1) when 'F' Then 'Future' else 'Option' end + '|' + td_exchange + '|' + replace(sm_symbol,'&','-')  + '|' + left(sm_productcd,1) + '|' + sm_expirydt + '|' + Rtrim(Ltrim(Convert(char,sm_strikeprice))) + '|' +  sm_callput + '|' +  sm_optionstyle + '|' +  td_Segment LinkCode";
            strsql += " from trades with (" + StrTradesIndex + "nolock), series_master with (nolock) ";
            strsql += " where td_clientcd in (" + StrCDC + ") and sm_exchange=td_exchange and sm_Segment=td_Segment and td_seriesid=sm_seriesid ";
            strsql += " and td_dt between '" + FromDate + "' and '" + ToDate + "' and td_trxflag <> 'O'  ";
            strsql += " group by sm_symbol, sm_productcd,td_exchange,td_Segment, sm_expirydt, sm_strikeprice, sm_callput ,td_exchange,sm_optionstyle,td_clientcd";
            strsql += " union all ";
            strsql += " select ex_clientcd,4 ,'','' as td_trxdate,'' as td_isin_code,'' as sc_company_name,cast((case when  sum(ex_aqty-ex_eqty)=0 then 0 else sum((ex_aqty-ex_eqty)*ex_diffrate *case ex_eaflag When 'A' Then -1 else 1 end)/sum(ex_aqty-ex_eqty) end)as decimal(15,2) ) as rate , ";
            strsql += " Case When ex_Segment='K' then 'Currency ' else 'Equity ' end + case ex_eaflag when 'E' then 'Exercise ' else 'Assignment ' end Td_Type, '','', rtrim(sm_symbol), case left(sm_productcd,1) when 'F' then 'Fut ' else 'Opt ' end+ rtrim(sm_symbol)+'  Exp: '+ ltrim(rtrim(convert(char,convert(datetime,sm_expirydt),103))) + case left(sm_productcd,1) when 'F' then '' else ''+rtrim(convert(char(9),sm_strikeprice))+sm_callput+sm_optionstyle end, sum(ex_aqty) Bqty, ";
            strsql += " convert(decimal(15,2),sum(ex_aqty*ex_diffrate *case ex_eaflag When 'A' Then -1 else 1 end *sm_multiplier)) BAmt, sum(ex_eqty) Sqty, convert(decimal(15,2),sum(ex_eqty*ex_diffrate *case ex_eaflag When 'A' Then -1 else 1 end  *sm_multiplier)) SAmt, ";
            strsql += " sum(ex_aqty-ex_eqty) NQty, convert(decimal(15,2),sum((ex_aqty-ex_eqty)*ex_diffrate *case ex_eaflag When 'A' Then -1 else 1 end    *sm_multiplier)) NAmt,'' as td_debit_credit,0,'', ";
            strsql += " Case When ex_segment='K' then 'Currency' else 'Equity' end + ";
            strsql += " case ex_eaflag when 'E' then 'Exercise' else 'Assignment' end + '|' + ex_exchange + '|' + replace(sm_symbol,'&','-')  + '|' + left(sm_productcd,1) + '|' + sm_expirydt + '|' + Rtrim(Ltrim(Convert(char,sm_strikeprice))) + '|' +  sm_callput + '|' +  sm_optionstyle + '|' +  ex_Segment LinkCode";
            strsql += " from exercise with (nolock), series_master with (nolock) where ex_clientcd in (" + StrCDC + ")";
            strsql += " and ex_exchange=sm_exchange and ex_Segment=sm_Segment and ex_seriesid=sm_seriesid ";
            strsql += " and ex_dt between '" + FromDate + "' and '" + ToDate + "' group by ex_eaflag, sm_symbol,ex_exchange,ex_Segment,sm_productcd,sm_expirydt, sm_strikeprice, sm_callput ,sm_optionstyle,ex_clientcd ";


            if (_configuration["IsTradeWeb"] == "O")//Live
            {
                if (_configuration["Commex"] != null && _configuration["Commex"] != string.Empty)
                {

                    string StrCommexConn = "";
                    StrCommexConn = objUtility.GetCommexConnection();
                    if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb(StrCommexConn + ".sysobjects a, " + StrCommexConn + ".sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
                    { StrComTradesIndex = "index(idx_trades_clientcd),"; }

                    strsql += " union all ";
                    strsql += " select td_clientcd,case left(sm_productcd,1) when 'F' then 5 else 6 end,'', '','' as td_isin_code,";
                    strsql += " '' as sc_company_name,   cast((case when  sum(td_bqty-td_sqty)=0 then 0 else ";
                    strsql += " sum((td_bqty-td_sqty)*td_rate)/sum(td_bqty-td_sqty) end)as decimal(15,2) ) as rate,";
                    strsql += " case left(sm_productcd,1) when 'F' then ";
                    strsql += " 'Future (Commodities)' else 'Option (Commodities)' end Td_Type,rtrim(sm_symbol), sm_expirydt,rtrim(sm_symbol), case left(sm_productcd,1) ";
                    strsql += " when 'F' then 'Fut ' else 'Opt ' end+ rtrim(sm_symbol)+'  Exp: '+ ";
                    strsql += " ltrim(rtrim(convert(char,convert(datetime,sm_expirydt),103))) + ";
                    strsql += " case left(sm_productcd,1) when 'F' then '' else ''+rtrim(convert(char(9),sm_strikeprice))+sm_callput end, ";
                    strsql += " sum(td_bqty) Bqty, convert(decimal(15,2), sum(td_bqty*td_rate *sm_multiplier)) BAmt,  sum(td_sqty) Sqty, convert(decimal(15,2), sum(td_sqty*td_rate*sm_multiplier)) SAmt, ";
                    strsql += " sum(td_bqty-td_sqty) NQty, convert(decimal(15,2),sum((td_bqty-td_sqty)*td_rate*sm_multiplier)) NAmt,'' as td_debit_credit ,sm_strikeprice, sm_callput,'Commodities' + '|' + td_exchange + '|' + replace(sm_symbol,'&','-')  + '|' + sm_expirydt LinkCode ";
                    strsql += " from " + StrCommexConn + ".trades with(" + StrComTradesIndex + "nolock), " + StrCommexConn + ".series_master with (nolock) ";
                    strsql += " where td_clientcd in (" + StrCDC + ") and sm_exchange=td_exchange and td_seriesid=sm_seriesid and td_dt ";
                    strsql += " between '" + FromDate + "' and '" + ToDate + "' and td_trxflag <> 'O'  ";
                    strsql += " group by sm_symbol, sm_productcd,td_exchange, sm_expirydt, sm_strikeprice, sm_callput,td_clientcd  ";
                }
            }

            strsql += "  ) X ";
            strsql += " Group BY Td_Type,td_scripnm,snm";
            strsql += " Order by Td_Type,snm,td_scripnm";

            ObjDataSet = new DataSet();
            ObjDataSet = objUtility.OpenDataSet(strsql);
            return ObjDataSet;
        }

        public DataSet ShowFamilyTrxReceipts(string SelectedCLCode, string SelectedValue, string FromDate, string ToDate)
        {
            string StrCDC = "";
            char[] ArrSeparters = new char[1];
            ArrSeparters[0] = '/';
            string[] StrClCd;
            StrClCd = Convert.ToString(SelectedCLCode).Split(ArrSeparters);
            int i = 0;
            for (i = 0; i < StrClCd.Length - 1; i++)
            {
                StrCDC += "'" + StrClCd[i].Trim().Split('~')[0].Trim() + "',";
            }
            StrCDC = Strings.Left(StrCDC, StrCDC.Length - 1);

            if (SelectedValue == "Receipts/Payments")
            {
                strsql = " select ltrim(rtrim(ld_clientcd)) ld_clientcd,cm_name,";
                strsql += " convert(decimal(15,2),sum(case ld_documenttype When 'R' Then Abs(ld_amount) else 0 end))  RAmt, ";
                strsql += " convert(decimal(15,2),sum(case ld_documenttype When 'P' Then (ld_amount) else 0 end))  PAmt ";
                strsql += " from ledger with (nolock),Client_master with (nolock)";
                strsql += " where cm_cd = ld_clientcd and ld_documenttype in ('R','P')";
                strsql += " and ld_clientcd in (" + StrCDC + ") and ld_dt between '" + FromDate + "' and '" + ToDate + "'";
                strsql += " group by  ld_clientcd,cm_name ";
                strsql += " order by cm_name ";

                DataSet ObjDataSet = objUtility.OpenDataSet(strsql);
                return ObjDataSet;
            }
            else
            {
                strsql = " select ld_clientcd,cm_name, ";
                strsql += " sum(case ld_debitflag when 'D' then convert(decimal(15,2),ld_amount)else 0 end)  Dr, ";
                strsql += " sum(case ld_debitflag when 'D' then 0 else convert(decimal(15,2),-ld_amount) end ) Cr ";
                strsql += " from ledger with (nolock),Client_master with (nolock) where cm_cd = ld_clientcd and ld_documenttype='J' ";
                strsql += " and ld_clientcd in (" + StrCDC + ")";
                strsql += "  and ld_dt between '" + FromDate + "' and '" + ToDate + "'";
                strsql += " group by cm_name,ld_clientcd order by cm_name";

                DataSet ObjDataSet = objUtility.OpenDataSet(strsql);
                return ObjDataSet;
            }
        }

        public DataSet FamilyTransactionDetailQuery(string Client, string Type, string FromDate, string ToDate)
        {
            if (Type.Trim() == "R" || Type.Trim() == "P")
            {
                strsql = " select 'DP Transaction' Td_Type,ld_documentno , ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) Date , ld_Particular , ld_Chequeno,";
                strsql += "convert(decimal(15,2),case ld_documenttype When 'R' Then (-1) else 1 end*ld_amount)  Amount from ledger with (nolock)";
                strsql += "where ld_documenttype = '" + Type.Trim() + "'";
                strsql += "and ld_clientcd='" + Client.Trim() + "' and ld_dt between '" + FromDate + "' and '" + ToDate + "'";
                strsql += "order by ld_dt desc ";
            }
            else
            {
                strsql = "select 'DP Transaction' Td_Type,ld_documentno , ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) Date , ";
                strsql = strsql + " ld_Particular  , case ld_debitflag when 'D' then convert(decimal(15,2),ld_amount) else 0 end  Debit,";
                strsql = strsql + " case ld_debitflag when 'D' then 0 else convert(decimal(15,2),-ld_amount) end  Credit";
                strsql = strsql + " from ledger with (nolock) where ld_documenttype= 'J'";
                strsql = strsql + " and ld_clientcd='" + Client.Trim() + "'";
                strsql = strsql + " and ld_dt between '" + FromDate + "' and '" + ToDate + "'";
                strsql = strsql + " order by ld_dt desc";
            }
            DataSet ObjDataSet = new DataSet();
            ObjDataSet = objUtility.OpenDataSet(strsql);
            return ObjDataSet;
        }
        #endregion

        public DataSet FamilyRetainedStokeQuery(List<string> UCC_Codes)
        {
            string SelectedCLCode = "";
            string uccName = "";
            foreach (var uccCode in UCC_Codes)
            {
                uccName = objUtility.fnFireQueryTradeWeb("client_master", "cm_name", "cm_cd", uccCode, true);

                if (!String.IsNullOrEmpty(uccName))
                {
                    SelectedCLCode = SelectedCLCode + uccCode.Trim().ToUpper() + "~" + uccName.Trim() + "/";
                }
            }

            string Strsql = string.Empty;
            DataSet ObjDataSet = new DataSet();

            string strdate = DateTime.Today.Date.ToString("yyyyMMdd");
            int i = 0;
            string strstat = string.Empty;
            Strsql = "select st_sysvalue from stationary where st_parmcd='DMTCOLLATDP' and st_exchange = 'B' ";
            ObjDataSet = objUtility.OpenDataSet(Strsql);
            if (ObjDataSet.Tables[0].Rows.Count != 0)
                strstat = ObjDataSet.Tables[0].Rows[0][0].ToString().Trim();
            char[] ArrSeprator = new char[1];
            ArrSeprator[0] = ',';
            string[] arrstat = strstat.Split(ArrSeprator);
            string strcollat = "( ";
            for (i = 0; i <= arrstat.Length - 1; i++)
            {
                strcollat = strcollat + "'" + arrstat[i] + "',";
            }
            strcollat = strcollat + ")";
            strcollat = strcollat.Replace(",)", ")");

            string StrSelect = "";
            string StrCDC = "";
            char[] ArrSeparters = new char[1];
            ArrSeparters[0] = '/';
            string[] StrClCd;
            string StrDataFields = "";
            string StrSubTotalFields = "";
            string StrHeaderTitles = "";
            string StrTextAlign = "";
            string StrTextLength = "";

            StrClCd = Convert.ToString(SelectedCLCode).Split(ArrSeparters);

            for (i = 0; i < StrClCd.Length - 1; i++)
            {
                StrSelect += " cast(sum(Case When dm_clientcd = '" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then qty else 0 end) as decimal(15,0)) as  '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_Qty',cast(sum(Case When dm_clientcd = '" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then (ss_bserate*qty) else 0 end) as decimal(15,2)) as  '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_Valuation',";
                StrCDC += "'" + StrClCd[i].Trim().Split('~')[0].Trim() + "',";
                StrDataFields += StrClCd[i].Trim().Split('~')[0].Trim() + "_Qty," + StrClCd[i].Trim().Split('~')[0].Trim() + "_Valuation,";
                StrSubTotalFields += StrClCd[i].Trim().Split('~')[0].Trim() + "_Valuation,";
                StrHeaderTitles += "Qty,Valuation,";
                StrTextAlign += "R,R,";
                StrTextLength += "15,15,";
            }

            StrCDC = Strings.Left(StrCDC, StrCDC.Length - 1);
            SqlConnection con;
            using (var db = new DataContext())
            {
                con = new SqlConnection((db.Database.GetDbConnection()).ConnectionString);
                con.Open();
                objUtility.GetHairCut(con);
                strsql = "select dm_isin,ss_name,bh_type, " + StrSelect;
                strsql += " cast(sum(qty)  as decimal(15,0)) as  'TotalQty', ";
                strsql += " cast(sum((ss_bserate*qty)) as decimal(15,2)) as  'TotalVal' ";
                strsql += " from ( ";
                strsql += objUtility.GetSqlTradeHolding(con.ConnectionString, StrCDC, strdate, strcollat, con);
                strsql += " ) a group by dm_isin,ss_name,bh_type having abs(sum(qty)) > 0 ";

                ObjDataSet = new DataSet();
                ObjDataSet = objUtility.OpenDataSetTmp(strsql, con);
            }
            return ObjDataSet;

            #region old code
            //List<FamilyRetainedStokeResponse> familyRetainedStokeModels = new List<FamilyRetainedStokeResponse>();

            //ArrSeparters[0] = '_';
            //string[] code;

            //foreach (DataColumn column in ObjDataSet.Tables[0].Columns)
            //{

            //    if (column.ColumnName != "dm_isin" && column.ColumnName != "ss_name" && column.ColumnName != "Total")
            //    {
            //        FamilyRetainedStokeResponse familyRetained = new FamilyRetainedStokeResponse();
            //        familyRetained.Name = column.ColumnName;
            //        code = column.ColumnName.Split(ArrSeparters);
            //        familyRetained.Code = code[0];
            //        familyRetained.StokeDetails = new List<StokeDetails>();
            //        for (i = 0; i < ObjDataSet.Tables[0].Rows.Count; i++)
            //        {
            //            if (familyRetained.StokeDetails != null)
            //            {

            //                //var abc = dr.ItemArray[].ToString();
            //                familyRetained.StokeDetails.Add(new StokeDetails
            //                {

            //                    ExchSeg = ObjDataSet.Tables[0].Rows[i]["heading"].ToString(),
            //                    Balance = ObjDataSet.Tables[0].Rows[i][column.ColumnName].ToString(),
            //                });
            //            }
            //            else
            //            {
            //                familyRetained.StokeDetails.Add(new StokeDetails
            //                {
            //                    ExchSeg = ObjDataSet.Tables[0].Rows[i]["heading"].ToString(),
            //                    Balance = ObjDataSet.Tables[0].Rows[i][column.ColumnName].ToString(),
            //                });
            //            }
            //        }

            //        familyRetainedStokeModels.Add(familyRetained);
            //    }
            //}
            #endregion
        }

        public DataSet FamilyHoldingQuery(List<string> UCC_Codes)
        {
            string SelectedCLCode = "";
            string uccName = "";
            foreach (var uccCode in UCC_Codes)
            {
                uccName = objUtility.fnFireQueryTradeWeb("client_master", "cm_name", "cm_cd", uccCode, true);

                if (!String.IsNullOrEmpty(uccName))
                {
                    SelectedCLCode = SelectedCLCode + uccCode.Trim().ToUpper() + "~" + uccName.Trim() + "/";
                }
            }

            string StrSelect = "";
            string StrCDC = "";
            char[] ArrSeparters = new char[1];
            ArrSeparters[0] = '/';
            string[] StrClCd;
            string StrDataFields = "";
            string StrSubTotalFields = "";
            string StrHeaderTitles = "";
            string StrTextAlign = "";
            string StrTextLength = "";
            StrClCd = Convert.ToString(SelectedCLCode).Split(ArrSeparters);
            int i = 0;
            for (i = 0; i < StrClCd.Length - 1; i++)
            {
                StrSelect += " cast(sum(Case When CM.cm_blsavingcd = '" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then a.hld_ac_pos else 0 end) as decimal(15,0)) as  '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_Qty',cast(sum(Case When CM.cm_blsavingcd = '" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then (a.hld_ac_pos * sc_Rate) else 0 end) as decimal(15,2)) as  '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_Valuation',";
                StrCDC += "'" + StrClCd[i].Trim().Split('~')[0].Trim() + "',";
                StrDataFields += StrClCd[i].Trim().Split('~')[0].Trim() + "_Qty," + StrClCd[i].Trim().Split('~')[0].Trim() + "_Valuation,";
                StrSubTotalFields += StrClCd[i].Trim().Split('~')[0].Trim() + "_Valuation,";
                StrHeaderTitles += "Qty,Valuation,";
                StrTextAlign += "R,R,";
                StrTextLength += "15,15,";
            }
            StrCDC = Strings.Left(StrCDC, StrCDC.Length - 1);

            if (_configuration["IsTradeWeb"] == "O")
            {//-----------------------------------------------------------Live----------------------------------------------------------------------------------------
                char[] ArrSeparators = new char[1];
                ArrSeparators[0] = '/';
                if (_configuration["Cross"] != "") // strBoid  LEft 2 <>IN
                {
                    string[] ArrCross = _configuration["Cross"].Split(ArrSeparators);
                    strsql = "select a.hld_isin_code,b.sc_isinname,bt_description as BType, " + StrSelect + " cast(sum(a.hld_ac_pos) as decimal(15,0)) as  'TotalQty', cast(sum(a.hld_ac_pos * sc_Rate) as decimal(15,2)) as  'TotalVal' ";
                    strsql += " from " + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".Holding a,";
                    strsql += ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".Security b ," + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".client_master CM, ";
                    strsql += ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".Beneficiary_type d where a.hld_isin_code = b.sc_isincode ";
                    strsql += " and d.bt_code = a.hld_ac_type and CM.cm_cd = a.hld_ac_code and CM.cm_blsavingcd in (" + StrCDC + ") group by a.hld_isin_code,b.sc_isinname,bt_description ";
                }
                else if (_configuration["Estro"] != "")
                {
                    string[] ArrEstro = _configuration["Estro"].Split(ArrSeparators);
                    strsql = "select a.hld_isin_code,b.sc_company_name,bt_description as BType, " + StrSelect + " cast(sum(a.hld_ac_pos) as decimal(15,0)) as  'TotalQty', cast(sum(a.hld_ac_pos * sc_Rate) as decimal(15,2)) as  'TotalVal' ";
                    strsql += " from " + ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".Holding a,";
                    strsql += ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".Security b ,";
                    strsql += ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".Beneficiary_type d, ";
                    strsql += ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".sysParameter, " + ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".client_master CM  ";
                    strsql += " where a.hld_isin_code = b.sc_isincode and sp_parmcd = 'DPID' and CM.cm_blsavingcd in (" + StrCDC + ") ";
                    strsql += " and d.bt_code = a.hld_ac_type and CM.cm_cd = a.hld_ac_code group by a.hld_isin_code,b.sc_company_name,bt_description ";
                }
            }
            DataSet ObjDataSet = new DataSet();
            ObjDataSet = objUtility.OpenDataSet(strsql);
            return ObjDataSet;
        }

        public DataSet FamilyPositionQuery(List<string> UCC_Codes)
        {
            string SelectedCLCode = "";
            string uccName = "";
            foreach (var uccCode in UCC_Codes)
            {
                uccName = objUtility.fnFireQueryTradeWeb("client_master", "cm_name", "cm_cd", uccCode, true);

                if (!String.IsNullOrEmpty(uccName))
                {
                    SelectedCLCode = SelectedCLCode + uccCode.Trim().ToUpper() + "~" + uccName.Trim() + "/";
                }
            }

            string StrSelect = "";
            string StrCDC = "";
            char[] ArrSeparters = new char[1];
            ArrSeparters[0] = '/';
            string[] StrClCd;
            string StrDataFields = "";
            string StrHeaderTitles = "";
            string StrTextAlign = "";
            string StrTextLength = "";
            StrClCd = Convert.ToString(SelectedCLCode).Split(ArrSeparters);
            int i = 0;
            for (i = 0; i < StrClCd.Length - 1; i++)
            {
                StrSelect += " sum(case when td_clientcd ='" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then (td_bqty-td_sqty) else 0 end ) as '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_Net',";
                StrSelect += " convert(decimal(15,2), case sum(case when td_clientcd ='" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then (td_bqty -td_sqty) else 0 end ) when 0 then 0 ";
                StrSelect += " else abs(sum(case when td_clientcd ='" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then ((td_bqty -td_sqty)*td_rate) else 0 end )/sum(case when td_clientcd='" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then (td_bqty-td_sqty) else 0 end ))end) '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_Avgrate', ";
                StrCDC += "'" + StrClCd[i].Trim().Split('~')[0].Trim() + "',";
                StrDataFields += StrClCd[i].Trim().Split('~')[0].Trim() + "_Net," + StrClCd[i].Trim().Split('~')[0].Trim() + "_Avgrate,";
                StrHeaderTitles += "Net,AvgRate,";
                StrTextAlign += "R,R,";
                StrTextLength += "15,15,";
                //Strcolhide += i + 1 + "," + i + 2 + ",";
            }
            StrCDC = Strings.Left(StrCDC, StrCDC.Length - 1);

            string StrTradesIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
            { StrTradesIndex = "index(idx_trades_clientcd),"; }

            if (_configuration["IsTradeWeb"] == "O")//Connect to Live DataBase
            {
                string StrCommexConn = "";
                StrCommexConn = objUtility.GetCommexConnection();

                //Query To Fecth Record From TradePlus DataBase
                strsql = "Select ltrim(rtrim(sm_desc)) sm_desc," + StrSelect;
                strsql += " convert(decimal(15,2), (isnull((select ms_lastprice from Market_summary with (nolock) where ms_exchange = td_exchange and ms_Segment = td_Segment and ms_seriesid = td_seriesid and ms_dt = (select max(ms_dt) from Market_summary with (nolock) where ms_exchange = td_exchange and ms_Segment = td_Segment and ms_seriesid = td_seriesid and  ms_dt <= '" + DateTime.Today.Date.ToString("yyyyMMdd") + "')),0) + case  when right(sm_prodtype,1) <> 'F' then  sm_strikeprice  else 0 end) ) ot_closeprice,  ";
                strsql += " convert(decimal(15,2), (isnull((select ms_lastprice from Market_summary with (nolock) where ms_exchange = td_exchange and ms_Segment = td_Segment and ms_seriesid = td_seriesid  and ms_dt = (select max(ms_dt) from Market_summary with (nolock) where ms_exchange = td_exchange and ms_Segment = td_Segment and ms_seriesid = td_seriesid and  ms_dt <= '" + DateTime.Today.Date.ToString("yyyyMMdd") + "')),0)   + case when right(sm_prodtype,1) <> 'F' then sm_strikeprice  else 0 end)	 *sum(td_bqty-td_sqty) * sm_multiplier ) Closing,  ";
                strsql += " case sm_prodtype when 'IF' then 1 when 'EF' then 2 when 'IO' then 3 when 'EO' then 4 else 5 end listorder, ";
                strsql += " case td_Segment when 'K' then case td_exchange when 'N' then 'NSEFX' when 'M' then 'MCXFX' when 'B' then 'BSEFX' end  when 'F' then Case td_exchange when 'B' then 'BSE' when 'N' then 'NSE' when 'M' then 'MCX' when 'X' then  Case td_exchange when 'B' then 'BSE' when 'N' then 'NSE' when 'M' then 'MCX' end  end end strExchange, ";
                strsql += " case right(sm_prodtype,1) when 'F' then 'Future' else 'Option' end+ case td_segment when 'X' then '(Commodities)' else (case sm_prodtype when 'CF' then ' (Currency)'  else ''end ) end  as tdType ";
                strsql += " from Trades  with (" + StrTradesIndex + "nolock), Series_master with (nolock)   ";
                strsql += " where td_seriesid=sm_seriesid and td_exchange = sm_exchange and td_Segment = sm_Segment  ";
                strsql += " and td_clientcd in (" + StrCDC + ") and td_dt <= '" + DateTime.Today.Date.ToString("yyyyMMdd") + "' and sm_expirydt >= '" + DateTime.Today.Date.ToString("yyyyMMdd") + "'  ";
                strsql += " group by sm_desc,td_exchange,td_Segment,td_seriesid,sm_prodtype,sm_strikeprice,sm_multiplier ";
                strsql += " having sum(case when td_clientcd in (" + StrCDC + ")  then (td_bqty-td_sqty) else 0 end )  <> 0";

                string StrComTradesIndex = "";
                //Query To Fecth Record From Commex DataBase
                if (_configuration["Commex"] != null && _configuration["Commex"] != string.Empty)
                {
                    if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb(StrCommexConn + ".sysobjects a, " + StrCommexConn + ".sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
                    { StrComTradesIndex = "index(idx_trades_clientcd),"; }

                    strsql += " Union all ";
                    strsql += " Select ltrim(rtrim(sm_desc)) sm_desc, " + StrSelect;
                    strsql += " convert(decimal(15,2), (isnull((select ms_lastprice from " + StrCommexConn + ".Market_summary with (nolock) where ms_exchange = td_exchange and ms_seriesid = td_seriesid and ms_dt = (select max(ms_dt) from " + StrCommexConn + ".Market_summary with (nolock) where ms_exchange = td_exchange and ms_seriesid = td_seriesid and  ms_dt <= '" + DateTime.Today.Date.ToString("yyyyMMdd") + "')),0)   + case  when right(sm_prodtype,1) <> 'F' then  sm_strikeprice  else 0 end) ) ot_closeprice, ";
                    strsql += " convert(decimal(15,2), (isnull((select ms_lastprice from " + StrCommexConn + ".Market_summary with (nolock) where ms_exchange = td_exchange and ms_seriesid = td_seriesid  and ms_dt = (select max(ms_dt) from " + StrCommexConn + ".Market_summary with (nolock) where ms_exchange = td_exchange and ms_seriesid = td_seriesid and  ms_dt <= '" + DateTime.Today.Date.ToString("yyyyMMdd") + "')),0)   + case  when right(sm_prodtype,1) <> 'F' then sm_strikeprice  else 0 end)	  *sum(td_bqty-td_sqty) * sm_multiplier ) Closing, ";
                    strsql += " case sm_prodtype when 'CF'then 11 else 12 end listorder, ";
                    strsql += " case td_exchange when 'M' then 'MCX' when 'N' then 'NCDEX' when 'S' then 'NSEL' when 'F' Then 'NCDEX' end strExchange ,";
                    strsql += " case right(sm_prodtype,1)when 'F' then 'Future (Commodities)' else 'Option (Commodities)' end as tdType ";
                    strsql += " from " + StrCommexConn + ".Trades with(nolock), " + StrCommexConn + ".Series_master with (nolock) ";
                    strsql += " where td_seriesid=sm_seriesid and td_exchange = sm_exchange and td_clientcd  in (" + StrCDC + ") ";
                    strsql += " and td_dt <= '" + DateTime.Today.Date.ToString("yyyyMMdd") + "' and sm_expirydt > '" + DateTime.Today.Date.ToString("yyyyMMdd") + "' ";
                    strsql += " group by sm_desc,td_exchange,td_seriesid,sm_prodtype,sm_strikeprice,sm_multiplier ";
                    strsql += " having sum(case when td_clientcd in (" + StrCDC + ")  then (td_bqty-td_sqty) else 0 end )  <> 0";
                    strsql += " order by listorder,sm_desc";
                }
            }
            DataSet ObjDataSet = new DataSet();
            ObjDataSet = objUtility.OpenDataSet(strsql);
            return ObjDataSet;
        }

        public List<FamilyRetainedStokeResponse> FamilyRetainedStokeJson(List<string> UCC_Codes)
        {
            string SelectedCLCode = "";
            string uccName = "";
            foreach (var uccCode in UCC_Codes)
            {
                uccName = objUtility.fnFireQueryTradeWeb("client_master", "cm_name", "cm_cd", uccCode, true);

                if (!String.IsNullOrEmpty(uccName))
                {
                    SelectedCLCode = SelectedCLCode + uccCode.Trim().ToUpper() + "~" + uccName.Trim() + "/";
                }
            }

            string Strsql = string.Empty;
            DataSet ObjDataSet = new DataSet();

            string strdate = DateTime.Today.Date.ToString("yyyyMMdd");
            int i = 0;
            string strstat = string.Empty;
            Strsql = "select st_sysvalue from stationary where st_parmcd='DMTCOLLATDP' and st_exchange = 'B' ";
            ObjDataSet = objUtility.OpenDataSet(Strsql);
            if (ObjDataSet.Tables[0].Rows.Count != 0)
                strstat = ObjDataSet.Tables[0].Rows[0][0].ToString().Trim();
            char[] ArrSeprator = new char[1];
            ArrSeprator[0] = ',';
            string[] arrstat = strstat.Split(ArrSeprator);
            string strcollat = "( ";
            for (i = 0; i <= arrstat.Length - 1; i++)
            {
                strcollat = strcollat + "'" + arrstat[i] + "',";
            }
            strcollat = strcollat + ")";
            strcollat = strcollat.Replace(",)", ")");

            string StrSelect = "";
            string StrCDC = "";
            char[] ArrSeparters = new char[1];
            ArrSeparters[0] = '/';
            string[] StrClCd;
            string StrDataFields = "";
            string StrSubTotalFields = "";
            string StrHeaderTitles = "";
            string StrTextAlign = "";
            string StrTextLength = "";

            StrClCd = Convert.ToString(SelectedCLCode).Split(ArrSeparters);

            for (i = 0; i < StrClCd.Length - 1; i++)
            {
                StrSelect += " cast(sum(Case When dm_clientcd = '" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then qty else 0 end) as decimal(15,0)) as  '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_Qty',cast(sum(Case When dm_clientcd = '" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then (ss_bserate*qty) else 0 end) as decimal(15,2)) as  '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_Valuation',";
                StrCDC += "'" + StrClCd[i].Trim().Split('~')[0].Trim() + "',";
                StrDataFields += StrClCd[i].Trim().Split('~')[0].Trim() + "_Qty," + StrClCd[i].Trim().Split('~')[0].Trim() + "_Valuation,";
                StrSubTotalFields += StrClCd[i].Trim().Split('~')[0].Trim() + "_Valuation,";
                StrHeaderTitles += "Qty,Valuation,";
                StrTextAlign += "R,R,";
                StrTextLength += "15,15,";
            }

            StrCDC = Strings.Left(StrCDC, StrCDC.Length - 1);
            SqlConnection con;
            using (var db = new DataContext())
            {
                con = new SqlConnection((db.Database.GetDbConnection()).ConnectionString);
                con.Open();
                objUtility.GetHairCut(con);
                strsql = "select dm_isin,ss_name,bh_type, " + StrSelect;
                strsql += " cast(sum(qty)  as decimal(15,0)) as  'TotalQty', ";
                strsql += " cast(sum((ss_bserate*qty)) as decimal(15,2)) as  'TotalVal' ";
                strsql += " from ( ";
                strsql += objUtility.GetSqlTradeHolding(con.ConnectionString, StrCDC, strdate, strcollat, con);
                strsql += " ) a group by dm_isin,ss_name,bh_type having abs(sum(qty)) > 0 ";

                ObjDataSet = new DataSet();
                ObjDataSet = objUtility.OpenDataSetTmp(strsql, con);
            }

            List<FamilyRetainedStokeResponse> familyRetainedStokeResponses = new List<FamilyRetainedStokeResponse>();
            StokeDetails stokeDetails;
            FamilyRetainedStokeResponse familyRetainedStoke;
            ArrSeparters[0] = '_';
            string[] code;

            for (int k = 3; k < ObjDataSet.Tables[0].Columns.Count; k += 2)
            {
                familyRetainedStoke = new FamilyRetainedStokeResponse();
                familyRetainedStoke.StokeDetails = new List<StokeDetails>();
                var uccCode = ObjDataSet.Tables[0].Columns[k].ColumnName.Split(ArrSeparters)[0].Trim();
                uccName = objUtility.fnFireQueryTradeWeb("client_master", "cm_name", "cm_cd", uccCode, true);
                if (!string.IsNullOrEmpty(uccName))
                {
                    familyRetainedStoke.FamilyCode = uccCode;
                    familyRetainedStoke.FamilyName = uccCode + "_" + uccName;
                }
                else
                {
                    familyRetainedStoke.FamilyCode = "Total";
                    familyRetainedStoke.FamilyName = "Total";
                }

                for (i = 0; i < ObjDataSet.Tables[0].Rows.Count; i++)
                {
                    stokeDetails = new StokeDetails();
                    stokeDetails.ISIN = ObjDataSet.Tables[0].Rows[i]["dm_isin"].ToString();
                    stokeDetails.ISINName = ObjDataSet.Tables[0].Rows[i]["ss_name"].ToString();
                    stokeDetails.Quantity = ObjDataSet.Tables[0].Rows[i][k].ToString();
                    stokeDetails.Valuation = ObjDataSet.Tables[0].Rows[i][k + 1].ToString();
                    familyRetainedStoke.StokeDetails.Add(stokeDetails);
                }

                familyRetainedStokeResponses.Add(familyRetainedStoke);
            }

            return familyRetainedStokeResponses;

        }

        public List<FamilyHoldingResponse> FamilyHoldingJson(List<string> UCC_Codes)
        {
            string SelectedCLCode = "";
            string uccName = "";
            foreach (var uccCode in UCC_Codes)
            {
                uccName = objUtility.fnFireQueryTradeWeb("client_master", "cm_name", "cm_cd", uccCode, true);

                if (!String.IsNullOrEmpty(uccName))
                {
                    SelectedCLCode = SelectedCLCode + uccCode.Trim().ToUpper() + "~" + uccName.Trim() + "/";
                }
            }

            string StrSelect = "";
            string StrCDC = "";
            char[] ArrSeparters = new char[1];
            ArrSeparters[0] = '/';
            string[] StrClCd;
            string StrDataFields = "";
            string StrSubTotalFields = "";
            string StrHeaderTitles = "";
            string StrTextAlign = "";
            string StrTextLength = "";
            StrClCd = Convert.ToString(SelectedCLCode).Split(ArrSeparters);
            int i = 0;
            for (i = 0; i < StrClCd.Length - 1; i++)
            {
                StrSelect += " cast(sum(Case When CM.cm_blsavingcd = '" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then a.hld_ac_pos else 0 end) as decimal(15,0)) as  '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_Qty',cast(sum(Case When CM.cm_blsavingcd = '" + StrClCd[i].Trim().Split('~')[0].Trim() + "' then (a.hld_ac_pos * sc_Rate) else 0 end) as decimal(15,2)) as  '" + StrClCd[i].Trim().Split('~')[0].Trim() + "_Valuation',";
                StrCDC += "'" + StrClCd[i].Trim().Split('~')[0].Trim() + "',";
                StrDataFields += StrClCd[i].Trim().Split('~')[0].Trim() + "_Qty," + StrClCd[i].Trim().Split('~')[0].Trim() + "_Valuation,";
                StrSubTotalFields += StrClCd[i].Trim().Split('~')[0].Trim() + "_Valuation,";
                StrHeaderTitles += "Qty,Valuation,";
                StrTextAlign += "R,R,";
                StrTextLength += "15,15,";
            }
            StrCDC = Strings.Left(StrCDC, StrCDC.Length - 1);

            if (_configuration["IsTradeWeb"] == "O")
            {//-----------------------------------------------------------Live----------------------------------------------------------------------------------------
                char[] ArrSeparators = new char[1];
                ArrSeparators[0] = '/';
                if (_configuration["Cross"] != "") // strBoid  LEft 2 <>IN
                {
                    string[] ArrCross = _configuration["Cross"].Split(ArrSeparators);
                    strsql = "select a.hld_isin_code,b.sc_isinname,bt_description as BType, " + StrSelect + " cast(sum(a.hld_ac_pos) as decimal(15,0)) as  'TotalQty', cast(sum(a.hld_ac_pos * sc_Rate) as decimal(15,2)) as  'TotalVal' ";
                    strsql += " from " + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".Holding a,";
                    strsql += ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".Security b ," + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".client_master CM, ";
                    strsql += ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".Beneficiary_type d where a.hld_isin_code = b.sc_isincode ";
                    strsql += " and d.bt_code = a.hld_ac_type and CM.cm_cd = a.hld_ac_code and CM.cm_blsavingcd in (" + StrCDC + ") group by a.hld_isin_code,b.sc_isinname,bt_description ";
                }
                else if (_configuration["Estro"] != "")
                {
                    string[] ArrEstro = _configuration["Estro"].Split(ArrSeparators);
                    strsql = "select a.hld_isin_code,b.sc_company_name,bt_description as BType, " + StrSelect + " cast(sum(a.hld_ac_pos) as decimal(15,0)) as  'TotalQty', cast(sum(a.hld_ac_pos * sc_Rate) as decimal(15,2)) as  'TotalVal' ";
                    strsql += " from " + ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".Holding a,";
                    strsql += ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".Security b ,";
                    strsql += ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".Beneficiary_type d, ";
                    strsql += ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".sysParameter, " + ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".client_master CM  ";
                    strsql += " where a.hld_isin_code = b.sc_isincode and sp_parmcd = 'DPID' and CM.cm_blsavingcd in (" + StrCDC + ") ";
                    strsql += " and d.bt_code = a.hld_ac_type and CM.cm_cd = a.hld_ac_code group by a.hld_isin_code,b.sc_company_name,bt_description ";
                }
            }
            DataSet ObjDataSet = new DataSet();
            ObjDataSet = objUtility.OpenDataSet(strsql);

            List<FamilyHoldingResponse> familyHoldingResponses = new List<FamilyHoldingResponse>();
            HoldingDetails holdingDetails;
            FamilyHoldingResponse familyHolding;
            ArrSeparters[0] = '_';

            for (int k = 3; k < ObjDataSet.Tables[0].Columns.Count; k += 2)
            {
                familyHolding = new FamilyHoldingResponse();
                familyHolding.HoldingDetails = new List<HoldingDetails>();
                var uccCode = ObjDataSet.Tables[0].Columns[k].ColumnName.Split(ArrSeparters)[0].Trim();
                uccName = objUtility.fnFireQueryTradeWeb("client_master", "cm_name", "cm_cd", uccCode, true);
                if (!string.IsNullOrEmpty(uccName))
                {
                    familyHolding.FamilyCode = uccCode;
                    familyHolding.FamilyName = uccCode + "_" + uccName;
                }
                else
                {
                    familyHolding.FamilyCode = "Total";
                    familyHolding.FamilyName = "Total";
                }

                for (i = 0; i < ObjDataSet.Tables[0].Rows.Count; i++)
                {
                    holdingDetails = new HoldingDetails();
                    holdingDetails.ISIN = ObjDataSet.Tables[0].Rows[i]["hld_isin_code"].ToString();
                    holdingDetails.ISINName = ObjDataSet.Tables[0].Rows[i]["sc_isinname"].ToString();
                    holdingDetails.Quantity = ObjDataSet.Tables[0].Rows[i][k].ToString();
                    holdingDetails.Valuation = ObjDataSet.Tables[0].Rows[i][k + 1].ToString();
                    familyHolding.HoldingDetails.Add(holdingDetails);
                }

                familyHoldingResponses.Add(familyHolding);
            }

            return familyHoldingResponses;
        }

        public dynamic FamilyTransactionJson(FamilyTransactionModel model)
        {
            char[] ArrSeparters = new char[1];
            string SelectedCLCode = "";
            string uccName = "";
            foreach (var uccCode in model.UCC_Codes)
            {
                uccName = objUtility.fnFireQueryTradeWeb("client_master", "cm_name", "cm_cd", uccCode, true);

                if (!String.IsNullOrEmpty(uccName))
                {
                    SelectedCLCode = SelectedCLCode + uccCode.Trim().ToUpper() + "~" + uccName.Trim() + "/";
                }
            }

            if (model.SelectedValue == "Trades")
            {
                var ObjDataSet = ShowFamilyTransaction(SelectedCLCode, model.FromDate, model.ToDate);

                List<TransactionTradeResponse> familyTransactionResponses = new List<TransactionTradeResponse>();
                TransactionTradeDetails transactionDetails;
                TransactionTradeResponse transactionTrade;
                ArrSeparters[0] = '_';
                int i;

                for (int k = 3; k < ObjDataSet.Tables[0].Columns.Count; k += 2)
                {
                    transactionTrade = new TransactionTradeResponse();
                    transactionTrade.TransactionTradeDetails = new List<TransactionTradeDetails>();
                    var uccCode = ObjDataSet.Tables[0].Columns[k].ColumnName.Split(ArrSeparters)[0].Trim();
                    uccName = objUtility.fnFireQueryTradeWeb("client_master", "cm_name", "cm_cd", uccCode, true);
                    if (!string.IsNullOrEmpty(uccName))
                    {
                        transactionTrade.FamilyCode = uccCode;
                        transactionTrade.FamilyName = uccCode + "_" + uccName;
                    }
                    else
                    {
                        transactionTrade.FamilyCode = "Total";
                        transactionTrade.FamilyName = "Total";
                    }

                    for (i = 0; i < ObjDataSet.Tables[0].Rows.Count; i++)
                    {
                        transactionDetails = new TransactionTradeDetails();
                        transactionDetails.Scrip = ObjDataSet.Tables[0].Rows[i]["td_scripnm"].ToString();
                        transactionDetails.Name = ObjDataSet.Tables[0].Rows[i]["snm"].ToString();
                        transactionDetails.Quantity = ObjDataSet.Tables[0].Rows[i][k].ToString();
                        transactionDetails.Rate = ObjDataSet.Tables[0].Rows[i][k + 1].ToString();
                        transactionTrade.TransactionTradeDetails.Add(transactionDetails);
                    }

                    familyTransactionResponses.Add(transactionTrade);
                }

                return familyTransactionResponses;
            }

            if (model.SelectedValue == "Journals" || model.SelectedValue == "Receipts/Payments")
            {
                var ObjDataSet = ShowFamilyTrxReceipts(SelectedCLCode, model.SelectedValue, model.FromDate, model.ToDate);

                if (model.SelectedValue == "Receipts/Payments")
                {
                    List<TransactionRecieptResponse> transactionReciepts = new List<TransactionRecieptResponse>();

                    for (int i = 0; i < ObjDataSet.Tables[0].Rows.Count; i++)
                    {
                        transactionReciepts.Add(new TransactionRecieptResponse
                        {
                            FamilyCode = ObjDataSet.Tables[0].Rows[i]["ld_clientcd"].ToString(),
                            FamilyName = ObjDataSet.Tables[0].Rows[i]["cm_name"].ToString(),
                            Receipt = ObjDataSet.Tables[0].Rows[i]["RAmt"].ToString(),
                            Payment = ObjDataSet.Tables[0].Rows[i]["PAmt"].ToString(),
                        });
                    }
                    return transactionReciepts;
                }
                else
                {
                    List<TransactionJournalResponse> transactionJournal = new List<TransactionJournalResponse>();

                    for (int i = 0; i < ObjDataSet.Tables[0].Rows.Count; i++)
                    {
                        transactionJournal.Add(new TransactionJournalResponse
                        {
                            FamilyCode = ObjDataSet.Tables[0].Rows[i]["ld_clientcd"].ToString(),
                            FamilyName = ObjDataSet.Tables[0].Rows[i]["cm_name"].ToString(),
                            Debit = ObjDataSet.Tables[0].Rows[i]["Dr"].ToString(),
                            Credit = ObjDataSet.Tables[0].Rows[i]["Cr"].ToString(),
                        });
                    }
                    return transactionJournal;
                }
            }

            return null;
        }

        public dynamic FamilyTransactionDetailJson(string Client, string Type, string FromDate, string ToDate)
        {
            DataSet ObjDataSet = new DataSet();
            if (Type.Trim() == "R" || Type.Trim() == "P")
            {
                strsql = " select 'DP Transaction' Td_Type,ld_documentno , ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) Date , ld_Particular , ld_Chequeno,";
                strsql += "convert(decimal(15,2),case ld_documenttype When 'R' Then (-1) else 1 end*ld_amount)  Amount from ledger with (nolock)";
                strsql += "where ld_documenttype = '" + Type.Trim() + "'";
                strsql += "and ld_clientcd='" + Client.Trim() + "' and ld_dt between '" + FromDate + "' and '" + ToDate + "'";
                strsql += "order by ld_dt desc ";
                ObjDataSet = objUtility.OpenDataSet(strsql);

                List<TransactionDetailReceiptResponse> transactionReceipt = new List<TransactionDetailReceiptResponse>();

                for (int i = 0; i < ObjDataSet.Tables[0].Rows.Count; i++)
                {
                    transactionReceipt.Add(new TransactionDetailReceiptResponse
                    {
                        RefNo = ObjDataSet.Tables[0].Rows[i]["ld_documentno"].ToString(),
                        Date = ObjDataSet.Tables[0].Rows[i]["Date"].ToString(),
                        Particulars = ObjDataSet.Tables[0].Rows[i]["ld_Particular"].ToString(),
                        Instrument = ObjDataSet.Tables[0].Rows[i]["ld_Chequeno"].ToString(),
                        Amount = ObjDataSet.Tables[0].Rows[i]["Amount"].ToString(),
                    });
                }
                return transactionReceipt;
            }
            else
            {
                strsql = "select 'DP Transaction' Td_Type,ld_documentno , ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) Date , ";
                strsql = strsql + " ld_Particular  , case ld_debitflag when 'D' then convert(decimal(15,2),ld_amount) else 0 end  Debit,";
                strsql = strsql + " case ld_debitflag when 'D' then 0 else convert(decimal(15,2),-ld_amount) end  Credit";
                strsql = strsql + " from ledger with (nolock) where ld_documenttype= 'J'";
                strsql = strsql + " and ld_clientcd='" + Client.Trim() + "'";
                strsql = strsql + " and ld_dt between '" + FromDate + "' and '" + ToDate + "'";
                strsql = strsql + " order by ld_dt desc";
                ObjDataSet = objUtility.OpenDataSet(strsql);

                List<TransactionDetailJournalResponse> transactionJournals = new List<TransactionDetailJournalResponse>();

                for (int i = 0; i < ObjDataSet.Tables[0].Rows.Count; i++)
                {
                    transactionJournals.Add(new TransactionDetailJournalResponse
                    {
                        RefNo = ObjDataSet.Tables[0].Rows[i]["ld_documentno"].ToString(),
                        Date = ObjDataSet.Tables[0].Rows[i]["Date"].ToString(),
                        Particulars = ObjDataSet.Tables[0].Rows[i]["ld_Particular"].ToString(),
                        Debit = ObjDataSet.Tables[0].Rows[i]["Debit"].ToString(),
                        Credit = ObjDataSet.Tables[0].Rows[i]["Credit"].ToString(),
                    });
                }
                return transactionJournals;
            }
        }
        #endregion

        #endregion

        #region new margin handler

        // For getting margin data
        public dynamic MarginMainData(string cm_cd, string strCompanyCode, string date)
        {
            try
            {
                var ds = GetQueryMarginMainData(cm_cd, strCompanyCode, date);
                if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                {
                    List<MarginResponse> marginResponse = new List<MarginResponse>();
                    for (int i = 0; i < ds.Tables[0].Rows.Count; i++)
                    {
                        marginResponse.Add(new MarginResponse
                        {
                            ExchSeg = ds.Tables[0].Rows[i]["ExchSeg"].ToString(),
                            EOD_Margin_Required = Convert.ToDecimal(ds.Tables[0].Rows[i]["fm_TotalMrgn"]),
                            EOD_Margin_Available = Convert.ToDecimal(ds.Tables[0].Rows[i]["Collected"]),
                            EOD_ShortFall_Amount = Convert.ToDecimal(ds.Tables[0].Rows[i]["TotalShort"]),
                            EOD_ShortFall_Per = Convert.ToDecimal(ds.Tables[0].Rows[i]["TotalShortPER"]),
                            Peak_Margin_Required = Convert.ToDecimal(ds.Tables[0].Rows[i]["Tmp_NFiller4"]),
                            Peak_Margin_To_Be_Collected = Convert.ToDecimal(ds.Tables[0].Rows[i]["Tmp_PeakMargin"]),
                            Peak_Margin_Available = Convert.ToDecimal(ds.Tables[0].Rows[i]["fm_TotalMrgn"]),
                            Peak_Margin_Shortfall = Convert.ToDecimal(ds.Tables[0].Rows[i]["PeakShort"]),
                            Peak_Margin_Highest_Shortfall = Convert.ToDecimal(ds.Tables[0].Rows[i]["Tmp_HighestShortFall"])
                        });
                    }
                    
                    return marginResponse;
                }
                return new List<string>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        // get data for margin main grid
        private DataSet GetQueryMarginMainData(string cm_cd, string strCompanyCode, string date)
        {
            DataSet objdataset = new DataSet();
            if (_configuration["IsTradeWeb"] == "O")//Live DB {
            {
                objdataset = fnNewGetRptSQL(true, cm_cd, strCompanyCode, date);
            }
            else
            {
                strsql = " Select case right(fm_companycode,2) When 'BF' Then 'BSE F&O' When 'NF' Then 'NSE F&O' When 'MF' Then 'MCX F&O' else '' end ExchSeg,";
                strsql += " fm_clientcd,cast(fm_spanmargin as decimal(15,2)) as fm_spanmargin,cast(fm_exposurevalue as decimal(15,2)) as fm_exposurevalue, cast(fm_buypremmargin as decimal(15,2)) as fm_buypremmargin,";
                strsql += " cast(fm_initialmargin as decimal(15,2)) as fm_initialmargin,cast(fm_additionalmargin as decimal(15,2)) as fm_additionalmargin, cast(fm_collected as decimal(15,2)) as fm_collected,cast(isnull(fm_spanmargin,0 )+ isnull(fm_exposurevalue,0) as decimal(15,2)) 'margin',";
                strsql += " cast(case when (fm_initialmargin + fm_exposurevalue- case when fm_collected > 0 then fm_collected else 0 end) >0 then (fm_initialmargin + fm_exposurevalue- case when fm_collected > 0 then fm_collected else 0 end) else 0 end as decimal(15,2)) ShortFall";
                strsql += " ,convert(char,convert(datetime, fm_dt),103) as DisplayDate from fmargins where right(fm_companycode,1) = 'F' and fm_dt = (select max(fm_Dt) from fmargins Where right(fm_companycode,1) = 'F' ) and fm_clientcd='" + cm_cd + "'";
                strsql += " union all";
                strsql += " Select case right(fm_companycode,2) When 'BK' Then 'BSE FX' When 'NK' Then 'NSE FX' When 'MK' Then 'MCX FX' else '' end ExchSeg,";
                strsql += " fm_clientcd,cast(fm_spanmargin as decimal(15,2)) as fm_spanmargin,cast(fm_exposurevalue as decimal(15,2)) as fm_exposurevalue, cast(fm_buypremmargin as decimal(15,2)) as fm_buypremmargin,";
                strsql += " cast(fm_initialmargin as decimal(15,2)) as fm_initialmargin,cast(fm_additionalmargin as decimal(15,2)) as fm_additionalmargin, cast(fm_collected as decimal(15,2)) as fm_collected,cast(isnull(fm_spanmargin,0 )+ isnull(fm_exposurevalue,0) as decimal(15,2)) 'margin',";
                strsql += " cast(case when (fm_initialmargin + fm_exposurevalue- case when fm_collected > 0 then fm_collected else 0 end) >0 then (fm_initialmargin + fm_exposurevalue- case when fm_collected > 0 then fm_collected else 0 end) else 0 end as decimal(15,2)) ShortFall";
                strsql += " ,convert(char,convert(datetime, fm_dt),103) as DisplayDate from fmargins where right(fm_companycode,1) = 'K' and fm_dt = (select max(fm_Dt) from fmargins Where right(fm_companycode,1) = 'K' ) and fm_clientcd='" + cm_cd + "'";
                strsql += " union all";
                strsql += " Select case right(fm_companycode,2) When 'MX' Then 'MCX Commodity' When 'NX' Then 'NCDEX Commodity' else '' end ExchSeg,fm_clientcd,cast(fm_spanmargin as decimal(15,2)) as fm_spanmargin,";
                strsql += " cast(fm_exposurevalue as decimal(15,2)) as fm_exposurevalue, cast(fm_buypremmargin as decimal(15,2)) as fm_buypremmargin,cast(fm_initialmargin as decimal(15,2)) as fm_initialmargin,cast(fm_additionalmargin as decimal(15,2)) as fm_additionalmargin, cast(fm_collected as decimal(15,2)) as fm_collected,";
                strsql += " cast(isnull(fm_spanmargin,0 )+ isnull(fm_exposurevalue,0) as decimal(15,2)) 'margin',cast(case when (fm_initialmargin + fm_exposurevalue- case when fm_collected > 0 then fm_collected else 0 end) >0 then (fm_initialmargin + fm_exposurevalue- case when fm_collected > 0 then fm_collected else 0 end) else 0 end as decimal(15,2)) ShortFall,convert(char,convert(datetime, fm_dt),103) as  DisplayDate ";
                strsql += " from fmargins  where right(fm_companycode,1) = 'X' and fm_dt = (select max(fm_Dt)  from fmargins Where right(fm_companycode,1) = 'X' ) and fm_clientcd='" + cm_cd + "'";
                objdataset = objUtility.OpenDataSet(strsql);

            }
            return objdataset;
        }

        // used for getting margin main data
        public DataSet fnNewGetRptSQL(bool blnisLetter, string cm_cd, string strCompanyCode, string date)
        {
            string StrConn = _configuration.GetConnectionString("DefaultConnection");
            string strCase = "";
            string strFild = "";
            string strExchSegme = "";
            string strSegCode = "";
            DataSet rsExcgSeg;

            string strTotalShortFallX = "";
            string strTotalCollectedX = "";
            string strClientWhere = "";

            using (SqlConnection ObjConnectionTmp = new SqlConnection(StrConn))
            {
                string StrCommexConn = "";
                if (objUtility.GetWebParameter("Commex") != null && objUtility.GetWebParameter("Commex") != string.Empty)
                {
                    StrCommexConn = objUtility.GetCommexConnection();
                }

                string strSql = "";
                ObjConnectionTmp.Open();
                objUtility.ExecuteSQLTmp("if OBJECT_ID('tempdb..#TmpPeakColl') is not null Drop Table #TmpPeakColl", ObjConnectionTmp);
                objUtility.ExecuteSQLTmp("Create Table #TmpPeakColl (Tmp_Clientcd VarChaR(8),Tmp_CompanyCode VarChaR(3),Tmp_PeakMargin Money,Tmp_PeakColl Money,Tmp_Shortfall Money, Tmp_exchange char(1), Tmp_segment char(1) , Tmp_Nfiller4 money, Tmp_Nfiller5 money )", ObjConnectionTmp);

                string strdate = "";
                strdate = objUtility.fnFireQueryTradeWeb("Fmargins", "max(fm_Dt)", "1", "1", true).ToString().Trim();
                if (Conversion.Val(objUtility.fnFireQueryTradeWeb("Fmargin_PeakMargin", "Count(0)", "fc_companycode='" + strCompanyCode + "' and fc_dt ", "(select max(fc_dt) from Fmargin_PeakMargin)", true).ToString()) > 0)
                {

                    strSql = " insert into #TmpPeakColl ";
                    strSql += " select fm_clientcd,'" + strCompanyCode + "'+fm_exchange+fm_Segment ,isNull(fm_NFiller4,0), isNull(fm_NFiller5,0) , 0,  fm_exchange, fm_Segment ,isNull(fm_NFiller4,0), isNull(fm_NFiller5,0) ";
                    strSql += " from Fmargins, Client_master ";
                    strSql += " Where fm_clientcd=cm_cd and fm_Companycode ='" + strCompanyCode + "' and fm_dt = '" + date + "'";
                    if (objUtility.GetWebParameter("Commex") != null && objUtility.GetWebParameter("Commex") != string.Empty)
                    {
                        strSql += " union ";
                        strSql += " select fm_clientcd,'" + strCompanyCode + "'+fm_exchange+'X',isNull(fm_PeakMargin,0)+isNull(fm_Filler2,0),isNull(fm_Filler1,0), 0 ,  fm_exchange, 'X' fm_Segment ,isNull(fm_PeakMargin,0)+isNull(fm_Filler2,0),isNull(fm_Filler1,0)";
                        strSql += " from " + StrCommexConn + ".Fmargins, Client_master ";
                        strSql += " Where fm_clientcd=cm_cd and fm_Companycode ='" + strCompanyCode + "' and fm_dt = '" + date + "' and fm_clientcd ='" + cm_cd + "'";
                    }
                    objUtility.ExecuteSQLTmp(strSql, ObjConnectionTmp);

                    strSql = " Update #TmpPeakColl set Tmp_PeakMargin = Round(Tmp_PeakMargin * " + objUtility.fnPeakFactor(strdate) + "/100,2) Where Right(Tmp_CompanyCode,2) <> 'MX' ";
                    objUtility.ExecuteSQLTmp(strSql, ObjConnectionTmp);
                }
                else
                {
                    strSql = " insert into #TmpPeakColl ";
                    strSql += " select fm_clientcd,fm_exchange+fm_Segment,isNull(fm_NFiller4,0), 0 , 0 , fm_exchange, fm_Segment ,isNull(fm_NFiller4,0), isNull(fm_NFiller5,0)";
                    strSql += " from Fmargins, Client_master ";
                    strSql += " Where fm_clientcd=cm_cd and fm_Companycode ='" + strCompanyCode + "' and fm_dt = '" + date + "' ";
                    if (objUtility.GetWebParameter("Commex") != null && objUtility.GetWebParameter("Commex") != string.Empty)
                    {
                        strSql += " union ";
                        strSql += " select fm_clientcd,fm_exchange+'X',isNull(fm_PeakMargin,0)+isNull(fm_Filler2,0), 0 , 0 , fm_exchange, 'X' fm_Segment, isNull(fm_PeakMargin,0)+isNull(fm_Filler2,0),isNull(fm_Filler1,0) ";
                        strSql += " from " + StrCommexConn + ".Fmargins, Client_master ";
                        strSql += " Where fm_clientcd=cm_cd and fm_Companycode ='" + strCompanyCode + "' and fm_dt = '" + date + "'";
                    }
                    objUtility.ExecuteSQLTmp(strSql, ObjConnectionTmp);

                    strSql = " Update #TmpPeakColl set Tmp_PeakMargin = Round(Tmp_PeakMargin * " + objUtility.fnPeakFactor(strdate) + "/100,2) Where Right(Tmp_CompanyCode,2) <> 'MX'";
                    objUtility.ExecuteSQLTmp(strSql, ObjConnectionTmp);

                    strSql = " Update #TmpPeakColl set Tmp_PeakColl = case When isNull(fc_FillerN9,0) > 0 then isNull(fc_FillerN9,0) else 0 end ";
                    strSql += " from Fmargin_clients ";
                    strSql += " Where fc_clientcd=Tmp_clientcd and fc_Companycode ='" + strCompanyCode + "' and fc_Exchange = '' and fc_dt = '" + date + "'";
                    objUtility.ExecuteSQLTmp(strSql, ObjConnectionTmp);
                }
                strSql = " Update #TmpPeakColl set Tmp_PeakMargin=0,Tmp_PeakColl=0 Where Tmp_PeakColl>=Tmp_PeakMargin";
                objUtility.ExecuteSQLTmp(strSql, ObjConnectionTmp);


                if (blnisLetter)
                {
                    strSql = " Update #TmpPeakColl set Tmp_ShortFall = case When (Tmp_PeakMargin-Tmp_PeakColl) > 0 then (Tmp_PeakMargin-Tmp_PeakColl) else 0 end";
                    objUtility.ExecuteSQLTmp(strSql, ObjConnectionTmp);
                }

                strClientWhere += " and cm_cd = '" + cm_cd + "'";
                strClientWhere += " and cm_type <> 'I'";


                strCase = "";
                strFild = "";

                if (objUtility.GetWebParameter("Commex") != null && objUtility.GetWebParameter("Commex") != string.Empty)
                {
                    try
                    {
                        strSql = "Drop table #FmarginsRpt";
                        objUtility.ExecuteSQLTmp(strSql, ObjConnectionTmp);
                    }
                    catch (Exception ex)
                    {
                    }
                    finally
                    {
                        strSql = " CREATE TABLE [#FmarginsRpt]( ";
                        strSql += " [fm_companycode] [char](1) NOT NULL,[fm_exchange] [char](1) NOT NULL,[fm_dt] [char](8) NOT NULL, ";
                        strSql += " [fm_clientcd] [char](8) NOT NULL,[fm_spanmargin] [money] NOT NULL,[fm_buypremmargin] [money] NOT NULL, ";
                        strSql += " [fm_initialmargin] [money] NOT NULL,[fm_exposurevalue] [money] NOT NULL,[fm_clienttype] [char](1) NOT NULL, ";
                        strSql += " [fm_additionalmargin] [money] NOT NULL,[fm_collected] [money] NOT NULL,[fm_mainbrcd] [char](8) NOT NULL, ";
                        strSql += " [mkrid] [char](8) NOT NULL,[mkrdt] [char](8) NOT NULL,[fm_Regmargin] [money] NULL,[fm_Tndmargin] [money] NULL, ";
                        strSql += " [fm_Dlvmargin] [money] NULL,[fm_SpreadBen] [money] NULL,[fm_SplMargin] [money] NULL,[fm_collectedT2] [money] NOT NULL, ";
                        strSql += " [fm_InitShort] [money] NOT NULL,[fm_MTMAddShort] [money] NOT NULL,[fm_OthShort] [money] NOT NULL,[fm_ConcMargin] [money] NOT NULL, ";
                        strSql += " [fm_DelvPMargin] [money] NOT NULL,[fm_MTMLoss] [money] NOT NULL) ";
                        objUtility.ExecuteSQLTmp(strSql, ObjConnectionTmp);
                    }


                    strSql = " Insert into #FmarginsRpt select fm_companycode,fm_exchange,fm_dt,fm_clientcd,Sum(fm_spanmargin),Sum(fm_buypremmargin),Sum(fm_initialmargin),Sum(fm_exposurevalue),''fm_clienttype,";
                    strSql += " Sum(fm_additionalmargin),Sum(fm_collected),'' fm_mainbrcd,'' mkrid,'' mkrdt,Sum(fm_Regmargin),Sum(fm_Tndmargin),Sum(fm_Dlvmargin),Sum(fm_SpreadBen),Sum(fm_SplMargin),Sum(fm_collectedT2),Sum(fm_InitShort),";
                    strSql += " Sum(fm_MTMAddShort),Sum(fm_OthShort),Sum(fm_ConcMargin),Sum(fm_DelvPMargin),Sum(fm_MTMLoss) from ( ";
                    strSql += " select fm_companycode,fm_exchange,fm_dt,fm_clientcd,fm_spanmargin,fm_buypremmargin,fm_initialmargin,fm_exposurevalue,''fm_clienttype,fm_additionalmargin,fm_collected,";
                    strSql += " ''fm_mainbrcd,''mkrid,''mkrdt,fm_Regmargin,fm_Tndmargin,fm_Dlvmargin,fm_SpreadBen,fm_SplMargin,fm_collectedT2,fm_InitShort,fm_MTMAddShort,fm_OthShort,fm_ConcMargin,fm_DelvPMargin,0 fm_MTMLoss ";
                    strSql += " from  " + StrCommexConn + ".Fmargins, " + StrCommexConn + ".Client_master ";
                    strSql += " Where fm_clientcd = cm_cd and fm_Companycode ='" + strCompanyCode + "' and fm_dt = '" + date + "'" + strClientWhere;
                    strSql += " union all ";
                    strSql += " select po_companycode,po_exchange,po_dt,po_clientcd,0 fm_spanmargin,0 fm_buypremmargin,0 fm_initialmargin,0 fm_exposurevalue,0 fm_clienttype,0 fm_additionalmargin,";
                    strSql += " 0 fm_collected,'' fm_mainbrcd,'' mkrid,'' mkrdt,0 fm_Regmargin,0 fm_Tndmargin,0 fm_Dlvmargin,0 fm_SpreadBen,0 fm_SplMargin,0 fm_collectedT2,0 fm_InitShort,";
                    strSql += " 0 fm_MTMAddShort,0 fm_OthShort,0 fm_ConcMargin,0 fm_DelvPMargin,case When -sum(po_futvalue) > 0 Then -sum(po_futvalue) else 0 end MarginReq ";
                    strSql += " from  " + StrCommexConn + ".Fpositions,  " + StrCommexConn + ".Client_master";
                    strSql += " Where po_companycode='" + strCompanyCode + "' and po_clientcd = cm_cd ";
                    strSql += " and po_dt = '" + date + "' " + strClientWhere;
                    strSql += " Group by po_clientcd,po_companycode,po_exchange,po_dt";
                    strSql += " Having case When -sum(po_futvalue) > 0 Then -sum(po_futvalue) else 0 end > 0 ";
                    strSql += " ) a Group by fm_companycode,fm_exchange,fm_dt,fm_clientcd ";
                    objUtility.ExecuteSQLTmp(strSql, ObjConnectionTmp);

                    strSql = " Update #FmarginsRpt set fm_collected = fc_collected , fm_collectedT2  = fc_Collected1 ";
                    strSql += " From Fmargin_Clients  ";
                    strSql += " Where fc_companycode = '" + strCompanyCode + "' and fc_exchange = fm_Exchange and fc_Segment = 'X' and fc_dt = '" + date + "' and fm_clientcd = fc_clientcd ";
                    strSql += " and not exists ( Select fm_clientcd from " + StrCommexConn + ".Fmargins Where fm_companycode = '" + strCompanyCode + "' and fm_dt = '" + date + "' and fc_clientcd  = fm_clientcd and fm_Exchange = fc_Exchange ) ";
                    objUtility.ExecuteSQLTmp(strSql, ObjConnectionTmp);

                }

                strSql = "select  distinct  '0' 'Product' ,(fm_exchange+fm_segment) fm_ExchSeg,fm_segment Seg from Fmargins, Client_master ";
                strSql += " Where fm_clientcd=cm_cd and fm_Companycode ='" + strCompanyCode + "' and fm_dt = '" + date + "'";
                if (objUtility.GetWebParameter("Commex") != null && objUtility.GetWebParameter("Commex") != string.Empty)
                {
                    strSql += " union ";
                    strSql += " select distinct '1' 'Product' ,(fm_exchange+'X') fm_ExchSeg,'X' Seg from #FmarginsRpt ";
                    strSql += " Where fm_Companycode ='" + strCompanyCode + "' and fm_dt = '" + date + "' ";
                }
                strSql += " Order by Product,Seg,fm_ExchSeg ";
                rsExcgSeg = objUtility.OpenDataSetTmp(strSql, ObjConnectionTmp);
                string strTemp = "";
                strFild = "";
                foreach (DataRow objrow in rsExcgSeg.Tables[0].Rows)
                {
                    strExchSegme = objrow["fm_ExchSeg"].ToString();
                    if (Strings.Right(strExchSegme, 1) == "C")
                    { strSegCode = "Cash"; }
                    else if (Strings.Right(strExchSegme, 1) == "F")
                    { strSegCode = "Fo"; }
                    else if (Strings.Right(strExchSegme, 1) == "K")
                    { strSegCode = "Fx"; }
                    else if (Strings.Right(strExchSegme, 1) == "X")
                    { strSegCode = "Cx"; }

                    strCase = strCase + " case fm_exchange+fm_Segment when '" + strExchSegme + "' then case When (fm_TotalMrgn-(fm_collected+fm_collected1)) > 0 Then (fm_TotalMrgn-(fm_collected+fm_collected1)) else 0 end else 0 end TotalShort" + strSegCode + strExchSegme + ",";
                    strTotalShortFallX += "sum(TotalShort" + strSegCode + strExchSegme + ")+";

                    strCase = strCase + " case fm_exchange+fm_Segment when '" + strExchSegme + "' then (fm_collected+fm_collected1) else 0 end Collected" + strSegCode + strExchSegme + ",";
                    strTotalCollectedX += "sum(Collected" + strSegCode + strExchSegme + ")+";

                    strCase = strCase + " case fm_exchange+fm_Segment when '" + strExchSegme + "' then fm_TotalMrgn else 0 end TotalMrgn" + strSegCode + strExchSegme + ",";
                    strTemp = strTemp + "sum(TotalMrgn" + strSegCode + strExchSegme + ") TotalMrgn" + strSegCode + strExchSegme + ",";
                }

                if (strCase.Trim() != "")
                //{ Return "";}
                {
                    if (blnisLetter)
                    {
                        strCase = Strings.Left(strCase.Trim(), Strings.Len(strCase.Trim()) - 1);
                        strFild = "";
                        strFild = " fm_clientcd,fm_exchange,fm_segment,isNull(cm_Name,'Not Found') cm_Name,cm_email,bm_email,cm_brboffcode,bm_branchname,bm_add1 ,cm_add1,bm_add2 ,cm_add2,bm_add3 ,cm_add3,";
                        strFild += " Tmp_Shortfall PeakShort, Tmp_NFiller4, Tmp_NFiller5 , Tmp_PeakColl , Tmp_PeakMargin ,";

                        if (Strings.Right(strTotalShortFallX.Trim(), 1) == "+")
                        {
                            strTotalShortFallX = Strings.Left(strTotalShortFallX, Strings.Len(strTotalShortFallX) - 1) + " TotalShort";
                            strFild += strTotalShortFallX + ", ";
                        }
                        if (Strings.Right(strTotalCollectedX.Trim(), 1) == "+")
                        {
                            strTotalCollectedX = Strings.Left(strTotalCollectedX, Strings.Len(strTotalCollectedX) - 1) + " Collected";
                            strFild += strTotalCollectedX + ", ";
                        }
                        strFild += strTemp;
                        strFild = Strings.Left(strFild.Trim(), Strings.Len(strFild.Trim()) - 1);

                        strCase += " , fm_TotalMrgn ";
                        strFild += " , Sum(fm_TotalMrgn) fm_TotalMrgn ";

                        strSql = " Select case fm_exchange when 'B' then 'BSE-' when 'N' then 'NSE-' when 'M' then 'MCX-' when 'F' then 'NCDEX-' else '' end + case fm_segment when 'C' then 'CASH'  when 'F' then 'FO'  when 'K' then 'FX' when 'M' then 'MF' when 'X' then 'COMM' else '' end ExchSeg,cast(fm_TotalMrgn as decimal(15,2)) fm_TotalMrgn,cast(Collected as decimal(15,2)) Collected,cast(TotalShort as decimal(15,2)) TotalShort,";
                        strSql += " cast(case when fm_TotalMrgn > 0 then ((TotalShort * 100)/ fm_TotalMrgn) else 0 end as decimal(15,2)) TotalShortPER,cast(Tmp_NFiller4 as decimal(15,2)) Tmp_NFiller4,cast(Tmp_PeakMargin as decimal(15,2)) Tmp_PeakMargin,";
                        strSql += " cast(Tmp_NFiller5 as decimal(15,2)) Tmp_NFiller5,cast(PeakShort as decimal(15,2)) PeakShort,cast(Case When PeakShort > TotalShort then PeakShort else TotalShort end as decimal(15,2)) 'Tmp_HighestShortFall' ";

                        if (Conversion.Val(objUtility.fnFireQueryTradeWeb("Sysparameter", "Count(0)", "sp_parmcd", "MGPENALTY", true)) > 0)
                        {
                            strSql += ",";
                            strSql += " Convert(Decimal(15,2), Round(( ((Case When PeakShort > TotalShort then PeakShort else TotalShort end)) *  ";
                            strSql += " (Case When ((Case When PeakShort > TotalShort then PeakShort else TotalShort end)) < 100000  ";
                            strSql += " and Case When  fm_TotalMrgn = 0 then 0 else ((((Case When PeakShort > TotalShort then PeakShort else TotalShort end))*100) / (fm_TotalMrgn)) end < 10 Then 0.5 Else 1 End/100)),2)  ";
                            strSql += " ) ShortPenalty ";
                        }
                        strSql += "  from ( ";
                        strSql += "  select " + strFild + " From (";
                        strSql += " Select fm_clientcd,fm_exchange,fm_segment," + strCase;
                        strSql += " from ( select fm_clientcd,fm_exchange,fm_Segment,Sum(fm_TotalMrgn) fm_TotalMrgn,Sum(fm_collected) fm_collected,Sum(fm_collected1) fm_collected1 from ( ";
                        strSql += " select fm_clientcd,fm_exchange,fm_Segment,fm_TotalMrgn,fm_collected,fm_collected1  from Fmargins, Client_master Where cm_cd=fm_clientcd and fm_Companycode ='" + strCompanyCode + "' and fm_dt = '" + date + "'";
                        if (objUtility.GetSysParmSt("FMRGCombined", "").Trim() == "F")
                        {
                            strSql += " union all ";
                            strSql += " select fc_Filler1,fc_exchange,fc_Segment,0 fm_TotalMrgn,fc_collected,fc_collected1  from Fmargin_Clients, Client_master Where cm_cd=fc_clientcd and fc_exchange <> '' and fc_Companycode ='" + strCompanyCode + "' and fc_dt = '" + date + "'";
                        }
                        strSql += " ) a Group By fm_clientcd,fm_exchange,fm_Segment ";
                        strSql += " ) z , Client_master,branch_master ";
                        strSql += " Where fm_clientcd = cm_cd and cm_brboffcode = bm_branchcd " + strClientWhere;
                        if (objUtility.GetWebParameter("Commex") != null && objUtility.GetWebParameter("Commex") != string.Empty)
                        {
                            strSql += " union all ";
                            string strTotalMargin = "";
                            strTotalMargin = " (case fm_exchange When 'M' Then fm_Regmargin + fm_exposurevalue + fm_buypremmargin else fm_initialmargin + fm_exposurevalue end) + case fm_Exchange When 'M' Then fm_additionalmargin + fm_Tndmargin + fm_Dlvmargin - fm_SpreadBen + fm_ConcMargin + fm_DelvPMargin else fm_additionalmargin + fm_SplMargin end + fm_MTMLoss ";
                            strCase = strCase.Replace("fm_Segment", "'X'");
                            strCase = strCase.Replace("fm_TotalMrgn", strTotalMargin);
                            strCase = strCase.Replace("(fm_collected+fm_collected1)", "(fm_collected+fm_collectedt2)");
                            strSql += " Select fm_clientcd,fm_exchange,'X' fm_Segment," + strCase;
                            strSql += " from #FmarginsRpt," + StrCommexConn + ".Client_master," + StrCommexConn + ".branch_master Where fm_clientcd = cm_cd and cm_brboffcode = bm_branchcd and fm_Companycode ='" + strCompanyCode + "' and fm_dt = '" + date + "'";
                        }
                        strSql += " ) a , Client_master,branch_master , #TmpPeakColl p ";
                        strSql += " Where fm_clientcd = cm_cd and fm_exchange=tmp_exchange and fm_segment=tmp_segment and cm_brboffcode = bm_branchcd and fm_clientcd = Tmp_Clientcd ";
                        strSql += " Group by fm_clientcd,fm_exchange,fm_Segment,cm_Name,cm_email,bm_email,cm_brboffcode,bm_branchname,bm_add1 ,cm_add1,bm_add2 ,cm_add2,bm_add3 ,cm_add3,Tmp_Shortfall,Tmp_NFiller4, Tmp_Nfiller5, Tmp_PeakColl, Tmp_PeakMargin ";
                        if (Conversion.Val(objUtility.fnFireQueryTradeWeb("Sysparameter", "Count(0)", "sp_parmcd", "MGPENALTY", true)) > 0)
                        {
                            strSql += " Having Sum(fm_TotalMrgn) > 0  ";
                        }
                        strSql += " ) b ";

                        //strSql += " Where PeakShort > 0 Or TotalShort > 0 ";

                        strSql += " Order By fm_clientcd ";
                    }
                    return objUtility.OpenDataSetTmp(strSql, ObjConnectionTmp);
                }
                return null;
            }
        }

        #endregion
    }
}
