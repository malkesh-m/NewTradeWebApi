using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.IdentityModel.Tokens;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Net;
using System.Security.Claims;
using System.Text;
using System.Threading.Tasks;
using TradeWeb.API.Data;
using TradeWeb.API.Entity;
using TradeWeb.API.Models;
using TradeWeb.API.Repository;
using TradeWeb.Entity;

namespace TradeWeb.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class TradeWebController : Controller
    {
        #region Class level declarations.
        private readonly UserManager<AppUser> _userManager;
        private readonly IConfiguration _configuration;
        private readonly UtilityCommon objUtility;
        private readonly IHttpContextAccessor _httpContextAccessor;
        private readonly SignInManager<AppUser> _signInManager;
        #endregion

        #region Constructor
        public TradeWebController(UserManager<AppUser> userManager, IConfiguration configuration, UtilityCommon objUtility, IHttpContextAccessor httpContextAccessor, SignInManager<AppUser> signInManager)
        {
            _userManager = userManager;
            _configuration = configuration;
            this.objUtility = objUtility;
            _httpContextAccessor = httpContextAccessor;
            _signInManager = signInManager;
        }
        #endregion

        #region API Methods
        /// <summary>
        /// Login
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        [HttpPost("Login", Name = "Login")]
        public IActionResult Login([FromBody] TradeWebLoginModel data)
        {
            // TODO : IsValid returns false then the Model contains some validation error and the controller would not allow the model to pass into the controller.
            if (ModelState.IsValid)
            {
                try
                {
                    // LoginHandler logHandler = new LoginHandler();
                    var userList = UserDetails(data.username, data.password);
                    if (userList != null)
                    {
                        var tokenString = GenerateJSONWebToken(data);
                        FillConfigParametersString();
                        return Ok(new tokenResponse { status = true, message = "success", token = tokenString, status_code = (int)HttpStatusCode.OK, data = userList });
                    }
                    return Ok(new commonResponse { status = false, message = "blank", status_code = (int)HttpStatusCode.NotFound, error_message = "records not found" });
                }
                catch (Exception ex)
                {
                    return BadRequest(new commonResponse { status = false, message = "error", status_code = (int)HttpStatusCode.InternalServerError, error_message = ex.Message.ToString() });
                }
            }
            return BadRequest();
        }

        //[HttpPost("LogOut", Name = "LogOut")]
        //public async Task<IActionResult> LogOut()
        //{
        //    var handler = new JwtSecurityTokenHandler();
        //    string authHeader = Request.Headers["Authorization"];
        //    authHeader = authHeader.Replace("Bearer ", "");
        //    var jsonToken = handler.ReadToken(authHeader);
        //    var tokenS = handler.ReadToken(authHeader) as JwtSecurityToken;
        //    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

        //    _httpContextAccessor.HttpContext.Request.Headers.Remove("Authorization"); 
        //    string authHeader2 = Request.Headers["Authorization"];

        //    Response.Cookies.Delete("Authorization");
        //    _httpContextAccessor.HttpContext.Response.Cookies.Delete("Authorization");

        //    await _signInManager.SignOutAsync();

        //    return Ok();
        //}

        /// <summary>
        /// Ledger_Summary
        /// </summary>
        /// <param name="type"></param>
        /// <param name="exchSeg"></param>
        /// <param name="fromDate"></param>
        /// <param name="toDate"></param>
        /// <returns></returns>
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("Ledger_Summary", Name = "Ledger_Summary")]
        public IActionResult Ledger_Summary(string type, string exchSeg, string fromDate, string toDate)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    JwtSecurityToken token = GetToken();
                    var CompCode = token.Claims.First(claim => claim.Type == "companyCode").Value;
                    var userId = token.Claims.First(claim => claim.Type == "username").Value;

                    var dataList = Ledger_Summary(userId, type, exchSeg, fromDate, toDate, CompCode);
                    if (dataList != null)
                    {
                        return Ok(new commonResponse { status = true, message = "success", status_code = (int)HttpStatusCode.OK, data = dataList });
                    }
                    return Ok(new commonResponse { status = true, message = "success", status_code = (int)HttpStatusCode.NotFound, data = null });
                }
                catch (Exception ex)
                {
                    return BadRequest(new { response = ex.Message.ToString() });
                }
            }
            return BadRequest();
        }

        ///// <summary>
        ///// GetLedgerDetailsData
        ///// </summary>
        ///// <returns></returns>//[Authorize(AuthenticationSchemes = "Bearer")]
        //[HttpGet("GetLedgerDetailsData", Name = "GetLedgerDetailsData")]
        //public async Task<IActionResult> GetLedgerDetailsData(string exchangeTitle, string exchangeCode, Boolean blnexseg, string fromdt, string todt, string exchangeCodeIn, string exchangeMTFTitle, string exchangeMTFCodeIn, string strNBFC, string exchangeTitlemar, string exchangeCodeMar, string exchangeCodemarIn, string exchCommCode, string exchCommTitle, string exchCommCodeMar, string exchCommTitleMar, string exchangeCommCodeIn, string exchangeCommMarIn)
        //{
        //    if (ModelState.IsValid)
        //    {
        //        try
        //        {
        //            var bb = _configuration["Commex"];

        //            var tokenS = GetToken();
        //            var userName = tokenS.Claims.First(claim => claim.Type == "username").Value;

        //            var getData = GetLedgerDetailsData(userName, exchangeTitle, exchangeCode, blnexseg, fromdt, todt, exchangeCodeIn, exchangeMTFTitle, exchangeMTFCodeIn, strNBFC, exchangeTitlemar, exchangeCodeMar, exchangeCodemarIn, exchCommCode, exchCommTitle, exchCommCodeMar, exchCommTitleMar, exchangeCommCodeIn, exchangeCommMarIn);
        //            if (getData != null)
        //            {
        //                return Ok(new commonResponse { status = true, message = "success", status_code = (int)HttpStatusCode.OK, data = getData });
        //            }
        //            else
        //            {
        //                return NotFound(new commonResponse { status = false, message = "blank", status_code = (int)HttpStatusCode.NotFound, error_message = "records not found" });
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            return BadRequest(new commonResponse { status = false, message = "error", status_code = (int)HttpStatusCode.InternalServerError, error_message = ex.Message.ToString() });
        //        }
        //    }
        //    return BadRequest();
        //}



        private JwtSecurityToken GetToken()
        {
            var handler = new JwtSecurityTokenHandler();
            string authHeader = Request.Headers["Authorization"];
            authHeader = authHeader.Replace("Bearer ", "");
            //var jsonToken = handler.ReadToken(authHeader);
            var token = handler.ReadToken(authHeader) as JwtSecurityToken;
            return token;
        }
        #endregion

        #region Other methods for JWT Token
        // TODO : Write a method for generate jwt token.
        private string GenerateJSONWebToken(TradeWebLoginModel userInfo)
        {
            var securityKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(_configuration["Jwt:Key"]));
            var credentials = new SigningCredentials(securityKey, SecurityAlgorithms.HmacSha256);

            string Strsql = "";
            if (_configuration["IsTradeWeb"] == "T" || _configuration["IsTradeWeb"] == "O")
            {
                string apiRes3 = objUtility.fnFireQueryTradeWeb("Entity_master", "count(0)", " em_cd='B' and em_name like 'SYKES & RAY EQUITIES%' and 1", "1", true);
                if (Convert.ToInt16(apiRes3) > 0)
                    Strsql = " select em_name OrgName,em_cd CompnyCd from Entity_Master with (nolock) where em_cd='B'";
                else
                    Strsql = " select em_name OrgName,em_cd CompnyCd from Entity_Master with (nolock) where em_cd =(select min(em_cd) from Entity_master Where len(em_cd) = 1)";
            }
            else if (_configuration["IsTradeWeb"] == "C")
            {
                Strsql = "select sp_sysvalue OrgName,'' CompnyCd from sysparameter with (nolock) where sp_parmcd='NAME'";
            }

            else if (_configuration["IsTradeWeb"] == "E")
            {
                Strsql = "select sp_sysvalue OrgName,'' CompnyCd from sysparameter with (nolock) where sp_parmcd='NAME'";
            }

            else if (_configuration["IsTradeWeb"] == "B")
            {
                Strsql = " select em_name OrgName,em_cd CompnyCd from Entity_Master with (nolock) where em_cd =(select min(em_cd) from Entity_master )";
            }

            var ObjDataSet = objUtility.OpenDataSet(Strsql);

            var claims = new[] {
                new Claim(JwtRegisteredClaimNames.Sub, userInfo.username),
                new Claim(JwtRegisteredClaimNames.Email, userInfo.username),
                new Claim(type: "companyCode", value: ObjDataSet.Tables[0].Rows[0]["CompnyCd"].ToString().Trim()),
                new Claim(JwtRegisteredClaimNames.Jti, Guid.NewGuid().ToString()),
                new Claim(type: "username", value: userInfo.username),
            };
            var token = new JwtSecurityToken(_configuration["Jwt:Issuer"],
                _configuration["Jwt:Issuer"],
                claims,
                expires: DateTime.Now.AddMinutes(120),
                signingCredentials: credentials);

            return new JwtSecurityTokenHandler().WriteToken(token);
        }
        #endregion

        #region Fill Configuration
        private WebParamterLoginEntity FillConfigParameters()
        {
            try
            {
                // TODO : Added code of web from Login.aspx(125 -175)

                WebParamterLoginEntity wp = new WebParamterLoginEntity();
                wp.Segments = objUtility.GetWebParameter("Segments");
                wp.CLR_TITLE = objUtility.GetWebParameter("CLR_TITLE");
                wp.CLR_MENU = objUtility.GetWebParameter("CLR_MENU");
                wp.CLR_HEADER = objUtility.GetWebParameter("CLR_HEADER");
                wp.CLR_TOTALS = objUtility.GetWebParameter("CLR_TOTALS");
                wp.IsTradeWeb = objUtility.GetWebParameter("IsTradeWeb");
                wp.TPlus = objUtility.GetWebParameter("TPlus");
                wp.TPlusES = objUtility.GetWebParameter("TPlusES");
                wp.Cross = objUtility.GetWebParameter("Cross");
                wp.CrossEs = objUtility.GetWebParameter("CrossEs");
                wp.Estro = objUtility.GetWebParameter("Estro");
                wp.EstroEs = objUtility.GetWebParameter("EstroEs");
                wp.Commex = objUtility.GetWebParameter("Commex");
                wp.CommexEs = objUtility.GetWebParameter("CommexEs");

                wp.CAPTCHA = objUtility.GetWebParameter("CAPTCHA");
                wp.SMTPHOST = objUtility.GetWebParameter("SMTPHOST");
                wp.PANASPASSWORD = objUtility.GetWebParameter("PANASPASSWORD");
                wp.GroupLogin = objUtility.GetWebParameter("GroupLogin");
                wp.ShowRupeeSymbol = objUtility.GetWebParameter("ShowRupeeSymbol");
                wp.CLR_WEBSKIN = objUtility.GetWebParameter("CLR_WEBSKIN");
                wp.FTSOSURL = objUtility.GetWebParameter("FTSOSURL");
                wp.PMSUrl = objUtility.GetWebParameter("PMSUrl");
                wp.KYCPDF = objUtility.GetWebParameter("KYCPDF");
                wp.OrganizationName = objUtility.GetWebParameter("OrganizationName");
                wp.TNetInvplUrl = objUtility.GetWebParameter("TNetInvplUrl");
                wp.OUTPOSURL = objUtility.GetWebParameter("OUTPOSURL");
                wp.SMTPSERVER = objUtility.GetWebParameter("SMTPSERVER");
                wp.SECURITYPROT = objUtility.GetWebParameter("SECURITYPROT");

                return wp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private string FillConfigParametersString()
        {
            try
            {
                _configuration["Segments"] = objUtility.GetWebParameter("segments");
                _configuration["ColourTitle"] = objUtility.GetWebParameter("cLR_TITLE");
                _configuration["ColourMenu"] = objUtility.GetWebParameter("cLR_MENU");
                _configuration["ColourHeader"] = objUtility.GetWebParameter("cLR_HEADER");
                _configuration["ColourTotals"] = objUtility.GetWebParameter("cLR_TOTALS");
                _configuration["IsTradeWeb"] = objUtility.GetWebParameter("isTradeWeb");
                _configuration["TPlus"] = objUtility.AddBracket(objUtility.GetWebParameter("tPlus"));
                _configuration["TPlusES"] = objUtility.AddBracket(objUtility.GetWebParameter("tPlusES"));
                _configuration["Cross"] = objUtility.AddBracket(objUtility.GetWebParameter("cross"));
                _configuration["CrossEs"] = objUtility.AddBracket(objUtility.GetWebParameter("crossEs"));
                _configuration["Estro"] = objUtility.AddBracket(objUtility.GetWebParameter("estro"));
                _configuration["EstroEs"] = objUtility.AddBracket(objUtility.GetWebParameter("estroEs"));
                _configuration["Commex"] = objUtility.AddBracket(objUtility.GetWebParameter("commex"));
                _configuration["CommexEs"] = objUtility.AddBracket(objUtility.GetWebParameter("commexEs"));
                _configuration["CAPTCHA"] = objUtility.AddBracket(objUtility.GetWebParameter("cAPTCHA"));

                _configuration["SMTPHOST"] = objUtility.AddBracket(objUtility.GetWebParameter("sMTPHOST"));
                _configuration["PANASPASSWORD"] = objUtility.AddBracket(objUtility.GetWebParameter("pANASPASSWORD"));
                _configuration["GroupLogin"] = objUtility.AddBracket(objUtility.GetWebParameter("groupLogin"));
                _configuration["ShowRupeeSymbol"] = objUtility.AddBracket(objUtility.GetWebParameter("showRupeeSymbol"));
                _configuration["CLR_WEBSKIN"] = objUtility.AddBracket(objUtility.GetWebParameter("cLR_WEBSKIN"));
                _configuration["FTSOSURL"] = objUtility.AddBracket(objUtility.GetWebParameter("fTSOSURL"));
                _configuration["PMSUrl"] = objUtility.AddBracket(objUtility.GetWebParameter("pMSUrl"));
                _configuration["KYCPDF"] = objUtility.AddBracket(objUtility.GetWebParameter("kYCPDF"));
                _configuration["OrganizationName"] = objUtility.AddBracket(objUtility.GetWebParameter("organizationName"));
                _configuration["TNetInvplUrl"] = objUtility.AddBracket(objUtility.GetWebParameter("tNetInvplUrl"));
                _configuration["OUTPOSURL"] = objUtility.AddBracket(objUtility.GetWebParameter("oUTPOSURL"));
                _configuration["SMTPSERVER"] = objUtility.AddBracket(objUtility.GetWebParameter("sMTPSERVER"));
                _configuration["SECURITYPROT"] = objUtility.AddBracket(objUtility.GetWebParameter("sECURITYPROT"));

                return "success";
            }
            catch (Exception ex)
            {
                return ex.Message.ToString();

            }
        }
        #endregion

        #region Handler
        

        private dynamic Ledger_Summary(string cm_cd, string type, string exchSeg, string Fromdate, string Todate, string CompCode)
        {
            string qury = GetQueryMainData(Fromdate, Todate, cm_cd, CompCode);
            try
            {
                var ds = objUtility.OpenDataSet(qury);
                if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                {
                    var data = ds.Tables[0]; //.AsEnumerable().Where(a => a.Field<string>("Type") == type && a.Field<string>("ExchSeg") == exchSeg);
                    return JsonConvert.SerializeObject(data, Formatting.Indented);
                }
                return new List<LedgerSummaryEntity>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private dynamic GetLedgerDetailsData(string Username, string ExchangeTitle, string ExchangeCode, Boolean blnexseg, string Fromdt, string Todt, string ExchangeCodeIn, string ExchangeMTFTitle, string ExchangeMTFCodeIn, string strNBFC, string ExchangeTitlemar, string ExchangeCodeMar, string ExchangeCodemarIn, string ExchCommCode, string ExchCommTitle, string ExchCommCodeMar, string ExchCommTitleMar, string ExchangeCommCodeIn, string ExchangeCommMarIn)
        {
            string qury = GetQueryForDetailsData(Username, ExchangeTitle, ExchangeCode, blnexseg, Fromdt, Todt, ExchangeCodeIn, ExchangeMTFTitle, ExchangeMTFCodeIn, strNBFC, ExchangeTitlemar, ExchangeCodeMar, ExchangeCodemarIn, ExchCommCode, ExchCommTitle, ExchCommCodeMar, ExchCommTitleMar, ExchangeCommCodeIn, ExchangeCommMarIn);
            try
            {
                var ds = CommonRepository.FillDataset(qury);
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

        private dynamic UserDetails(string userId, string password)
        {
            try
            {
                string qury = "select cm_pwd,'' cm_Panno from Client_master with (nolock) where cm_cd='" + userId + "'  and cm_pwd='" + password + "'";
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
        #endregion

        #region Query
        private string GetQueryMainData(string Fromdt, string Todt, string Username, string CompCode)
        {
            string strsql = "select * from ( ";
            strsql = strsql +  "select 'Trading' as [Type],ld_clientcd as [ClientCode], sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then ld_amount else 0 end) OpeningBalance,sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then 0 else case ld_debitflag when 'D' then ld_amount else 0 end end) Debit, sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then 0 else case ld_debitflag     when 'D' then 0 else ld_amount end end) Credit, sum(ld_amount) Balance ,";
            strsql = strsql + " case substring(ld_dpid,2,1)  ";
            strsql = strsql + " when 'B' then 'BSE-' when 'N' then 'NSE-' when 'M' then 'MCX-'  when 'F' then 'NCDEX-' else '' end + ";
            strsql = strsql + " case substring(ld_dpid,3,1)  ";
            strsql = strsql + " when 'C' then 'CASH'  when 'F' then 'DERIVATIVE'  when 'K' then 'FX' when 'M' then 'MF' when 'X' then 'COMM' else '' end ";
            strsql = strsql + " as [ExchSeg], ";
            strsql = strsql + "'" + CompCode + "' as CESCD";
            strsql = strsql + " from ledger with (nolock) where ld_clientcd='" + Username + "' and ld_dt <= '" + Todt + "' group by ld_dpid , ld_clientcd";
            strsql = strsql + " union all select 'Margin' as [Type],ld_clientcd as [ClientCode], sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then ld_amount else 0 end) OpeningBalance,sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then 0 else case ld_debitflag  ";
            strsql = strsql + " when 'D' then ld_amount else 0 end end) Debit, sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then 0 else case ld_debitflag when 'D' then 0 else ld_amount end end) Credit, sum(ld_amount) Balance, ";
            strsql = strsql + " case substring(ld_dpid,2,1)  ";
            strsql = strsql + " when 'B' then 'BSE-' when 'N' then 'NSE-' when 'M' then 'MCX-' when 'F' then 'NCDEX-' else '' end + ";
            strsql = strsql + " case substring(ld_dpid,3,1)  ";
            strsql = strsql + " when 'C' then 'CASH'  when 'F' then 'DERIVATIVE'  when 'K' then 'FX'  when 'X' then 'COMM' else '' end + '(M)'";
            strsql = strsql + " as exchtitle, ";
            strsql = strsql + "'" + CompCode + "' as CompanyCode";
            strsql = strsql + " from ledger where  ld_clientcd=(select distinct cm_brkggroup from client_master with (nolock) where cm_cd='" + Username + "') and  ";
            strsql = strsql + "  ld_dt <= '" + Todt + "' group by ld_dpid , ld_clientcd ";

            //MTF ledger
            if (Convert.ToInt32(objUtility.fnFireQueryTradeWeb("sysobjects", "count(*)", "name", "MrgTdgFin_TRX", true)) > 0)
            {
                strsql = strsql + " union all ";
                strsql = strsql + "select 'MTF' as account,ld_clientcd, sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then ld_amount else 0 end) OpeningBalance,sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then 0 else case ld_debitflag when 'D' then ld_amount else 0 end end) Debit, sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then 0 else case ld_debitflag     when 'D' then 0 else ld_amount end end) Credit, sum(ld_amount) Balance ,";
                strsql = strsql + " case substring(ld_dpid,2,1)  ";
                strsql = strsql + " when 'B' then 'BSE-' when 'N' then 'NSE-' when 'M' then 'MCX-' else '' end + 'MTF'";
                strsql = strsql + " as exchtitle, ";
                strsql = strsql + "'" + CompCode + "' as CompanyCode";
                strsql = strsql + " from ledger with (nolock),MrgTdgFin_Clients with (nolock)  where ld_clientcd='" + Username + objUtility.GetSysParmSt("MTFP_SUFFIX", "") + "' and ld_dt <= '" + Todt + "'";
                strsql = strsql + " and ld_clientcd =  Rtrim(MTFC_CMCD) + '" + objUtility.GetSysParmSt("MTFP_SUFFIX", "") + "' ";
                strsql = strsql + " group by ld_dpid , ld_clientcd";
            }

            // NBFC
            if (Convert.ToInt32(objUtility.fnFireQueryTradeWeb("sysobjects", "count(*)", "name", "nbfc_clients", true)) > 0)
            {
                strsql = strsql + " union all ";
                strsql += "select 'NBFC' as account,ld_clientcd, sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then ld_amount else 0 end) OpeningBalance,sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then 0 else case ld_debitflag when 'D' then ld_amount else 0 end end) Debit, sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then 0 else case ld_debitflag     when 'D' then 0 else ld_amount end end) Credit, sum(ld_amount) Balance ,";
                strsql += " 'NBFC' exchtitle,";
                strsql += " '" + CompCode + "' as CompanyCode";
                strsql = strsql + " from NBFC_Ledger with (nolock) where ld_clientcd='" + Username + "' and ld_dt <= '" + Todt + "' group by ld_dpid , ld_clientcd";

            }
            if (_configuration["Commex"] != null && _configuration["Commex"] != string.Empty)
            {

                string StrCommexConn = objUtility.GetCommexConnectionNew(_configuration["Commex"]);
                strsql = strsql + " union all select 'Commodity' as account,ld_clientcd, sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then ld_amount else 0 end) OpeningBalance,sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then 0 else case ld_debitflag when 'D' then ld_amount else 0 end end) Debit, sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then 0 else case ld_debitflag when 'D' then 0 else ld_amount end end) Credit, sum(ld_amount) Balance, ";
                strsql = strsql + " case substring(ld_dpid,2,1) when 'M' then 'MCX-COMM' when 'N' then 'NCDEX-COMM' when 'S' then 'NSEL-COMM' when 'D' then 'NSX-COMM' end as exchtitle, ";
                strsql = strsql + "'" + CompCode + "' as CompanyCode";
                strsql = strsql + " from   " + StrCommexConn + ".ledger";
                strsql = strsql + " where ld_clientcd='" + Username + "' and ld_dt <= '" + Todt + "' ";
                strsql = strsql + " group by ld_dpid , ld_clientcd ";

                strsql = strsql + " union all select 'Commodity Margin' as account,ld_clientcd, sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then ld_amount else 0 end) OpeningBalance,sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then 0 else case ld_debitflag when 'D' then ld_amount else 0 end end) Debit, sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then 0 else case ld_debitflag when 'D' then 0 else ld_amount end end) Credit, sum(ld_amount) Balance, ";
                strsql = strsql + " case substring(ld_dpid,2,1) when 'M' then 'MCX-COMM' when 'N' then 'NCDEX-COMM' when 'S' then 'NSEL-COMM' when 'D' then 'NSX' end + ' (M)' as exchtitle, ";
                strsql = strsql + "'" + CompCode + "' as CompanyCode";
                strsql = strsql + " from   " + StrCommexConn + ".ledger";
                strsql = strsql + " where ld_clientcd=(select distinct cm_brkggroup from " + StrCommexConn + ".client_master where cm_cd='" + Username + "') and ld_dt <= '" + Todt + "' ";
                strsql = strsql + " group by ld_dpid , ld_clientcd";
            }
            strsql = strsql + " ) a ";
            strsql = strsql + " order by [Type],ClientCode,CESCD ";

            return strsql;
        }

        ////// TODO : For Details grid data query(business logic) (TW TPlus/Ledger.cs Page line no. 663-805)
        private string GetQueryForDetailsData(string Username, string ExchangeTitle, string ExchangeCode, Boolean blnexseg, string Fromdt, string Todt, string ExchangeCodeIn, string ExchangeMTFTitle, string ExchangeMTFCodeIn, string strNBFC, string ExchangeTitlemar, string ExchangeCodeMar, string ExchangeCodemarIn, string ExchCommCode, string ExchCommTitle, string ExchCommCodeMar, string ExchCommTitleMar, string ExchangeCommCodeIn, string ExchangeCommMarIn)
        {
            string strTable = " Ledger ";
            string strsql = "";
            string strCommTable = string.Empty;
            string strCommClientMaster = string.Empty;
            if (_configuration["IsTradeWeb"] == "O")
            {
                if (_configuration["Commex"] != null && _configuration["Commex"] != string.Empty)
                {
                    string StrCommexConn = "";
                    StrCommexConn = objUtility.GetCommexConnectionNew(_configuration["Commex"]);
                    strCommTable = StrCommexConn + ".ledger";
                    strCommClientMaster = StrCommexConn + ".Client_master";
                }
            }
            else
            {
                strCommTable = "Cledger";
                strCommClientMaster = "Client_master";
            }
            //Ledger------------------------    
            strsql = string.Empty;
            if (!string.IsNullOrEmpty(ExchangeTitle) && !string.IsNullOrEmpty(ExchangeCode))
            {
                if (blnexseg == true)
                {

                    strsql = " select '" + Fromdt + "' as FromDate, '" + Todt + "' as ToDate,  substring(ld_dpid,2,2) as ExCode," + ExchangeTitle.Substring(0, ExchangeTitle.Length - 5) + " End as ExchangeTitle,";
                    strsql = strsql + " 'C' as acc,ld_clientcd,convert(char,convert(datetime,'" + Fromdt + "'),103) ld_dt,cast(sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then ld_amount ";
                    strsql = strsql + " else 0 end)as decimal(15,2)) as ld_amount,'Opening Balance' ld_particular, case sign(sum(ld_amount)) when 1 then 'D' else 'C' end as ";
                    strsql = strsql + " ld_debitflag ,'' ld_chequeno, 'O' ld_documenttype,''ld_common ,convert(datetime,'" + Fromdt + "') Ldate,ld_dpid";
                    strsql = strsql + " from " + strTable + " with (nolock) ";
                    strsql = strsql + " where ld_clientcd = '" + Username + "'  ";
                    strsql = strsql + " and ld_dt < '" + Fromdt + "' and right(rtrim(ld_dpid),2)in (" + ExchangeCodeIn + ")";
                    strsql = strsql + " group by ld_clientcd,ld_dpid having sum(ld_amount)<> 0 ";
                    strsql = strsql + " union all ";
                    strsql = strsql + " select  '" + Fromdt + "' as FromDate, '" + Todt + "' as ToDate,substring(ld_dpid,2,2) as ExCode," + ExchangeTitle.Substring(0, ExchangeTitle.Length - 5) + " End as ExchangeTitle,'', ";
                    strsql = strsql + " ld_clientcd, ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) as ld_dt ,cast(ld_amount as decimal(15,2)),ld_particular,ld_debitflag,ld_chequeno,ld_documenttype,ld_common,convert(datetime,ld_dt) Ldate,ld_dpid ";
                    strsql = strsql + " from " + strTable + " with (nolock) ";
                    strsql = strsql + " where ld_clientcd = '" + Username + "'  ";
                    strsql = strsql + "  and ld_dt between '" + Fromdt + "' and '" + Todt + "' and right(rtrim(ld_dpid),2)in (" + ExchangeCodeIn + ")";
                    //if (ExchCommCode == string.Empty && ExchCommTitle == string.Empty)
                    //    strsql = strsql + " order by Ldate";
                }
            }
            //MTF
            if (!string.IsNullOrEmpty(ExchangeMTFTitle) && !string.IsNullOrEmpty(ExchangeMTFCodeIn))
            {
                if (blnexseg == true)
                {
                    strsql = strsql + " union all ";
                }

                strsql = strsql + " select '" + Fromdt + "' as FromDate, '" + Todt + "' as ToDate,  substring(ld_dpid,2,2) as ExCode," + ExchangeMTFTitle.Substring(0, ExchangeMTFTitle.Length - 5) + " End as ExchangeTitle,";
                strsql = strsql + " 'C' as acc,ld_clientcd,convert(char,convert(datetime,'" + Fromdt + "'),103) ld_dt,cast(sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then ld_amount ";
                strsql = strsql + " else 0 end)as decimal(15,2)) as ld_amount,'Opening Balance' ld_particular, case sign(sum(ld_amount)) when 1 then 'D' else 'C' end as ";
                strsql = strsql + " ld_debitflag ,'' ld_chequeno, 'O' ld_documenttype,''ld_common ,convert(datetime,'" + Fromdt + "') Ldate,ld_dpid";
                strsql = strsql + " from " + strTable + " with (nolock) ";
                strsql = strsql + " where ld_clientcd = '" + Username + objUtility.GetSysParmSt("MTFP_SUFFIX", "") + "'  ";
                strsql = strsql + " and ld_dt < '" + Fromdt + "' and right(rtrim(ld_dpid),2)in (" + ExchangeMTFCodeIn + ")";
                strsql = strsql + " group by ld_clientcd,ld_dpid having sum(ld_amount)<> 0 ";
                strsql = strsql + " union all ";
                strsql = strsql + " select  '" + Fromdt + "' as FromDate, '" + Todt + "' as ToDate,substring(ld_dpid,2,2) as ExCode," + ExchangeMTFTitle.Substring(0, ExchangeMTFTitle.Length - 5) + " End as ExchangeTitle,'L' acc, ";
                strsql = strsql + " ld_clientcd, ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) as ld_dt ,cast(ld_amount as decimal(15,2)) ld_amount ,ld_particular,ld_debitflag,ld_chequeno,ld_documenttype,ld_common,convert(datetime,ld_dt) Ldate,ld_dpid ";
                strsql = strsql + " from " + strTable + " with (nolock) ";
                strsql = strsql + " where ld_clientcd = '" + Username + objUtility.GetSysParmSt("MTFP_SUFFIX", "") + "'  ";
                strsql = strsql + "  and ld_dt between '" + Fromdt + "' and '" + Todt + "' and right(rtrim(ld_dpid),2)in (" + ExchangeMTFCodeIn + ")";
            }
            //NBFC

            if (strNBFC == "Y")
            {
                if (strsql != "")
                { strsql = strsql + " union all "; }

                strsql = strsql + " select '" + Fromdt + "' as FromDate, '" + Todt + "' as ToDate,  substring(ld_dpid,2,2) as ExCode, 'NBFC' as ExchangeTitle,";
                strsql = strsql + " 'C' as acc,ld_clientcd,convert(char,convert(datetime,'" + Fromdt + "'),103) ld_dt,cast(sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then ld_amount ";
                strsql = strsql + " else 0 end)as decimal(15,2)) as ld_amount,'Opening Balance' ld_particular, case sign(sum(ld_amount)) when 1 then 'D' else 'C' end as ";
                strsql = strsql + " ld_debitflag ,'' ld_chequeno, 'O' ld_documenttype,''ld_common ,convert(datetime,'" + Fromdt + "') Ldate,ld_dpid";
                strsql = strsql + " from NBFC_Ledger with (nolock) ";
                strsql = strsql + " where ld_clientcd = '" + Username + "'  ";
                strsql = strsql + " and ld_dt < '" + Fromdt + "'";
                strsql = strsql + " group by ld_clientcd,ld_dpid having sum(ld_amount)<> 0 ";
                strsql = strsql + " union all ";
                strsql = strsql + " select  '" + Fromdt + "' as FromDate, '" + Todt + "' as ToDate,substring(ld_dpid,2,2) as ExCode, 'NBFC' ExchangeTitle,'', ";
                strsql = strsql + " ld_clientcd, ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) as ld_dt ,cast(ld_amount as decimal(15,2)) ld_amount ,ld_particular,ld_debitflag,ld_chequeno,ld_documenttype,ld_common,convert(datetime,ld_dt) Ldate,ld_dpid ";
                strsql = strsql + " from NBFC_Ledger  with (nolock) ";
                strsql = strsql + " where ld_clientcd = '" + Username + "'  ";
                strsql = strsql + "  and ld_dt between '" + Fromdt + "' and '" + Todt + "'";
            }
            string aa = string.IsNullOrEmpty(ExchangeCodeMar) ? "5" : "Hemant";
            if (!string.IsNullOrEmpty(ExchangeTitlemar) && !string.IsNullOrEmpty(ExchangeCodeMar))
            {
                if (blnexseg == true)
                {
                    strsql = strsql + " union all ";
                }
                strsql = strsql + " select '" + Fromdt + "' as FromDate, '" + Todt + "' as ToDate,  substring(ld_dpid,2,2) as ExCode," + ExchangeTitlemar.Substring(0, ExchangeTitlemar.Length - 5) + " End as ExchangeTitle,";
                strsql = strsql + " 'C' as acc,ld_clientcd,convert(char,convert(datetime,'" + Fromdt + "'),103) ld_dt,cast(sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then ld_amount ";
                strsql = strsql + " else 0 end)as decimal(15,2)) as ld_amount,'Opening Balance' ld_particular, case sign(sum(ld_amount)) when 1 then 'D' else 'C' end as ";
                strsql = strsql + " ld_debitflag ,'' ld_chequeno, 'O' ld_documenttype,''ld_common ,convert(datetime,'" + Fromdt + "') Ldate,ld_dpid";
                strsql = strsql + " from " + strTable + " with (nolock) ";
                strsql = strsql + " where ld_clientcd = (select distinct cm_brkggroup from client_master where cm_cd='" + Username + "')  ";
                strsql = strsql + " and ld_dt < '" + Fromdt + "' and right(rtrim(ld_dpid),2)in (" + ExchangeCodemarIn + ")";
                strsql = strsql + " group by ld_clientcd,ld_dpid having sum(ld_amount)<> 0 ";
                strsql = strsql + " union all ";
                strsql = strsql + " select  '" + Fromdt + "' as FromDate, '" + Todt + "' as ToDate,substring(ld_dpid,2,2) as ExCode," + ExchangeTitlemar.Substring(0, ExchangeTitlemar.Length - 5) + " End as ExchangeTitle,'', ";
                strsql = strsql + " ld_clientcd, ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) as ld_dt ,cast(ld_amount as decimal(15,2)),ld_particular,ld_debitflag,ld_chequeno,ld_documenttype,ld_common,convert(datetime,ld_dt) Ldate,ld_dpid ";
                strsql = strsql + " from " + strTable + " with (nolock) ";
                strsql = strsql + " where ld_clientcd = (select distinct cm_brkggroup from client_master where cm_cd='" + Username + "')  ";
                strsql = strsql + "  and ld_dt between '" + Fromdt + "' and '" + Todt + "' and right(rtrim(ld_dpid),2)in (" + ExchangeCodemarIn + ")";
                //if (ExchCommCode == string.Empty && ExchCommTitle == string.Empty)
                //    strsql = strsql + " order by Ldate";
            }

            //Cledger===============
            if (!string.IsNullOrEmpty(ExchCommCode) && !string.IsNullOrEmpty(ExchCommTitle)) //MCX and NCDEX
            {
                if ((!string.IsNullOrEmpty(ExchangeTitle) && !string.IsNullOrEmpty(ExchangeCode)) || (!string.IsNullOrEmpty(ExchangeTitlemar) && !string.IsNullOrEmpty(ExchangeCodeMar)))
                {
                    strsql = strsql + " union all ";
                }
                strsql = strsql + " select '" + Fromdt + "' as FromDate, '" + Todt + "' as ToDate,  substring(ld_dpid,2,2) as ExCode," + ExchCommTitle.Substring(0, ExchCommTitle.Length - 5) + " End as ExchangeTitle,";
                strsql = strsql + " 'C' as acc,ld_clientcd, Convert(char,Convert(datetime,'" + Fromdt + "'),103) ld_dt,cast(sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then ld_amount ";
                strsql = strsql + " else 0 end) as decimal(15,2)) as ld_amount,'Opening Balance' ld_particular, case sign(sum(ld_amount)) when 1 then 'D' else 'C' end as ";
                strsql = strsql + " ld_debitflag ,'' ld_chequeno, 'O' ld_documenttype,''ld_common,Convert(char,Convert(datetime,'" + Fromdt + "'),103) as Ldate,ld_dpid ";
                strsql = strsql + " from " + strCommTable + "";
                strsql = strsql + " where ld_clientcd = '" + Username + "' and ld_dt < '" + Fromdt + "' ";
                strsql = strsql + " and right(rtrim(ld_dpid),2)in (" + ExchangeCommCodeIn + ")";
                strsql = strsql + " group by ld_clientcd,ld_dpid having sum(ld_amount)<> 0 ";
                strsql = strsql + " union all ";
                strsql = strsql + " select  '" + Fromdt + "' as FromDate, '" + Todt + "' as ToDate,substring(ld_dpid,2,2) as ExCode," + ExchCommTitle.Substring(0, ExchCommTitle.Length - 5) + " End as ExchCommTitle,'', ";
                strsql = strsql + " ld_clientcd, ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) as ld_dt ,cast(ld_amount as decimal(15,2)),ld_particular,ld_debitflag,ld_chequeno,ld_documenttype,ld_common,convert(datetime,ld_dt) Ldate,ld_dpid ";
                strsql = strsql + " from " + strCommTable + "";
                strsql = strsql + " where ld_clientcd = '" + Username + "' and ld_dt between '" + Fromdt + "' and '" + Todt + "'";
                strsql = strsql + " and right(rtrim(ld_dpid),2)in (" + ExchangeCommCodeIn + ")";
                //strsql = strsql + " order by Ldate";
            }
            if (!string.IsNullOrEmpty(ExchCommCodeMar) && !string.IsNullOrEmpty(ExchCommTitleMar))
            {
                if ((!string.IsNullOrEmpty(ExchangeTitle) && !string.IsNullOrEmpty(ExchangeCode)) || (!string.IsNullOrEmpty(ExchangeTitlemar) && !string.IsNullOrEmpty(ExchangeCodeMar)) ||
                    (!string.IsNullOrEmpty(ExchCommCode) && !string.IsNullOrEmpty(ExchCommTitle)))
                {
                    strsql = strsql + " union all ";
                }
                strsql = strsql + " select '" + Fromdt + "' as FromDate, '" + Todt + "' as ToDate,  substring(ld_dpid,2,2) as ExCode," + ExchCommTitleMar.Substring(0, ExchCommTitleMar.Length - 5) + " End as ExchangeTitle,";
                strsql = strsql + " 'C' as acc,ld_clientcd, Convert(char,Convert(datetime,'" + Fromdt + "'),103) ld_dt,cast(sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then ld_amount ";
                strsql = strsql + " else 0 end) as decimal(15,2)) as ld_amount,'Opening Balance' ld_particular, case sign(sum(ld_amount)) when 1 then 'D' else 'C' end as ";
                strsql = strsql + " ld_debitflag ,'' ld_chequeno, 'O' ld_documenttype,''ld_common,Convert(char,Convert(datetime,'" + Fromdt + "'),103) as Ldate,ld_dpid ";
                strsql = strsql + " from " + strCommTable + " where  ld_clientcd=(select distinct cm_brkggroup from client_master where cm_cd='" + Username + "') ";
                strsql = strsql + " and ld_dt < '" + Fromdt + "' ";
                strsql = strsql + " and right(rtrim(ld_dpid),2)in (" + ExchangeCommMarIn + ")";
                strsql = strsql + " group by ld_clientcd,ld_dpid having sum(ld_amount)<> 0 ";
                strsql = strsql + " union all ";
                strsql = strsql + " select  '" + Fromdt + "' as FromDate, '" + Todt + "' as ToDate,substring(ld_dpid,2,2) as ExCode," + ExchCommTitleMar.Substring(0, ExchCommTitleMar.Length - 5) + " End as ExchCommTitle,'', ";
                strsql = strsql + " ld_clientcd, ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) as ld_dt ,cast(ld_amount as decimal(15,2)),ld_particular,ld_debitflag,ld_chequeno,ld_documenttype,ld_common,convert(datetime,ld_dt) Ldate,ld_dpid ";
                strsql = strsql + " from " + strCommTable + " where  ld_clientcd=(select distinct cm_brkggroup from " + strCommClientMaster + " where cm_cd='" + Username + "') ";
                strsql = strsql + " and ld_dt between '" + Fromdt + "' and '" + Todt + "'";
                strsql = strsql + " and right(rtrim(ld_dpid),2)in (" + ExchangeCommMarIn + ")";
            }
            strsql = strsql + " order by Ldate";

            return strsql;
        }
        #endregion
    }
}
