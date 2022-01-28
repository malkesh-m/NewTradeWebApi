﻿using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.IdentityModel.Tokens;
using Microsoft.VisualBasic;
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
        private string strsql = "";
        private string strConnecton = "";
        private string strTable = "";
        private string strsegment = "";
        private string strexchange = "";
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

        #region Ledger Api

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

        /// <summary>
        /// GetLedgerDetailsData
        /// </summary>
        /// <param name="exchangeTitle"></param>
        /// <param name="exchangeCode"></param>
        /// <param name="blnExSeg"></param>
        /// <param name="fromDate"></param>
        /// <param name="toDate"></param>
        /// <param name="exchangeCodeIn"></param>
        /// <param name="exchangeMTFTitle"></param>
        /// <param name="exchangeMTFCodeIn"></param>
        /// <param name="NBFC"></param>
        /// <param name="exchangeTitleMar"></param>
        /// <param name="exchangeCodeMar"></param>
        /// <param name="exchangeCodeMarIn"></param>
        /// <param name="exchCommCode"></param>
        /// <param name="exchCommTitle"></param>
        /// <param name="exchCommCodeMar"></param>
        /// <param name="exchCommTitleMar"></param>
        /// <param name="exchangeCommCodeIn"></param>
        /// <param name="exchangeCommMarIn"></param>
        /// <returns></returns>
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetLedgerDetailsData", Name = "GetLedgerDetailsData")]
        public IActionResult GetLedgerDetailsData(string exchangeTitle, string exchangeCode, Boolean blnExSeg, string fromDate, string toDate, string exchangeCodeIn, string exchangeMTFTitle, string exchangeMTFCodeIn, string NBFC, string exchangeTitleMar, string exchangeCodeMar, string exchangeCodeMarIn, string exchCommCode, string exchCommTitle, string exchCommCodeMar, string exchCommTitleMar, string exchangeCommCodeIn, string exchangeCommMarIn)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var bb = _configuration["Commex"];

                    var tokenS = GetToken();
                    var userName = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = GetLedgerDetailsData(userName, exchangeTitle, exchangeCode, blnExSeg, fromDate, toDate, exchangeCodeIn, exchangeMTFTitle, exchangeMTFCodeIn, NBFC, exchangeTitleMar, exchangeCodeMar, exchangeCodeMarIn, exchCommCode, exchCommTitle, exchCommCodeMar, exchCommTitleMar, exchangeCommCodeIn, exchangeCommMarIn);
                    if (getData != null)
                    {
                        return Ok(new commonResponse { status = true, message = "success", status_code = (int)HttpStatusCode.OK, data = getData });
                    }
                    else
                    {
                        return NotFound(new commonResponse { status = false, message = "blank", status_code = (int)HttpStatusCode.NotFound, error_message = "records not found" });
                    }
                }
                catch (Exception ex)
                {
                    return BadRequest(new commonResponse { status = false, message = "error", status_code = (int)HttpStatusCode.InternalServerError, error_message = ex.Message.ToString() });
                }
            }
            return BadRequest();
        }
        #endregion

        #region Transaction Api
        /// <summary>
        ///   Transaction API  
        /// </summary>
        /// <param name="tradeType"></param>
        /// <param name="selectType"></param>
        /// <param name="cashType"></param>
        /// <param name="stockType"></param>
        /// <param name="fromDate"></param>
        /// <param name="toDate"></param>
        // TODO : For getting Transaction main form data
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetTransaction", Name = "GetTransaction")]
        public IActionResult GetTransaction([FromQuery] string tradeType, string selectType, string cashType, string stockType, string fromDate, string toDate)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var companyCode = tokenS.Claims.First(claim => claim.Type == "companyCode").Value;
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    //TransactionHandler _handler = new TransactionHandler();
                    var getData = GetTransactionData(userId, tradeType, selectType, cashType, stockType, companyCode, fromDate, toDate);

                    if (getData != null)
                    {
                        return Ok(new commonResponse { status = true, message = "success", status_code = (int)HttpStatusCode.OK, data = getData });
                    }
                    else
                    {
                        return NotFound(new commonResponse { status = false, message = "blank", status_code = (int)HttpStatusCode.NotFound, error_message = "records not found" });
                    }

                }
                catch (Exception ex)
                {
                    return BadRequest(new commonResponse { status = false, message = "error", status_code = (int)HttpStatusCode.InternalServerError, error_message = ex.Message.ToString() });
                }
            }
            return BadRequest();
        }


        /// <summary>
        ///   Transaction API  
        /// </summary>
        /// <param name="transactionType"></param>
        /// <param name="linkCode"></param>
        /// <param name="tradeScripnm"></param>
        /// <param name="fromDate"></param>
        /// <param name="toDate"></param>
        // TODO : For getting Itemwise transaction details data
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetItemWiseTransactionDetails", Name = "GetItemWiseTransactionDetails")]
        public IActionResult GetItemWiseTransactionDetails([FromQuery] string transactionType, string linkCode, string tradeScripnm, string fromDate, string toDate)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = ItemWiseDetails(userId, transactionType, linkCode, tradeScripnm, fromDate, toDate);

                    if (getData != null)
                    {
                        return Ok(new commonResponse { status = true, message = "success", status_code = (int)HttpStatusCode.OK, data = getData });
                    }
                    else
                    {
                        return NotFound(new commonResponse { status = true, message = "blank", status_code = (int)HttpStatusCode.NotFound, error_message = "records not found" });
                    }
                }
                catch (Exception ex)
                {
                    return BadRequest(new commonResponse { status = false, message = "error", status_code = (int)HttpStatusCode.InternalServerError, error_message = ex.Message.ToString() });
                }
            }
            return BadRequest();
        }


        /// <summary>
        ///   Transaction API  
        /// </summary>
        /// <param name="tradeType"></param>
        /// <param name="linkCode"></param>
        /// <param name="settelment"></param>
        /// <param name="date"></param>
        /// <param name="fromDate"></param>
        /// <param name="toDate"></param>
        /// <param name="header"></param>
        // TODO : For getting Itemwise transaction details data
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetDateWiseTransactionDetails", Name = "GetDateWiseTransactionDetails")]
        public IActionResult GetDateWiseTransactionDetails([FromQuery] string tradeType, string linkCode, string settelment, string date, string fromDate, string toDate, string header)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = DateWiseDetails(userId, tradeType, linkCode, settelment, date, fromDate, toDate, header);

                    if (getData != null)
                    {
                        return Ok(new commonResponse { status = true, message = "success", status_code = (int)HttpStatusCode.OK, data = getData });
                    }
                    else
                    {
                        return NotFound(new commonResponse { status = true, message = "blank", status_code = (int)HttpStatusCode.NotFound, error_message = "records not found" });
                    }
                }
                catch (Exception ex)
                {
                    return BadRequest(new commonResponse { status = false, message = "error", status_code = (int)HttpStatusCode.InternalServerError, error_message = ex.Message.ToString() });
                }
            }
            return BadRequest();
        }
        #endregion

        #region OutStanding Api
        /// <summary>
        ///  OutStanding data 
        /// </summary>
        /// <returns></returns>
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetOutStanding", Name = "GetOutStanding")]
        public IActionResult GetOutStanding()
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = GetOutStandingData(userId);
                    if (getData != null)
                    {
                        return Ok(new commonResponse { status = true, message = "success", status_code = (int)HttpStatusCode.OK, data = getData });
                    }
                    else
                    {
                        return NotFound(new commonResponse { status = false, message = "blank", status_code = (int)HttpStatusCode.NotFound, error_message = "records not found" });
                    }
                }
                catch (Exception ex)
                {
                    return BadRequest(new commonResponse { status = false, message = "error", status_code = (int)HttpStatusCode.InternalServerError, error_message = ex.Message.ToString() });
                }
            }
            return BadRequest();
        }

        /// <summary>
        /// OutStanding details data
        /// </summary>
        /// <param name="order"></param>
        /// <param name="seriesId"></param>
        /// <param name="seriesId"></param>
        /// <param name="exchange"></param>
        /// <param name="segment"></param>
        /// <returns></returns>
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetOutStandingDetailData", Name = "GetOutStandingDetailData")]
        public IActionResult GetOutStandingDetailData([FromQuery] string order, string seriesId, string exchange, string segment)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = GetOutStandingDetailData(userId, order, seriesId, exchange, segment);
                    if (getData != null)
                    {
                        return Ok(new commonResponse { status = true, message = "success", status_code = (int)HttpStatusCode.OK, data = getData });
                    }
                    else
                    {
                        return NotFound(new commonResponse { status = false, message = "blank", status_code = (int)HttpStatusCode.NotFound, error_message = "records not found" });
                    }
                }
                catch (Exception ex)
                {
                    return BadRequest(new commonResponse { status = false, message = "error", status_code = (int)HttpStatusCode.InternalServerError, error_message = ex.Message.ToString() });
                }
            }
            return BadRequest();
        }
        #endregion

        #region Portfolio Api

        /// <summary>
        /// Get Portfolio Main Data
        /// </summary>
        /// <param name="segmentType"></param>
        /// <param name="exchangeType"></param>
        /// <param name="fromDate"></param>
        /// <param name="toDate"></param>
        /// <returns></returns>
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetPortfolioData", Name = "GetPortfolioData")]
        public IActionResult GetPortfolioData([FromQuery] string segmentType, string exchangeType, string fromDate, string toDate)
        {
            if (ModelState.IsValid)
            {
                #region
                try
                {
                    var tokenS = GetToken();
                    var companyCode = tokenS.Claims.First(claim => claim.Type == "companyCode").Value;
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = GetPortfolioData(userId, segmentType, exchangeType, fromDate, toDate, companyCode);
                    if (getData != null)
                    {
                        return Ok(new commonResponse { status = true, message = "success", status_code = (int)HttpStatusCode.OK, data = getData });
                    }
                    else
                    {
                        return NotFound(new commonResponse { status = false, message = "blank", status_code = (int)HttpStatusCode.NotFound, error_message = "records not found" });
                    }
                }
                catch (Exception ex)
                {
                    return BadRequest(new commonResponse { status = false, message = "error", status_code = (int)HttpStatusCode.InternalServerError, error_message = ex.Message.ToString() });
                }
                #endregion
            }
            return BadRequest();
        }


        /// <summary>
        /// Get Portfolio Detail Data
        /// </summary>
        /// <param name="fromDate"></param>
        /// <param name="toDate"></param>
        /// <param name="scripcd"></param>
        /// <returns></returns>
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetPortfolioDetailData", Name = "GetPortfolioDetailData")]
        public IActionResult GetPortfolioDetailData([FromQuery] string fromDate, string toDate, string scripcd)
        {
            if (ModelState.IsValid)
            {
                #region
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = Get_Portfolio_Detail_Data(userId, fromDate, toDate, scripcd);
                    if (getData != null)
                    {
                        return Ok(new commonResponse { status = true, message = "success", status_code = (int)HttpStatusCode.OK, data = getData });
                    }
                    else
                    {
                        return NotFound(new commonResponse { status = false, message = "blank", status_code = (int)HttpStatusCode.NotFound, error_message = "records not found" });
                    }
                }
                catch (Exception ex)
                {
                    return BadRequest(new commonResponse { status = false, message = "error", status_code = (int)HttpStatusCode.InternalServerError, error_message = ex.Message.ToString() });
                }
                #endregion
            }
            return BadRequest();
        }
        #endregion

        #region Holding Api

        /// <summary>
        ///  Get Holding Page Load Data
        /// </summary> 
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetHoldingPageLoadData", Name = "GetHoldingPageLoadData")]
        public IActionResult GetHoldingPageLoadData()
        {
            if (ModelState.IsValid)
            {
                #region
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = GetPageLoadData(userId);
                    if (getData != null)
                    {
                        return Ok(new commonResponse { status = true, message = "success", status_code = (int)HttpStatusCode.OK, data = getData });
                    }
                    else
                    {
                        return NotFound(new commonResponse { status = false, message = "blank", status_code = (int)HttpStatusCode.NotFound, error_message = "records not found" });
                    }
                }
                catch (Exception ex)
                {
                    return BadRequest(new commonResponse { status = false, message = "error", status_code = (int)HttpStatusCode.InternalServerError, error_message = ex.Message.ToString() });
                }
                #endregion
            }
            return BadRequest();
        }

        /// <summary>
        /// Get Current Holding grid data
        /// </summary>
        /// <param name="selectType"></param>
        /// <param name="holdingType"></param>
        /// <param name="boId"></param>
        /// <returns></returns>
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetCurrentHoldingData", Name = "GetCurrentHoldingData")]
        public IActionResult GetCurrentHoldingData(string selectType, string holdingType, string boId)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var dd = _configuration["Cross"];
                    var getData = GetCurrentHoldingDataHandler(selectType, holdingType, boId);
                    if (getData != null)
                    {
                        return Ok(new commonResponse { status = true, message = "success", status_code = (int)HttpStatusCode.OK, data = getData });
                    }
                    else
                    {
                        return NotFound(new commonResponse { status = false, message = "blank", status_code = (int)HttpStatusCode.NotFound, error_message = "records not found" });
                    }
                }
                catch (Exception ex)
                {
                    return BadRequest(new commonResponse { status = false, message = "error", status_code = (int)HttpStatusCode.InternalServerError, error_message = ex.Message.ToString() });
                }
            }
            return BadRequest();
        }

        #endregion
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

        #region Ledger handler methods
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
        #endregion

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

        #region Transaction Handler methods
        //TODO : For getting Transaction data
        private dynamic GetTransactionData(string cm_cd, string Trade_Type, string Select_Type, string Cash_Type, string Stock_Type, string CompanyCode, string FromDate, string ToDate)
        {
            DataSet DsNew = new DataSet();
            if (Trade_Type == "Trades" || Trade_Type == "Mutual Fund")
            {
                BindGrid(cm_cd, Trade_Type, Select_Type, Cash_Type, Stock_Type, CompanyCode, FromDate, ToDate);
            }

            else if (Trade_Type == "AGTS")
            {
                DsNew = BindGridAGTS(cm_cd, Trade_Type, Select_Type, Cash_Type, CompanyCode, FromDate, ToDate);
            }

            else if (Trade_Type == "Deliveries")
            {
                BindGridDPHolding(cm_cd, Trade_Type, Select_Type, FromDate, ToDate);
            }

            else if (Trade_Type == "Receipts" || Trade_Type == "Payments" || Trade_Type == "Journals" || Trade_Type == "Bills")
            {
                ShowGridLedger(cm_cd, Trade_Type, Select_Type, FromDate, ToDate);
            }

            // TODO : Fill dataset by using dynamic query and pass that dataset to Web application.
            List<string> entityList = new List<string>();
            try
            {
                if (Trade_Type == "AGTS")
                {
                    if (DsNew != null)
                    {
                        var json = JsonConvert.SerializeObject(DsNew.Tables[0]);
                        return json;
                    }
                }
                else
                {
                    var ds = CommonRepository.FillDataset(strsql);
                    if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                    {
                        var json = JsonConvert.SerializeObject(ds.Tables[0], Formatting.Indented);
                        return json;
                    }
                }
                return entityList.ToList();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private void BindGrid(string cm_cd, string Trade_Type, string Select_Type, string Cash_Type, string Stock_Type, string CompanyCode, string FromDate, string ToDate)
        {
            if (Trade_Type == "Trades" || Trade_Type == "Deliveries")
            {
                if (Select_Type == "0")
                {
                    ShowMasterGridItemWise(cm_cd, Trade_Type, Select_Type, FromDate, ToDate);
                }
                else
                {
                    ShowMasterGridDateWise(cm_cd, Trade_Type, Select_Type, FromDate, ToDate);
                }
            }

            else if (Trade_Type == "Mutual Fund")
            {
                BindGridMF(cm_cd, Trade_Type, Select_Type, Stock_Type, CompanyCode, FromDate, ToDate);
            }
            else
            {
                ShowGridLedger(cm_cd, Trade_Type, Select_Type, FromDate, ToDate);
            }
        }

        private void ShowMasterGridItemWise(string cm_cd, string Trade_Type, string Select_Type, string FromDate, string ToDate)
        {
            string StrTradesIndex = "";
            string StrComTradesIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
            { StrTradesIndex = "index(idx_trades_clientcd),"; }

            string StrTRXIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'Trx' and b.name", "idx_Trx_Clientcd", true)) == 1)
            { StrTRXIndex = "index(idx_trx_clientcd),"; }

            DataSet dt = new DataSet();
            if (_configuration["IsTradeWeb"] == "O")//Live DB
            {

                strsql = " select 1 Td_order,'' as td_ac_type,'' as td_trxdate,'' as td_isin_code,'' as sc_company_name,";
                strsql = strsql + " cast((case when sum(td_bqty-td_sqty)=0 then 0 else sum((td_bqty-td_sqty)*td_rate)/sum(td_bqty-td_sqty) end)as";
                strsql = strsql + " decimal(15,4) )   as rate,'Equity' Td_Type,'' as FScripNm,'' as FExDt,";
                strsql = strsql + " rtrim(td_scripcd)td_scripnm , rtrim(ss_name) snm,";
                strsql = strsql + " sum(td_bqty) Bqty, convert(decimal(15,2), sum(td_bqty*td_rate)) BAmt, ";
                strsql = strsql + " sum(td_sqty) Sqty, convert(decimal(15,2), sum(td_sqty*td_rate)) SAmt, sum(td_bqty-td_sqty) NQty, convert(decimal(15,2), sum((td_bqty-td_sqty)*td_rate)) NAmt, ";
                strsql = strsql + " '' as td_debit_credit,0 as sm_strikeprice,'' as sm_callput,'Equity|'+''+'|'+td_scripcd  LinkCode ";
                strsql = strsql + " from Trx with (" + StrTRXIndex + "nolock) , securities with(nolock)";
                strsql = strsql + " where td_clientcd='" + cm_cd + "' and td_dt between '" + FromDate + "' and '" + ToDate + "' ";
                strsql = strsql + " and td_Scripcd = ss_cd";
                strsql = strsql + " group by td_scripcd, ss_name,'Equity|'+''+'|'+td_scripcd ";
            }
            else//TradeWeb-pragya
            {
                strsql = "select 1 Td_order,'' as td_ac_type,'' as td_trxdate,'' as td_isin_code,'' as sc_company_name, ";
                strsql = strsql + " cast((case when sum(td_bqty-td_sqty)=0 then 0 else sum((td_bqty-td_sqty)*td_rate)/sum(td_bqty-td_sqty) end)as decimal(15,4) )   as rate,";
                strsql = strsql + "'Equity' Td_Type,'' as FScripNm,'' as FExDt, rtrim(td_scripcd) td_scripnm, ";
                strsql = strsql + " rtrim(td_scripnm) snm, sum(td_bqty) Bqty, convert(decimal(15,2),sum(td_bqty*td_rate)) BAmt, ";
                strsql = strsql + "sum(td_sqty) Sqty, convert(decimal(15,2),sum(td_sqty*td_rate)) SAmt, sum(td_bqty-td_sqty) NQty, convert(decimal(15,2),sum((td_bqty-td_sqty)*td_rate)) NAmt, ";
                strsql = strsql + "'' as td_debit_credit,0 as sm_strikeprice,'' as sm_callput,'Equity|'+''+'|'+td_scripcd LinkCode";
                strsql = strsql + " from trx with (" + StrTRXIndex + "nolock) where td_clientcd='" + cm_cd + "' ";
                strsql = strsql + "and td_dt between '" + FromDate + "' and '" + ToDate + "' ";
                strsql = strsql + "group by td_scripcd, td_scripnm ,'Equity|'+''+'|'+td_scripcd ";
            }
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
            strsql = strsql + " where td_clientcd='" + cm_cd + "' and sm_exchange=td_exchange and sm_Segment=td_Segment and td_seriesid=sm_seriesid ";
            strsql = strsql + " and td_dt between '" + FromDate + "' and '" + ToDate + "' and td_trxflag <> 'O'  ";
            strsql = strsql + " group by sm_symbol, sm_productcd,td_exchange,td_Segment, sm_expirydt, sm_strikeprice, sm_callput ,sm_optionstyle";
            strsql = strsql + " union all ";
            strsql = strsql + " select 4 ,'','' as td_trxdate,'' as td_isin_code,'' as sc_company_name,cast((case when  sum(ex_aqty-ex_eqty)=0 then 0 else sum((ex_aqty-ex_eqty)*ex_diffrate *case ex_eaflag When 'A' Then -1 else 1 end)/sum(ex_aqty-ex_eqty) end)as decimal(15,2) ) as rate , ";
            strsql = strsql + " Case When ex_Segment='K' then 'Currency ' else 'Equity ' end + case ex_eaflag when 'E' then 'Exercise ' else 'Assignment ' end Td_Type, '','', rtrim(sm_symbol), case left(sm_productcd,1) when 'F' then 'Fut ' else 'Opt ' end+ rtrim(sm_symbol)+'  Exp: '+ ltrim(rtrim(convert(char,convert(datetime,sm_expirydt),103))) + case left(sm_productcd,1) when 'F' then '' else ''+rtrim(convert(char(9),sm_strikeprice))+sm_callput+sm_optionstyle end, sum(ex_aqty) Bqty, ";
            strsql = strsql + " convert(decimal(15,2),sum(ex_aqty*ex_diffrate *case ex_eaflag When 'A' Then -1 else 1 end *sm_multiplier)) BAmt, sum(ex_eqty) Sqty, convert(decimal(15,2),sum(ex_eqty*ex_diffrate *case ex_eaflag When 'A' Then -1 else 1 end  *sm_multiplier)) SAmt, ";
            strsql = strsql + " sum(ex_aqty-ex_eqty) NQty, convert(decimal(15,2),sum((ex_aqty-ex_eqty)*ex_diffrate *case ex_eaflag When 'A' Then -1 else 1 end    *sm_multiplier)) NAmt,'' as td_debit_credit,0,'', ";
            strsql = strsql + " Case When ex_segment='K' then 'Currency' else 'Equity' end + ";
            strsql = strsql + " case ex_eaflag when 'E' then 'Exercise' else 'Assignment' end + '|' + '' + '|' + replace(sm_symbol,'&','-')  + '|' + left(sm_productcd,1) + '|' + sm_expirydt + '|' + Rtrim(Ltrim(Convert(char,sm_strikeprice))) + '|' +  sm_callput + '|' +  sm_optionstyle + '|' +  ex_Segment LinkCode";
            strsql = strsql + " from exercise with (nolock), series_master with (nolock) where ex_clientcd='" + cm_cd + "' ";
            strsql = strsql + " and ex_exchange=sm_exchange and ex_Segment=sm_Segment and ex_seriesid=sm_seriesid ";
            strsql = strsql + " and ex_dt between '" + FromDate + "' and '" + ToDate + "' group by ex_eaflag, sm_symbol,ex_Segment,sm_productcd,sm_expirydt, sm_strikeprice, sm_callput ,sm_optionstyle ";

            if (_configuration["IsTradeWeb"] == "O")//Live
            {
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
                    strsql = strsql + " where td_clientcd='" + cm_cd + "' and sm_exchange=td_exchange and td_seriesid=sm_seriesid and td_dt ";
                    strsql = strsql + " between '" + FromDate + "' and '" + ToDate + "' and td_trxflag <> 'O'  ";
                    strsql = strsql + " group by sm_symbol, sm_productcd,sm_expirydt, sm_strikeprice, sm_callput  ";
                    strsql = strsql + " order by td_order, td_type, snm ,td_scripnm";
                }
            }
            else
            {
                string StrCTradesIndex = "";
                if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'Ctrades' and b.name", "idx_Ctrades_clientcd", true)) == 1)
                { StrCTradesIndex = "index(idx_Ctrades_clientcd),"; }

                strsql = strsql + " union all ";
                strsql = strsql + " select case left(sm_productcd,1) when 'F' then 5 else 6 end,'', '','' as td_isin_code,";
                strsql = strsql + " '' as sc_company_name,   cast((case when  sum(td_bqty-td_sqty)=0 then 0 else ";
                strsql = strsql + " sum((td_bqty-td_sqty)*td_rate)/sum(td_bqty-td_sqty) end)as decimal(15,4) ) as rate,";
                strsql = strsql + " 'Commodities ' Td_Type,rtrim(sm_symbol), sm_expirydt,rtrim(sm_symbol), case left(sm_productcd,1) ";
                strsql = strsql + " when 'F' then 'Fut ' else 'Opt ' end+ rtrim(sm_symbol)+'  Exp: '+ ";
                strsql = strsql + " ltrim(rtrim(convert(char,convert(datetime,sm_expirydt),103))) + ";
                strsql = strsql + " case left(sm_productcd,1) when 'F' then '' else ''+rtrim(convert(char(9),sm_strikeprice))+sm_callput end, ";
                strsql = strsql + " sum(td_bqty) Bqty, convert(decimal(15,2), sum(td_bqty*td_rate *sm_multiplier)) BAmt,  sum(td_sqty) Sqty, convert(decimal(15,2), sum(td_sqty*td_rate*sm_multiplier)) SAmt, ";
                strsql = strsql + " sum(td_bqty-td_sqty) NQty, convert(decimal(15,2),sum((td_bqty-td_sqty)*td_rate*sm_multiplier)) NAmt,'' as td_debit_credit ,sm_strikeprice, sm_callput,";
                strsql = strsql + " 'Commodities' + '|' + '' + '|' + replace(sm_symbol,'&','-')  + '|' + sm_expirydt LinkCode";
                strsql = strsql + " from Ctrades with (" + StrCTradesIndex + "nolock), Cseries_master with(nolock) ";
                strsql = strsql + " where td_clientcd='" + cm_cd + "' and sm_exchange=td_exchange and td_seriesid=sm_seriesid and td_dt ";
                strsql = strsql + " between '" + FromDate + "' and '" + ToDate + "' and td_trxflag <> 'O'  ";
                strsql = strsql + " group by sm_symbol, sm_productcd,sm_expirydt, sm_strikeprice, sm_callput  ";
                strsql = strsql + " order by Td_order,td_type, snm, td_scripnm";
            }

        }

        private void ShowMasterGridDateWise(string cm_cd, string Trade_Type, string Select_Type, string FromDate, string ToDate)
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
            strsql = strsql + "convert(decimal(15,2),sum((td_bqty-td_sqty)*td_rate)) NAmt, 'Equity|'+Left(td_Stlmnt,1)+'|' LinkCode, td_dt Dt1  from trx with (" + StrTRXIndex + "nolock) where td_clientcd='" + cm_cd + "' and ";
            strsql = strsql + "td_dt between '" + FromDate + "' and '" + ToDate + "' group by  td_stlmnt,td_dt  ";
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
            strsql = strsql + "where td_clientcd='" + cm_cd + "' and sm_exchange=td_exchange and sm_Segment=td_Segment and td_seriesid=sm_seriesid and  ";
            strsql = strsql + "td_dt between '" + FromDate + "' and '" + ToDate + "' and td_trxflag <> 'O' ";
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
            strsql = strsql + "where ex_clientcd='" + cm_cd + "' and ex_exchange=sm_exchange and ex_Segment=sm_Segment and ex_seriesid=sm_seriesid and  ";
            strsql = strsql + "ex_dt between '" + FromDate + "' and '" + ToDate + "' group by ex_Dt,ex_eaflag ,ex_exchange,ex_Segment,sm_prodtype   ";

            if (_configuration["IsTradeWeb"] == "O")
            {
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
                    strsql = strsql + "where td_clientcd='" + cm_cd + "' and sm_exchange=td_exchange and td_seriesid=sm_seriesid and  ";
                    strsql = strsql + "td_dt between '" + FromDate + "' and '" + ToDate + "' group by td_Dt,sm_productcd,td_exchange ,sm_prodtype  ";
                }
            }
            else
            {
                string StrCTradesIndex = "";
                if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'Ctrades' and b.name", "idx_Ctrades_clientcd", true)) == 1)
                { StrCTradesIndex = "index(idx_Ctrades_clientcd),"; }

                strsql = strsql + "union all  ";
                strsql = strsql + "select case left(sm_productcd,1) when 'F' then 6 else 7 end , ";
                strsql = strsql + " 'Commodities ' + '[' + case td_exchange When 'M' Then 'MCX' When 'N' Then 'NCDEX' else '' end + ']' Td_Type, ";
                strsql = strsql + " ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) Dt, 'Commodity' + case right(sm_prodtype,1) when 'F' then ' Future' else ' Option' end, sum(td_bqty) Bqty,  ";
                strsql = strsql + " convert(decimal(15,2),sum(td_bqty*td_rate*sm_multiplier)) BAmt, sum(td_sqty) Sqty, convert(decimal(15,2),sum(td_sqty*td_rate*sm_multiplier)) SAmt,  ";
                strsql = strsql + "sum(td_bqty-td_sqty) NQty, ";
                strsql = strsql + "convert(decimal(15,2),sum((td_bqty-td_sqty)*td_rate*sm_multiplier)) NAmt,'Commodities' + '|' + td_exchange + '|' +   + '|'  LinkCode ,td_dt Dt1 ";
                strsql = strsql + "from ctrades with (" + StrCTradesIndex + "nolock), Cseries_master with (nolock)  ";
                strsql = strsql + "where td_clientcd='" + cm_cd + "' and sm_exchange=td_exchange and td_seriesid=sm_seriesid and  ";
                strsql = strsql + "td_dt between '" + FromDate + "' and '" + ToDate + "' and td_trxflag <> 'O' group by td_Dt,sm_productcd,td_exchange ,sm_prodtype  ";
            }
            if (_configuration["IsTradeWeb"] == "O")
            {

                string strsql1 = "select da_DPID from dematact with (nolock) where da_clientcd='" + cm_cd + "' and da_defaultYN = 'Y'";
                dt = objUtility.OpenDataSet(strsql1);
                strConnecton = dt.Tables[0].Rows[0][0].ToString().Trim();

                if (_configuration["Cross"] != "" && Strings.Left(strConnecton, 2) != "IN")
                {
                    string[] ArrCross = _configuration["Cross"].Split(ArrSeparators);
                    strsql = strsql + " union all ";
                    strsql = strsql + " select 8 Td_order,'DP Transaction' Td_Type, convert(char, td_trxdate,112)Dt ,'' ,  sum(case td_debit_credit when 'C' then ";
                    strsql = strsql + " td_qty else 0 end ) Bqty , 0 ,  sum(case td_debit_credit when 'D' then td_qty else 0 end ) sqty , 0,0 NQty, 0 NAmt,'DP Transaction' + '|'   LinkCode ,td_trxdate Dt1 ";
                    strsql = strsql + " from " + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".TrxDetail a with (nolock) , " + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".security with (nolock), " + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".client_master with (nolock) ";
                    strsql = strsql + " where td_isin_code = sc_isincode and td_ac_code = cm_cd  ";
                    strsql = strsql + " and td_ac_code = '" + cm_cd + "' and  td_trxdate between '" + FromDate + "' and '" + ToDate + "' ";
                    strsql = strsql + " group by td_trxdate order by  td_order,td_type, Dt1, td_stlmnt ";
                }
                if (_configuration["Estro"] != "" && Strings.Left(strConnecton, 2) == "IN")
                {
                    string[] ArrEstro = _configuration["Estro"].Split(ArrSeparators);
                    strsql = strsql + " union all ";
                    strsql = strsql + " select 8 Td_order,'DP Transaction' Td_Type, convert(char, td_trxdate,112) Dt , '' ,  sum(case td_debit_credit when 'C' then ";
                    strsql = strsql + " td_qty else 0 end ) Bqty , 0 ,  sum(case td_debit_credit when 'D' then td_qty else 0 end ) sqty , 0,0 NQty, 0 NAmt,'DP Transaction' + '|'   LinkCode,td_trxdate Dt1 ";
                    strsql = strsql + " from " + ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".TrxDetail a with (nolock) , " + ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".security with (nolock), " + ArrEstro[0].Trim() + "." + ArrEstro[1].Trim() + "." + ArrEstro[2].Trim() + ".client_master with (nolock) ";
                    strsql = strsql + " where td_isin_code = sc_isincode and td_ac_code = cm_cd  ";
                    strsql = strsql + " and td_ac_code = '" + cm_cd + "' and  td_trxdate between '" + FromDate + "' and '" + ToDate + "' ";
                    strsql = strsql + " group by td_trxdate order by  td_order,td_type ,Dt1 ,td_stlmnt ";
                }
            }
            else
            {
                strsql = strsql + " union all ";
                strsql = strsql + "select 8 Td_order,'DP Transaction' Td_Type, convert(char, td_trxdate,112) Dt , '' ,  sum(case td_debit_credit when 'C' then ";
                strsql = strsql + "td_qty else 0 end ) Bqty , 0 ,  sum(case td_debit_credit when 'D' then td_qty else 0 end ) sqty , 0,0 NQty, 0 NAmt,'DP Transaction' + '|'   LinkCode ,td_trxdate Dt1 ";
                strsql = strsql + "from trxweb a with (nolock) , security with (nolock),  client_master with (nolock) ";
                strsql = strsql + "where td_isin_code = sc_isincode and td_ac_code = cm_cd  ";
                strsql = strsql + "and td_ac_code = '" + cm_cd + "' and  td_trxdate between '" + FromDate + "' and '" + ToDate + "' ";
                strsql = strsql + " group by td_trxdate order by  td_order,td_type, Dt1,td_stlmnt";
            }
        }

        private void BindGridMF(string cm_cd, string Trade_Type, string Select_Type, string Stock_Type, string CompanyCode, string FromDate, string ToDate)
        {
            string strSql = "";
            string strAttachType = objUtility.GetSysParmSt("DEMLATFMT", "").Trim().ToUpper();

            strSql = "select MTd_srno,ltrim(rtrim(convert(char,convert(datetime,MTd_dt),103))) MTd_dt, MTd_ISIN , MTd_Bqty,MTd_Sqty,MTd_Rate,cast(Mtd_MarketRate as decimal(15,4)) Mtd_MarketRate,MTd_Brokerage,MTd_OrderDt,MTd_OrderTime,MTd_TerminalCd,MTd_Billdt,";
            strSql += "case When MTd_Exchange = 'B' Then MFS_BSchemeName else MFS_NSchemeName End SchemeName,MTd_OldClientcd,";
            strSql += "case when Mtd_MarketRate=0 then 0 else (mtd_Brokerage*100)/Mtd_MarketRate End Brokper,mtd_broktype,MTd_FolioNumber,MTd_Stlmnt,";
            strSql += "((mtd_bqty+mtd_sqty)*mtd_Brokerage) as Brokerage,cm_name,cm_cd,Mtd_marginyn,cast(((MTd_Sqty-MTd_Bqty) * MTd_Rate) as decimal(15,2)) Value ";
            strSql += " From MFTrades, MFSecurities, Client_master left outer join SubBrokers on cm_subbroker = rm_cd  ,Group_master a,Family_master,Branch_master";
            strSql += " Where MTd_ISIN=MFS_ISIN and MTd_ClientCd = cm_cd and MTd_dt between '" + FromDate + "' and '" + ToDate + "' ";
            strSql += " and cm_brboffcode = bm_branchcd ";
            strSql += " and cm_groupcd = a.gr_cd and cm_familycd  = fm_cd ";
            strSql += " and MTd_CompanyCode = '" + CompanyCode + "' and MTd_Exchange = SUBSTRING('" + Stock_Type + "',2,1)";
            //strSql += " and MTd_CompanyCode = '" + Session+ "CompanyCode" + "' and MTd_Exchange = SUBSTRING('" + Trade_Type.Trim() + "',2,1)";
            strSql += " and MTd_ClientCd = '" + cm_cd + "'";
            strSql += " order by MTd_dt,MFS_NSchemeName";
            DataSet dt = new DataSet();
        }

        private void ShowGridLedger(string cm_cd, string Trade_Type, string Select_Type, string FromDate, string ToDate)
        {
            DataSet dt = new DataSet();
            if (Trade_Type == "Receipts" || Trade_Type == "Payments")
            {
                strsql = " select 'DP Transaction' Td_Type,ld_documentno , ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) Date , ld_Particular , ld_Chequeno,";
                strsql = strsql + "convert(decimal(15,2),case ld_documenttype When 'R' Then (-1) else 1 end*ld_amount)  Amount from ledger with (nolock)";
                strsql = strsql + "where ld_documenttype=";
                if (Trade_Type == "Receipts")
                {
                    strsql = strsql + "'R'";
                }
                else
                {
                    strsql = strsql + "'P'";
                }
                strsql = strsql + "and ld_clientcd='" + cm_cd + "' and ld_dt between '" + FromDate + "' and '" + ToDate + "'";
                strsql = strsql + "order by ld_dt desc ";
            }
            else
            {
                strsql = "select 'DP Transaction' Td_Type,ld_documentno , ltrim(rtrim(convert(char,convert(datetime,ld_dt),103))) Date , ";
                strsql = strsql + " ld_Particular  , case ld_debitflag when 'D' then convert(decimal(15,2),ld_amount) else 0 end  Debit,";
                strsql = strsql + " case ld_debitflag when 'D' then 0 else convert(decimal(15,2),-ld_amount) end  Credit";
                strsql = strsql + " from ledger with (nolock) where ld_documenttype=";
                if (Trade_Type == "Journals")
                {
                    strsql = strsql + "'J'";
                }
                else
                {
                    strsql = strsql + "'B'";
                }
                strsql = strsql + " and ld_clientcd='" + cm_cd + "'";
                strsql = strsql + " and ld_dt between '" + FromDate + "' and '" + ToDate + "'";
                strsql = strsql + " order by ld_dt desc";
            }
        }

        private DataSet BindGridAGTS(string cm_cd, string Trade_Type, string Select_Type, string Cash_Type, string CompanyCode, string FromDate, string ToDate)
        {
            SqlConnection ObjConnection;
            DataSet ObjDataSetCash = new DataSet();
            using (var db = new DataContext())
            {
                ObjConnection = new SqlConnection((db.Database.GetDbConnection()).ConnectionString);
                if (ObjConnection.State == ConnectionState.Closed)
                    ObjConnection.Open();
            }
            string StrSelect, StrGroupBy, StrOrderBy = "";
            // TODO : We replace _config to string type Cash_Type.
            if (Cash_Type == "C")
            {
                prCreateTableCash(ObjConnection);
                strsql = "Select GLOBAL_TRX.* ";
                strsql += " , Case td_bsflag When 'B' then td_bqty When 'S' then td_sqty else 0 end BSQuantity ";
                strsql += "  , Case td_bsflag When 'B' then Round((td_bqty * td_rate),2) When 'S' then Round((td_sqty * td_rate),2) else 0 end BSValue  ";
                strsql += " From GLOBAL_TRX, Client_master  ";
                strsql += " Where td_clientcd = cm_cd and td_dt between '" + FromDate + "' and '" + ToDate + "'";
                strsql += " and td_clientcd = '" + cm_cd + "'";
                strsql += " and td_companycode = '" + CompanyCode + "'";
                DataSet dt = new DataSet();
                dt = objUtility.OpenDataSet(strsql);

                if (dt.Tables[0].Rows.Count > 0)
                {
                    int i = 0;
                    for (i = 0; i < dt.Tables[0].Rows.Count; i++)
                    {
                        strsql = " Insert Into #tmpGlobalC Values (";
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

                    strsql = " Update #tmpGlobalC set td_TotalChrg =  Convert(numeric(18,3),td_Chrg2) + Convert(numeric(18,3),td_Chrg3) +Convert(numeric(18,3),td_Chrg4) + Convert(numeric(18,3),td_Chrg5) + Convert(numeric(18,3),td_Chrg6) + Convert(numeric(18,3),td_ExchTrx) ";
                    objUtility.ExecuteSQLTmp(strsql, ObjConnection);

                    if (Select_Type == "0")
                    {
                        StrSelect = " Case td_BSFlag when 'B' then cast(SUM(td_Qty) as decimal(15,0)) when 'S' then cast(SUM(td_Qty) as decimal(15,0)) * (-1) else 0 end td_BSQty , ";
                        StrSelect += "'Scrip : ' + LTRIM(RTRIM(ss_name)) +  ' [' + td_scripcd + ']'  as Ttype, ltrim(rtrim(convert(char,convert(datetime,td_dt),103))) as 'Col2', td_stlmnt as 'Col3',";
                        StrGroupBy = " Group by td_scripcd,td_dt,td_stlmnt,td_clientcd,td_BSFlag, ss_name,td_Exch,td_trdtype ";
                        StrOrderBy = " Order by Col3, Col2 ";
                    }
                    else
                    {
                        StrSelect = " cast(SUM(td_Qty) as decimal(15,0))  td_BSQty,'Trade Type : ' +  ltrim(rtrim(convert(char,convert(datetime,td_dt),103)))  + ' [' + td_stlmnt + ']'  as Ttype,";
                        StrSelect += " td_scripcd as 'Col2', LTRIM(RTRIM(ss_name)) as 'Col3', ";
                        StrGroupBy = " Group by td_dt,td_stlmnt,td_clientcd,td_scripcd,td_BSFlag, ss_name,td_Exch, td_trdtype";
                        StrOrderBy = " Order by td_dt,td_stlmnt, Col3,td_BSFlag  ";
                    }

                    strsql = "select td_clientcd," + StrSelect;
                    strsql += " case td_trdtype when 'SQ' then 'SQ' +  Case td_BSFlag when 'B' then '(B)' when 'S' then '(S)' else '' end else Case td_BSFlag when 'B' then 'Buy' when 'S' then 'Sell' else '' end  end  td_BSFlag,";
                    strsql += " Case td_BSFlag when 'B' then cast(sum(td_Value) * -1 as decimal(15,2)) when 'S' then  cast(sum(td_Value) as decimal(15,2)) else 0 end td_BSValue, ";
                    strsql += " cast(sum(td_MarketRate * td_Qty)/sum(td_Qty) as decimal(15,2))  td_MarketRate,  ";
                    //strsql += " cast(sum(td_Brokerage * td_Qty)/sum(td_Qty) as decimal(15,4))  td_Brokerage, ";
                    strsql += " cast(sum(td_Brokerage * td_Qty) as decimal(15,4))  td_Brokerage, ";
                    strsql += " cast(sum(td_Netrate * td_Qty)/sum(td_Qty) as decimal(15,4)) td_Netrate, ";
                    strsql += " cast(Sum(cast(td_Chrg1_1 as Money)) as decimal(15,2)) td_Chrg1_1 ,";
                    strsql += " cast(Sum(cast(td_Chrg1_2 as Money)) as decimal(15,2)) td_Chrg1_2 ,";
                    strsql += " cast(Sum(cast(td_Chrg2 as Money)) as decimal(15,2)) td_Chrg2 ,";
                    strsql += " cast(Sum(cast(td_Chrg3 as Money)) as decimal(15,2)) td_Chrg3 ,";
                    strsql += " cast(Sum(cast(td_Chrg4 as Money)) as decimal(15,2)) td_Chrg4 , ";
                    strsql += " cast(Sum(cast(td_Chrg5 as Money)) as decimal(15,2)) td_Chrg5 ,";
                    strsql += " cast(Sum(cast(td_Chrg6 as Money)) as decimal(15,2)) td_Chrg6 ,";
                    strsql += " cast(Sum(cast(td_Chrg7 as Money)) as decimal(15,2)) td_Chrg7 , ";
                    strsql += " td_Exch, ";
                    strsql += " cast(Sum(cast(td_ExchTrx as Money)) as decimal(15,2)) td_ExchTrx, ";
                    strsql += " Case td_BSFlag when 'B' then cast(sum(td_Value + td_TotalChrg)  * -1 as decimal(15,2)) when 'S' then cast(sum(td_Value - td_TotalChrg) as decimal(15,2)) end   as Amount ";
                    strsql += " from #tmpGlobalC, securities ";
                    strsql += " Where td_scripcd = ss_cd  ";
                    strsql += StrGroupBy + StrOrderBy;

                    ObjDataSetCash = objUtility.OpenDataSetTmp(strsql, ObjConnection);

                }

                else if (Cash_Type == "F" || Cash_Type == "K")
                {
                    prCreateTableFO(ObjConnection);
                    strsql = "Select Global_Trades.* ,";
                    strsql += " Case td_bsflag When 'B' then td_bqty When 'S' then td_sqty else 0 end BSQuantity, ";
                    strsql += " Case td_bsflag When 'B' then Round((td_bqty * td_rate * sm_multiplier),2) When 'S' then Round((td_sqty * td_rate * sm_multiplier),2) else 0 end BSValue  ";
                    strsql += " From Global_Trades, client_master, Series_Master ";
                    strsql += " Where  td_seriesId = sm_seriesid and td_exchange = SM_exchange and td_Segment = sm_Segment ";
                    strsql += " and td_clientcd = cm_cd and td_dt between '" + FromDate + "' and '" + ToDate + "'";
                    strsql += " and td_clientcd = '" + cm_cd + "'";
                    strsql += " and td_companycode = '" + CompanyCode + "'";

                    //DataSet dt = new DataSet();
                    dt = objUtility.OpenDataSet(strsql);

                    if (dt.Tables[0].Rows.Count > 0)
                    {
                        int i = 0;
                        for (i = 0; i < dt.Tables[0].Rows.Count; i++)
                        {
                            strsql = " Insert Into #tmpGlobalFO Values (";
                            strsql += "'" + dt.Tables[0].Rows[i]["td_exchange"].ToString() + "',";
                            strsql += "'" + dt.Tables[0].Rows[i]["td_segment"].ToString() + "',";
                            strsql += "'" + dt.Tables[0].Rows[i]["td_dt"].ToString() + "',";
                            strsql += "'" + dt.Tables[0].Rows[i]["td_clientcd"].ToString() + "',";
                            strsql += "'" + dt.Tables[0].Rows[i]["td_seriesid"].ToString() + "',";
                            strsql += "'" + dt.Tables[0].Rows[i]["td_bsflag"].ToString() + "',";
                            strsql += dt.Tables[0].Rows[i]["BSQuantity"].ToString() + ",";
                            strsql += dt.Tables[0].Rows[i]["BSValue"].ToString() + ",";
                            strsql += dt.Tables[0].Rows[i]["td_marketrate"].ToString() + ",";
                            strsql += dt.Tables[0].Rows[i]["td_brokerage"].ToString() + ",";
                            strsql += dt.Tables[0].Rows[i]["td_rate"].ToString() + ",";
                            strsql += "'" + DecryptN(dt.Tables[0].Rows[i]["td_Chrg1"].ToString()) + "', ";
                            strsql += "'" + DecryptN(dt.Tables[0].Rows[i]["td_Chrg2"].ToString()) + "', ";
                            strsql += "'" + DecryptN(dt.Tables[0].Rows[i]["td_Chrg3"].ToString()) + "', ";
                            strsql += "'" + DecryptN(dt.Tables[0].Rows[i]["td_Chrg4"].ToString()) + "', ";
                            strsql += "'" + DecryptN(dt.Tables[0].Rows[i]["td_Chrg5"].ToString()) + "', ";
                            strsql += "'" + DecryptN(dt.Tables[0].Rows[i]["td_Chrg6"].ToString()) + "', ";
                            strsql += "'" + DecryptN(dt.Tables[0].Rows[i]["td_Chrg7"].ToString()) + "', ";
                            strsql += "'" + mfnGetExchangeCode2Desc(dt.Tables[0].Rows[i]["td_exchange"].ToString()) + "') ";
                            objUtility.ExecuteSQLTmp(strsql, ObjConnection);
                        }
                        if (Select_Type == "0")
                        {
                            StrSelect = "  ltrim(rtrim(convert(char,convert(datetime,td_Date),103))) dt,'Symbol : ' +  ltrim(rtrim(sm_symbol)) + case When Right(sm_prodtype,1) = 'F' Then ' (Future) ' else ' (Options) ' End as Ttype, Case td_BSFlag when 'B' then SUM(td_Qty) when 'S' then SUM(td_Qty) * (-1) else 0 end td_BSQty,";
                            StrGroupBy = " Group By td_exchange, td_segment,td_date, td_clientcd, td_seriesid, sm_sname,td_BSFlag, sm_symbol,sm_prodtype,sm_expirydt,sm_strikeprice,sm_callput+sm_optionstyle,td_Exch ";
                            StrOrderBy = " Order By sm_symbol,sm_prodtype,sm_expirydt,sm_strikeprice,sm_callput+sm_optionstyle,td_date,td_seriesid,td_SeriesNm,td_exchange,td_BSFlag";
                        }
                        else
                        {
                            StrSelect = " cast(SUM(td_Qty) as decimal(15,0))  td_BSQty,'Date : ' +  ltrim(rtrim(convert(char,convert(datetime,td_Date),103)))  as Ttype,";
                            StrGroupBy = " Group By td_exchange, td_segment,td_date, td_clientcd, td_seriesid, sm_sname,td_BSFlag,td_Exch ";
                            StrOrderBy = " Order By td_date, td_exchange, td_SeriesNm,td_BSFlag ";
                        }

                        strsql = " select td_exchange, td_segment,td_Date, td_clientcd, td_seriesid, LTRIM(RTRIM(sm_sname)) td_SeriesNm,  ";
                        strsql += " Case td_BSFlag when 'B' then 'Buy' when 'S' then 'Sell' else '' end  td_BSFlag, ";
                        strsql += StrSelect;
                        strsql += " Case td_BSFlag when 'B' then cast(sum(td_Value) * -1 as decimal(15,2)) when 'S' then  cast(sum(td_Value) as decimal(15,2)) else 0 end td_BSValue, ";
                        strsql += " cast(sum(td_MarketRate * td_Qty)/sum(td_Qty) as decimal(15,2))  td_MarketRate,  ";
                        //strsql += " cast(sum(td_Brokerage * td_Qty)/sum(td_Qty) as decimal(15,4))  td_Brokerage, ";
                        strsql += " cast(sum(td_Brokerage * td_Qty) as decimal(15,4))  td_Brokerage, ";
                        strsql += " cast(sum(td_Netrate * td_Qty)/sum(td_Qty) as decimal(15,4)) td_Netrate, ";
                        strsql += " cast(Sum(cast(td_Chrg1 as Money)) as decimal(15,2)) td_Chrg1 ,";
                        strsql += " cast(Sum(cast(td_Chrg2 as Money)) as decimal(15,2)) td_Chrg2 ,";
                        strsql += " cast(Sum(cast(td_Chrg3 as Money)) as decimal(15,2)) td_Chrg3 ,";
                        strsql += " cast(Sum(cast(td_Chrg4 as Money)) as decimal(15,2)) td_Chrg4 , ";
                        strsql += " cast(Sum(cast(td_Chrg5 as Money)) as decimal(15,2)) td_Chrg5 ,";
                        strsql += " cast(Sum(cast(td_Chrg6 as Money)) as decimal(15,2)) td_Chrg6 ,";
                        strsql += " cast(Sum(cast(td_Chrg7 as Money)) as decimal(15,2)) td_Chrg7 , ";
                        strsql += " td_Exch ";
                        strsql += " from #tmpGlobalFO, Series_Master ";
                        strsql += " Where td_seriesId = sm_seriesid ";
                        strsql += " and td_exchange = SM_exchange and td_Segment = sm_Segment and td_Segment = '" + Cash_Type + "' ";
                        //strsql += " and td_exchange = SM_exchange and td_Segment = sm_Segment and td_Segment = '" + Trade_Type.Trim() + "' ";
                        strsql += StrGroupBy + StrOrderBy;

                        ObjDataSetCash = objUtility.OpenDataSetTmp(strsql, ObjConnection);

                    }

                    else if ((_configuration["Commex"] != null && _configuration["Commex"] != string.Empty))
                    {
                        string StrCommexConn = "";
                        StrCommexConn = objUtility.GetCommexConnectionNew(_configuration["Commex"]);

                        prCreateTableFO(ObjConnection);
                        strsql = "Select " + StrCommexConn + ".Global_Trades.* ,";
                        strsql += " Case td_bsflag When 'B' then td_bqty When 'S' then td_sqty else 0 end BSQuantity, ";
                        strsql += " Case td_bsflag When 'B' then Round((td_bqty * td_rate * sm_multiplier),2) When 'S' then Round((td_sqty * td_rate * sm_multiplier),2) else 0 end BSValue  ";
                        strsql += " From  " + StrCommexConn + ".Global_Trades,  " + StrCommexConn + ".client_master,  " + StrCommexConn + ".Series_Master ";
                        strsql += " Where  td_seriesId = sm_seriesid and td_exchange = SM_exchange ";
                        strsql += " and td_clientcd = cm_cd and td_dt between '" + FromDate + "' and '" + ToDate + "'";
                        strsql += " and td_clientcd = '" + cm_cd + "'";
                        strsql += " and td_companycode = '" + CompanyCode + "'";

                        //DataSet dt = new DataSet();
                        dt = objUtility.OpenDataSet(strsql);

                        if (dt.Tables[0].Rows.Count > 0)
                        {
                            int i = 0;
                            for (i = 0; i < dt.Tables[0].Rows.Count; i++)
                            {
                                strsql = " Insert Into #tmpGlobalFO Values (";
                                strsql += "'" + dt.Tables[0].Rows[i]["td_exchange"].ToString() + "',";
                                strsql += "'',";
                                strsql += "'" + dt.Tables[0].Rows[i]["td_dt"].ToString() + "',";
                                strsql += "'" + dt.Tables[0].Rows[i]["td_clientcd"].ToString() + "',";
                                strsql += "'" + dt.Tables[0].Rows[i]["td_seriesid"].ToString() + "',";
                                strsql += "'" + dt.Tables[0].Rows[i]["td_bsflag"].ToString() + "',";
                                strsql += dt.Tables[0].Rows[i]["BSQuantity"].ToString() + ",";
                                strsql += dt.Tables[0].Rows[i]["BSValue"].ToString() + ",";
                                strsql += dt.Tables[0].Rows[i]["td_marketrate"].ToString() + ",";
                                strsql += dt.Tables[0].Rows[i]["td_brokerage"].ToString() + ",";
                                strsql += dt.Tables[0].Rows[i]["td_rate"].ToString() + ",";
                                strsql += "'" + DecryptN(dt.Tables[0].Rows[i]["td_Chrg1"].ToString()) + "', ";
                                strsql += "'" + DecryptN(dt.Tables[0].Rows[i]["td_Chrg2"].ToString()) + "', ";
                                strsql += "'" + DecryptN(dt.Tables[0].Rows[i]["td_Chrg3"].ToString()) + "', ";
                                strsql += "'" + DecryptN(dt.Tables[0].Rows[i]["td_Chrg4"].ToString()) + "', ";
                                strsql += "'" + DecryptN(dt.Tables[0].Rows[i]["td_Chrg5"].ToString()) + "', ";
                                strsql += "'" + DecryptN(dt.Tables[0].Rows[i]["td_Chrg6"].ToString()) + "', ";
                                strsql += "'" + DecryptN(dt.Tables[0].Rows[i]["td_Chrg7"].ToString()) + "', ";
                                strsql += "'" + mfnGetExchangeCode2DescComm(dt.Tables[0].Rows[i]["td_exchange"].ToString()) + "') ";
                                objUtility.ExecuteSQLTmp(strsql, ObjConnection);
                            }
                            DataSet dt1 = new DataSet();
                            dt1 = objUtility.OpenDataSetTmp("select * from  #tmpGlobalFO", ObjConnection);

                            if (Select_Type == "0")
                            {
                                StrSelect = "  ltrim(rtrim(convert(char,convert(datetime,td_Date),103))) dt,'Symbol : ' +  ltrim(rtrim(sm_symbol)) + case When Right(sm_prodtype,1) = 'F' Then ' (Future) ' else ' (Options) ' End as Ttype, Case td_BSFlag when 'B' then SUM(td_Qty) when 'S' then SUM(td_Qty) * (-1) else 0 end td_BSQty,";
                                StrGroupBy = " Group By td_exchange,td_date, td_clientcd, td_seriesid, sm_sname,td_BSFlag, sm_symbol,sm_prodtype,sm_expirydt,sm_strikeprice,sm_callput+sm_optionstyle,td_Exch ";
                                StrOrderBy = " Order By sm_symbol,sm_prodtype,sm_expirydt,sm_strikeprice,sm_callput+sm_optionstyle,td_date,td_seriesid,td_SeriesNm,td_exchange,td_BSFlag";
                            }
                            else
                            {
                                StrSelect = " cast(SUM(td_Qty) as decimal(15,0))  td_BSQty,'Date : ' +  ltrim(rtrim(convert(char,convert(datetime,td_Date),103)))  as Ttype,";
                                StrGroupBy = " Group By td_exchange,td_date, td_clientcd, td_seriesid, sm_sname,td_BSFlag,td_Exch ";
                                StrOrderBy = " Order By td_date, td_exchange, td_SeriesNm,td_BSFlag ";
                            }

                            strsql = " select td_exchange,td_Date, td_clientcd, td_seriesid, LTRIM(RTRIM(sm_sname)) td_SeriesNm,  ";
                            strsql += " Case td_BSFlag when 'B' then 'Buy' when 'S' then 'Sell' else '' end  td_BSFlag, ";
                            strsql += StrSelect;
                            strsql += " Case td_BSFlag when 'B' then cast(sum(td_Value) * -1 as decimal(15,2)) when 'S' then  cast(sum(td_Value) as decimal(15,2)) else 0 end td_BSValue, ";
                            strsql += " cast(sum(td_MarketRate * td_Qty)/sum(td_Qty) as decimal(15,2))  td_MarketRate,  ";
                            //strsql += " cast(sum(td_Brokerage * td_Qty)/sum(td_Qty) as decimal(15,4))  td_Brokerage, ";
                            strsql += " cast(sum(td_Brokerage * td_Qty) as decimal(15,4))  td_Brokerage, ";
                            strsql += " cast(sum(td_Netrate * td_Qty)/sum(td_Qty) as decimal(15,4)) td_Netrate, ";
                            strsql += " cast(Sum(cast(td_Chrg1 as Money)) as decimal(15,2)) td_Chrg1 ,";
                            strsql += " cast(Sum(cast(td_Chrg2 as Money)) as decimal(15,2)) td_Chrg2 ,";
                            strsql += " cast(Sum(cast(td_Chrg3 as Money)) as decimal(15,2)) td_Chrg3 ,";
                            strsql += " cast(Sum(cast(td_Chrg4 as Money)) as decimal(15,2)) td_Chrg4 , ";
                            strsql += " cast(Sum(cast(td_Chrg5 as Money)) as decimal(15,2)) td_Chrg5 ,";
                            strsql += " cast(Sum(cast(td_Chrg6 as Money)) as decimal(15,2)) td_Chrg6 ,";
                            strsql += " cast(Sum(cast(td_Chrg7 as Money)) as decimal(15,2)) td_Chrg7 , ";
                            strsql += " td_Exch ";
                            strsql += " from #tmpGlobalFO, " + StrCommexConn + ".Series_Master  ";
                            strsql += " Where td_seriesId = sm_seriesid ";
                            strsql += " and td_exchange = SM_exchange ";
                            strsql += StrGroupBy + StrOrderBy;

                            ObjDataSetCash = objUtility.OpenDataSetTmp(strsql, ObjConnection);

                        }
                    }
                }
            }
            else if (Cash_Type == "F" || Cash_Type == "K")
            {
                prCreateTableFO(ObjConnection);
                strsql = "Select Global_Trades.* ,";
                strsql += " Case td_bsflag When 'B' then td_bqty When 'S' then td_sqty else 0 end BSQuantity, ";
                strsql += " Case td_bsflag When 'B' then Round((td_bqty * td_rate * sm_multiplier),2) When 'S' then Round((td_sqty * td_rate * sm_multiplier),2) else 0 end BSValue  ";
                strsql += " From Global_Trades, client_master, Series_Master ";
                strsql += " Where  td_seriesId = sm_seriesid and td_exchange = SM_exchange and td_Segment = sm_Segment ";
                strsql += " and td_clientcd = cm_cd and td_dt between '" + FromDate + "' and '" + ToDate + "'";
                strsql += " and td_clientcd = '" + cm_cd + "'";
                strsql += " and td_companycode = '" + CompanyCode + "'";

                DataSet ObjDataSet = new DataSet();
                ObjDataSet = objUtility.OpenDataSet(strsql);

                if (ObjDataSet.Tables[0].Rows.Count > 0)
                {
                    int i = 0;
                    for (i = 0; i < ObjDataSet.Tables[0].Rows.Count; i++)
                    {
                        strsql = " Insert Into #tmpGlobalFO Values (";
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

                    if (Select_Type == "0")
                    {
                        StrSelect = "  ltrim(rtrim(convert(char,convert(datetime,td_Date),103))) dt,'Symbol : ' +  ltrim(rtrim(sm_symbol)) + case When Right(sm_prodtype,1) = 'F' Then ' (Future) ' else ' (Options) ' End as Ttype, Case td_BSFlag when 'B' then SUM(td_Qty) when 'S' then SUM(td_Qty) * (-1) else 0 end td_BSQty,";
                        StrGroupBy = " Group By td_exchange, td_segment,td_date, td_clientcd, td_seriesid, sm_sname,td_BSFlag, sm_symbol,sm_prodtype,sm_expirydt,sm_strikeprice,sm_callput+sm_optionstyle,td_Exch ";
                        StrOrderBy = " Order By sm_symbol,sm_prodtype,sm_expirydt,sm_strikeprice,sm_callput+sm_optionstyle,td_date,td_seriesid,td_SeriesNm,td_exchange,td_BSFlag";
                    }
                    else
                    {
                        StrSelect = " cast(SUM(td_Qty) as decimal(15,0))  td_BSQty,'Date : ' +  ltrim(rtrim(convert(char,convert(datetime,td_Date),103)))  as Ttype,";
                        StrGroupBy = " Group By td_exchange, td_segment,td_date, td_clientcd, td_seriesid, sm_sname,td_BSFlag,td_Exch ";
                        StrOrderBy = " Order By td_date, td_exchange, td_SeriesNm,td_BSFlag ";
                    }

                    strsql = " select td_exchange, td_segment,td_Date, td_clientcd, td_seriesid, LTRIM(RTRIM(sm_sname)) td_SeriesNm,  ";
                    strsql += " Case td_BSFlag when 'B' then 'Buy' when 'S' then 'Sell' else '' end  td_BSFlag, ";
                    strsql += StrSelect;
                    strsql += " Case td_BSFlag when 'B' then cast(sum(td_Value) * -1 as decimal(15,2)) when 'S' then  cast(sum(td_Value) as decimal(15,2)) else 0 end td_BSValue, ";
                    strsql += " cast(sum(td_MarketRate * td_Qty)/sum(td_Qty) as decimal(15,2))  td_MarketRate,  ";
                    //strsql += " cast(sum(td_Brokerage * td_Qty)/sum(td_Qty) as decimal(15,4))  td_Brokerage, ";
                    strsql += " cast(sum(td_Brokerage * td_Qty) as decimal(15,4))  td_Brokerage, ";
                    strsql += " cast(sum(td_Netrate * td_Qty)/sum(td_Qty) as decimal(15,4)) td_Netrate, ";
                    strsql += " cast(Sum(cast(td_Chrg1 as Money)) as decimal(15,2)) td_Chrg1 ,";
                    strsql += " cast(Sum(cast(td_Chrg2 as Money)) as decimal(15,2)) td_Chrg2 ,";
                    strsql += " cast(Sum(cast(td_Chrg3 as Money)) as decimal(15,2)) td_Chrg3 ,";
                    strsql += " cast(Sum(cast(td_Chrg4 as Money)) as decimal(15,2)) td_Chrg4 , ";
                    strsql += " cast(Sum(cast(td_Chrg5 as Money)) as decimal(15,2)) td_Chrg5 ,";
                    strsql += " cast(Sum(cast(td_Chrg6 as Money)) as decimal(15,2)) td_Chrg6 ,";
                    strsql += " cast(Sum(cast(td_Chrg7 as Money)) as decimal(15,2)) td_Chrg7 , ";
                    strsql += " td_Exch ";
                    strsql += " from #tmpGlobalFO, Series_Master ";
                    strsql += " Where td_seriesId = sm_seriesid ";
                    strsql += " and td_exchange = SM_exchange and td_Segment = sm_Segment and td_Segment = '" + Cash_Type + "' ";
                    strsql += StrGroupBy + StrOrderBy;

                    ObjDataSetCash = objUtility.OpenDataSetTmp(strsql, ObjConnection);

                }
            }
            else if ((_configuration["Commex"] != null && _configuration["Commex"] != string.Empty))
            {
                string StrCommexConn = "";
                StrCommexConn = objUtility.GetCommexConnection();

                prCreateTableFO(ObjConnection);
                strsql = "Select " + StrCommexConn + ".Global_Trades.* ,";
                strsql += " Case td_bsflag When 'B' then td_bqty When 'S' then td_sqty else 0 end BSQuantity, ";
                strsql += " Case td_bsflag When 'B' then Round((td_bqty * td_rate * sm_multiplier),2) When 'S' then Round((td_sqty * td_rate * sm_multiplier),2) else 0 end BSValue  ";
                strsql += " From  " + StrCommexConn + ".Global_Trades,  " + StrCommexConn + ".client_master,  " + StrCommexConn + ".Series_Master ";
                strsql += " Where  td_seriesId = sm_seriesid and td_exchange = SM_exchange ";
                strsql += " and td_clientcd = cm_cd and td_dt between '" + FromDate + "' and '" + ToDate + "'";
                strsql += " and td_clientcd = '" + cm_cd + "'";
                strsql += " and td_companycode = '" + CompanyCode + "'";

                DataSet ObjDataSet = new DataSet();
                ObjDataSet = objUtility.OpenDataSet(strsql);

                if (ObjDataSet.Tables[0].Rows.Count > 0)
                {
                    int i = 0;
                    for (i = 0; i < ObjDataSet.Tables[0].Rows.Count; i++)
                    {
                        strsql = " Insert Into #tmpGlobalFO Values (";
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
                    DataSet ObjDataSet1 = new DataSet();
                    ObjDataSet1 = objUtility.OpenDataSetTmp("select * from  #tmpGlobalFO", ObjConnection);

                    if (Select_Type == "0")
                    {
                        StrSelect = "  ltrim(rtrim(convert(char,convert(datetime,td_Date),103))) dt,'Symbol : ' +  ltrim(rtrim(sm_symbol)) + case When Right(sm_prodtype,1) = 'F' Then ' (Future) ' else ' (Options) ' End as Ttype, Case td_BSFlag when 'B' then SUM(td_Qty) when 'S' then SUM(td_Qty) * (-1) else 0 end td_BSQty,";
                        StrGroupBy = " Group By td_exchange,td_date, td_clientcd, td_seriesid, sm_sname,td_BSFlag, sm_symbol,sm_prodtype,sm_expirydt,sm_strikeprice,sm_callput+sm_optionstyle,td_Exch ";
                        StrOrderBy = " Order By sm_symbol,sm_prodtype,sm_expirydt,sm_strikeprice,sm_callput+sm_optionstyle,td_date,td_seriesid,td_SeriesNm,td_exchange,td_BSFlag";
                    }
                    else
                    {
                        StrSelect = " cast(SUM(td_Qty) as decimal(15,0))  td_BSQty,'Date : ' +  ltrim(rtrim(convert(char,convert(datetime,td_Date),103)))  as Ttype,";
                        StrGroupBy = " Group By td_exchange,td_date, td_clientcd, td_seriesid, sm_sname,td_BSFlag,td_Exch ";
                        StrOrderBy = " Order By td_date, td_exchange, td_SeriesNm,td_BSFlag ";
                    }

                    strsql = " select td_exchange,td_Date, td_clientcd, td_seriesid, LTRIM(RTRIM(sm_sname)) td_SeriesNm,  ";
                    strsql += " Case td_BSFlag when 'B' then 'Buy' when 'S' then 'Sell' else '' end  td_BSFlag, ";
                    strsql += StrSelect;
                    strsql += " Case td_BSFlag when 'B' then cast(sum(td_Value) * -1 as decimal(15,2)) when 'S' then  cast(sum(td_Value) as decimal(15,2)) else 0 end td_BSValue, ";
                    strsql += " cast(sum(td_MarketRate * td_Qty)/sum(td_Qty) as decimal(15,2))  td_MarketRate,  ";
                    //strsql += " cast(sum(td_Brokerage * td_Qty)/sum(td_Qty) as decimal(15,4))  td_Brokerage, ";
                    strsql += " cast(sum(td_Brokerage * td_Qty) as decimal(15,4))  td_Brokerage, ";
                    strsql += " cast(sum(td_Netrate * td_Qty)/sum(td_Qty) as decimal(15,4)) td_Netrate, ";
                    strsql += " cast(Sum(cast(td_Chrg1 as Money)) as decimal(15,2)) td_Chrg1 ,";
                    strsql += " cast(Sum(cast(td_Chrg2 as Money)) as decimal(15,2)) td_Chrg2 ,";
                    strsql += " cast(Sum(cast(td_Chrg3 as Money)) as decimal(15,2)) td_Chrg3 ,";
                    strsql += " cast(Sum(cast(td_Chrg4 as Money)) as decimal(15,2)) td_Chrg4 , ";
                    strsql += " cast(Sum(cast(td_Chrg5 as Money)) as decimal(15,2)) td_Chrg5 ,";
                    strsql += " cast(Sum(cast(td_Chrg6 as Money)) as decimal(15,2)) td_Chrg6 ,";
                    strsql += " cast(Sum(cast(td_Chrg7 as Money)) as decimal(15,2)) td_Chrg7 , ";
                    strsql += " td_Exch ";
                    strsql += " from #tmpGlobalFO, " + StrCommexConn + ".Series_Master  ";
                    strsql += " Where td_seriesId = sm_seriesid ";
                    strsql += " and td_exchange = SM_exchange ";
                    strsql += StrGroupBy + StrOrderBy;

                    ObjDataSetCash = objUtility.OpenDataSetTmp(strsql, ObjConnection);

                }
            }

            return ObjDataSetCash;

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
            else
            {
                ShowGridLedger(cm_cd, Trade_Type, Select_Type, FromDate, ToDate);
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
            else//Tradeweb
            {
                DataSet dt = new DataSet();
                strsql = " select convert (char,convert(datetime,td_trxdate),103) td_trxdate,td_reference, sc_isinname + '   [ ' + td_isin_code + ' ]' ISINCode,";
                strsql += " da_name + '  [ '+ td_dpid+td_ac_code +' ]' as DPID,cm_cd,td_dpid+td_ac_code,da_name,";
                strsql += " isnull((select sum(case td_debit_credit when 'C' then td_qty else td_qty * (-1) end) From trxweb with (nolock)";
                strsql += " Where td_ac_code = a.td_ac_code and td_dpid = a.td_dpid and td_isin_code = a.td_isin_code  ";
                strsql += " and  td_ac_type = a.td_ac_type   and td_trxdate <  '" + FromDate + "'),0) 'holding',";
                strsql += " Rtrim(td_text) + case Rtrim(td_settlement) when '' Then '' else ' / '+ td_settlement End 'td_text',";
                strsql += " Case td_debit_credit  when 'D' then cast((td_qty)as decimal(15,0)) else 0 end  'Debit', ";
                strsql += " Case td_debit_credit  when 'C' then cast((td_qty)as decimal(15,0)) else 0 end  'Credit',";
                strsql += " '0' 'Balance',td_qty,td_ac_type,bt_description as acdesc ,td_isin_code,";
                strsql += " td_ac_code,cm_name 'Boid'";
                strsql += " from trxweb a with (nolock) ,Security with (nolock),Client_master with (nolock),Beneficiary_type with (nolock),DematAct with(nolock)";
                strsql += " where td_isin_code = sc_isincode  And td_ac_type = bt_code ";
                strsql += " and td_trxdate between  '" + FromDate + "' and '" + ToDate + "'";
                strsql += " and td_dpid+td_ac_code =  da_actno and da_clientcd = cm_cd";
                strsql += " and cm_cd = '" + cm_cd + "'  ";
                strsql += " Order By cm_cd,td_dpid+td_ac_code,da_name,sc_company_name, sc_isinname, td_isin_code, td_ac_type,convert(datetime,td_trxdate) ,td_debit_credit ";
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

        private void prCreateTableCash(SqlConnection con)
        {
            try
            {
                strsql = "Drop Table #tmpGlobalC";
                objUtility.ExecuteSQLTmp(strsql, con);

                strsql = "Create Table #tmpGlobalC(";
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
                objUtility.ExecuteSQLTmp(strsql, con);
            }
            catch
            {
                strsql = "Create Table #tmpGlobalC(";
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
                strsql += " td_ExchTrx varchar(20) Not null,";
                strsql += " td_TRDType char(2) Not null, ";
                strsql += " td_TotalChrg numeric (18,3) Not null ";
                strsql += " )";
                objUtility.ExecuteSQLTmp(strsql, con);
            }
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

        private void prCreateTableFO(SqlConnection con)
        {
            try
            {
                strsql = "Drop Table #tmpGlobalFO";
                objUtility.ExecuteSQLTmp(strsql, con);

                strsql = "Create Table #tmpGlobalFO(";
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
                objUtility.ExecuteSQLTmp(strsql, con);
            }
            catch
            {
                strsql = "Create Table #tmpGlobalFO(";
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
                objUtility.ExecuteSQLTmp(strsql, con);
            }
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

        #region For Details transaction Handler
        //TODO : For getting Itemwise details Transaction data
        private dynamic ItemWiseDetails(string cm_cd, string td_type, string LinkCode, string td_scripnm, string FromDt, string ToDt)
        {
            List<string> EntityList = new List<string>();
            string qury = ItemWiseDetailsQuery(cm_cd, td_type, LinkCode, td_scripnm, FromDt, ToDt);
            try
            {
                var ds = CommonRepository.FillDataset(qury);
                if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                {
                    var json = JsonConvert.SerializeObject(ds.Tables[0], Formatting.Indented);
                    //var jsonret = json.Replace("\\", "");
                    //var str = jsonret.Replace("\r\n", ""); //Regex.Unescape("\r \n a");
                    //var ss = JsonConvert.DeserializeObject(json);
                    //var aa = JObject.Parse(json);
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
        private dynamic DateWiseDetails(string cm_cd, string td_type, string LinkCode, string td_stlmnt, string Dt, string FromDt, string ToDt, string txtheader)
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
        #endregion

        #region OutStanding Handler Method
        // For getting Outstanding data
        private dynamic GetOutStandingData(string cm_cd)
        {
            List<string> loginList = new List<string>();
            var qury = GetQueryForData(cm_cd);
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
                return loginList.ToList();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //For getting Outstanding details data
        private dynamic GetOutStandingDetailData(string cm_cd, string Td_order, string ot_seriesid, string ot_exchange, string ot_Segment)
        {
            List<string> loginList = new List<string>();
            //string qury = "Select cm_cd,cm_pwd,cm_name From Client_master Where cm_cd='" + cm_cd + "'";
            var qury = GetQueryForDetailsData(cm_cd, Td_order, ot_seriesid, ot_exchange, ot_Segment);
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
                return loginList.ToList();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region Portfolio Handler Method
        //TODO : For Getting Portfolio Detail Data
        private dynamic Get_Portfolio_Detail_Data(string cm_cd, string From_Date, string To_Date, string strscripcd)
        {
            List<string> EntityList = new List<string>();
            var ds = Get_QueryFor_Portfolio_Detail_Data(cm_cd, From_Date, To_Date, strscripcd);
            try
            {
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

        //TODO : For Getting Portfolio Detail Data
        private dynamic GetPortfolioData(string cm_cd, string Segment_Type, string Exchange_Type, string From_Date, string To_Date, string CompanyCode)
        {
            List<string> EntityList = new List<string>();
            var ds = Get_QueryFor_Portfolio_Data(cm_cd, Segment_Type, Exchange_Type, From_Date, To_Date, CompanyCode);
            try
            {
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

        #endregion

        #region Holding handler method

        //TODO : For getting Holding Page Load data
        private dynamic GetPageLoadData(string cm_cd)
        {
            List<string> EntityList = new List<string>();
            //HoldingBL _bl = new HoldingBL();
            string qury = GetQueryPageLoadData(cm_cd);
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

        //TODO : For getting Holding Details Data
        private dynamic GetCurrentHoldingDataHandler(string Select_Type, string Holding_Type, string strBoid)
        {
            string qury = GetQueryForCurrentHoldingData(Select_Type, Holding_Type, strBoid);
            try
            {
                var ds = CommonRepository.FillDataset(qury);
                if (ds?.Tables?.Count > 0 && ds?.Tables[0]?.Rows?.Count > 0)
                {
                    //DataTable dt = new DataTable();
                    //EntityList = ConvertData.ConvertDataTable<CurrentHoldingEntity>(ds.Tables[0]);
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

        #endregion





        #region Query

        #region Ledger Query
        // For Ledger summary main data
        private string GetQueryMainData(string Fromdt, string Todt, string Username, string CompCode)
        {
            string strsql = "select * from ( ";
            strsql = strsql + "select 'Trading' as [Type],ld_clientcd as [ClientCode], sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then ld_amount else 0 end) OpeningBalance,sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then 0 else case ld_debitflag when 'D' then ld_amount else 0 end end) Debit, sum(case sign(datediff(d,'" + Fromdt + "',ld_dt)) when -1 then 0 else case ld_debitflag     when 'D' then 0 else ld_amount end end) Credit, sum(ld_amount) Balance ,";
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

        ////// TODO : For Details grid data query
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

        #region OutStanding Query
        ////// For Main grid data query
        private string GetQueryForData(string Username)
        {
            string StrTradesIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
            { StrTradesIndex = "index(idx_trades_clientcd),"; }

            string Strsql = string.Empty;
            DataSet ObjDataSet = new DataSet();
            if (_configuration["IsTradeWeb"] == "O")//Connect to Live DataBase
            {
                string StrCommexConn = "";
                StrCommexConn = objUtility.GetCommexConnection();

                //DateTime.Today.Date.ToString("yyyyMMdd") //HH
                string ddd = DateTime.Now.AddMonths(-9).ToString("yyyyMMdd");

                //Query To Fecth Record From TradePlus DataBase
                Strsql = " Select case td_exchange when 'N' then 1 else 2 end Td_order,";
                Strsql = Strsql + " case right(sm_prodtype,1) when 'F' then 'Future' else 'Option' end+ case td_segment when 'X' then '(Commodities)' else (case sm_prodtype when 'CF' then ' (Currency)'  else ''end )";
                Strsql = Strsql + " end  as tdtype,ltrim(rtrim(sm_desc)) sm_desc,sm_sname,";
                Strsql = Strsql + " sum(td_bqty) ot_bqty, sum(td_sqty) ot_sqty,sum(td_bqty-td_sqty) as net,td_companycode compcode, ";
                Strsql = Strsql + " convert(decimal(15,2), case sum(td_bqty -td_sqty) when 0 then 0 else abs(sum((td_bqty -td_sqty)*td_rate)/sum(td_bqty-td_sqty)) end) ot_avgrate,";
                Strsql = Strsql + " convert(decimal(15,2), (isnull((select ms_lastprice from Market_summary with (nolock) where ms_exchange = td_exchange and ms_Segment = td_Segment and ms_seriesid = td_seriesid and ms_dt = (select max(ms_dt) from Market_summary with (nolock) where ms_exchange = td_exchange and ms_Segment = td_Segment and ms_seriesid = td_seriesid and  ms_dt <= '" + ddd + "')),0)";
                Strsql = Strsql + " + case  when right(sm_prodtype,1) <> 'F' then  sm_strikeprice  else 0 end) ) ot_closeprice,";
                Strsql = Strsql + " convert(decimal(15,2), (isnull((select ms_lastprice from Market_summary with (nolock) where ms_exchange = td_exchange and ms_Segment = td_Segment and ms_seriesid = td_seriesid ";
                Strsql = Strsql + " and ms_dt = (select max(ms_dt) from Market_summary with (nolock) where ms_exchange = td_exchange and ms_Segment = td_Segment and ms_seriesid = td_seriesid and  ms_dt <= '" + ddd + "')),0) ";
                Strsql = Strsql + "  + case when right(sm_prodtype,1) <> 'F' then sm_strikeprice  else 0 end)	";
                Strsql = Strsql + " *sum(td_bqty-td_sqty) * sm_multiplier ) Closing,";
                Strsql = Strsql + " case sm_prodtype when 'IF' then 1 when 'EF' then 2 when 'IO' then 3 when 'EO' then 4 else 5 end listorder,";
                Strsql = Strsql + " '" + ddd + "' ot_dt ,";
                Strsql = Strsql + " case td_Segment when 'K' then case td_exchange when 'N' then 'NSEFX' when 'M' then 'MCXFX' when 'B' then 'BSEFX' end ";
                Strsql = Strsql + " when 'F' then Case td_exchange when 'B' then 'BSE' when 'N' then 'NSE' when 'M' then 'MCX' when 'X' then  Case td_exchange when 'B' then 'BSE' when 'N' then 'NSE' when 'M' then 'MCX' end  end end as strExchange,td_seriesid ot_seriesid,td_exchange ot_exchange,td_segment ot_segment ";
                Strsql = Strsql + " from Trades with (" + StrTradesIndex + "nolock), Series_master with (nolock) ";
                Strsql = Strsql + " where td_seriesid=sm_seriesid and td_exchange = sm_exchange and td_Segment = sm_Segment and td_clientcd='" + Username + "'";
                Strsql = Strsql + " and td_dt <= '" + ddd + "' and sm_expirydt >= '" + ddd + "'";
                Strsql = Strsql + " Group by td_companyCode,td_clientcd ,td_seriesid,td_exchange,td_Segment,sm_sname,sm_prodtype,sm_desc,sm_multiplier,sm_strikeprice";

                string StrComTradesIndex = "";
                //Query To Fecth Record From Commex DataBase
                if (objUtility.GetWebParameter("Commex") != null && objUtility.GetWebParameter("Commex") != string.Empty)
                {
                    if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb(StrCommexConn + ".sysobjects a, " + StrCommexConn + ".sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
                    { StrComTradesIndex = "index(idx_trades_clientcd),"; }

                    Strsql = Strsql + " Union all";
                    Strsql = Strsql + " Select 3 Td_order,case right(sm_prodtype,1)when 'F' then 'Future (Commodities)' else 'Option (Commodities)'";
                    Strsql = Strsql + " end as tdtype,ltrim(rtrim(sm_desc)) sm_desc,sm_sname,sum(td_bqty) ot_bqty,sum(td_sqty) ot_sqty,sum(td_bqty-td_sqty) as net,td_companycode compcode,";
                    Strsql = Strsql + " convert(decimal(15,2), case sum(td_bqty -td_sqty) when 0 then 0 else abs(sum((td_bqty -td_sqty)*td_rate)/sum(td_bqty-td_sqty)) end) ot_avgrate,";
                    Strsql = Strsql + " convert(decimal(15,2), (isnull((select ms_lastprice from " + StrCommexConn + ".Market_summary with (nolock) where ms_exchange = td_exchange and ms_seriesid = td_seriesid and ms_dt = (select max(ms_dt) from " + StrCommexConn + ".Market_summary with (nolock) where ms_exchange = td_exchange and ms_seriesid = td_seriesid and  ms_dt <= '" + ddd + "')),0) ";
                    Strsql = Strsql + "  + case  when right(sm_prodtype,1) <> 'F' then  sm_strikeprice  else 0 end) ) ot_closeprice,";
                    Strsql = Strsql + " convert(decimal(15,2), (isnull((select ms_lastprice from " + StrCommexConn + ".Market_summary with (nolock) where ms_exchange = td_exchange and ms_seriesid = td_seriesid ";
                    Strsql = Strsql + " and ms_dt = (select max(ms_dt) from " + StrCommexConn + ".Market_summary with (nolock) where ms_exchange = td_exchange and ms_seriesid = td_seriesid and  ms_dt <= '" + ddd + "')),0) ";
                    Strsql = Strsql + "  + case  when right(sm_prodtype,1) <> 'F' then sm_strikeprice  else 0 end)	";
                    Strsql = Strsql + "  *sum(td_bqty-td_sqty) * sm_multiplier ) Closing,";
                    Strsql = Strsql + " case sm_prodtype when 'CF'then 11 else 12 end listorder, '" + ddd + "' ot_dt ,";
                    Strsql = Strsql + " case td_exchange when 'M' then 'MCX' when 'N' then 'NCDEX' when 'S' then 'NSEL' when 'F' Then 'NCDEX' end as strExchange,td_seriesid ot_seriesid,td_exchange ot_exchange,'' ot_segment ";
                    Strsql = Strsql + " from " + StrCommexConn + ".Trades with(" + StrComTradesIndex + "nolock), " + StrCommexConn + ".Series_master with (nolock)";
                    Strsql = Strsql + " where td_seriesid=sm_seriesid and td_exchange = sm_exchange and td_clientcd='" + Username + "'";
                    Strsql = Strsql + " and td_dt <= '" + ddd + "' and sm_expirydt > '" + ddd + "'";
                    Strsql = Strsql + " Group by td_companyCode,td_clientcd ,td_seriesid,td_exchange,sm_sname,sm_prodtype,sm_desc,sm_multiplier,sm_strikeprice";
                    Strsql = Strsql + " order by listorder,sm_desc";
                }
            }
            else //Connect to TradeWeb DataBase
            {
                Strsql = " Select case ot_exchange when 'N' then 1 else 2 end Td_order, case left(sm_sname,1) when 'F' then 'Future' else 'Option' end+case sm_prodtype when 'CF' then ' (Currency)' else '' end  as tdtype,ltrim(rtrim(sm_desc)) sm_desc,sm_sname,";
                Strsql = Strsql + " ot_bqty,ot_sqty,(ot_bqty-ot_sqty) as net,ot_companycode compcode, cast(ot_avgrate as decimal(15,2)) ot_avgrate,";
                Strsql = Strsql + " cast(ot_closeprice as decimal(15,2))ot_closeprice,";
                Strsql = Strsql + " cast(((ot_bqty-ot_sqty)*ot_closeprice*sm_multiplier)as decimal(15,2)) as Closing,";
                Strsql = Strsql + " case sm_prodtype when 'IF' then 1 when 'EF' then 2 when 'IO' then 3 when 'EO' then 4 else 5 end listorder,";
                Strsql = Strsql + " convert(char,convert(datetime,ot_dt),103) ot_dt ,";
                Strsql = Strsql + " Case ot_segment when 'F' then case ot_exchange when 'B' then 'BSE' when 'N' then 'NSE' end ";
                Strsql = Strsql + " when 'K' then Case ot_exchange when 'B' then 'BSEFX' when 'N' then 'NSEFX' when 'M' then 'MCFFX' end ";
                Strsql = Strsql + " end  as strExchange, ";
                Strsql = Strsql + " ot_seriesid,ot_exchange,ot_Segment";
                Strsql = Strsql + " from Foutstanding with (nolock), Series_master with (nolock) ";
                Strsql = Strsql + " where ot_seriesid=sm_seriesid and ot_exchange = sm_exchange and ot_Segment = sm_Segment and ot_clientcd='" + Username + "'";
                Strsql = Strsql + " union all";
                Strsql = Strsql + " Select 3 Td_order,case left(sm_sname,1)when 'F' then 'Future (Commodities)' else 'Option (Commodities)'";
                Strsql = Strsql + " end as tdtype,ltrim(rtrim(sm_desc)) sm_desc,sm_sname,ot_bqty,ot_sqty,(ot_bqty-ot_sqty) as net,ot_companycode compcode,";
                Strsql = Strsql + " cast(ot_avgrate as decimal(15,2))ot_avgrate,cast(ot_closeprice as decimal(15,2))ot_closeprice,";
                Strsql = Strsql + " cast(((ot_bqty-ot_sqty)*ot_closeprice*sm_multiplier)as decimal(15,2)) as Closing, case sm_prodtype when 'CF'";
                Strsql = Strsql + " then 11 else 12 end listorder, convert(char,convert(datetime,ot_dt),103) ot_dt ,";
                Strsql = Strsql + " case ot_exchange when 'M' then 'MCX' when 'N' then 'NCDEX' when 'S' then 'NSEL' end as strExchange,ot_seriesid,ot_exchange,'' ot_Segment";
                Strsql = Strsql + " from Coutstanding with (nolock),CSeries_master with (nolock)";
                Strsql = Strsql + " where ot_seriesid=sm_seriesid and ot_exchange = sm_exchange and ot_clientcd='" + Username + "'";
                Strsql = Strsql + " order by listorder,sm_desc";
            }

            return Strsql;
        }

        ////// For Main grids detail data query
        private string GetQueryForDetailsData(string Username, string Td_order, string ot_seriesid, string ot_exchange, string ot_Segment)
        {
            string StrTradesIndex = "";

            string Strsql = string.Empty;

            DataSet ObjDataSet = new DataSet();
            if (Td_order == "1" || Td_order == "2")
            {
                if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
                { StrTradesIndex = "index(idx_trades_clientcd),"; }

                Strsql = " select case left(sm_productcd,1) when 'F' then 'Future' else 'Option' end td_Type, ";
                Strsql = Strsql + " case left(sm_productcd,1) when 'F' then '' else rtrim(sm_callput)+''+ltrim(convert(char,round(sm_strikeprice,0))) ";
                Strsql = Strsql + " end callput,convert(char,convert(datetime,td_dt),103) td_dt, convert(char,convert(datetime,sm_expirydt),103) as expiry, sum(td_bqty) Bqty, convert(decimal(15,2),sum(td_bqty*td_rate) * sm_multiplier ) BAmt, sum(td_sqty) Sqty, ";
                Strsql = Strsql + " convert(decimal(15,2),sum(td_sqty*td_rate) * sm_multiplier ) SAmt, sum(td_bqty-td_sqty) NQty, ";
                Strsql = Strsql + "  convert(decimal(15,2), (sum((td_bqty-td_sqty)*td_rate)) * sm_multiplier )  NAmt ";
                Strsql = Strsql + " from trades with (" + StrTradesIndex + "nolock), series_master with (nolock) where td_clientcd='" + Username + "' ";
                Strsql = Strsql + " and td_exchange=sm_exchange and td_Segment=sm_Segment and td_seriesid=sm_seriesid ";
                Strsql = Strsql + " and td_seriesid='" + ot_seriesid + "' and td_exchange='" + ot_exchange + "' ";
                if (ot_Segment != "")
                {
                    Strsql = Strsql + " and td_Segment='" + ot_Segment + "' ";
                }
                Strsql = Strsql + " and td_trxflag <> 'O' ";
                Strsql = Strsql + " group by td_dt, sm_expirydt, sm_productcd, sm_callput, sm_strikeprice,sm_multiplier";
            }
            if (Td_order == "3")
            {
                string StrComTradesIndex = "";
                Strsql = " select case left(sm_productcd,1) when 'F' then 'Future' else 'Option' end td_Type,";
                Strsql = Strsql + " case left(sm_productcd,1) when 'F' then '' else rtrim(sm_callput)+''+ltrim(convert(char,round(sm_strikeprice,0))) ";
                Strsql = Strsql + " end callput, convert(char,convert(datetime,td_dt),103) td_dt,  convert(char,convert(datetime,sm_expirydt),103) as expiry, sum(td_bqty) Bqty, convert(decimal(15,2),sum(td_bqty*td_rate) * sm_multiplier ) BAmt,";
                Strsql = Strsql + " sum(td_sqty) Sqty, convert(decimal(15,2),sum(td_sqty*td_rate) * sm_multiplier ) SAmt, sum(td_bqty-td_sqty) NQty, ";
                Strsql = Strsql + " convert(decimal(15,2), (sum((td_bqty-td_sqty)*td_rate)) * sm_multiplier )  NAmt ";

                if (_configuration["IsTradeWeb"] == "O")//Connect to Live DataBase
                {
                    string StrCommexConn = "";
                    StrCommexConn = objUtility.GetCommexConnection();
                    if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb(StrCommexConn + ".sysobjects a, " + StrCommexConn + ".sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
                    { StrComTradesIndex = "index(idx_trades_clientcd),"; }

                    Strsql = Strsql + " from " + StrCommexConn + ".trades with (" + StrComTradesIndex + "nolock), " + StrCommexConn + ".series_master with (nolock)";
                }
                else
                {
                    string StrCTradesIndex = "";
                    if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'Ctrades' and b.name", "idx_Ctrades_clientcd", true)) == 1)
                    { StrCTradesIndex = "index(idx_Ctrades_clientcd),"; }

                    Strsql = Strsql + " from ctrades with (" + StrCTradesIndex + "nolock), cseries_master with (nolock)";
                }
                Strsql = Strsql + " where td_exchange=sm_exchange and td_seriesid=sm_seriesid and td_clientcd='" + Username + "'";
                Strsql = Strsql + " and td_seriesid='" + ot_seriesid + "' and td_exchange='" + ot_exchange + "' and td_trxflag <> 'O' ";
                Strsql = Strsql + " group by td_dt, sm_expirydt, sm_productcd, sm_callput, sm_strikeprice ,sm_multiplier ";
            }

            return Strsql;
        }

        #endregion

        #region Portfolio Query
        private DataSet Get_QueryFor_Portfolio_Detail_Data(string cm_cd, string From_Date, string To_Date, string strscripcd)
        {
            DataSet ds = new DataSet();
            SqlConnection con;
            using (var db = new DataContext())
            {
                con = new SqlConnection((db.Database.GetDbConnection()).ConnectionString);
                con.Open();
                ds = BindGridCashDetail(cm_cd, From_Date, To_Date, strscripcd, con);
            }
            return ds;
        }
        private DataSet BindGridCashDetail(string cm_cd, string From_Date, string To_Date, string strscripcd, SqlConnection con)
        {
            prgenerateCash(cm_cd, From_Date, To_Date, strscripcd, con);
            strsql = "update #VX set td_desc = CAse td_flag when 'Y' then 'Square off' else 'Delivery' end ";
            objUtility.ExecuteSQLTmp(strsql, con);

            DataSet ObjDataSet2 = new DataSet();

            if (_configuration["IsTradeWeb"] == "O")//Live DB
            {
                //detail
                strsql = "select '1' as Ord, ss_Lname td_scripnm ,td_stlmnt , sum(td_bqty) BQty, convert(decimal(15,2),sum(td_bqty*td_rate)) BAmount ,  case when SUM(td_bqty) <> 0 then convert(decimal(15,2),sum(td_bqty*td_rate)/sum(td_bqty)) else 0 end BRate,  sum(td_sqty) SQty , convert(decimal(15,2),sum(td_sqty*td_rate)) SAmount ,  case when SUM(td_sqty) <> 0 then convert(decimal(15,2),sum(td_sqty*td_rate)/sum(td_sqty)) else 0 end SRate,isnull(td_desc,'') td_desc";
                strsql += ", Convert(char,Convert(datetime,td_dt ),103) td_dt , td_dt as dt ";
                strsql += " from #VX,securities with(nolock) where td_scripcd=ss_cd  and  td_scripcd ='" + strscripcd + "'";
                strsql += " Group by ss_Lname,td_stlmnt,td_desc,td_dt ";
                if (strscripcd == "")
                {
                    strsql += " Union all ";
                    strsql += " select '2' Ord,'',ic_desc,0,0,0,0,0,Convert(decimal(15,2),sum(ic_amount)) * (-1) amt,'', '', '' from #invcharges";
                    strsql += " where ic_amount > 0";
                    strsql += " group by ic_desc ";
                    strsql += " order by Ord,ss_Lname, dt";
                }

            }
            else
            {
                strsql = "select '1' as Ord, ss_Lname td_scripnm ,td_stlmnt , sum(td_bqty) BQty, convert(decimal(15,2),sum(td_bqty*td_rate)) BAmount ,  case when SUM(td_bqty) <> 0 then convert(decimal(15,2),sum(td_bqty*td_rate)/sum(td_bqty)) else 0 end BRate,  sum(td_sqty) SQty , convert(decimal(15,2),sum(td_sqty*td_rate)) SAmount ,  case when SUM(td_sqty) <> 0 then convert(decimal(15,2),sum(td_sqty*td_rate)/sum(td_sqty)) else 0 end SRate,isnull(td_desc,'') td_desc";
                strsql += ", Convert(char,Convert(datetime,td_dt ),103) td_dt, td_dt as dt ";
                strsql += " from #VX,TPSecurities with (nolock) where td_scripcd=ss_cd and  td_scripcd ='" + strscripcd + "' ";
                strsql += " Group by ss_Lname,td_stlmnt,td_desc,td_dt ";
                if (strscripcd == "")
                {
                    strsql += " Union all ";
                    strsql += " select '2' Ord,'',ic_desc,0,0,0,0,0,Convert(decimal(15,2),sum(ic_amount)) * (-1) amt,'', '', '' from #invcharges";
                    strsql += " where ic_amount > 0";
                    strsql += " group by ic_desc ";
                    strsql += " order by Ord,ss_Lname , dt ";
                }

            }

            ObjDataSet2 = objUtility.OpenDataSetTmp(strsql, con);
            return ObjDataSet2;

        }
        private void prgenerateCash(string cm_cd, string From_Date, string To_Date, string strscripcd, SqlConnection con)
        {
            string StrTRXIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'Trx' and b.name", "idx_Trx_Clientcd", true)) == 1)
            { StrTRXIndex = "index(idx_trx_clientcd),"; }
            string Fromdt = From_Date;
            string Todt = To_Date;
            DataSet ObjDataSet = new DataSet();
            try
            {
                strsql = " CREATE TABLE [#VX] (";
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
            }
            catch (Exception e)
            {
                strsql = "Drop table #VX";
                objUtility.ExecuteSQLTmp(strsql, con);

                strsql = " CREATE TABLE [#VX] (";
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
            }
            try
            {
                strsql = " CREATE TABLE [#invcharges] (";
                strsql += " [ic_stlmnt] [char] (9) NOT NULL ,";
                strsql += " [ic_clientcd] [char] (8) NOT NULL ,";
                strsql += " [ic_desc] [char] (20) NOT NULL ,";
                strsql += " [ic_amount] money";
                strsql += " ) ON [PRIMARY]";
                objUtility.ExecuteSQLTmp(strsql, con);
            }
            catch (Exception ex)
            {
                strsql = "Drop table #invcharges";
                objUtility.ExecuteSQLTmp(strsql, con);

                strsql = " CREATE TABLE [#invcharges] (";
                strsql += " [ic_stlmnt] [char] (9) NOT NULL ,";
                strsql += " [ic_clientcd] [char] (8) NOT NULL ,";
                strsql += " [ic_desc] [char] (20) NOT NULL ,";
                strsql += " [ic_amount] money";
                strsql += " ) ON [PRIMARY]";
                objUtility.ExecuteSQLTmp(strsql, con);
            }

            strsql = " CREATE index #idx_invcharges_clientcd on #invcharges (ic_clientcd) ";
            objUtility.ExecuteSQLTmp(strsql, con);

            if (_configuration["IsTradeWeb"] == "O")//Live DB
            {
                strsql = "insert into #VX SELECT ";
                strsql += " td_companycode ,td_stlmnt ,td_clientcd ,td_scripcd,";
                strsql += " td_dt , td_bsflag , sum(td_bqty) ,sum(td_sqty) ,sum(td_rate*(td_bqty+td_sqty)), sum(td_marketrate*(td_bqty+td_sqty)),'Y' td_flag,ss_name,''";
                strsql += " FROM Trx with(" + StrTRXIndex + "nolock) , securities with(nolock) where td_dt between '" + Fromdt + "' and '" + Todt + "'";
                strsql += " and td_cfflag = 'N' and td_clientcd = '" + cm_cd + "' and td_Scripcd = ss_cd  and  td_Scripcd ='" + strscripcd + "'";
                strsql += " group by td_companycode ,td_stlmnt ,td_clientcd ,td_scripcd, td_dt , td_bsflag,ss_name ";

                //strsql += "Union all SELECT ";
                //strsql += " td_companycode ,td_stlmnt ,td_clientcd ,td_scripcd,";
                //strsql += " td_dt , td_bsflag , sum(td_sqty) ,sum(td_bqty) ,sum(td_marketrate*(td_sqty+td_bqty)), sum(td_marketrate*(td_sqty+td_bqty)),'Y' td_flag,ss_name,''";
                //strsql += " FROM Trx with(" + StrTRXIndex + "nolock) , securities with(nolock) where td_dt between '" + Fromdt + "' and '" + Todt + "'";
                //strsql += " and td_cfflag = 'N' and td_clientcd = '" + cm_cd + "' and td_Scripcd = ss_cd and td_marginyn='B' ";
                //strsql += " group by td_companycode ,td_stlmnt ,td_clientcd ,td_scripcd, td_dt , td_bsflag,ss_name ";

                objUtility.ExecuteSQLTmp(strsql, con);
            }
            else
            {
                strsql = "insert into #VX SELECT ";
                strsql += " td_companycode ,td_stlmnt ,td_clientcd ,td_scripcd,";
                strsql += " td_dt , td_bsflag , sum(td_bqty) ,sum(td_sqty) ,sum(td_rate*(td_bqty+td_sqty)), sum(td_marketrate*(td_bqty+td_sqty)),'Y' td_flag,ss_Name,''";
                strsql += " FROM Trx with(" + StrTRXIndex + "nolock),TPSecurities with (nolock) where td_Scripcd=ss_cd and td_dt between '" + Fromdt + "' and '" + Todt + "'";
                strsql += " and td_cfflag = 'N' and td_clientcd = '" + cm_cd + "' and td_scripcd='" + strscripcd + "'";
                strsql += " group by td_companycode ,td_stlmnt ,td_clientcd ,td_scripcd, td_dt , td_bsflag,ss_Name ";
                objUtility.ExecuteSQLTmp(strsql, con);
            }

            strsql = "insert into #invcharges";
            strsql += " select sh_stlmnt,sh_clientcd,left(sh_desc,12),sh_amount ";
            strsql += " from Specialcharges with (nolock),Settlements with (nolock) ";
            strsql += " Where sh_stlmnt = se_stlmnt and se_endt between '" + Fromdt + "' and '" + Todt + "' and sh_clientcd =('" + cm_cd + "')";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "insert into #invcharges";
            strsql += " select sh_stlmnt,sh_clientcd,left('Service Tax',12),sh_servicetax ";
            strsql += " from Specialcharges with (nolock),Settlements with (nolock) where sh_stlmnt = se_stlmnt";
            strsql += " and sh_servicetaxyn = 'Y' and se_endt between '" + Fromdt + "' and '" + Todt + "' ";
            strsql += " and sh_clientcd ='" + cm_cd + "' ";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "Insert into  #invcharges";
            strsql += " select bc_stlmnt,bc_clientcd,left(cg_desc,12),";
            strsql += " bc_amount from Cbilled_charges with (nolock) ,Charges_master with (nolock) ,Settlements with (nolock)";
            strsql += " Where bc_companycode = cg_companycode And Left(bc_stlmnt, 1) = cg_exchange";
            strsql += " and bc_chargecode = cg_cd and bc_stlmnt = se_stlmnt ";
            strsql += " and se_endt between '" + Fromdt + "' and '" + Todt + "' ";
            strsql += " and bc_clientcd ='" + cm_cd + "' ";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "update #VX set td_rate = Case when (td_bqty+td_sqty) > 0 then td_rate/(td_bqty+td_sqty) else 0 end , td_marketrate= Case When (td_bqty+td_sqty) > 0 then td_marketrate/(td_bqty+td_sqty) else 0 end ";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "Create ";
            strsql += " INDEX [VX_clientscripstlmnt] ON [dbo].[#VX]";
            strsql += " ([td_clientcd], [td_scripcd],[td_dt],[td_stlmnt])";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "update a set a.td_flag = 'N' from #VX a";
            strsql += " where a.td_clientcd + a.td_scripcd + a.td_stlmnt";
            strsql += " in(select b.td_clientcd + b.td_scripcd + b.td_stlmnt";
            strsql += " from #VX b where a.td_clientcd = b.td_clientcd";
            strsql += " and a.td_scripcd = b.td_scripcd";
            strsql += " and a.td_stlmnt = b.td_stlmnt";
            strsql += " group by td_clientcd,td_scripcd,td_stlmnt";
            strsql += " having sum(td_bqty) = 0 or sum(td_sqty) = 0)";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "SELECT td_Stlmnt,td_clientcd , td_scripcd,cm_name,td_scripnm,";
            strsql += " sum(td_bqty) td_bqty ,sum(td_sqty) td_sqty, sum(td_bqty-td_sqty) net";
            strsql += " FROM #VX,Client_master with (nolock) ";
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
                    strsql = "select * from #VX where td_clientcd = '" + strclient + "' and td_scripcd = '" + strscrip + "'";
                    strsql += " and td_stlmnt = '" + strstlmnt + "'";
                    strsql += " and td_bsflag = '" + strDelSide + "' order by td_dt desc,td_stlmnt desc";
                    //SqlDataAdapter ObjAdapter1 = new SqlDataAdapter(strsql, ObjConnection);
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
                                strsql = " insert into #VX select td_companycode ,td_stlmnt,td_clientcd , td_scripcd, td_dt, td_bsflag,";
                                if (strDelSide == "B")
                                {
                                    strsql += lngBalqty + ", td_sqty";
                                }
                                else
                                {
                                    strsql += " td_bqty ," + lngBalqty;
                                }
                                strsql += ", td_rate, td_marketrate,td_flag,td_scripnm,'' from #VX where td_srno =" + lngCurSerial;
                                objUtility.ExecuteSQLTmp(strsql, con);

                                strsql = "update #VX set td_flag = 'N' ";
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
                                strsql = "update #VX set td_flag = 'N' where td_srno = " + lngCurSerial;
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
            strsql += " FROM #VX,Client_master with (nolock) ";
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
                    strsql = "select * from #VX where td_clientcd = '" + strclient + "' and td_scripcd = '" + strscrip + "'";
                    strsql += " and td_bsflag = '" + strDelSide + "' and td_flag = 'N' order by td_dt desc,td_stlmnt desc";
                    //SqlDataAdapter ObjAdapter3 = new SqlDataAdapter(strsql, ObjConnection);
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
                                strsql = " insert into #VX select td_companycode ,td_stlmnt,td_clientcd , td_scripcd, td_dt, td_bsflag,";
                                if (strDelSide == "B")
                                {
                                    strsql += lngBalqty + ", td_sqty";
                                }
                                else
                                {
                                    strsql += " td_bqty ," + lngBalqty;
                                }
                                strsql += ", td_rate, td_marketrate,td_flag,td_scripnm,'' from #VX where td_srno =" + lngCurSerial;
                                objUtility.ExecuteSQLTmp(strsql, con);

                                strsql = "update #VX set td_flag = 'X' ";
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
                                strsql = "update #VX set td_flag = 'X' where td_srno = " + lngCurSerial;
                                objUtility.ExecuteSQLTmp(strsql, con);
                                lngDelQty = lngDelQty - (long.Parse(objdatarow1["td_bqty"].ToString()) + long.Parse(objdatarow1["td_Sqty"].ToString()));
                            }
                            if (lngDelQty <= 0)
                            { break; }
                        }
                    }
                }
            }
        }

        private DataSet Get_QueryFor_Portfolio_Data(string cm_cd, string Segment_Type, string Exchange_Type, string From_Date, string To_Date, string CompanyCode)
        {
            DataSet ds = new DataSet();
            if (cm_cd != "" && Segment_Type != "" && Exchange_Type != "" && From_Date != "" && To_Date != "")
            {
                ds = BindGrid(cm_cd, Segment_Type, Exchange_Type, From_Date, To_Date, CompanyCode);
            }
            return ds;
        }
        private DataSet BindGrid(string cm_cd, string Segment_Type, string Exchange_Type, string From_Date, string To_Date, string CompanyCode)
        {
            DataSet ds = new DataSet();
            string Fromdt = From_Date.ToString();
            string Todt = To_Date.ToString();
            string strexchange = Strings.Left(Exchange_Type, 1);
            if (Exchange_Type.Trim() == "NCDEX")
            {
                strexchange = (objUtility.GetSysParmStComm("CHGNCDEXCD", "") == "Y" ? "F" : "N");
            }
            string strsegment = "";
            strTable = "";
            if (Segment_Type == "COMMODITY")
            {
                if (_configuration["IsTradeWeb"] == "O")
                {
                    if (_configuration["Commex"] != null && _configuration["Commex"] != string.Empty)
                    {
                        string StrCommexConn = "";
                        StrCommexConn = objUtility.GetCommexConnection();
                        strTable = StrCommexConn + ".";
                    }
                    if (Exchange_Type.Trim() == "NSE" || Exchange_Type.Trim() == "BSE")
                    {
                        strsegment = "X";
                        strTable = "";
                    }
                }
                else
                {
                    BindGridFO(cm_cd, Segment_Type, Exchange_Type, From_Date, To_Date);
                }
            }
            else
            {
                strsegment = Segment_Type == "FO" ? "F" : "K";
            }
            if (Segment_Type == "CASH" && Exchange_Type == "NSE")
            {
                SqlConnection con;
                using (var db = new DataContext())
                {
                    con = new SqlConnection((db.Database.GetDbConnection()).ConnectionString);
                    con.Open();
                    prgenerateCash(cm_cd, Segment_Type, Exchange_Type, From_Date, To_Date, con);
                    ds = BindGridCash(cm_cd, Segment_Type, Exchange_Type, From_Date, To_Date, con);
                }
            }
            if (Segment_Type == "FO" || Segment_Type == "FX")
            {
                if (Exchange_Type == "NSE" || Exchange_Type == "MCX" || Exchange_Type == "BSE")
                {
                    if (_configuration["IsTradeWeb"] == "O")//Live DB
                    {
                        SqlConnection con;
                        using (var db = new DataContext())
                        {
                            con = new SqlConnection((db.Database.GetDbConnection()).ConnectionString);
                            con.Open();
                            prGenerateFOTplus(cm_cd, Segment_Type, Exchange_Type, From_Date, To_Date, CompanyCode, con);
                            ds = BindGridFOTplus(cm_cd, Segment_Type, Exchange_Type, From_Date, To_Date, CompanyCode, con);
                        }
                    }
                }
            }
            else if (Segment_Type == "COMMODITY")
            {
                if (Exchange_Type == "MCX" || Exchange_Type == "NCDEX")
                {
                    if (_configuration["IsTradeWeb"] == "O")//Live DB
                    {
                        SqlConnection con;
                        using (var db = new DataContext())
                        {
                            con = new SqlConnection((db.Database.GetDbConnection()).ConnectionString);
                            con.Open();
                            {
                                prGenerateCommodity(cm_cd, Segment_Type, Exchange_Type, From_Date, To_Date, CompanyCode, con);
                                ds = BindGridFOTplus(cm_cd, Segment_Type, Exchange_Type, From_Date, To_Date, CompanyCode, con);
                            }
                        }
                    }
                }
                else
                {
                    ds = BindGridFO(cm_cd, Segment_Type, Exchange_Type, From_Date, To_Date);
                }
            }
            return ds;
        }
        private DataSet BindGridFO(string cm_cd, string Segment_Type, string Exchange_Type, string From_Date, string To_Date)
        {
            string StrTradesIndex = "";
            string Fromdt = From_Date;
            string Todt = To_Date;
            if (Segment_Type == "FO" || Segment_Type == "FX")
            {
                if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
                { StrTradesIndex = "index(idx_trades_clientcd),"; }
                strsql = " select '1' ord,convert(char(10), td_seriesid) td_seriesid,sm_desc ,sum(BF) BF, sum(td_bqty) td_bqty , sum(td_sqty) td_sqty, sum(Exercise) Exercise, ";
                strsql += " sum(assignment) assignment, ";
                strsql += " sum(outqty) outqty, ";
                strsql += " sum(closeqty) closeqty, ";
                strsql += " cast(sum(MTM) as decimal(15,2)) MTM ";
                strsql += " from ";
                strsql += " (select td_seriesid, sm_desc, ";
                strsql += " sum(case when td_trxflag ='O' and td_dt = '" + Fromdt + "' Then td_bqty-td_sqty else 0 end) 'BF', ";
                strsql += " sum(case when td_trxflag  <> 'O' then td_bqty else 0 end) td_bqty , ";
                strsql += " sum(case when td_trxflag  <> 'O' then td_sqty else 0 end) td_sqty , ";
                strsql += " 0 Exercise, 0 assignment, ";
                strsql += " sum(Case Right(sm_prodtype,1) When 'O' Then Case When sm_expirydt > convert(char,getdate(),112) then td_bqty-td_sqty Else 0 end ";
                strsql += " else case when td_trxflag ='O' Then case when td_dt = '" + Fromdt + "' Then td_bqty-td_sqty else 0 end else td_bqty-td_sqty end end) outqty, ";
                strsql += " 0 closeqty, sum(((td_bqty-td_sqty)*(td_rate-case When Right(sm_prodtype,1) ='F' Then td_lastclose else 0 end)) * sm_multiplier) * (-1) MTM ";
                strsql += " from Trades with(" + StrTradesIndex + "nolock),Series_master with (nolock) where td_seriesid=sm_seriesid and td_exchange=sm_exchange ";
                strsql += " and td_segment=sm_segment and  td_clientcd  = '" + cm_cd + "' and td_dt between '" + Fromdt + "' and '" + Todt + "' ";
                strsql += " and td_segment='" + strsegment + "' and td_exchange='" + strexchange + "'";
                strsql += " group by td_seriesid,sm_desc,td_trxflag ";
                strsql += " Union all ";
                strsql += " Select ex_seriesid, sm_desc, 0,0,0,Case ex_eaflag when 'E' then sum(ex_eqty) else 0 end , ";
                strsql += " Case ex_eaflag when 'A' then sum(ex_aqty) else 0 end ,0,0,sum((ex_aqty-ex_eqty)*ex_diffbrokrate * sm_multiplier) * Case ex_eaflag when 'E' then (-1) else 1 end ";
                strsql += " from Exercise with (nolock),Series_master with (nolock) where ex_seriesid=sm_seriesid and ex_exchange=sm_exchange ";
                strsql += " and ex_segment=sm_segment and ex_clientcd = '" + cm_cd + "' and ex_dt between '" + Fromdt + "' and '" + Todt + "' ";
                strsql += " and ex_segment='" + strsegment + "' and ex_exchange='" + strexchange + "'";
                strsql += " group by ex_seriesid,sm_desc,ex_eaflag ";
                strsql += " ) XYZ ";
                strsql += " group by td_seriesid,sm_desc";
                strsql += " union all ";
                strsql += " select '2' ord,'',fc_desc,0,0,0,0,0,0,0,cast(sum(fc_amount) * (-1) as decimal(15,2)) from Fspecialcharges with (nolock) ";
                strsql += " where fc_clientcd = '" + cm_cd + "' and fc_dt between '" + Fromdt + "' and '" + Todt + "' and fc_Segment='" + strsegment + "' and fc_exchange='" + strexchange + "'";
                strsql += " and fc_desc not like '%Exposure Margin%'";
                strsql += " group by fc_desc having round(sum(fc_amount),2) <> 0 order by ord, sm_desc asc";
            }
            else
            {
                if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'Ctrades' and b.name", "idx_Ctrades_clientcd", true)) == 1)
                { StrTradesIndex = "index(idx_Ctrades_clientcd),"; }

                strsql = " select '1' ord,convert(char(10), td_seriesid) td_seriesid,sm_desc ,sum(BF) BF, sum(td_bqty) td_bqty , sum(td_sqty) td_sqty, sum(Exercise) Exercise, ";
                strsql += " sum(assignment) assignment, ";
                strsql += " sum(BF+ td_bqty - td_sqty) outqty, ";
                strsql += " sum(closeqty) closeqty, ";
                strsql += " cast(sum(MTM) as decimal(15,2)) MTM ";
                strsql += " from ";
                strsql += " (select td_seriesid, sm_desc, ";
                strsql += " sum(case when td_trxflag ='O' and td_dt = '" + Fromdt + "' Then td_bqty-td_sqty else 0 end) 'BF', ";
                strsql += " sum(case when td_trxflag  <> 'O' then td_bqty else 0 end) td_bqty , ";
                strsql += " sum(case when td_trxflag  <> 'O' then td_sqty else 0 end) td_sqty , ";
                strsql += " 0 Exercise, 0 assignment, ";
                strsql += " 0 outqty, 0 closeqty, sum((td_bqty-td_sqty)*(td_rate-td_lastclose)*sm_multiplier) * (-1) MTM ";
                strsql += " from Ctrades (" + StrTradesIndex + "nolock),CSeries_master with (nolock) where td_seriesid=sm_seriesid and td_exchange=sm_exchange ";
                strsql += " and td_clientcd  = '" + cm_cd + "' and td_dt between '" + Fromdt + "' and '" + Todt + "' ";
                strsql += " and td_exchange='" + strexchange + "' ";
                strsql += " group by td_seriesid,sm_desc,td_trxflag ";
                strsql += " ) XYZ ";
                strsql += " group by td_seriesid,sm_desc";
                //include chagee[Manish]
                strsql += " union all ";
                strsql += " select '2' ord,'',fc_desc,0,0,0,0,0,0,0,cast(sum(fc_amount) * (-1) as decimal(15,2)) from CFspecialCharges with (nolock)";
                strsql += " where fc_clientcd = '" + cm_cd + "' and fc_dt between '" + Fromdt + "' and '" + Todt + "'  and fc_exchange='" + strexchange + "'";
                strsql += " and fc_desc not like '%Exposure Margin%'";
                strsql += " group by fc_desc having round(sum(fc_amount),2) <> 0 order by ord, sm_desc asc";

            }

            DataSet ObjDataSet2 = new DataSet();
            ObjDataSet2 = objUtility.OpenDataSet(strsql);
            return ObjDataSet2;
        }
        private void prgenerateCash(string cm_cd, string Segment_Type, string Exchange_Type, string From_Date, string To_Date, SqlConnection con)
        {
            string StrTRXIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'Trx' and b.name", "idx_Trx_Clientcd", true)) == 1)
            { StrTRXIndex = "index(idx_trx_clientcd),"; }
            string Fromdt = From_Date.ToString();
            string Todt = To_Date.ToString();
            DataSet ObjDataSet = new DataSet();
            try
            {
                strsql = " CREATE TABLE [#VX] (";
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
            }
            catch (Exception e)
            {
                strsql = "Drop table #VX";
                objUtility.ExecuteSQLTmp(strsql, con);

                strsql = " CREATE TABLE [#VX] (";
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
            }
            try
            {
                strsql = " CREATE TABLE [#invcharges] (";
                strsql += " [ic_stlmnt] [char] (9) NOT NULL ,";
                strsql += " [ic_clientcd] [char] (8) NOT NULL ,";
                strsql += " [ic_desc] [char] (20) NOT NULL ,";
                strsql += " [ic_amount] money";
                strsql += " ) ON [PRIMARY]";
                objUtility.ExecuteSQLTmp(strsql, con);
            }
            catch (Exception ex)
            {
                strsql = "Drop table #invcharges";
                objUtility.ExecuteSQLTmp(strsql, con);

                strsql = " CREATE TABLE [#invcharges] (";
                strsql += " [ic_stlmnt] [char] (9) NOT NULL ,";
                strsql += " [ic_clientcd] [char] (8) NOT NULL ,";
                strsql += " [ic_desc] [char] (20) NOT NULL ,";
                strsql += " [ic_amount] money";
                strsql += " ) ON [PRIMARY]";
                objUtility.ExecuteSQLTmp(strsql, con);
            }

            strsql = " CREATE index #idx_invcharges_clientcd on #invcharges (ic_clientcd) ";
            objUtility.ExecuteSQLTmp(strsql, con);

            if (_configuration["IsTradeWeb"] == "O")//Live DB
            {
                strsql = "insert into #VX SELECT ";
                strsql += " td_companycode ,td_stlmnt ,td_clientcd ,td_scripcd,";
                strsql += " td_dt , td_bsflag , sum(td_bqty) ,sum(td_sqty) ,sum(td_rate*(td_bqty+td_sqty)), sum(td_marketrate*(td_bqty+td_sqty)),'Y' td_flag,ss_name,''";
                strsql += " FROM Trx with(" + StrTRXIndex + "nolock) , securities with(nolock) where td_dt between '" + Fromdt + "' and '" + Todt + "'";
                strsql += " and td_cfflag = 'N' and td_clientcd = '" + cm_cd + "' and td_Scripcd = ss_cd";
                strsql += " group by td_companycode ,td_stlmnt ,td_clientcd ,td_scripcd, td_dt , td_bsflag,ss_name ";

                objUtility.ExecuteSQLTmp(strsql, con);
            }
            else
            {
                strsql = "insert into #VX SELECT ";
                strsql += " td_companycode ,td_stlmnt ,td_clientcd ,td_scripcd,";
                strsql += " td_dt , td_bsflag , sum(td_bqty) ,sum(td_sqty) ,sum(td_rate*(td_bqty+td_sqty)), sum(td_marketrate*(td_bqty+td_sqty)),'Y' td_flag,ss_Name,''";
                strsql += " FROM Trx with(" + StrTRXIndex + "nolock),TPSecurities with (nolock) where td_Scripcd=ss_cd and td_dt between '" + Fromdt + "' and '" + Todt + "'";
                strsql += " and td_cfflag = 'N' and td_clientcd = '" + cm_cd + "'";
                strsql += " group by td_companycode ,td_stlmnt ,td_clientcd ,td_scripcd, td_dt , td_bsflag,ss_Name ";
                objUtility.ExecuteSQLTmp(strsql, con);
            }

            strsql = "insert into #invcharges";
            strsql += " select sh_stlmnt,sh_clientcd,left(sh_desc,12),sh_amount ";
            strsql += " from Specialcharges with (nolock),Settlements with (nolock) ";
            strsql += " Where sh_stlmnt = se_stlmnt and se_endt between '" + Fromdt + "' and '" + Todt + "' and sh_clientcd =('" + cm_cd + "')";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "insert into #invcharges";
            strsql += " select sh_stlmnt,sh_clientcd,left('Service Tax',12),sh_servicetax ";
            strsql += " from Specialcharges with (nolock),Settlements with (nolock) where sh_stlmnt = se_stlmnt";
            strsql += " and sh_servicetaxyn = 'Y' and se_endt between '" + Fromdt + "' and '" + Todt + "' ";
            strsql += " and sh_clientcd ='" + cm_cd + "' ";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "Insert into  #invcharges";
            strsql += " select bc_stlmnt,bc_clientcd,left(cg_desc,12),";
            strsql += " bc_amount from Cbilled_charges with (nolock) ,Charges_master with (nolock) ,Settlements with (nolock)";
            strsql += " Where bc_companycode = cg_companycode And Left(bc_stlmnt, 1) = cg_exchange";
            strsql += " and bc_chargecode = cg_cd and bc_stlmnt = se_stlmnt ";
            strsql += " and se_endt between '" + Fromdt + "' and '" + Todt + "' ";
            strsql += " and bc_clientcd ='" + cm_cd + "' ";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "update #VX set td_rate = Case when (td_bqty+td_sqty) > 0 then td_rate/(td_bqty+td_sqty) else 0 end , td_marketrate= Case When (td_bqty+td_sqty) > 0 then td_marketrate/(td_bqty+td_sqty) else 0 end ";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "Create ";
            strsql += " INDEX [VX_clientscripstlmnt] ON [dbo].[#VX]";
            strsql += " ([td_clientcd], [td_scripcd],[td_dt],[td_stlmnt])";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "update a set a.td_flag = 'N' from #VX a";
            strsql += " where a.td_clientcd + a.td_scripcd + a.td_stlmnt";
            strsql += " in(select b.td_clientcd + b.td_scripcd + b.td_stlmnt";
            strsql += " from #VX b where a.td_clientcd = b.td_clientcd";
            strsql += " and a.td_scripcd = b.td_scripcd";
            strsql += " and a.td_stlmnt = b.td_stlmnt";
            strsql += " group by td_clientcd,td_scripcd,td_stlmnt";
            strsql += " having sum(td_bqty) = 0 or sum(td_sqty) = 0)";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "SELECT td_Stlmnt,td_clientcd , td_scripcd,cm_name,td_scripnm,";
            strsql += " sum(td_bqty) td_bqty ,sum(td_sqty) td_sqty, sum(td_bqty-td_sqty) net";
            strsql += " FROM #VX,Client_master with (nolock) ";
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
                    strsql = "select * from #VX where td_clientcd = '" + strclient + "' and td_scripcd = '" + strscrip + "'";
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
                                strsql = " insert into #VX select td_companycode ,td_stlmnt,td_clientcd , td_scripcd, td_dt, td_bsflag,";
                                if (strDelSide == "B")
                                {
                                    strsql += lngBalqty + ", td_sqty";
                                }
                                else
                                {
                                    strsql += " td_bqty ," + lngBalqty;
                                }
                                strsql += ", td_rate, td_marketrate,td_flag,td_scripnm,'' from #VX where td_srno =" + lngCurSerial;
                                objUtility.ExecuteSQLTmp(strsql, con);

                                strsql = "update #VX set td_flag = 'N' ";
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
                                strsql = "update #VX set td_flag = 'N' where td_srno = " + lngCurSerial;
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
            strsql += " FROM #VX,Client_master with (nolock) ";
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
                    strsql = "select * from #VX where td_clientcd = '" + strclient + "' and td_scripcd = '" + strscrip + "'";
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
                                strsql = " insert into #VX select td_companycode ,td_stlmnt,td_clientcd , td_scripcd, td_dt, td_bsflag,";
                                if (strDelSide == "B")
                                {
                                    strsql += lngBalqty + ", td_sqty";
                                }
                                else
                                {
                                    strsql += " td_bqty ," + lngBalqty;
                                }
                                strsql += ", td_rate, td_marketrate,td_flag,td_scripnm,'' from #VX where td_srno =" + lngCurSerial;
                                objUtility.ExecuteSQLTmp(strsql, con);

                                strsql = "update #VX set td_flag = 'X' ";
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
                                strsql = "update #VX set td_flag = 'X' where td_srno = " + lngCurSerial;
                                objUtility.ExecuteSQLTmp(strsql, con);
                                lngDelQty = lngDelQty - (long.Parse(objdatarow1["td_bqty"].ToString()) + long.Parse(objdatarow1["td_Sqty"].ToString()));
                            }
                            if (lngDelQty <= 0)
                            { break; }
                        }
                    }
                }
            }
        }
        private DataSet BindGridCash(string cm_cd, string Segment_Type, string Exchange_Type, string From_Date, string To_Date, SqlConnection con)
        {
            strsql = "update #VX set td_desc = CAse td_flag when 'Y' then 'Square off' else 'Delivery' end ";
            objUtility.ExecuteSQLTmp(strsql, con);

            DataSet ObjDataSet2 = new DataSet();
            strsql = "select '1' as Ord,td_scripcd,td_scripnm";
            if (_configuration["IsTradeWeb"] == "O")//Live DB
            {

                strsql = " select '1' as Ord, case td_flag When 'Y' Then 1 else 2 end td_flag,ss_Lname td_scripnm ,''td_stlmnt , sum(td_bqty) BQty, convert(decimal(15,2),sum(td_bqty*td_rate)) BAmount ,  case when SUM(td_bqty) <> 0 then convert(decimal(15,2),sum(td_bqty*td_rate)/sum(td_bqty)) else 0 end BRate,  sum(td_sqty) SQty , convert(decimal(15,2),sum(td_sqty*td_rate)) SAmount ,  case when SUM(td_sqty) <> 0 then convert(decimal(15,2),sum(td_sqty*td_rate)/sum(td_sqty)) else 0 end SRate, ";
                strsql += " convert(decimal(15,2), sum( case When td_flag <> 'X' Then ((td_sqty-td_bqty) *td_rate) else 0 end)) RProfitLoss,  ";
                strsql += " convert(decimal(15,2), sum( case When td_flag = 'X' Then (td_bqty-td_sqty)* ((Case Left(td_stlmnt,1) when 'N' then ss_NSERate else ss_BSERate end) - td_rate) else 0 end)) UProfitLoss,  ";
                strsql += " convert(decimal(15,2), sum( case When td_flag = 'X' Then ((td_bqty-td_sqty)) else 0 end)) NetQty,  convert(decimal(15,2), sum( case When td_flag = 'X' Then ((td_sqty-td_bqty) * (Case Left(td_stlmnt,1) when 'N' then ss_NSERate else ss_BSERate end) ) else 0 end))  NetAmount,  ";
                strsql += " case When sum( case when td_flag = 'X'  Then (td_bqty-td_sqty) else 0 end) <> 0 then abs(convert(decimal(15,2),sum(Case When td_flag = 'X' Then (td_sqty-td_bqty)*(Case Left(td_stlmnt,1) when 'N' then ss_NSERate else ss_BSERate end) else 0 end )/sum(td_bqty-td_sqty))) else 0 end Nrate, ";
                strsql += " isnull(td_desc,'') td_desc,td_scripcd  ";
                strsql += " from #VX,securities with(nolock) where td_scripcd=ss_cd ";
                strsql += " Group by ss_Lname,case td_flag When 'Y' Then 1 else 2 end ,td_desc,td_scripcd ";
                strsql += " Union all ";
                strsql += " select '2' Ord,'','Charges' ic_desc,'',0,0,0,0,0,0,convert(decimal(15,2),sum(ic_amount)) * (-1),0,0,0,0 amt ,'','' from #invcharges";
                strsql += " where ic_amount > 0";
                //strsql += " group by ic_desc ";
                strsql += " order by Ord,ss_Lname,td_flag desc";

            }
            else
            {

                strsql = " select '1' as Ord, case td_flag When 'Y' Then 1 else 2 end td_flag,ss_Lname td_scripnm ,''td_stlmnt , sum(td_bqty) BQty, convert(decimal(15,2),sum(td_bqty*td_rate)) BAmount ,  case when SUM(td_bqty) <> 0 then convert(decimal(15,2),sum(td_bqty*td_rate)/sum(td_bqty)) else 0 end BRate,  sum(td_sqty) SQty , convert(decimal(15,2),sum(td_sqty*td_rate)) SAmount ,  case when SUM(td_sqty) <> 0 then convert(decimal(15,2),sum(td_sqty*td_rate)/sum(td_sqty)) else 0 end SRate, ";
                strsql += " convert(decimal(15,2), sum( case When td_flag <> 'X' Then ((td_sqty-td_bqty) *td_rate) else 0 end)) RProfitLoss,  ";
                strsql += " convert(decimal(15,2), sum( case When td_flag = 'X' Then (td_bqty-td_sqty)* ((Case Left(td_stlmnt,1) when 'N' then ss_NSERate else ss_BSERate end) - td_rate) else 0 end)) UProfitLoss,  ";
                strsql += " convert(decimal(15,2), sum( case When td_flag = 'X' Then ((td_bqty-td_sqty)) else 0 end)) NetQty,  convert(decimal(15,2), sum( case When td_flag = 'X' Then ((td_sqty-td_bqty) * (Case Left(td_stlmnt,1) when 'N' then ss_NSERate else ss_BSERate end) ) else 0 end))  NetAmount,  ";
                strsql += " case When sum( case when td_flag = 'X'  Then (td_bqty-td_sqty) else 0 end) <> 0 then abs(convert(decimal(15,2),sum(Case When td_flag = 'X' Then (td_sqty-td_bqty)* ";
                strsql += " (Case Left(td_stlmnt,1) when 'N' then ss_NSERate else ss_BSERate end) else 0 end )/sum(td_bqty-td_sqty))) else 0 end Nrate, isnull(td_desc,'') td_desc,td_scripcd ";
                strsql += " from #VX,TPSecurities with (nolock) where td_scripcd=ss_cd ";
                strsql += " Group by ss_Lname,case td_flag When 'Y' Then 1 else 2 end ,td_desc,td_scripcd ";
                strsql += " Union all ";
                strsql += " select '2' Ord,'','Charges' ic_desc,'',0,0,0,0,0,0, convert(decimal(15,2),sum(ic_amount)) * (-1),0,0,0,0 amt ,'','' from #invcharges";
                strsql += " where ic_amount > 0";
                //strsql += " group by ic_desc ";
                strsql += " order by Ord,ss_Lname,td_flag desc";
            }

            ObjDataSet2 = objUtility.OpenDataSetTmp(strsql, con);
            return ObjDataSet2;
        }
        private void prGenerateFOTplus(string cm_cd, string Segment_Type, string Exchange_Type, string From_Date, string To_Date, string CompanyCode, SqlConnection con)
        {
            string Fromdt = From_Date;
            string Todt = To_Date;
            string StrTradesIndex = "";
            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb("sysobjects a, sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
            { StrTradesIndex = "index(idx_trades_clientcd),"; }

            string strFirstDate = string.Empty;
            string strLastDate = string.Empty;

            prCreatetemptableDate(From_Date, To_Date, CompanyCode, con);
            prCreatetemptableCharges(con);
            prCreateTempTable(con);

            strsql = "select isnull(min(bd_dt),'" + Fromdt + "'),isNull(max(bd_dt),'') from #finvdates ";
            DataSet ObjDatasetH = new DataSet();

            ObjDatasetH = objUtility.OpenDataSetTmp(strsql, con);
            strFirstDate = ObjDatasetH.Tables[0].Rows[0][0].ToString();
            strLastDate = ObjDatasetH.Tables[0].Rows[0][1].ToString();

            //Futures opening       
            strsql = "Insert into #tmpfinvestorrep ";
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
            strsql += " and td_exchange = '" + strexchange + "' and td_segment = '" + strsegment + "' and sm_prodtype in('IF','EF','CF','RF','TF')";
            strsql += " and td_clientcd = '" + cm_cd + "'";
            strsql += " and cm_type <> 'C'";
            strsql += " group by td_exchange,td_clientcd,td_seriesid,sm_prodtype,sm_multiplier,td_segment";
            strsql += " having sum(td_bqty - td_sqty) <> 0";
            objUtility.ExecuteSQLTmp(strsql, con);

            //Opening for Options
            strsql = "insert into #tmpfinvestorrep ";
            strsql += " select '" + strFirstDate + "',td_clientcd,td_exchange,";
            strsql += " td_seriesid, sum(case sale when 0 then buy else 0 end) td_bqty,";
            strsql += " 0,";
            strsql += " sum(case sale when 0 then 0 else sale end) td_sqty,0,";
            strsql += " 0,0,0,0 td_closeprice,0,";
            strsql += " case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'TF' then 2 when 'RF' then 2 when 'EF' then 2 when 'IO' then 3 else 4 end td_sortlist,";
            strsql += " 1 td_controlflag,sm_prodtype,'N','O',sm_multiplier,td_segment";
            strsql += " From vwFoutstandingpos  ";
            strsql += " Where sm_expirydt >= '" + Fromdt + "' and  td_dt < '" + Fromdt + "'";
            strsql += " and td_clientcd = '" + cm_cd + "'";
            strsql += " and td_exchange = '" + strexchange + "' and td_segment = '" + strsegment + "' and sm_prodtype in('IO','EO','CO')";
            strsql += " and cm_type <> 'C'";
            strsql += " group by td_exchange,td_clientcd,td_seriesid,sm_prodtype,sm_multiplier,td_segment";
            strsql += " having sum(sale - buy) <> 0 ";
            objUtility.ExecuteSQLTmp(strsql, con);

            //Futures/Options running
            strsql = "insert into #tmpfinvestorrep ";
            strsql += " select td_dt,td_clientcd,td_exchange,";
            strsql += " td_seriesid,td_bqty,0,td_sqty,0,0,0,";
            strsql += " td_rate,0.0000 td_closeprice,0 mtm,";
            strsql += " case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'TF' then 2 when 'RF' then 2 when 'EF' then 2 when 'IO' then 3 else 4 end td_sortlist,";
            strsql += " 2,sm_prodtype,'N','Y',sm_multiplier,td_segment";
            strsql += " From Trades with(index(idx_trades_dt_clientcd)) , Series_master,Client_master";
            strsql += " Where td_clientcd = cm_cd and td_exchange = sm_exchange and td_segment = sm_segment and td_seriesid = sm_seriesid";
            strsql += " and td_exchange = '" + strexchange + "' and td_segment = '" + strsegment + "'";
            strsql += " and sm_expirydt >= '" + strFirstDate + "' and td_dt between '" + strFirstDate + "' and '" + strLastDate + "'";
            strsql += " and td_clientcd = '" + cm_cd + "'";
            objUtility.ExecuteSQLTmp(strsql, con);

            //Exercise/Assignments
            strsql = "insert into #tmpfinvestorrep ";
            strsql += " select ex_dt,ex_clientcd,ex_exchange,";
            strsql += " ex_seriesid,ex_eqty,0,ex_aqty,0,0,0,";
            strsql += " ex_diffbrokrate,ex_settlerate,0,";
            strsql += " case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'TF' then 2  when 'RF' then 2 when 'EF' then 2 when 'IO' then 3 else 4 end + 5 td_sortlist,";
            strsql += " case ex_eaflag when 'E' then 3 else 4 end td_controlflag,sm_prodtype,'N','Y',sm_multiplier,ex_segment";
            strsql += " From Exercise, Series_master,Client_master";
            strsql += " Where ex_clientcd = cm_cd and ex_exchange = sm_exchange and ex_segment = sm_segment And ex_seriesid = sm_seriesid";
            strsql += " and ex_exchange = '" + strexchange + "' and ex_segment ='" + strsegment + "'";
            strsql += " and sm_expirydt >= '" + strFirstDate + "' and  ex_dt between '" + strFirstDate + "' and '" + strLastDate + "'";
            strsql += " and ex_clientcd = '" + cm_cd + "'";
            objUtility.ExecuteSQLTmp(strsql, con);

            //Reverse Future Outstanding
            strsql = "insert into #tmpfinvestorrep ";
            strsql += " select '" + strLastDate + "',fi_clientcd,fi_exchange,";
            strsql += " fi_seriesid,case sign(sum(case fi_controlflag when 1 then case fi_dt when '" + strFirstDate + "' then fi_bqty - fi_sqty else 0 end when 2 then fi_bqty - fi_sqty else fi_sqty - fi_bqty end)) when -1 then abs(sum(case fi_controlflag when 1 then case fi_dt when '" + strFirstDate + "' then fi_bqty - fi_sqty else 0 end when 2 then fi_bqty - fi_sqty else fi_sqty - fi_bqty end)) else 0 end td_bqty,";
            strsql += " 0,";
            strsql += " case sign(sum(case fi_controlflag when 1 then case fi_dt when '" + strFirstDate + "' then fi_bqty - fi_sqty else 0 end when 2 then fi_bqty - fi_sqty else fi_sqty - fi_bqty end)) when 1 then abs(sum(case fi_controlflag when 1 then case fi_dt when '" + strFirstDate + "' then fi_bqty - fi_sqty else 0 end when 2 then fi_bqty - fi_sqty else fi_sqty - fi_bqty end)) else 0 end td_sqty,0,";
            strsql += " 0,0,0,0 td_closeprice,0,";
            strsql += " case fi_prodtype when 'IF' then 1 when 'CF' then 1 when 'TF' then 2 when 'RF' then 2 when 'EF' then 2 when 'IO' then 3 else 4 end + 6 td_sortlist,";
            strsql += " 5 td_controlflag,fi_prodtype,'R','N',sm_multiplier,fi_segment";
            strsql += " From #tmpfinvestorrep ,Series_master";
            strsql += " Where fi_exchange = sm_exchange and fi_segment = sm_segment and sm_seriesid = fi_seriesid and fi_prodtype in('IF','EF','CF','RF','TF') ";
            strsql += " and sm_expirydt <= '" + strLastDate + "'";
            strsql += " group by fi_exchange,fi_clientcd,fi_seriesid,fi_prodtype,sm_multiplier,fi_segment";
            strsql += " having sum(case fi_controlflag when 1 then case fi_dt when '" + strFirstDate + "' then fi_bqty - fi_sqty else 0 end when 2 then fi_bqty - fi_sqty else fi_sqty - fi_bqty end) <> 0";
            objUtility.ExecuteSQLTmp(strsql, con);

            //Update Last Market Price for Options
            strsql = "update #tmpfinvestorrep  set fi_rate = case fi_type when 'R' then ms_lastprice else fi_rate end,fi_closeprice = ms_lastprice from #tmpfinvestorrep ,Market_summary";
            strsql += " where ms_seriesid = fi_seriesid ";
            strsql += " and ms_exchange = fi_exchange and ms_segment = fi_segment ";
            strsql += " and ms_dt = (select max(ms_dt) from Market_summary where ms_exchange = fi_exchange and ms_segment = fi_segment ";
            strsql += " and ms_seriesid = fi_seriesid and ms_lastprice <> 0 and ms_dt <= '" + strLastDate + "' )";
            strsql += " and fi_prodtype in('IO','EO','CO')";
            objUtility.ExecuteSQLTmp(strsql, con);

            //Update Previous close and today's close prices
            strsql = "update #tmpfinvestorrep  set fi_closeprice =  isNull((select ms_lastprice From Market_summary ";
            strsql += " Where ms_exchange = '" + strexchange + "' and ms_segment ='" + strsegment + "' and ms_seriesid = fi_seriesid ";
            strsql += " and ms_dt = (select Max(ms_dt) From Market_Summary ";
            strsql += " Where ms_exchange = '" + strexchange + "' and ms_segment ='" + strsegment + "' and ms_seriesid = fi_seriesid ";
            strsql += " and ms_dt <='" + strLastDate + "')),0) ";
            strsql += " where fi_controlflag in('1','2') and fi_prodtype in('IF','EF','CF','RF','TF') ";
            objUtility.ExecuteSQLTmp(strsql, con);

            //Update close Price If Expiry Trade is Not Generated.
            strsql = "update #tmpfinvestorrep  set fi_rate =  ms_lastprice  , fi_closeprice = ms_lastprice ";
            strsql += " from #tmpfinvestorrep ,Market_summary , Series_master ";
            strsql += " where sm_Exchange= '" + strexchange + "' and sm_segment = '" + strsegment + "' and sm_seriesid = fi_seriesid ";
            strsql += " and sm_exchange = ms_exchange and sm_segment = ms_segment and sm_seriesid = ms_seriesid  and sm_expirydt = ms_dt ";
            strsql += " and ms_dt < '" + strLastDate + "'";
            strsql += " and fi_prodtype in('IF','EF','CF','RF','TF') and fi_type = 'R' ";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "update #tmpfinvestorrep  set fi_rate = ms_prcloseprice from #tmpfinvestorrep ,Market_summary";
            strsql += " where ms_seriesid = fi_seriesid and fi_controlflag = 1";
            strsql += " and ms_exchange = '" + strexchange + "' and ms_segment ='" + strsegment + "'";
            strsql += " and ms_dt = fi_dt";
            strsql += " and fi_prodtype in('IF','EF','CF','RF','TF')";
            objUtility.ExecuteSQLTmp(strsql, con);
            //End of updation of close prices

            //Service tax here for Trades
            strsql = "insert into #tmpFinvcharges select td_dt,td_clientcd,'SERVICE TAX',round(sum(td_servicetax),2),0 from Trades with(index(idx_trades_dt_clientcd)) ,#finvdates,Client_master,Series_master";
            strsql += " where td_clientcd = cm_cd and td_dt = bd_dt";
            strsql += " and td_exchange = sm_exchange and td_segment = sm_segment ";
            strsql += " and td_seriesid = sm_seriesid";
            strsql += " and td_exchange = '" + strexchange + "' and td_segment = '" + strsegment + "'";
            strsql += " group by td_dt,td_clientcd having sum(td_servicetax) <> 0";
            objUtility.ExecuteSQLTmp(strsql, con);

            //Service tax here for Exercise
            strsql = "insert into #tmpFinvcharges select ex_dt,ex_clientcd,'SERVICE TAX',round(sum(ex_servicetax),2),0 from Exercise,#finvdates,Client_master,Series_master";
            strsql += " where ex_clientcd = cm_cd and ex_dt = bd_dt";
            strsql += " and ex_exchange = sm_exchange and ex_segment = sm_segment ";
            strsql += " and ex_seriesid = sm_seriesid";
            strsql += " and ex_exchange = '" + strexchange + "' and ex_segment ='" + strsegment + "'";
            strsql += " group by ex_dt,ex_clientcd having sum(ex_servicetax) <> 0";
            objUtility.ExecuteSQLTmp(strsql, con);

            //Charges here
            //-----------from specialcharges start;
            strsql = "insert into #tmpFinvcharges select fc_dt,fc_clientcd,fc_desc,round(sum(fc_amount),2),0 from Fspecialcharges,#finvdates,Client_master";
            strsql += " where  fc_clientcd = cm_cd and fc_dt = bd_dt";
            strsql += " and fc_clientcd = '" + cm_cd + "'";
            strsql += " and fc_desc not like '%{New%' and fc_exchange='" + strexchange + "' and fc_segment='" + strsegment + "' and fc_desc not like '%{Old%'";
            strsql += " and fc_chargecode not in ('EMR') ";
            strsql += " group by fc_dt,fc_clientcd,fc_desc having round(sum(fc_amount),2) <> 0";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "insert into #tmpFinvcharges select fc_dt,fc_clientcd,'SERVICE TAX',round(sum(fc_servicetax),2),0 from Fspecialcharges,#finvdates,Client_master";
            strsql += " where fc_exchange='" + strexchange + "' and fc_segment='" + strsegment + "' and fc_clientcd = cm_cd and fc_dt = bd_dt";
            strsql += " and fc_clientcd = '" + cm_cd + "'";
            strsql += " group by fc_dt,fc_clientcd,fc_desc having round(sum(fc_servicetax),2) <> 0";
            objUtility.ExecuteSQLTmp(strsql, con);
            //-----------from specialcharges end

            //---------------Update values for MTM and Premium
            strsql = "update #tmpfinvestorrep  set fi_bvalue = fi_bqty*fi_rate*fi_multiplier,fi_svalue = fi_sqty*fi_rate*fi_multiplier,";
            strsql += "fi_netqty = fi_bqty - fi_sqty,fi_netvalue = (fi_bqty - fi_sqty)*fi_rate*fi_multiplier";
            strsql += " where fi_controlflag not in(3,4)";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "update #tmpfinvestorrep  set fi_bvalue = fi_bqty*fi_rate*fi_multiplier,fi_svalue = fi_sqty*fi_rate*fi_multiplier,";
            strsql += "fi_netqty = fi_sqty - fi_bqty,fi_netvalue = (fi_bqty + fi_sqty)*fi_rate*fi_multiplier";
            strsql += " where fi_controlflag in(3,4)";
            objUtility.ExecuteSQLTmp(strsql, con);

            strsql = "update #tmpfinvestorrep  set fi_mtm = round((((fi_sqty - fi_bqty)*fi_rate*fi_multiplier) - ((fi_sqty - fi_bqty)*fi_closeprice*fi_multiplier)),4)";
            strsql += " where fi_prodtype in('IF','EF','CF','RF','TF')";
            objUtility.ExecuteSQLTmp(strsql, con);


            strsql = "update #tmpfinvestorrep  set fi_mtm = round(((case fi_controlflag when 3  then (fi_bqty + fi_sqty) when 4 then (fi_bqty + fi_sqty) else (fi_bqty - fi_sqty)*(-1) end) *fi_rate*fi_multiplier),4)";
            strsql += " where fi_prodtype in('IO','EO','CO')";
            objUtility.ExecuteSQLTmp(strsql, con);
        }
        private DataSet BindGridFOTplus(string cm_cd, string Segment_Type, string Exchange_Type, string From_Date, string To_Date, string CompanyCode, SqlConnection con)
        {

            string Todt = To_Date;
            strsql = " select Convert(char(12), sm_seriesid) sm_seriesid,sm_symbol,sm_desc, ";
            strsql += " case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'TF' then 2 when 'RF' then 2 when 'EF' then 2 when 'IO' then 3 else 4 end listorder,";
            strsql += " sum(case fi_balfield when 'O' then fi_bqty - fi_sqty else 0 end) BF,";
            strsql += " sum(case fi_type when 'N' then case fi_controlflag when 2 then fi_bqty else 0 end else 0 end) buyqty,";
            strsql += " sum(case fi_type when 'N' then case fi_controlflag when 2 then fi_sqty else 0 end else 0 end) sellqty,";
            strsql += " sum(case fi_type when 'N' then case fi_controlflag when 3 then abs(fi_bqty) else 0 end else 0 end) exerqty,";
            strsql += " sum(case fi_type when 'N' then case fi_controlflag when 4 then abs(fi_sqty) else 0 end else 0 end) assgnqty,";
            strsql += " sum(case when sm_expirydt <= '" + Todt + "' then 0 else case fi_balfield when 'N' then  0 else (fi_bqty)*case fi_controlflag when 3 then -1 else 1 end  - (fi_sqty)*case fi_controlflag when 4 then -1 else 1 end end end) outqty,";
            strsql += " sum(case fi_type when 'R' then  fi_sqty - fi_bqty else 0 end) closeqty,";
            strsql += " convert(decimal(15,2), sum(fi_mtm)) mtm,";
            strsql += " convert(decimal(15,2),sum(case fi_type when 'N' then case fi_controlflag when 2 then fi_bvalue else 0 end else 0 end)) buyvalue,";
            strsql += " convert(decimal(15,2),sum(case fi_type when 'N' then case fi_controlflag when 2 then fi_svalue else 0 end else 0 end)) sellvalue,";
            strsql += " convert(decimal(15,2),case when (sum(case fi_type when 'N' then case fi_controlflag when 2 then fi_bqty else 0 end else 0 end) >0) then ((sum(fi_bvalue)/sum(fi_bqty))/ fi_multiplier) else 0 end) Brate, ";
            strsql += " convert(decimal(15,2),case when (sum(case fi_type when 'N' then case fi_controlflag when 2 then fi_sqty else 0 end else 0 end) >0) then ((sum(fi_svalue) /sum(fi_sqty ))/ fi_multiplier) else 0 end) Srate ";
            strsql += " ,convert(decimal(15,2),case when (sum(case when sm_expirydt <= '" + Todt + "' then 0 else case fi_balfield when 'N' then  0 else (fi_bqty)*case fi_controlflag when 3 then -1 else 1 end  - (fi_sqty)*case fi_controlflag when 4 then -1 else 1 end end end) >0 and fi_multiplier > 0) then ((sum(fi_netvalue) /sum(case when sm_expirydt <= '" + Todt + "' then 0 else case fi_balfield when 'N' then  0 else (fi_bqty)*case fi_controlflag when 3 then -1 else 1 end  - (fi_sqty)*case fi_controlflag when 4 then -1 else 1 end end end)) / fi_multiplier) else 0 end) outRate ";
            strsql += " from #tmpfinvestorrep   a,Client_master with (nolock) ," + strTable + "Series_master with (nolock)";
            strsql += " where fi_clientcd = cm_cd and fi_exchange = sm_exchange ";
            if (strTable == "")
            {
                strsql += " and fi_segment = sm_segment ";
            }
            strsql += " and fi_seriesid = sm_seriesid ";
            strsql += " group by fi_clientcd,fi_seriesid,cm_name,sm_seriesid,sm_prodtype,sm_symbol,sm_sname,sm_desc,fi_multiplier";
            strsql += "  Union all ";
            strsql += "select '','',bc_desc,'5',0,0,0,0,0,0,0,convert(decimal(15,2),sum(bc_amount*(-1)))  chrg,0,0,0,0,0 from #tmpFinvcharges";
            strsql += " where bc_clientcd = '" + cm_cd + "'";
            strsql += " group by bc_desc ";
            strsql += " Having sum(bc_amount*(-1)) <> 0 ";
            strsql += " order by listorder,sm_symbol";
            DataSet ObjDataSet2 = new DataSet();

            ObjDataSet2 = objUtility.OpenDataSetTmp(strsql, con);
            return ObjDataSet2;
        }
        private void prCreatetemptableDate(string From_Date, string To_Date, string CompanyCode, SqlConnection con)
        {
            DateTime strBillstDt;
            DateTime strBillenDt;
            string strdate = string.Empty;
            try
            {
                strsql = "CREATE TABLE [dbo].[#finvdates] (";
                strsql += "[bd_dt] [char] (8) NOT NULL ";
                strsql += ")";
                objUtility.ExecuteSQLTmp(strsql, con);
            }
            catch (Exception e)
            {
                strsql = "Drop table #finvdates";
                objUtility.ExecuteSQLTmp(strsql, con);

                strsql = "CREATE TABLE [dbo].[#finvdates] (";
                strsql += "[bd_dt] [char] (8) NOT NULL ";
                strsql += ")";
            }

            strBillstDt = objUtility.stod(From_Date.ToString());
            strBillenDt = objUtility.stod(To_Date.ToString());

            while (strBillstDt <= strBillenDt)
            {
                strdate = objUtility.dtos(strBillstDt.ToString());
                strsql = "select count(*) cnt from " + strTable + "Fholiday_master with (nolock) where hm_companycode = '" + Convert.ToString(CompanyCode) + "'";
                strsql += " and hm_exchange = '" + strexchange + "'";
                if (strTable.Trim() == "")
                {
                    strsql += " and hm_segment ='" + strsegment + "'";
                }
                strsql += " and hm_dt = '" + strdate + "'";
                DataSet ObjDatasetH = new DataSet();
                ObjDatasetH = objUtility.OpenDataSetTmp(strsql, con);
                if (Convert.ToInt64(ObjDatasetH.Tables[0].Rows[0]["cnt"]) == 0)
                {
                    objUtility.ExecuteSQLTmp("insert into #finvdates values('" + strdate + "')", con);
                }
                strBillstDt = strBillstDt.AddDays(1);
            }
        }
        private void prCreatetemptableCharges(SqlConnection con)
        {
            try
            {
                strsql = "CREATE TABLE [dbo].[#tmpFinvcharges] (";
                strsql += "[bc_dt] [char] (8) NOT NULL,";
                strsql += "[bc_clientcd] [char] (8) NOT NULL,";
                strsql += "[bc_desc] [char] (40) NOT NULL,";
                strsql += "[bc_amount] [money] NOT NULL,";
                strsql += "[bc_billno] [numeric] NOT NULL";
                strsql += ")";
                objUtility.ExecuteSQLTmp(strsql, con);
            }
            catch (Exception e)
            {
                strsql = "Drop table #tmpFinvcharges";
                objUtility.ExecuteSQLTmp(strsql, con);

                strsql = "CREATE TABLE [dbo].[#tmpFinvcharges] (";
                strsql += "[bc_dt] [char] (8) NOT NULL,";
                strsql += "[bc_clientcd] [char] (8) NOT NULL,";
                strsql += "[bc_desc] [char] (40) NOT NULL,";
                strsql += "[bc_amount] [money] NOT NULL,";
                strsql += "[bc_billno] [numeric] NOT NULL";
                strsql += ")";
                objUtility.ExecuteSQLTmp(strsql, con);
            }
        }
        private void prCreateTempTable(SqlConnection con)
        {
            try
            {
                strsql = "Create table #tmpfinvestorrep  (";
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
            }
            catch (Exception e)
            {
                strsql = "Drop table #tmpfinvestorrep  ";
                objUtility.ExecuteSQLTmp(strsql, con);


                strsql = "Create table #tmpfinvestorrep  (";
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
            }
        }
        private void prGenerateCommodity(string cm_cd, string Segment_Type, string Exchange_Type, string From_Date, string To_Date, string CompanyCode, SqlConnection con)
        {
            string strFirstDate = string.Empty;
            string strLastDate = string.Empty;
            string Fromdt = From_Date;
            string Todt = To_Date;

            prCreatetemptableDate(From_Date, To_Date, CompanyCode, con);
            prCreatetemptableCharges(con);
            prCreateTempTable(con);

            strsql = "select isnull(min(bd_dt),'" + Fromdt + "'),isNull(max(bd_dt),'') from #finvdates ";
            DataSet ObjDatasetH = new DataSet();

            ObjDatasetH = objUtility.OpenDataSetTmp(strsql, con);
            strFirstDate = ObjDatasetH.Tables[0].Rows[0][0].ToString();
            strLastDate = ObjDatasetH.Tables[0].Rows[0][1].ToString();

            string StrCommexConn = "";
            StrCommexConn = objUtility.GetCommexConnection();
            string StrComTradesIndex = string.Empty;

            if (Convert.ToInt16(objUtility.fnFireQueryTradeWeb(StrCommexConn + ".sysobjects a, " + StrCommexConn + ".sysindexes b", "COUNT(0)", "a.id = b.id and a.name = 'trades' and b.name", "idx_trades_clientcd", true)) == 1)
            { StrComTradesIndex = "index(idx_trades_clientcd),"; }

            //Futures opening
            strsql = "Insert into #tmpfinvestorrep  ";
            strsql += " Select '" + strFirstDate + "',td_clientcd,td_exchange,";
            strsql += " td_seriesid,case sign(sum(td_bqty - td_sqty)) when 1 then abs(sum(td_bqty - td_sqty)) else 0 end  td_bqty,0,";
            strsql += " case sign(sum(td_bqty - td_sqty)) when 1 then 0 else abs(sum(td_bqty - td_sqty)) end td_sqty,0,0,0,0,0 td_closeprice,0,";
            strsql += " case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'TF' then 2 when 'RF' then 2 when 'EF' then 2 when 'IO' then 3 else 4 end td_sortlist,";
            strsql += " 1 td_controlflag,sm_prodtype,'N','O',sm_multiplier,'X'";
            strsql += " From " + strTable + " Trades with (" + StrComTradesIndex + "nolock)," + strTable + "Series_master with (nolock)," + strTable + "Client_master with (nolock)";
            strsql += " Where td_clientcd = cm_cd and td_exchange = sm_exchange And td_seriesid = sm_seriesid";
            strsql += " and td_clientcd = '" + cm_cd + "'";
            strsql += " and sm_expirydt >= '" + strFirstDate + "' and  td_dt < '" + strFirstDate + "'";
            strsql += " and td_exchange = '" + strexchange + "' and sm_prodtype in('IF','EF','CF','RF','TF')";
            strsql += " and cm_type <> 'C'";
            strsql += " group by td_exchange,td_clientcd,td_seriesid,sm_prodtype,sm_multiplier";
            strsql += " having sum(td_bqty - td_sqty) <> 0";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            //Opening for Options
            strsql = "insert into #tmpfinvestorrep  ";
            strsql += " select '" + strFirstDate + "',td_clientcd,td_exchange,";
            strsql += " td_seriesid, sum(case sale when 0 then buy else 0 end) td_bqty,";
            strsql += " 0,";
            strsql += " sum(case sale when 0 then 0 else sale end) td_sqty,0,";
            strsql += " 0,0,sum((buy+sale)*td_rate) / sum((buy+sale)),0 td_closeprice,0,";
            strsql += " case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'TF' then 2 when 'RF' then 2 when 'EF' then 2 when 'IO' then 3 else 4 end td_sortlist,";
            strsql += " 1 td_controlflag,sm_prodtype,'N','O',sm_multiplier,'X'";
            strsql += " From " + strTable + "vwFoutstandingpos  ";
            strsql += " Where sm_expirydt >= '" + Fromdt + "' and  td_dt < '" + Fromdt + "'";
            strsql += " and td_clientcd = '" + cm_cd + "'";
            strsql += " and td_exchange = '" + strexchange + "' and sm_prodtype in('IO','EO','CO')";
            strsql += " and cm_type <> 'C'";
            strsql += " group by td_exchange,td_clientcd,td_seriesid,sm_prodtype,sm_multiplier";
            strsql += " having sum(sale - buy) <> 0 ";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            //Futures/Options running
            strsql = "insert into #tmpfinvestorrep  ";
            strsql += " select td_dt,td_clientcd,td_exchange,";
            strsql += " td_seriesid,td_bqty,0,td_sqty,0,0,0,";
            strsql += " td_rate,0.0000 td_closeprice,0 mtm,";
            strsql += " case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'TF' then 2 when 'RF' then 2 when 'EF' then 2 when 'IO' then 3 else 4 end td_sortlist,";
            strsql += " 2,sm_prodtype,'N','Y',sm_multiplier,'X'";
            strsql += " From " + strTable + " Trades with (" + StrComTradesIndex + "nolock) , " + strTable + "Series_master with (nolock)," + strTable + "Client_master with (nolock) ";
            strsql += " Where td_clientcd = cm_cd and td_exchange = sm_exchange and td_seriesid = sm_seriesid";
            strsql += " and td_clientcd = '" + cm_cd + "'";
            strsql += " and td_exchange = '" + strexchange + "'";
            strsql += " and sm_expirydt >= '" + strFirstDate + "' and td_dt between '" + strFirstDate + "' and '" + strLastDate + "'";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            //Exercise/Assignments
            strsql = "insert into #tmpfinvestorrep  ";
            strsql += " select ex_dt,ex_clientcd,ex_exchange,";
            strsql += " ex_seriesid,ex_eqty,0,ex_aqty,0,0,0,";
            strsql += " ex_diffbrokrate,ex_settlerate,0,";
            strsql += " case sm_prodtype when 'IF' then 1 when 'CF' then 1 when 'TF' then 2  when 'RF' then 2 when 'EF' then 2 when 'IO' then 3 else 4 end + 5 td_sortlist,";
            strsql += " case ex_eaflag when 'E' then 3 else 4 end td_controlflag,sm_prodtype,'N','Y',sm_multiplier,'X'";
            strsql += " From " + strTable + "Exercise with (nolock), " + strTable + "Series_master with (nolock)," + strTable + "Client_master with (nolock)";
            strsql += " Where ex_clientcd = cm_cd and ex_exchange = sm_exchange And ex_seriesid = sm_seriesid";
            strsql += " and ex_clientcd = '" + cm_cd + "'";
            strsql += " and ex_exchange = '" + strexchange + "'";
            strsql += " and sm_expirydt >= '" + strFirstDate + "' and  ex_dt between '" + strFirstDate + "' and '" + strLastDate + "'";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            //Reverse Future Outstanding
            strsql = "insert into #tmpfinvestorrep  ";
            strsql += " select '" + strLastDate + "',fi_clientcd,fi_exchange,";
            strsql += " fi_seriesid,case sign(sum(case fi_controlflag when 1 then case fi_dt when '" + strFirstDate + "' then fi_bqty - fi_sqty else 0 end when 2 then fi_bqty - fi_sqty else fi_sqty - fi_bqty end)) when -1 then abs(sum(case fi_controlflag when 1 then case fi_dt when '" + strFirstDate + "' then fi_bqty - fi_sqty else 0 end when 2 then fi_bqty - fi_sqty else fi_sqty - fi_bqty end)) else 0 end td_bqty,";
            strsql += " 0,";
            strsql += " case sign(sum(case fi_controlflag when 1 then case fi_dt when '" + strFirstDate + "' then fi_bqty - fi_sqty else 0 end when 2 then fi_bqty - fi_sqty else fi_sqty - fi_bqty end)) when 1 then abs(sum(case fi_controlflag when 1 then case fi_dt when '" + strFirstDate + "' then fi_bqty - fi_sqty else 0 end when 2 then fi_bqty - fi_sqty else fi_sqty - fi_bqty end)) else 0 end td_sqty,0,";
            strsql += " 0,0,0,0 td_closeprice,0,";
            strsql += " case fi_prodtype when 'IF' then 1 when 'CF' then 1 when 'TF' then 2 when 'RF' then 2 when 'EF' then 2 when 'IO' then 3 else 4 end + 6 td_sortlist,";
            strsql += " 5 td_controlflag,fi_prodtype,'R','N',sm_multiplier,'X'";
            strsql += " From #tmpfinvestorrep  ," + strTable + "Series_master with (nolock)";
            strsql += " Where fi_exchange = sm_exchange and sm_seriesid = fi_seriesid and fi_prodtype in('IF','EF','CF','RF','TF') ";
            strsql += " and sm_expirydt <= '" + Todt + "'";
            strsql += " group by fi_exchange,fi_clientcd,fi_seriesid,fi_prodtype,sm_multiplier";
            strsql += " having sum(case fi_controlflag when 1 then case fi_dt when '" + strFirstDate + "' then fi_bqty - fi_sqty else 0 end when 2 then fi_bqty - fi_sqty else fi_sqty - fi_bqty end) <> 0";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            //Update Last Market Price for Options
            strsql = "update #tmpfinvestorrep   set fi_rate = case fi_type when 'R' then ms_lastprice else fi_rate end,fi_closeprice = ms_lastprice from #tmpfinvestorrep  ,Market_summary with (nolock)";
            strsql += " where ms_seriesid = fi_seriesid ";
            strsql += " and ms_exchange = fi_exchange ";
            strsql += " and ms_dt = (select max(ms_dt) from " + strTable + "Market_summary with (nolock) where ms_exchange = fi_exchange ";
            strsql += " and ms_seriesid = fi_seriesid and ms_lastprice <> 0 and ms_dt <= '" + Todt + "' )";
            strsql += " and fi_prodtype in('IO','EO','CO')";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            //Update Previous close and today's close prices
            strsql = "update #tmpfinvestorrep   set fi_closeprice =  isNull((select ms_lastprice From " + strTable + "Market_summary with (nolock) ";
            strsql += " Where ms_exchange = '" + strexchange + "' and ms_seriesid = fi_seriesid ";
            strsql += " and ms_dt = (select Max(ms_dt) from " + strTable + "Market_Summary with (nolock) ";
            strsql += " Where ms_exchange = '" + strexchange + "' and ms_seriesid = fi_seriesid ";
            strsql += " and ms_dt <='" + Todt + "')),0) ";
            strsql += " where fi_controlflag in('1','2') and fi_prodtype in('IF','EF','CF','RF','TF') ";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            //Update close Price If Expiry Trade is Not Generated.
            strsql = "update #tmpfinvestorrep   set fi_rate =  ms_lastprice  , fi_closeprice = ms_lastprice ";
            strsql += " from #tmpfinvestorrep  ," + strTable + "Market_summary with (nolock) , " + strTable + "Series_master with (nolock) ";
            strsql += " where sm_Exchange= '" + strexchange + "' and sm_seriesid = fi_seriesid ";
            strsql += " and sm_exchange = ms_exchange and sm_seriesid = ms_seriesid  and sm_expirydt = ms_dt ";
            strsql += " and ms_dt < '" + Todt + "'";
            strsql += " and fi_prodtype in('IF','EF','CF','RF','TF') and fi_type = 'R' ";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            strsql = "update #tmpfinvestorrep   set fi_rate = ms_prcloseprice from #tmpfinvestorrep  ," + strTable + "Market_summary with (nolock) ";
            strsql += " where ms_seriesid = fi_seriesid and fi_controlflag = 1";
            strsql += " and ms_exchange = '" + strexchange + "' ";
            strsql += " and ms_dt = fi_dt";
            strsql += " and fi_prodtype in('IF','EF','CF','RF','TF')";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            //End of updation of close prices
            //Service tax here for Trades
            strsql = "insert into #tmpFinvcharges select td_dt,td_clientcd,'SERVICE TAX',round(sum(td_servicetax),2),0 from " + strTable + " Trades with (" + StrComTradesIndex + "nolock) ,#finvdates," + strTable + "Client_master with (nolock)," + strTable + "Series_master with (nolock)";
            strsql += " where td_clientcd = cm_cd and td_dt = bd_dt";
            strsql += " and td_exchange = sm_exchange ";
            strsql += " and td_seriesid = sm_seriesid";
            strsql += " and td_exchange = '" + strexchange + "' ";
            strsql += " and td_clientcd = '" + cm_cd + "'";
            strsql += " group by td_dt,td_clientcd having sum(td_servicetax) <> 0";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            //Service tax here for Exercise
            strsql = "insert into #tmpFinvcharges select ex_dt,ex_clientcd,'SERVICE TAX',round(sum(ex_servicetax),2),0 from " + strTable + "Exercise with (nolock),#finvdates," + strTable + "Client_master with (nolock)," + strTable + "Series_master with (nolock) ";
            strsql += " where ex_clientcd = cm_cd and ex_dt = bd_dt";
            strsql += " and ex_exchange = sm_exchange ";
            strsql += " and ex_seriesid = sm_seriesid";
            strsql += " and ex_clientcd = '" + cm_cd + "'";
            strsql += " and ex_exchange = '" + strexchange + "' ";
            strsql += " group by ex_dt,ex_clientcd having sum(ex_servicetax) <> 0";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            //Charges here
            //-----------from specialcharges start
            strsql = "insert into #tmpFinvcharges select fc_dt,fc_clientcd,fc_desc,round(sum(fc_amount),2),0 from " + strTable + "Fspecialcharges with (nolock),#finvdates," + strTable + "Client_master with (nolock) ";
            strsql += " where fc_clientcd = cm_cd and fc_dt = bd_dt";
            strsql += " and fc_clientcd = '" + cm_cd + "'";
            strsql += " and fc_desc not like '%{New%' and fc_exchange='" + strexchange + "' and fc_desc not like '%{Old%'";
            strsql += " and fc_chargecode not in ('EMR') ";
            strsql += " group by fc_dt,fc_clientcd,fc_desc having round(sum(fc_amount),2) <> 0";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            strsql = "insert into #tmpFinvcharges select fc_dt,fc_clientcd,'SERVICE TAX',round(sum(fc_servicetax),2),0 from " + strTable + "Fspecialcharges with (nolock) ,#finvdates," + strTable + "Client_master with (nolock) ";
            strsql += " where fc_exchange='" + strexchange + "' and fc_clientcd = cm_cd and fc_dt = bd_dt";
            strsql += " and fc_clientcd = '" + cm_cd + "'";
            strsql += " group by fc_dt,fc_clientcd,fc_desc having round(sum(fc_servicetax),2) <> 0";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            //-----------from specialcharges end        
            strsql = "update #tmpfinvestorrep  set fi_bvalue = fi_bqty*fi_rate*fi_multiplier,fi_svalue = fi_sqty*fi_rate*fi_multiplier,";
            strsql += "fi_netqty = fi_bqty - fi_sqty,fi_netvalue = (fi_bqty - fi_sqty)*fi_rate*fi_multiplier";
            strsql += " where fi_controlflag not in(3,4)";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            strsql = "update #tmpfinvestorrep  set fi_bvalue = fi_bqty*fi_rate*fi_multiplier,fi_svalue = fi_sqty*fi_rate*fi_multiplier,";
            strsql += "fi_netqty = fi_sqty - fi_bqty,fi_netvalue = (fi_bqty + fi_sqty)*fi_rate*fi_multiplier";
            strsql += " where fi_controlflag in(3,4)";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            strsql = "update #tmpfinvestorrep  set fi_mtm = round((((fi_sqty - fi_bqty)*fi_rate*fi_multiplier) - ((fi_sqty - fi_bqty)*fi_closeprice*fi_multiplier)),4)";
            strsql += " where fi_prodtype in('IF','EF','CF','RF','TF')";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            strsql = "update #tmpfinvestorrep  set fi_mtm = round(((case fi_controlflag when 3  then (fi_bqty + fi_sqty) when 4 then (fi_bqty + fi_sqty) else (fi_bqty - fi_sqty)*(-1) end) *fi_rate*fi_multiplier),4)";
            strsql += " where fi_prodtype in('IO','EO','CO')";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            strsql = "update #tmpfinvestorrep  set fi_netvalue = 0 where fi_type = 'R'";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            strsql = "update #tmpfinvestorrep  ";
            strsql += " set fi_netvalue = (fi_bqty - fi_sqty)*";
            strsql += " case sm_callput when 'C' then fi_closeprice - fi_rate else fi_rate - fi_closeprice end*fi_multiplier";
            strsql += " from #tmpfinvestorrep ,Series_master";
            strsql += " where fi_exchange = sm_exchange and fi_segment = sm_segment and fi_seriesid = sm_seriesid ";
            strsql += " and fi_type = 'R'";
            strsql += " and (fi_bqty - fi_sqty) < 0";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            strsql = "update #tmpfinvestorrep  ";
            strsql += " set fi_netvalue = (fi_bqty - fi_sqty)*";
            strsql += " case sm_callput when 'C' then fi_rate - fi_closeprice else fi_closeprice - fi_rate end*fi_multiplier";
            strsql += " from #tmpfinvestorrep ,Series_master";
            strsql += " where fi_exchange = sm_exchange and fi_segment = sm_segment and fi_seriesid = sm_seriesid ";
            strsql += " and fi_type = 'R'";
            strsql += " and (fi_bqty - fi_sqty) > 0";
            objUtility.ExecuteSQLTmp(strsql, con); ;

            strsql = "update #tmpfinvestorrep  set fi_mtm = fi_netvalue *(-1) where fi_type = 'R'";
            objUtility.ExecuteSQLTmp(strsql, con); ;
        }

        #endregion

        #region Holding Query
        ////// TODO : For Main grid data query
        private string GetQueryPageLoadData(string cm_cd)
        {
            strsql = "select  case left(da_dpid,2) when 'IN' then  rtrim(da_dpid)+rtrim(da_actno) else da_actno end as BOId,da_name as Name, da_clientcd  from dematact with (nolock) where da_clientcd = '" + cm_cd + "' ";

            return strsql;
        }

        //For Details grid data query
        private string GetQueryForCurrentHoldingData(string Select_Type, string Holding_Type, string strBoid)
        {
            string holdingQuery = "";
            if (Select_Type == "Dp" && Holding_Type == "Current Holding")
            {
                holdingQuery = ShowDetailsGrid(Select_Type, Holding_Type, strBoid);
            }
            return holdingQuery;
        }

        private string ShowDetailsGrid(string Select_Type, string Holding_Type, string strBoid)
        {
            string holdingQuery = "";
            char[] ArrSeparators = new char[1];
            ArrSeparators[0] = '/';
            if (_configuration["IsTradeWeb"] == "O")
            {//-----------------------------------------------------------Live----------------------------------------------------------------------------------------
                if (_configuration["Cross"] != "" && Microsoft.VisualBasic.Strings.Mid(strBoid, 2, 2) != "IN") // strBoid  LEft 2 <>IN
                {
                    if (Holding_Type == "Current Holding")
                    {
                        string[] ArrCross = _configuration["Cross"].Split(ArrSeparators);
                        holdingQuery = "select a.hld_isin_code,b.sc_company_name,b.sc_isinname,cast((a.hld_ac_pos) as decimal(15,3)) hld_ac_pos,a.hld_ac_type, ";
                        holdingQuery = holdingQuery + " d.bt_description 'bt_description', hld_market_type,a.hld_settlement,cast((sc_rate) as decimal(15,2)) as sc_security_rate, ";
                        holdingQuery = holdingQuery + " cast(( ( a.hld_ac_pos * sc_Rate)) as decimal(15,2))  as valuation,bt_description as BType";
                        holdingQuery = holdingQuery + " from " + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".Holding a,";
                        holdingQuery = holdingQuery + " " + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".Security b ,";
                        holdingQuery = holdingQuery + "" + ArrCross[0].Trim() + "." + ArrCross[1].Trim() + "." + ArrCross[2].Trim() + ".Beneficiary_type d where a.hld_ac_code in (" + strBoid + " ) and a.hld_isin_code = b.sc_isincode ";
                        holdingQuery = holdingQuery + " and d.bt_code = a.hld_ac_type order by BType,a.hld_ac_code,b.sc_company_name ";
                    }
                }
            }
            return holdingQuery;
        }

        #endregion
        #endregion
    }
}
