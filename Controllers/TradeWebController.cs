using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Hosting;
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
using System.Data;
using System.IdentityModel.Tokens.Jwt;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Claims;
using System.Text;
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
        private readonly ITradeWebRepository _tradeWebRepository;
        private readonly IHttpContextAccessor _httpContextAccessor;
        private readonly SignInManager<AppUser> _signInManager;
        private readonly IWebHostEnvironment _environment;
        private string strsql = "";
        #endregion

        #region Constructor
        public TradeWebController(UserManager<AppUser> userManager, IConfiguration configuration, UtilityCommon objUtility, IHttpContextAccessor httpContextAccessor, SignInManager<AppUser> signInManager, ITradeWebRepository tradeWebRepository, IWebHostEnvironment environment)
        {
            _userManager = userManager;
            _configuration = configuration;
            this.objUtility = objUtility;
            _httpContextAccessor = httpContextAccessor;
            _signInManager = signInManager;
            _tradeWebRepository = tradeWebRepository;
            _environment = environment;
        }
        #endregion

        #region API Methods

        #region Login

        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpPost("Home_UserProfile", Name = "Home_UserProfile")]
        public IActionResult Home_UserProfile()
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;
                    var getData = _tradeWebRepository.GetUserDetais(userId);
                    if (getData != null)
                    {
                        return Ok(getData);
                    }
                    else
                    {
                        return NotFound("records not found");
                    }
                }
                catch (Exception ex)
                {
                    return BadRequest(ex.Message.ToString());
                }
            }
            return BadRequest();
        }

        /// <summary>
        /// Login validate USER
        /// </summary>
        /// <param name="userId"></param>
        /// <returns></returns>
        [HttpPost("Login_validate_USER", Name = "Login_validate_USER")]
        public IActionResult Login_validate_USER(string userId)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var result = _tradeWebRepository.Login_validate_USER(userId);
                    if (result != null)
                    {
                        return Ok(result);
                    }
                    return Ok(result);
                }
                catch (Exception ex)
                {
                    return BadRequest(ex.Message.ToString());
                }
            }
            return BadRequest();
        }

        /// <summary>
        /// Login validate Password
        /// </summary>
        /// <param name="userId"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        [HttpPost("Login_validate_Password", Name = "Login_validate_Password")]
        public IActionResult Login_validate_Password(string userId, string password)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var result = _tradeWebRepository.Login_validate_Password(userId, password);
                    var userList = _tradeWebRepository.UserDetails(userId, password);
                    if (userList != null)
                    {
                        var tokenString = GenerateJSONWebToken(new TradeWebLoginModel { username = userId, password = password });
                        FillConfigParametersString();
                        return Ok(new tokenResponse { status = true, message = "success", token = tokenString, status_code = (int)HttpStatusCode.OK, data = userList });
                    }
                    return Ok("Invalid userid / password");
                }
                catch (Exception ex)
                {
                    return BadRequest(ex.Message.ToString());
                }
            }
            return BadRequest();
        }

        /// <summary>
        /// Login validate Password
        /// </summary>
        /// <param name="userId"></param>
        /// <returns></returns>
        [HttpPost("Login_GetPassword", Name = "Login_GetPassword")]
        public IActionResult Login_GetPassword(string userId)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var result = _tradeWebRepository.Login_GetPassword(userId);
                    if (result != "failed")
                    {
                        return Ok(result);
                    }
                    return Ok(result);
                }
                catch (Exception ex)
                {
                    return BadRequest(ex.Message.ToString());
                }
            }
            return BadRequest();
        }

        [HttpPost("Login_Password_GenerateOTP", Name = "Login_Password_GenerateOTP")]
        public IActionResult Login_Password_GenerateOTP(string userId, string mode)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var result = _tradeWebRepository.Login_Password_GenerateOTP(userId, mode);
                    if (result != "failed")
                    {
                        return Ok(result);
                    }
                    return Ok(result);
                }
                catch (Exception ex)
                {
                    return BadRequest(ex.Message.ToString());
                }
            }
            return BadRequest();
        }

        [HttpPost("Login_Password_Update", Name = "Login_Password_Update")]
        public IActionResult Login_Password_Update(string userId, string OTP, string oldPassword, string newPassword)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var result = _tradeWebRepository.Login_Password_Update(userId, OTP, oldPassword, newPassword);
                    if (result != "failed")
                    {
                        return Ok(result);
                    }
                    return Ok(result);
                }
                catch (Exception ex)
                {
                    return BadRequest(ex.Message.ToString());
                }
            }
            return BadRequest();
        }

        #endregion

        #region Ledger Api

        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("Ledger_Year", Name = "Ledger_Year")]
        public IActionResult Ledger_Year()
        {
            if (ModelState.IsValid)
            {
                try
                {

                    var getData = _tradeWebRepository.Ledger_Year();
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
        /// 
        /// </summary>
        /// <param name="type"></param>
        /// <param name="fromDate"></param>
        /// <param name="toDate"></param>
        /// <returns></returns>
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("Ledger_Summary", Name = "Ledger_Summary")]
        public IActionResult Ledger_Summary(string type, string fromDate, string toDate)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    JwtSecurityToken token = GetToken();
                    var CompCode = token.Claims.First(claim => claim.Type == "companyCode").Value;
                    var userId = token.Claims.First(claim => claim.Type == "username").Value;

                    var dataList = _tradeWebRepository.Ledger_Summary(userId, type, fromDate, toDate);
                    var strDataList = Newtonsoft.Json.JsonConvert.SerializeObject(dataList).Replace(@"\", String.Empty);
                    if (dataList != null)
                    {
                        return Ok(strDataList);
                    }
                    return NotFound("records not found");
                }
                catch (Exception ex)
                {
                    return BadRequest(new { response = ex.Message.ToString() });
                }
            }
            return BadRequest();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpPost("Ledger_Detail", Name = "Ledger_Detail")]
        public IActionResult Ledger_Detail(LedgerDetailsModel model /*string fromDate, string toDate, string type_cesCd*/)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var bb = _configuration["Commex"];

                    var tokenS = GetToken();
                    var userName = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Ledger_Detail(userName, model, model.fromDate, model.toDate);
                    if (getData != null)
                    {
                        return Ok(getData);
                    }
                    else
                    {
                        return NotFound("records not found");
                    }
                }
                catch (Exception ex)
                {
                    return BadRequest(ex.Message.ToString());
                }
            }
            return BadRequest();
        }



        #endregion

        #region OutStanding Api
        /// <summary>
        ///  OutStanding data 
        /// </summary>
        /// <param name="AsOnDt"></param>
        /// <returns></returns>
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("OutStandingPosition", Name = "OutStandingPosition")]
        public IActionResult OutStandingPosition(string AsOnDt)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.OutStandingPosition(userId, AsOnDt);
                    if (getData != null)
                    {
                        return Ok(getData);
                    }
                    else
                    {
                        return NotFound("records not found");
                    }
                }
                catch (Exception ex)
                {
                    return BadRequest(ex.Message.ToString());
                }
            }
            return BadRequest();
        }

        /// <summary>
        /// OutStanding details data
        /// </summary>
        /// <param name="CESCd"></param>
        /// <param name="seriesId"></param>
        /// <returns></returns>
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("OutStandingPosition_Detail", Name = "OutStandingPosition_Detail")]
        public IActionResult OutStandingPosition_Detail(string CESCd, string seriesId)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.OutStandingPosition_Detail(userId, seriesId, CESCd);
                    if (getData != null)
                    {
                        return Ok(getData);
                    }
                    else
                    {
                        return NotFound("records not found");
                    }
                }
                catch (Exception ex)
                {
                    return BadRequest(ex.Message.ToString());
                }
            }
            return BadRequest();
        }
        #endregion


        //[Authorize(AuthenticationSchemes = "Bearer")]
        //[HttpGet("GetINVPLTradeListingDelete", Name = "GetINVPLTradeListingDelete")]
        //public IActionResult GetINVPLTradeListingDelete(string srNo)
        //{
        //    if (ModelState.IsValid)
        //    {
        //        try
        //        {
        //            var tokenS = GetToken();
        //            var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

        //            var getData = _tradeWebRepository.GetINVPLTradeListingDelete(userId, srNo);
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
    }
}
