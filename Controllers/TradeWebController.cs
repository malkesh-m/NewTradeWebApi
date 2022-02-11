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
        private readonly IHttpContextAccessor _httpContextAccessor;
        private readonly SignInManager<AppUser> _signInManager;
        private readonly ITradeWebRepository _tradeWebRepository;
        private readonly IWebHostEnvironment _environment;
        private string strsql = "";
        private string strConnecton = "";
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


        #region Ledger Api

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
        /// 
        /// </summary>
        /// <param name="fromDate"></param>
        /// <param name="toDate"></param>
        /// <param name="type_cesCd"></param>
        /// <returns></returns>
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpPost("Ledger_Detail", Name = "Ledger_Detail")]
        public IActionResult Ledger_Detail( LedgerDetailsModel model /*string fromDate, string toDate, string type_cesCd*/)
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
        /// <param name="seriesId"></param>
        /// <param name="CESCd"></param>
        /// <param name="AsOnDt"></param>
        /// <returns></returns>
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("OutStandingPosition_Detail", Name = "OutStandingPosition_Detail")]
        public IActionResult OutStandingPosition_Detail(string seriesId, string CESCd, string AsOnDt)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.OutStandingPosition_Detail(userId, seriesId, CESCd, AsOnDt);
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

        #region Holding Api

        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("Holding_Broker_Current", Name = "Holding_Broker_Current")]
        public IActionResult Holding_Broker_Current()
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Holding_Broker_Current(userId);
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

        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("Holding_Broker_Ason", Name = "Holding_Broker_Ason")]
        public IActionResult Holding_Broker_Ason(string AsOnDt)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Holding_Broker_Ason(userId, AsOnDt);
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

        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("Holding_Demat_Current", Name = "Holding_Demat_Current")]
        public IActionResult Holding_Demat_Current()
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Holding_Demat_Current(userId);
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

        // Get data for bind dropdownlist combo as on
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetDropDownComboAsOnData", Name = "GetDropDownComboAsOnData")]
        public IActionResult GetDropDownComboAsOnData(string table)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var getData = _tradeWebRepository.GetDropDownComboAsOnDataHandler(table);
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

        // insert unpledge request
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("AddUnPledgeRequest", Name = "AddUnPledgeRequest")]
        public IActionResult AddUnPledgeRequest(string unPledge, string scripcd, string reqQty)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.AddUnPledgeRequest(userId, unPledge, scripcd, reqQty);
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

        #region Bill Api

        // get settelment type for dropdown settlement
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetSettlementData", Name = "GetSettlementData")]
        public IActionResult GetSettlementData([FromQuery] string exchange, string status)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var getData = _tradeWebRepository.GetSettelmentType(exchange, status);
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

        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetSysParmSt", Name = "GetSysParmSt")]
        public IActionResult GetSysParmSt(string parmId, string tableName)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var dataList = _tradeWebRepository.CommonGetSysParmStHandler(parmId, tableName);
                    if (dataList != null)
                    {
                        return Ok(new commonResponse { status = true, message = "success", status_code = (int)HttpStatusCode.OK, data = JsonConvert.SerializeObject(dataList) });
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

        // get bill main data
        //[Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetBillMainData", Name = "GetBillMainData")]
        public IActionResult GetBillMainData([FromQuery] string client, string exchangeType, string settelmentType, string fromDate)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var companyCode = tokenS.Claims.First(claim => claim.Type == "companyCode").Value;
                    var clientId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.GetBillMainData(clientId, client, exchangeType, settelmentType, fromDate, companyCode);
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

        #region Confirmation Api
        // get dropdown menu cumulative details data
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetCumulativeDetails", Name = "GetCumulativeDetails")]
        public IActionResult GetCumulativeDetails([FromQuery] string order, string scripCode, string bsflag, string date, string lookup)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.GetCumulativeDetailsHandler(userId, order, scripCode, bsflag, date, lookup);
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

        // get dropdown menu confirmation details data
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetConfirmationDetails", Name = "GetConfirmationDetails")]
        public IActionResult GetConfirmationDetails([FromQuery] string Order, string loopUp)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.GetConfirmationDetailsHandler(userId, Order, loopUp);
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

        // get confirmation main data
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetConfirmationData", Name = "GetConfirmationData")]
        public IActionResult GetConfirmationData([FromQuery] int lstConfirmationSelectIndex, string date)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.GetConfirmationMainDataHandler(userId, lstConfirmationSelectIndex, date);
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
                    if (result != "failed")
                    {
                        return Ok(new commonResponse { status = true, message = "success", status_code = (int)HttpStatusCode.OK, data = result });
                    }
                    return Ok(new commonResponse { status = false, message = "blank", status_code = (int)HttpStatusCode.NotFound, data = result });
                }
                catch (Exception ex)
                {
                    return BadRequest(new commonResponse { status = false, message = "error", status_code = (int)HttpStatusCode.InternalServerError, error_message = ex.Message.ToString() });
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
                        var tokenString = GenerateJSONWebToken(new TradeWebLoginModel { username=userId, password=password });
                        FillConfigParametersString();
                        return Ok(new tokenResponse { status = true, message = "success", token = tokenString, status_code = (int)HttpStatusCode.OK, data = userList });
                    }
                    return Ok(new commonResponse { status = false, message = "failed", status_code = (int)HttpStatusCode.NotFound, error_message = "Invalid userid / password" });
                }
                catch (Exception ex)
                {
                    return BadRequest(new commonResponse { status = false, message = "error", status_code = (int)HttpStatusCode.InternalServerError, error_message = ex.Message.ToString() });
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
                        return Ok(new commonResponse { status = true, message = "success", status_code = (int)HttpStatusCode.OK, data = result });
                    }
                    return Ok(new commonResponse { status = false, message = "failed", status_code = (int)HttpStatusCode.NotFound, data = result });
                }
                catch (Exception ex)
                {
                    return BadRequest(new commonResponse { status = false, message = "error", status_code = (int)HttpStatusCode.InternalServerError, error_message = ex.Message.ToString() });
                }
            }
            return BadRequest();
        }
        #endregion

        #region Agreement
        // TODO : Get Family Page_Load Data
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetAgreementPdf", Name = "GetAgreementPdf")]
        public IActionResult GetAgreementPdf()
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    string path = Path.Combine(this._environment.WebRootPath, objUtility.GetWebParameter("KYCPDF"), userId) + ".pdf";

                    var docBytes = System.IO.File.ReadAllBytes(path);
                    string docBase64 = "data:application/pdf;base64," + Convert.ToBase64String(docBytes);

                    if (docBase64 != null)
                    {
                        return Ok(new commonResponse { status = true, message = "success", status_code = (int)HttpStatusCode.OK, data = docBase64 });
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

        #region DigitalDocument Api

        // get dropdown Exchange list
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetDdlExchangeList", Name = "GetDdlExchangeList")]
        public IActionResult GetDdlExchangeList([FromQuery] string productType, string documentType)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var getData = _tradeWebRepository.GetDdlExchangeList(productType, documentType);
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

        // Download digital document pdf
        //[Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("DownloadDigitalDocument", Name = "DownloadDigitalDocument")]
        public IActionResult DownloadDigitalDocument(string docType, string date, string srNo)
        {
            if (ModelState.IsValid)
            {
                try
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
                            //Response.Clear();
                            ObjReader.Read();
                            //Response.AddHeader("content-disposition", "attachment;filename=" + DataSet.Tables[0].Rows[0]["dd_filename"].ToString().Trim() + "");
                            //Response.ContentType = "application/download";
                            //Response.BinaryWrite((byte[])ObjReader["dd_document"]);
                            //Response.End();
                            return File((byte[])ObjReader["dd_document"], "application/pdf", DataSet.Tables[0].Rows[0]["dd_filename"].ToString().Trim());
                        }
                        ObjReader.Close();
                    }
                    else
                    {
                        return NotFound(new commonResponse { status = false, message = "blank", status_code = (int)HttpStatusCode.NotFound, error_message = "records not found" });
                    }
                    return NotFound(new commonResponse { status = false, message = "blank", status_code = (int)HttpStatusCode.NotFound, error_message = "records not found" });
                }
                catch (Exception ex)
                {
                    return BadRequest(new commonResponse { status = false, message = "error", status_code = (int)HttpStatusCode.InternalServerError, error_message = ex.Message.ToString() });
                }
            }

            return BadRequest();
        }

        // get dropdown Exchange list
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetDigitalDocumentData", Name = "GetDigitalDocumentData")]
        public IActionResult GetDigitalDocumentData([FromQuery] string productType, string documentType, string exchangeType, string fromDate, string toDate)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.GetDigitalDocumentData(userId, productType, documentType, exchangeType, fromDate, toDate);
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

        // Add new item comodity in dropdown product list
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("AddDdlProductListItem", Name = "AddDdlProductListItem")]
        public IActionResult AddDdlProductListItem()
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var getData = _tradeWebRepository.AddDdlProductListItem();
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
    }
}
