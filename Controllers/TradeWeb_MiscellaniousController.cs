using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Data;
using System.Net;
using System.Threading.Tasks;
using TradeWeb.API.Entity;
using TradeWeb.API.Repository;
using INVPLService;
using Microsoft.Extensions.Configuration;
using System.IO;
using Microsoft.AspNetCore.Hosting;

namespace TradeWeb.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class TradeWeb_MiscellaniousController : ControllerBase
    {
        private readonly ITradeWebRepository _tradeWebRepository;
        private readonly UtilityCommon objUtility;
        private readonly IConfiguration _configuration;
        private readonly IWebHostEnvironment _environment;


        public TradeWeb_MiscellaniousController(ITradeWebRepository tradeWebRepository, UtilityCommon objUtility, IConfiguration configuration, IWebHostEnvironment environment)
        {
            _tradeWebRepository = tradeWebRepository;
            this.objUtility = objUtility;
            _configuration = configuration;
            _environment = environment;
        }

        #region Agreement Api
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
                        return Ok(docBase64);
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

        // Download digital document pdf
        //[Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("DownloadDigitalDocument", Name = "DownloadDigitalDocument")]
        public IActionResult DownloadDigitalDocument(string docType, string date, string srNo)
        {
            if (ModelState.IsValid)
            {
                string strsql = "";
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
                            ObjReader.Read();
                            return File((byte[])ObjReader["dd_document"], "application/pdf", DataSet.Tables[0].Rows[0]["dd_filename"].ToString().Trim());
                        }
                        ObjReader.Close();
                    }
                    else
                    {
                        return NotFound("records not found");
                    }
                    return NotFound("records not found");
                }
                catch (Exception ex)
                {
                    return BadRequest(ex.Message.ToString());
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

        
        private JwtSecurityToken GetToken()
        {
            var handler = new JwtSecurityTokenHandler();
            string authHeader = Request.Headers["Authorization"];
            authHeader = authHeader.Replace("Bearer ", "");
            var token = handler.ReadToken(authHeader) as JwtSecurityToken;
            return token;
        }

    }
}
