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
    public class MiscellaniousController : ControllerBase
    {
        private readonly ITradeWebRepository _tradeWebRepository;
        private readonly UtilityCommon objUtility;
        private readonly IConfiguration _configuration;
        private readonly IWebHostEnvironment _environment;


        public MiscellaniousController(ITradeWebRepository tradeWebRepository, UtilityCommon objUtility, IConfiguration configuration, IWebHostEnvironment environment)
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

        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("DigitalDocument_List", Name = "DigitalDocument_List")]
        public IActionResult DigitalDocument_List([FromQuery] int product, string fromDate, string toDate)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.DigitalDocument_List(userId, product, fromDate, toDate);
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


        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("DigitalDocument_File", Name = "DigitalDocument_File")]
        public IActionResult DigitalDocument_File([FromQuery] int Product, string date, string srNo)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.DigitalDocument_File(Product, date, srNo);
                    if (getData != null)
                    {
                        return Ok( new { Document = getData });
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
