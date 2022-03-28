using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using TradeWeb.API.Entity;
using TradeWeb.API.Repository;

namespace TradeWeb.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class MargingFinanceController : ControllerBase
    {
        private readonly ITradeWebRepository _tradeWebRepository;

        public MargingFinanceController(ITradeWebRepository tradeWebRepository)
        {
            _tradeWebRepository = tradeWebRepository;
        }

        #region Marging Finance Api

        // Get Trades Data
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetTradesData", Name = "GetTradesData")]
        public IActionResult GetTradesData([FromQuery] string fromDate, string toDate, string selectedIndex)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.GetTradesData(userId, fromDate, toDate, selectedIndex);
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

        // get Temp table RmsSummary Data for status module.
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetTempRMSSummaryData", Name = "GetTempRMSSummaryData")]
        public IActionResult GetTempRMSSummaryData()
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var compCode = tokenS.Claims.First(claim => claim.Type == "companyCode").Value;
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.GetTempRMSSummaryData(userId, compCode);
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


        // Get fund data of status module.
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetStatusFundData", Name = "GetStatusFundData")]
        public IActionResult GetStatusFundData()
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var compCode = tokenS.Claims.First(claim => claim.Type == "companyCode").Value;
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.GetStatusFundData(userId, compCode);
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


        // Get collateral data of status module.
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetStatusCollateralData", Name = "GetStatusCollateralData")]
        public IActionResult GetStatusCollateralData()
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var compCode = tokenS.Claims.First(claim => claim.Type == "companyCode").Value;
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.GetStatusCollateralData(userId, compCode);
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
        [HttpGet("GetprSecurityListRptCommon", Name = "GetprSecurityListRptCommon")]
        public IActionResult GetprSecurityListRptData(Boolean blnBSE, Boolean blnNSE)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var dataList = _tradeWebRepository.GetprSecurityListRptHandler(blnBSE, blnNSE);
                    if (dataList != null)
                    {
                        return Ok(dataList);
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


        //Get margin trading finance shortfall main grid data
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetFinanceShortFallMainGridData", Name = "GetFinanceShortFallMainGridData")]
        public IActionResult GetFinanceShortFallMainGridData([FromQuery] int days)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.GetShortFallMainGridData(userId, days);
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

        private JwtSecurityToken GetToken()
        {
            var handler = new JwtSecurityTokenHandler();
            string authHeader = Request.Headers["Authorization"];
            authHeader = authHeader.Replace("Bearer ", "");
            var token = handler.ReadToken(authHeader) as JwtSecurityToken;
            return token;
        }
        #endregion
    }
}
