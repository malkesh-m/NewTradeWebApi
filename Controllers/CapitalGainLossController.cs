using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Threading.Tasks;
using TradeWeb.API.Models;
using TradeWeb.API.Repository;

namespace TradeWeb.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CapitalGainLossController : ControllerBase
    {

        private readonly ITradeWebRepository _tradeWebRepository;

        public CapitalGainLossController(ITradeWebRepository tradeWebRepository)
        {
            _tradeWebRepository = tradeWebRepository;
        }

        #region CapitalGainLoss Api

        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("CapitalGainLoss_Dividend", Name = "CapitalGainLoss_Dividend")]
        public IActionResult CapitalGainLoss_Dividend(string fromDate, string toDate)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.GetINVPLDivListing(userId, fromDate, toDate);
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
        [HttpGet("CapitalGainLoss_ActualPLSummary", Name = "CapitalGainLoss_ActualPLSummary")]
        public IActionResult CapitalGainLoss_ActualPLSummary(string fromDate, string toDate, Boolean jobing, Boolean delivery, Boolean ignore112A, string type)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.GetINVPLGainLoss(userId, fromDate, toDate, jobing, delivery, ignore112A, type);
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
        [HttpGet("CapitalGainLoss_ActualPLDetail", Name = "CapitalGainLoss_ActualPLDetail")]
        public IActionResult CapitalGainLoss_ActualPLDetail(string fromDate, string toDate, string type, string ignore112A, string scripCode)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.GetINVPLGainLossDetails(userId, fromDate, toDate, type, ignore112A, scripCode);
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
        [HttpGet("CapitalGainLoss_TradeListingSummary", Name = "CapitalGainLoss_TradeListingSummary")]
        public IActionResult CapitalGainLoss_TradeListingSummary(string fromDate, string toDate)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.GetINVPLTradeListing(userId, fromDate, toDate);
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
        [HttpGet("CapitalGainLoss_TradeListingDetail", Name = "CapitalGainLoss_TradeListingDetail")]
        public IActionResult CapitalGainLoss_TradeListingDetail(string fromDate, string toDate, string scripCode)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.GeTINVPLTradeListingDetails(userId, fromDate, toDate, scripCode);
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
        [HttpPost("CapitalGainLoss_TradeInsert", Name = "CapitalGainLoss_TradeInsert")]
        public IActionResult CapitalGainLoss_TradeInsert(GainLossTradeInsertModel model)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.GetINVPLTradeListingSave(userId, model.Date, model.Settelment, model.Flag, model.Type, model.Quantity, model.NetRate, model.ServiceTax, model.STT, model.OtherCharge1, model.OtherCharge2, model.ScripCode);
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
        [HttpGet("CapitalGainLoss_NationalDetail", Name = "CapitalGainLoss_NationalDetail")]
        public IActionResult CapitalGainLoss_NationalDetail(string strDate, string type, string ignore112A, string scripCode)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.GeTINVPLNationalDetail(userId, strDate, type, ignore112A, scripCode);
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
        [HttpGet("CapitalGainLoss_NationalSummary", Name = "CapitalGainLoss_NationalSummary")]
        public IActionResult CapitalGainLoss_NationalSummary(string strDate, Boolean ignore112A, string type)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.GetINVPLNationalSummary(userId, strDate, ignore112A, type);
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
            //var jsonToken = handler.ReadToken(authHeader);
            var token = handler.ReadToken(authHeader) as JwtSecurityToken;
            return token;
        }
    }
}
