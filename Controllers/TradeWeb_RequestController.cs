﻿using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Net;
using TradeWeb.API.Entity;
using TradeWeb.API.Models;
using TradeWeb.API.Repository;

namespace TradeWeb.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class TradeWeb_RequestController : ControllerBase
    {
        private readonly ITradeWebRepository _tradeWebRepository;

        public TradeWeb_RequestController(ITradeWebRepository tradeWebRepository)
        {
            _tradeWebRepository = tradeWebRepository;
        }

        #region Request Api

        // update fund request or share requrest
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("UpdateFundAndSharesRequest", Name = "UpdateFundAndSharesRequest")]
        public IActionResult UpdateFundAndSharesRequest(bool isPostBack)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var getData = _tradeWebRepository.UpdateFundAndSharesRequest(isPostBack);
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

        // Radio button shares checked
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("Request_Get_ShareRequest", Name = "Request_Get_ShareRequest")]
        public IActionResult Request_Get_ShareRequest()
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Request_Get_ShareRequest(userId);
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

        // Radio button shares checked
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpPost("Request_Post_ShareRequest", Name = "Request_Post_ShareRequest")]
        public IActionResult Request_Post_ShareRequest(string scrip_Code, string request_Quantity)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Request_Post_ShareRequest(userId, scrip_Code, request_Quantity);
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

        // Get Rms Request
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("Request_Get_FundRequest", Name = "Request_Get_FundRequest")]
        public IActionResult Request_Get_FundRequest()
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.GetRmsRequest(userId);
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

        //// Execute page request report page load query
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("ExecuteRequestReportPageLoad", Name = "ExecuteRequestReportPageLoad")]
        public IActionResult ExecuteRequestReportPageLoad(bool isPostBack)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var getData = _tradeWebRepository.ExecuteRequestReportPageLoad(isPostBack);
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

        //// Insert Request value after button click
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("InsertRequestValues", Name = "InsertRequestValues")]
        public IActionResult InsertRequestValues([FromQuery] string lstSegment, string request, string fromDate, string toDate)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.InsertRequestValues(userId, lstSegment, request, fromDate, toDate);
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

        //// Insert Fund Request value after button click
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpPost("Request_Post_FundRequest", Name = "Request_Post_FundRequest")]
        public IActionResult Request_Post_FundRequest(FundRequestModel model)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.InsertFundRequestValue(userId, model.Amount, model.DpId);
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

        // TODO : Get margin pledge data
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("Request_Get_PledgeForMargin", Name = "Request_Get_PledgeForMargin")]
        public IActionResult Request_Get_PledgeForMargin([FromQuery] string dematActNo)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var compCode = tokenS.Claims.First(claim => claim.Type == "companyCode").Value;
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.GetMarginPledgeData(userId, userId.ToUpper(), compCode, dematActNo);
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

        // TODO : Get margin pledge request
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetCurrentPledgeRequest", Name = "GetCurrentPledgeRequest")]
        public IActionResult GetCurrentPledgeRequest()
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.GetCurrentPledgeRequest(userId.ToUpper());
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

        // TODO : Add margin pledge request
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpPost("Request_Post_PledgeForMargin", Name = "Request_Post_PledgeForMargin")]
        public IActionResult Request_Post_PledgeForMargin(PledgeForMarginModel model)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.AddPledgeRequest(userId.ToUpper(), model.DematActNo, model.Securities_Code, model.Request_Qty);
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


        // insert unpledge request
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpPost("Holding_Post_UnPledgeRequest", Name = "Holding_Post_UnPledgeRequest")]
        public IActionResult Holding_Post_UnPledgeRequest(UnPledgeRequestModel model)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.AddUnPledgeRequest(userId, "Pledge", model.Securities_Code, model.Request_Qty);
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

        // insert unpledge request
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpPost("Holding_Post_UnRePledgeRequest", Name = "Holding_Post_UnRePledgeRequest")]
        public IActionResult Holding_Post_UnRePledgeRequest(UnPledgeRequestModel model)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.AddUnPledgeRequest(userId, "Un-Re-Pledge", model.Securities_Code, model.Request_Qty);
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
