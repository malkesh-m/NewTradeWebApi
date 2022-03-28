using Microsoft.AspNetCore.Authorization;
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
    public class RequestController : ControllerBase
    {
        private readonly ITradeWebRepository _tradeWebRepository;

        public RequestController(ITradeWebRepository tradeWebRepository)
        {
            _tradeWebRepository = tradeWebRepository;
        }

        #region Request Api

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

                    var getData = _tradeWebRepository.Request_Get_FundRequest(userId);
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
        [HttpPost("Request_Post_FundRequest", Name = "Request_Post_FundRequest")]
        public IActionResult Request_Post_FundRequest(FundRequest_Model model)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Request_Post_FundRequest(userId, model.Amount, model.CESCd);
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

                    var getData = _tradeWebRepository.Request_Get_PledgeForMargin(userId, dematActNo);
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
        [HttpPost("Request_Post_PledgeForMargin", Name = "Request_Post_PledgeForMargin")]
        public IActionResult Request_Post_PledgeForMargin(PledgeForMarginModel model)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Request_Post_PledgeForMargin(userId.ToUpper(), model.DematActNo, model.Securities_Code, model.Request_Qty);
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
        [HttpPost("Request_Post_UnPledge", Name = "Request_Post_UnPledge")]
        public IActionResult Request_Post_UnPledge(UnPledgeRequestModel model)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Request_Post_UnPledge_UnRepledge(userId, "Pledge", model.ScripCode, model.Request_Qty);
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
        [HttpPost("Request_Post_UnRePledge", Name = "Request_Post_UnRePledge")]
        public IActionResult Request_Post_UnRePledge(UnPledgeRequestModel model)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Request_Post_UnPledge_UnRepledge(userId, "Un-Re-Pledge", model.ScripCode, model.Request_Qty);
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
        [HttpPost("Request_Post_Report", Name = "Request_Post_Report")]
        public IActionResult Request_Post_Report(string ExchSeg, string Report, string strFromDt, string strToDt)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Request_Post_Report(userId, ExchSeg, Report, strFromDt, strToDt);
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
