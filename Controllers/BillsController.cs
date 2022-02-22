using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
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
    public class BillsController : ControllerBase
    {
        private readonly ITradeWebRepository _tradeWebRepository;
        private readonly UtilityCommon objUtility;

        public BillsController(ITradeWebRepository tradeWebRepository, UtilityCommon objUtility)
        {
            _tradeWebRepository = tradeWebRepository;
            this.objUtility = objUtility;
        }

        #region Bill Api

        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("Bills_exchSeg", Name = "Bills_exchSeg")]
        public IActionResult Bills_exchSeg()
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var getData = _tradeWebRepository.Bills_exchSeg();
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

        // get settelment type for dropdown settlement
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("Bills_cash_settTypes_list", Name = "Bills_cash_settTypes_list")]
        public IActionResult Bills_cash_settTypes_list([FromQuery] string exch)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var getData = _tradeWebRepository.Bills_cash_settTypes_list(exch);
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
                        return Ok(JsonConvert.SerializeObject(dataList));
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

        // get bill main data

        [HttpGet("Bills_cash_stlmnt", Name = "Bills_cash_stlmnt")]
        public IActionResult Bills_cash_stlmnt([FromQuery] string stlmnt)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;
                    string dt = objUtility.fnFireQuery("settlements", "se_stdt", "se_stlmnt", stlmnt, true);
                    var getData = _tradeWebRepository.Bill_data(userId, stlmnt.Substring(0, 1) + 'C', stlmnt.Substring(1, 1), dt);
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
        [HttpGet("Bills_cash_settType", Name = "Bills_cash_settType")]
        public IActionResult Bills_cash_settType([FromQuery] string exch_settType, string dt)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Bill_data(userId, exch_settType.Substring(0, 1) + 'C', exch_settType.Substring(1, 1), dt);
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

        // get bill main data
        //[Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("Bills_FO", Name = "Bills_FO")]
        public IActionResult Bills_FO([FromQuery] string exch, string seg, string dt)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Bill_data(userId, exch + seg, "", dt);
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

        [HttpGet("Bills_Commodity", Name = "Bills_Commodity")]
        public IActionResult Bills_Commodity([FromQuery] string exch, string dt)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Bill_data(userId, exch + "X", "", dt);
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
            //var jsonToken = handler.ReadToken(authHeader);
            var token = handler.ReadToken(authHeader) as JwtSecurityToken;
            return token;
        }

        #endregion
    }
}
