﻿using Microsoft.AspNetCore.Authorization;
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
    public class Confirmation_TransactionController : ControllerBase
    {
        private readonly UtilityCommon objUtility;
        private readonly ITradeWebRepository _tradeWebRepository;

        public Confirmation_TransactionController(UtilityCommon objUtility, ITradeWebRepository tradeWebRepository)
        {
            this.objUtility = objUtility;
            _tradeWebRepository = tradeWebRepository;
        }


        #region Confirmation Api
        
        // get confirmation main data
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("Confirmation", Name = "Confirmation")]
        public IActionResult Confirmation([FromQuery] int type, string date)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Confirmation(userId, type, date);
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
        [HttpGet("Transaction_Detail", Name = "Transaction_Detail")]
        public IActionResult Transaction_Detail([FromQuery] string exch, string seg, int type, string fromdate, string todate, string scripCode)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Transaction_Detail(userId, exch, seg, type, fromdate, todate, scripCode);
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

        #region Transaction Api
        /// <summary>
        ///   Transaction API  
        /// </summary>
        /// <param name="type"></param>
        /// <param name="fromDate"></param>
        /// <param name="toDate"></param>
        // TODO : For getting Transaction main form data
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("Transaction_Summary", Name = "Transaction_Summary")]
        public IActionResult Transaction_Summary([FromQuery] string tradeType, string selectType, string fromDate, string toDate)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var companyCode = tokenS.Claims.First(claim => claim.Type == "companyCode").Value;
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Transaction_Summary(userId, tradeType, selectType, fromDate, toDate);

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
        [HttpGet("Transaction_Accounts", Name = "Transaction_Accounts")]
        public IActionResult Transaction_Accounts([FromQuery] string type, string fromDate, string toDate)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var companyCode = tokenS.Claims.First(claim => claim.Type == "companyCode").Value;
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    //TransactionHandler _handler = new TransactionHandler();
                    var getData = _tradeWebRepository.Transaction_Accounts(userId, type, fromDate, toDate);

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
        [HttpGet("Transaction_AGTS", Name = "Transaction_AGTS")]
        public IActionResult Transaction_AGTS([FromQuery] string segment, string fromDate, string toDate)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var companyCode = tokenS.Claims.First(claim => claim.Type == "companyCode").Value;
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Transaction_AGTS(userId, segment, fromDate, toDate);

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
