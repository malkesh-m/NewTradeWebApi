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
using TradeWeb.API.Models;
using TradeWeb.API.Repository;

namespace TradeWeb.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class TradeWeb_PledgeMarginController : ControllerBase
    {
        private readonly ITradeWebRepository _tradeWebRepository;

        public TradeWeb_PledgeMarginController(ITradeWebRepository tradeWebRepository)
        {
            _tradeWebRepository = tradeWebRepository;
        }

        #region Margin Api
        //// TODO : Get margin grid main data
        //[Authorize(AuthenticationSchemes = "Bearer")]
        //[HttpGet("GetMargin", Name = "GetMargin")]
        //public IActionResult GetMargin()
        //{
        //    if (ModelState.IsValid)
        //    {
        //        try
        //        {
        //            var tokenS = GetToken();
        //            var compCode = tokenS.Claims.First(claim => claim.Type == "companyCode").Value;
        //            var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

        //            var getData = _tradeWebRepository.GetMarginMainData(userId, compCode);
        //            if (getData != null)
        //            {
        //                return Ok(getData);
        //            }
        //            else
        //            {
        //                return NotFound("records not found");
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            return BadRequest(ex.Message.ToString());
        //        }
        //    }
        //    return BadRequest();
        //}

        // TODO : Get dropdown data

        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetDropdownData", Name = "GetDropdownData")]
        public IActionResult GetDropdownData()
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var compCode = tokenS.Claims.First(claim => claim.Type == "companyCode").Value;
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.GetDropdownListData(userId, compCode);
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

        #region New margin api

        // TODO : Get margin grid main data
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("Margin", Name = "Margin")]
        public IActionResult Margin(string date)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var compCode = tokenS.Claims.First(claim => claim.Type == "companyCode").Value;
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.MarginMainData(userId, compCode, date);
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
