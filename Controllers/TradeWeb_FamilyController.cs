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
using TradeWeb.API.Models;
using TradeWeb.API.Repository;

namespace TradeWeb.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class TradeWeb_FamilyController : ControllerBase
    {
        private readonly ITradeWebRepository _tradeWebRepository;

        public TradeWeb_FamilyController(ITradeWebRepository tradeWebRepository)
        {
            _tradeWebRepository = tradeWebRepository;
        }

        #region  Api Method

        // TODO : Get Family Page_Load Data
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetPageLoadData", Name = "GetPageLoadData")]
        public IActionResult GetPageLoadData()
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Get_Page_Load_Data(userId);
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

        // TODO : Get Buttons Data
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetButtonsData", Name = "GetButtonsData")]
        public IActionResult GetButtonsData([FromQuery] string clickValue, string selectedCLCode)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var getData = _tradeWebRepository.Get_Buttons_Data(clickValue, selectedCLCode);
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

        // TODO : Get Transaction Button Data
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetTransactionBtnData", Name = "GetTransactionBtnData")]
        public IActionResult GetTransactionBtnData([FromQuery] string clickValue, string selectedCLCode, string lstShowTransaction, string fromDate, string toDate)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var getData = _tradeWebRepository.Get_Transaction_Btn_Data(clickValue, selectedCLCode, lstShowTransaction, fromDate, toDate);
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

        // TODO : Get Transaction Button RPJ Detailed Data
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("GetTransactionBtnRPJDetailedData", Name = "GetTransactionBtnRPJDetailedData")]
        public IActionResult GetTransactionBtnRPJDetailedData([FromQuery] string client, string commandArgumentType, string fromDate, string toDate)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var getData = _tradeWebRepository.Get_Transaction_Btn_RPJ_Detailed_Data(client, commandArgumentType, fromDate, toDate);
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

        #region new family api

        // TODO : Get Family List
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("Family_List", Name = "Family_List")]
        public IActionResult Family_List()
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Family_List(userId);
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

        // TODO : Get Family List
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("Family_Add", Name = "Family_Add")]
        public IActionResult Family_Add(string uccCode, string password)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Family_Add(userId, password, uccCode);
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

        // TODO : Get Family List
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("Family_Remove", Name = "Family_Remove")]
        public IActionResult Family_Remove(string uccCode)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Family_Remove(uccCode);
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

        // TODO : Get Family List
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpPost("Family_Balance", Name = "Family_Balance")]
        public IActionResult Family_Balance(List<string> uccCode)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var getData = _tradeWebRepository.Family_Balance(uccCode);
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

        // TODO : Get Family List
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpPost("Family_RetainedStoke", Name = "Family_RetainedStoke")]
        public IActionResult Family_RetainedStoke(List<string> uccCode)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    //var tokenS = GetToken();
                    //var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Family_RetainedStoke(uccCode);
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

        // TODO : Get Family List
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpPost("Family_Holding", Name = "Family_Holding")]
        public IActionResult Family_Holding(List<string> uccCode)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var getData = _tradeWebRepository.Family_Holding(uccCode);
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

        // TODO : Get Family List
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpPost("Family_Position", Name = "Family_Position")]
        public IActionResult Family_Position(List<string> uccCode)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var getData = _tradeWebRepository.Family_Position(uccCode);
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

        // TODO : Get Family List
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpPost("Family_Transaction", Name = "Family_Transaction")]
        public IActionResult Family_Transaction(FamilyTransactionModel model)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var getData = _tradeWebRepository.Family_Transaction(model);
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
        [HttpPost("Family_Transaction_Details", Name = "Family_Transaction_Details")]
        public IActionResult Family_Transaction_Details(string client, string type, string fromDate, string toDate)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var getData = _tradeWebRepository.Family_Transaction_Details(client, type, fromDate, toDate);
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
        [HttpPost("Family_RetainedStokeJson", Name = "Family_RetainedStokeJson")]
        public IActionResult Family_RetainedStokeJson(List<string> uccCode)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var getData = _tradeWebRepository.Family_RetainedStokeJson(uccCode);
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
        [HttpPost("Family_HoldingJson", Name = "Family_HoldingJson")]
        public IActionResult Family_HoldingJson(List<string> uccCode)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var getData = _tradeWebRepository.Family_HoldingJson(uccCode);
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
        [HttpPost("Family_TransactionJson", Name = "Family_TransactionJson")]
        public IActionResult Family_TransactionJson(FamilyTransactionModel model)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var getData = _tradeWebRepository.Family_TransactionJson(model);
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
        [HttpPost("Family_Transaction_DetailsJson", Name = "Family_Transaction_DetailsJson")]
        public IActionResult Family_Transaction_DetailsJson(string Client, string Type, string FromDate, string ToDate)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var getData = _tradeWebRepository.Family_TransactionDetailJson(Client, Type, FromDate, ToDate);
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
