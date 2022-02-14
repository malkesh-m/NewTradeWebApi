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
                #region
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Get_Page_Load_Data(userId);
                    if (getData != null)
                    {
                        return Ok(new commonResponse { status = true, message = "success", status_code = (int)HttpStatusCode.OK, data = getData });
                    }
                    else
                    {
                        return NotFound(new commonResponse { status = false, message = "blank", status_code = (int)HttpStatusCode.NotFound, error_message = "records not found" });
                    }
                }
                catch (Exception ex)
                {
                    return BadRequest(new commonResponse { status = false, message = "error", status_code = (int)HttpStatusCode.InternalServerError, error_message = ex.Message.ToString() });
                }
                #endregion
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
                        return Ok(new commonResponse { status = true, message = "success", status_code = (int)HttpStatusCode.OK, data = getData });
                    }
                    else
                    {
                        return NotFound(new commonResponse { status = false, message = "blank", status_code = (int)HttpStatusCode.NotFound, error_message = "records not found" });
                    }
                }
                catch (Exception ex)
                {
                    return BadRequest(new commonResponse { status = false, message = "error", status_code = (int)HttpStatusCode.InternalServerError, error_message = ex.Message.ToString() });
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
                #region
                try
                {
                    var getData = _tradeWebRepository.Get_Transaction_Btn_Data(clickValue, selectedCLCode, lstShowTransaction, fromDate, toDate);
                    if (getData != null)
                    {
                        return Ok(new commonResponse { status = true, message = "success", status_code = (int)HttpStatusCode.OK, data = getData });
                    }
                    else
                    {
                        return NotFound(new commonResponse { status = false, message = "blank", status_code = (int)HttpStatusCode.NotFound, error_message = "records not found" });
                    }
                }
                catch (Exception ex)
                {
                    return BadRequest(new commonResponse { status = false, message = "error", status_code = (int)HttpStatusCode.InternalServerError, error_message = ex.Message.ToString() });
                }
                #endregion
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
                #region
                try
                {
                    var getData = _tradeWebRepository.Get_Transaction_Btn_RPJ_Detailed_Data(client, commandArgumentType, fromDate, toDate);
                    if (getData != null)
                    {
                        return Ok(new commonResponse { status = true, message = "success", status_code = (int)HttpStatusCode.OK, data = getData });
                    }
                    else
                    {
                        return NotFound(new commonResponse { status = false, message = "blank", status_code = (int)HttpStatusCode.NotFound, error_message = "records not found" });
                    }
                }
                catch (Exception ex)
                {
                    return BadRequest(new commonResponse { status = false, message = "error", status_code = (int)HttpStatusCode.InternalServerError, error_message = ex.Message.ToString() });
                }
                #endregion
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
                #region
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Family_List(userId);
                    if (getData != null)
                    {
                        return Ok(new commonResponse { status = true, message = "success", status_code = (int)HttpStatusCode.OK, data = getData });
                    }
                    else
                    {
                        return NotFound(new commonResponse { status = false, message = "blank", status_code = (int)HttpStatusCode.NotFound, error_message = "records not found" });
                    }
                }
                catch (Exception ex)
                {
                    return BadRequest(new commonResponse { status = false, message = "error", status_code = (int)HttpStatusCode.InternalServerError, error_message = ex.Message.ToString() });
                }
                #endregion
            }
            return BadRequest();
        }

        // TODO : Get Family List
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("Family_Add", Name = "Family_Add")]
        public IActionResult Family_Add(string UCC_Code, string password)
        {
            if (ModelState.IsValid)
            {
                #region
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Family_Add(userId, password, UCC_Code);
                    if (getData != null)
                    {
                        return Ok(new commonResponse { status = true, message = "success", status_code = (int)HttpStatusCode.OK, data = getData });
                    }
                    else
                    {
                        return NotFound(new commonResponse { status = false, message = "blank", status_code = (int)HttpStatusCode.NotFound, error_message = "records not found" });
                    }
                }
                catch (Exception ex)
                {
                    return BadRequest(new commonResponse { status = false, message = "error", status_code = (int)HttpStatusCode.InternalServerError, error_message = ex.Message.ToString() });
                }
                #endregion
            }
            return BadRequest();
        }

        // TODO : Get Family List
        [Authorize(AuthenticationSchemes = "Bearer")]
        [HttpGet("Family_Remove", Name = "Family_Remove")]
        public IActionResult Family_Remove(string UCC_Code)
        {
            if (ModelState.IsValid)
            {
                #region
                try
                {
                    var tokenS = GetToken();
                    var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

                    var getData = _tradeWebRepository.Family_Remove(UCC_Code);
                    if (getData != null)
                    {
                        return Ok(new commonResponse { status = true, message = "success", status_code = (int)HttpStatusCode.OK, data = getData });
                    }
                    else
                    {
                        return NotFound(new commonResponse { status = false, message = "blank", status_code = (int)HttpStatusCode.NotFound, error_message = "records not found" });
                    }
                }
                catch (Exception ex)
                {
                    return BadRequest(new commonResponse { status = false, message = "error", status_code = (int)HttpStatusCode.InternalServerError, error_message = ex.Message.ToString() });
                }
                #endregion
            }
            return BadRequest();
        }

        //// TODO : Get Family List
        //[Authorize(AuthenticationSchemes = "Bearer")]
        //[HttpPost("Family_Balance", Name = "Family_Balance")]
        //public IActionResult Family_Balance(List<string> UCC_Codes)
        //{
        //    if (ModelState.IsValid)
        //    {
        //        #region
        //        try
        //        {
        //            var tokenS = GetToken();
        //            var userId = tokenS.Claims.First(claim => claim.Type == "username").Value;

        //            var getData = _tradeWebRepository.Family_Balance(UCC_Codes);
        //            if (getData != null)
        //            {
        //                return Ok(new commonResponse { status = true, message = "success", status_code = (int)HttpStatusCode.OK, data = getData });
        //            }
        //            else
        //            {
        //                return NotFound(new commonResponse { status = false, message = "blank", status_code = (int)HttpStatusCode.NotFound, error_message = "records not found" });
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            return BadRequest(new commonResponse { status = false, message = "error", status_code = (int)HttpStatusCode.InternalServerError, error_message = ex.Message.ToString() });
        //        }
        //        #endregion
        //    }
        //    return BadRequest();
        //}


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
