using System.Net;
using System.Net.Http;
using System.Web.Http.Filters;
using Newtonsoft.Json;
using TylieSageApi.Data.Entities.DataTransferObjects.Response.Base;
using TylieSageApi.DomainLogic.Exceptions;

namespace TylieSageApi.Web.Api.Infrastructure
{
    public class AppExceptionFilter : ExceptionFilterAttribute
    {
        public override void OnException(HttpActionExecutedContext context)
        {
            HttpResponseMessage responseMsg = new HttpResponseMessage();
            BaseResponseDto respDto = new BaseResponseDto();
            AccountingException accException = context.Exception as AccountingException;

            string errorTitle = null;
            if (accException == null)
            {
                errorTitle = "Internal server error";
                responseMsg.StatusCode = HttpStatusCode.InternalServerError;
            }
            else
            {
                errorTitle = accException.Title;
                responseMsg.StatusCode = HttpStatusCode.BadRequest;
            }
            respDto.Status = (int) responseMsg.StatusCode;
            respDto.Errors.Add(new BaseResponseDto.Error(errorTitle, context.Exception.Message));
            string respDtoJson = JsonConvert.SerializeObject(respDto);
            responseMsg.Content = new JsonContent(respDtoJson);
            context.Response = responseMsg;

        }
    }
}