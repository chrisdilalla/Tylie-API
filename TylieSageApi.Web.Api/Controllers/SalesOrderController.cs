using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using System.Web.Http;
using TylieSageApi.Data.Entities.DataTransferObjects.Request;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;
using TylieSageApi.DomainLogic;
using TylieSageApi.Web.Api.Controllers.Abstract;
using TylieSageApi.Web.Api.Infrastructure;

namespace TylieSageApi.Web.Api.Controllers
{
    public class SalesOrderController : BaseController
    {
        private ISalesOrderDomainLogic _salesOrderDomainLogic;

        public SalesOrderController()
        {
            _salesOrderDomainLogic = new SalesOrderDomainLogic();
        }


        [Route("salesorders/{companyID}")]
        [ValidateActionParameters]
        public async Task<IHttpActionResult> Post([MinLength(1)][MaxLength(3)]string companyID,
            [Required][FromBody]SalesOrderRequestDto inputDto)
        {
            if (!ModelState.IsValid)
                ThrowModelStateException(ModelState);

            SalesOrderResponseDto result = await _salesOrderDomainLogic.AddSalesOrder(companyID, inputDto);
            return Ok(result);
        }
    }
}
