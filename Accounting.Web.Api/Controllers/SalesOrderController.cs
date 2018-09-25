using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using System.Web.Http;
using Accounting.Controllers.Abstract;
using Accounting.Data.DataTransferObjects.Request;
using Accounting.Data.DataTransferObjects.Response;
using Accounting.DomainLogic;
using Accounting.DomainLogic.Exceptions;
using Accounting.Infrastructure;

namespace Accounting.Controllers
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
