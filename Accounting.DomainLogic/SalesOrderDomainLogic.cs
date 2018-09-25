using System.Threading.Tasks;
using Accounting.Data.DataTransferObjects.CallbackRequest;
using Accounting.Data.DataTransferObjects.Request;
using Accounting.Data.DataTransferObjects.Response;
using Accounting.Utils;

namespace Accounting.DomainLogic
{
    public interface ISalesOrderDomainLogic
    {
        Task<SalesOrderResponseDto> AddSalesOrder(string companyID, SalesOrderRequestDto inputDto);
    }

    public class SalesOrderDomainLogic : ISalesOrderDomainLogic
    {
        public async Task<SalesOrderResponseDto> AddSalesOrder(string companyID, SalesOrderRequestDto inputDto)
        {
            return await QueueSalesOrder(companyID, inputDto);
        }

        private async Task<SalesOrderResponseDto> QueueSalesOrder(string companyID, SalesOrderRequestDto inputDto)
        {
            SalesOrderResponseDto result =  await Task.Factory.StartNew<SalesOrderResponseDto>(() =>
            {
                System.Threading.Thread.Sleep(10000); // simulates a long running operation
                WebApiInteraction webApiInteraction = new WebApiInteraction();
                webApiInteraction.PostAsync<SalesOrderCallbackRequestDto, object>(inputDto.CallbackUrl, new SalesOrderCallbackRequestDto());
                return new SalesOrderResponseDto();
            });
            return result;
        }
    }
}
