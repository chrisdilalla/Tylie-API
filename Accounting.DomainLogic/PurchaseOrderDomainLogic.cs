using System.Threading.Tasks;
using Accounting.Data.DataTransferObjects.CallbackRequest;
using Accounting.Data.DataTransferObjects.Request;
using Accounting.Data.DataTransferObjects.Response;
using Accounting.Utils;

namespace Accounting.DomainLogic
{
    public interface IPurchaseOrderDomainLogic
    {
        Task<PurchaseOrderResponseDto> AddPurchaseOrder(string companyID, PurchaseOrderRequestDto inputDto);
    }

    public class PurchaseOrderDomainLogic : IPurchaseOrderDomainLogic
    {
        public async Task<PurchaseOrderResponseDto> AddPurchaseOrder(string companyID, PurchaseOrderRequestDto inputDto)
        {
            return await QueuePurchaseOrder(companyID, inputDto);
        }

        private async Task<PurchaseOrderResponseDto> QueuePurchaseOrder(string companyID, PurchaseOrderRequestDto inputDto)
        {
            PurchaseOrderResponseDto result =  await Task.Factory.StartNew<PurchaseOrderResponseDto>(() =>
            {
                System.Threading.Thread.Sleep(10000); // simulates a long running operation
                WebApiInteraction webApiInteraction = new WebApiInteraction();
                webApiInteraction.PostAsync<PurchaseOrderCallbackRequestDto, object>(inputDto.CallbackUrl, new PurchaseOrderCallbackRequestDto() {CompanyID = "1", PurchaseOrderNo = "2"});
                return new PurchaseOrderResponseDto();
            });
            return result;
        }
    }
}
