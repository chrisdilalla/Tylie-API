using TylieSageApi.Data.Entities.DataTransferObjects.Request;

namespace TylieSageApi.DomainLogic
{
    public interface ICustomerSnapshotDomainLogic
    {
        void AddCustomer(string companyID, CustomerSnapshotRequestDto inputDto);
    }

    public class CustomerSnapshotDomainLogic : ICustomerSnapshotDomainLogic
    {
        public void AddCustomer(string companyID, CustomerSnapshotRequestDto inputDto)
        {
        }
    }
}
