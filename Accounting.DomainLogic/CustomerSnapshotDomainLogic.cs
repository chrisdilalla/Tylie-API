using Accounting.Data.DataTransferObjects.Request;

namespace Accounting.DomainLogic
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
