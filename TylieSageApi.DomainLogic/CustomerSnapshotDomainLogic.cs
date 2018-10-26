using System.Collections.Generic;
using TylieSageApi.Data;
using TylieSageApi.Data.Entities.DataTransferObjects.Request;
using TylieSageApi.Data.Entities.Entities;

namespace TylieSageApi.DomainLogic
{
    public interface ICustomerSnapshotDomainLogic
    {
        void AddCustomer(string companyID, CustomerSnapshotRequestDto inputDto);
        IEnumerable<Customer> GetCustomers(string companyID);
    }

    public class CustomerSnapshotDomainLogic : ICustomerSnapshotDomainLogic
    {
        private CustomerRepository _customerRepository;
        public CustomerSnapshotDomainLogic()
        {
            _customerRepository = new CustomerRepository();
        }

        public void AddCustomer(string companyID, CustomerSnapshotRequestDto inputDto)
        {
        }

        public IEnumerable<Customer> GetCustomers(string companyID)
        {
            IEnumerable<Customer> customerList = _customerRepository.GetByCompanyId(companyID);
            return customerList;
        }
    }
}
