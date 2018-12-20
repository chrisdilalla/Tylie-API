using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using TylieSageApi.Common;
using TylieSageApi.Data;
using TylieSageApi.Data.Entities.DataTransferObjects;
using TylieSageApi.Data.Entities.DataTransferObjects.Request;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;
using TylieSageApi.Data.Entities.DataTransferObjects.Response.Base;
using TylieSageApi.Data.Entities.Entities;
using TylieSageApi.DomainLogic.Utils;

namespace TylieSageApi.DomainLogic
{
    public interface ICustomerSnapshotDomainLogic
    {
        BaseResponseDto AddCustomer(string companyID, CustomerSnapshotRequestDto inputDto);
        IEnumerable<CustomerSnapshotItem> GetCustomers(string companyID);
    }

    public class CustomerSnapshotDomainLogic : ICustomerSnapshotDomainLogic
    {
        private ICustomerRepository _customerRepository;
        private ITransactionLogRepository _transactionLogRepository;
        private IUtils _utils;
        public CustomerSnapshotDomainLogic()
        {
            _customerRepository = new CustomerRepository();
            _transactionLogRepository = new TransactionLogRepository();
            _utils = new Utils.Utils();
        }

        public BaseResponseDto AddCustomer(string companyID, CustomerSnapshotRequestDto inputDto)
        {
            BaseResponseDto result = new BaseResponseDto();
            string transitID = _utils.GetGuidString();
            result.TransitID = transitID;
            TransactionLog transactionLog;
            try
            {
                List<Customer> customers = AutoMapper.Mapper.Map<List<Customer>>(inputDto.Data);
                _customerRepository.AddCustomers(customers);
                transactionLog = new TransactionLog(transitID, EventType.CustomerDataInsert,
                    "Customer Data Posted Successfully");
                _transactionLogRepository.AddRecord(transactionLog);
            }
            catch (Exception exception)
            {
                string errorTitle = "Customer Data Post error.";
                transactionLog = new TransactionLog(transitID, EventType.CustomerDataInsert,
                    $"{errorTitle} {exception.Message}");
                _transactionLogRepository.AddRecord(transactionLog);
                result.AddErrorsFromException(errorTitle, exception);
                return result;
            }

            Task.Factory.StartNew(() =>
            {
                try
                {
                    _customerRepository.MigrateCustomersToRealTables(transitID);
                    transactionLog = new TransactionLog(transitID, EventType.CustomerImportSP_Complete,
                        "Customer import stored procedure has completed successfully");
                }
                catch (Exception exception)
                {
                    transactionLog = new TransactionLog(transitID, EventType.CustomerImportSP_Complete,
                        $"Customer import stored procedure has completed with errors. {exception.Message}");
                }
                _transactionLogRepository.AddRecord(transactionLog);
            });
            transactionLog = new TransactionLog(transitID, EventType.CustomerImportSP_Called,
                "Customer Import Stored Procedure is called");
            _transactionLogRepository.AddRecord(transactionLog);
            return result;
        }

        public IEnumerable<CustomerSnapshotItem> GetCustomers(string companyID)
        {
            IEnumerable<Customer> entityList = _customerRepository.GetByCompanyId(companyID);
            IEnumerable<CustomerSnapshotItem> customerList = AutoMapper.Mapper.Map<IEnumerable<CustomerSnapshotItem>>(entityList);
            return customerList;
        }
    }
}
