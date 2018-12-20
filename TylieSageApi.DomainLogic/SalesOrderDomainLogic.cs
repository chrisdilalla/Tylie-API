using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using TylieSageApi.Common;
using TylieSageApi.Data;
using TylieSageApi.Data.Entities.DataTransferObjects.CallbackRequest;
using TylieSageApi.Data.Entities.DataTransferObjects.Request;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;
using TylieSageApi.Data.Entities.DataTransferObjects.Response.Base;
using TylieSageApi.Data.Entities.Entities;
using TylieSageApi.DomainLogic.Utils;

namespace TylieSageApi.DomainLogic
{
    public interface ISalesOrderDomainLogic
    {
        SalesOrderResponseDto AddSalesOrder(string companyID, SalesOrderRequestDto inputDto);
    }

    public class SalesOrderDomainLogic : ISalesOrderDomainLogic
    {
        private ISalesOrderRepository _salesOrderRepository;
        private ITransactionLogRepository _transactionLogRepository;
        private IUtils _utils;

        public SalesOrderDomainLogic()
        {
            _salesOrderRepository = new SalesOrderRepository();
            _transactionLogRepository = new TransactionLogRepository();
            _utils = new Utils.Utils();
        }

        public SalesOrderResponseDto AddSalesOrder(string companyID, SalesOrderRequestDto inputDto)
        {
            SalesOrderResponseDto result = new SalesOrderResponseDto();
            string transitID = _utils.GetGuidString();
            result.TransitID = transitID;
            TransactionLog transactionLog;
            try
            {
                List<SalesOrder> customers = AutoMapper.Mapper.Map<List<SalesOrder>>(inputDto);
                _salesOrderRepository.AddSalesOrders(customers);
                transactionLog = new TransactionLog(transitID, EventType.SalesOrderDataInsert,
                    "SalesOrder Data Posted Successfully");
                _transactionLogRepository.AddRecord(transactionLog);
            }
            catch (Exception exception)
            {
                string errorTitle = "SalesOrder Data Post error.";
                transactionLog = new TransactionLog(transitID, EventType.SalesOrderDataInsert,
                    $"{errorTitle} {exception.Message}");
                _transactionLogRepository.AddRecord(transactionLog);
                result.AddErrorsFromException(errorTitle, exception);
                return result;
            }

            Task.Factory.StartNew(() =>
            {
                try
                {
                    _salesOrderRepository.MigrateSalesOrdersToRealTables(transitID);
                    transactionLog = new TransactionLog(transitID, EventType.SalesOrderInsertSP_Complete,
                        "Sales orders insert stored procedure has completed successfully");
                }
                catch (Exception exception)
                {
                    transactionLog = new TransactionLog(transitID, EventType.SalesOrderInsertSP_Complete,
                        $"Sales orders insert stored procedure completed with errors. {exception.Message}");
                }
                _transactionLogRepository.AddRecord(transactionLog);
            });
            transactionLog = new TransactionLog(transitID, EventType.SalesOrderInsertSP_Called,
                "Sales orders insert stored procedure is called");
            _transactionLogRepository.AddRecord(transactionLog);
            return result;
        }

        private async Task<SalesOrderResponseDto> QueueSalesOrder(string companyID, SalesOrderRequestDto inputDto)
        {
            SalesOrderResponseDto result = await Task.Factory.StartNew<SalesOrderResponseDto>(() =>
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
