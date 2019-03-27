using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using TylieSageApi.Common;
using TylieSageApi.Data;
using TylieSageApi.Data.Entities.DataTransferObjects.Request;
using TylieSageApi.Data.Entities.DataTransferObjects.Request.SalesOrder;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;
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
        private IPurchaseOrderRepository _purchaseOrderRepository;
        private ITransactionLogRepository _transactionLogRepository;
        private IUtils _utils;

        public SalesOrderDomainLogic()
        {
            _salesOrderRepository = new SalesOrderRepository();
            _purchaseOrderRepository = new PurchaseOrderRepository();
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
                List<SalesOrder> orders = AutoMapper.Mapper.Map<List<SalesOrder>>(inputDto.Lines);
                foreach (SalesOrder salesOrder in orders)
                {
                    AutoMapper.Mapper.Map<SalesOrderRequestDto, SalesOrder>(inputDto, salesOrder);
                }

                _salesOrderRepository.AddSalesOrders(orders);

                foreach (PurchaseOrderItemInSalesOrder purchaseOrderDto in inputDto.PurchaseOrders)
                {
                    List<PurchaseOrder> purchaseOrders =
                        AutoMapper.Mapper.Map<List<PurchaseOrder>>(purchaseOrderDto.Lines);
                    foreach (PurchaseOrder purchaseOrder in purchaseOrders)
                    {
                        AutoMapper.Mapper.Map<PurchaseOrderItemInSalesOrder, PurchaseOrder>(purchaseOrderDto, purchaseOrder);
                    }
                    _purchaseOrderRepository.AddPurchaseOrders(purchaseOrders);
                }

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

            transactionLog = new TransactionLog(transitID, EventType.SalesOrderInsertSP_Called,
                "Sales orders insert stored procedure is called");
            _transactionLogRepository.AddRecord(transactionLog);

            try
            {
                _salesOrderRepository.MigrateSalesOrdersToRealTables(transitID);
                transactionLog = new TransactionLog(transitID, EventType.SalesOrderInsertSP_Complete,
                    "Sales orders insert stored procedure has completed successfully");
            }
            catch (Exception exception)
            {
                const string commonErrorText = "Sales orders insert stored procedure completed with errors.";
                transactionLog = new TransactionLog(transitID, EventType.SalesOrderInsertSP_Complete,
                    $"{commonErrorText} {exception.Message}");
                result.AddErrorsFromException(commonErrorText, exception);
            }
            _transactionLogRepository.AddRecord(transactionLog);
            return result;
        }
    }
}
