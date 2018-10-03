﻿using System.Threading.Tasks;
using TylieSageApi.Data.Entities.DataTransferObjects.CallbackRequest;
using TylieSageApi.Data.Entities.DataTransferObjects.Request;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;
using TylieSageApi.DomainLogic.Utils;

namespace TylieSageApi.DomainLogic
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