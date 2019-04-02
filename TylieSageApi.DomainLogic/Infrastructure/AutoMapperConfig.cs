using AutoMapper;
using TylieSageApi.Data.Entities.DataTransferObjects;
using TylieSageApi.Data.Entities.DataTransferObjects.Request;
using TylieSageApi.Data.Entities.DataTransferObjects.Request.SalesOrder;
using TylieSageApi.Data.Entities.Entities;

namespace TylieSageApi.DomainLogic.Infrastructure
{
    public static class AutoMapperConfig
    {
        public static void RegisterMappings()
        {
            Mapper.Initialize(cfg =>
            {
                cfg.CreateMap<CustomerSnapshotItem, Customer>()
                    .ForMember(item => item.CustID, configExpressionEntity => configExpressionEntity.MapFrom(entity => entity.CustomerID))
                    .ForMember(item => item.ContactName, configExpressionEntity => configExpressionEntity.MapFrom(entity => entity.CntctName))
                    .ForMember(item => item.ContactTitle, configExpressionEntity => configExpressionEntity.MapFrom(entity => entity.CntctTitle))
                    .ForMember(item => item.ContactFax, configExpressionEntity => configExpressionEntity.MapFrom(entity => entity.CntctFax))
                    .ForMember(item => item.ContactPhone, configExpressionEntity => configExpressionEntity.MapFrom(entity => entity.CntctPhone))
                    .ForMember(item => item.ContactEmail, configExpressionEntity => configExpressionEntity.MapFrom(entity => entity.CntctEmail));
                cfg.CreateMap<Customer, CustomerSnapshotItem>()
                    .ForMember(item => item.CustomerID, configExpressionEntity => configExpressionEntity.MapFrom(entity => entity.CustID))
                    .ForMember(item => item.CntctName, configExpressionEntity => configExpressionEntity.MapFrom(entity => entity.ContactName))
                    .ForMember(item => item.CntctTitle, configExpressionEntity => configExpressionEntity.MapFrom(entity => entity.ContactTitle))
                    .ForMember(item => item.CntctFax, configExpressionEntity => configExpressionEntity.MapFrom(entity => entity.ContactFax))
                    .ForMember(item => item.CntctPhone, configExpressionEntity => configExpressionEntity.MapFrom(entity => entity.ContactPhone))
                    .ForMember(item => item.CntctEmail, configExpressionEntity => configExpressionEntity.MapFrom(entity => entity.ContactEmail));
                 cfg.CreateMap<SalesOrderRequestDto, SalesOrder>()
                .ForMember(item => item.SoNumber, configExpressionEntity => configExpressionEntity.MapFrom(entity => entity.SalesOrder));
                cfg.CreateMap<SalesOrderItem, SalesOrder>();
                cfg.CreateMap<PurchaseOrderItem, PurchaseOrder>();
                cfg.CreateMap<PurchaseOrderItemInSalesOrder, PurchaseOrder>();

            });
        }
    }
}