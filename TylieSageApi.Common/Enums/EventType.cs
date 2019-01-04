namespace TylieSageApi.Common
{
    public enum EventType
    {
        CustomerDataInsert = 0,
        CustomerImportSP_Called = 1,
        CustomerImportSP_Complete = 2,
        SalesOrderDataInsert = 3,
        SalesOrderInsertSP_Called = 4,
        SalesOrderInsertSP_Complete = 5,
        VendorsSnapshotDataRetrievalCompleted = 6,
        ItemsSnapshotDataRetrievalCompleted = 7,
        ContractPricingSnapshotDataRetrievalCompleted = 8,
    }
}
