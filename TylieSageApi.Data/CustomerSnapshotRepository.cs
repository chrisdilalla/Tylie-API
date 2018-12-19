using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TylieSageApi.Data.Entities.DataTransferObjects.Request;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;
using TylieSageApi.Data.Entities.Entities;

namespace TylieSageApi.Data
{
    public interface ICustomerRepository
    {
        IEnumerable<Customer> GetByCompanyId(string companyId);
        int MigrateCustomersToRealTables(string guid);
    }

    public class CustomerRepository : BaseRepository, ICustomerRepository
    {
        public IEnumerable<Customer> GetByCompanyId(string companyId)
        {
            var parameters = new { CompanyId = companyId };
            IEnumerable<Customer> list = Query<Customer>(
                @"select a.companyID,
                    custkey,
                    custid,
                    custclassid,
                    custClassName,
                    addrLine1,
                    addrLine2,
                    city,
                    stateID,
                    countryID,
                    postalCode,
                    d.name as cntctName,
                    d.Title cntctTitle,
                        d.fax as cntctFax,
                    d.phone as cntctPhone,
                    d.EMailAddr as cntctEmail,
                    0 as printAck,
                    0 as requireAck,
                    case when a.status = 1 then 'Active'
                    when a.status = 2 then 'InActive'
                    when a.status = 3 then 'Temporary'
                    when a.status = 4 then 'Deleted' end,
                    'Temp123' as brand
                    from tarcustomer a
                        inner join tarcustclass b on a.custclasskey = b.custclasskey
                    inner join tciaddress c on a.primaryAddrKey = c.AddrKey
                    inner join tcicontact d on a.PrimaryCntctKey = d.CntctKey
                    where a.companyid = @CompanyId",
                parameters);
            return list;
        }

        public void AddCustomers(IList<Customer> data)
        {
            string sql = @"insert into stgCustomer_Tylie (
                [CompanyID],
                [Key],
                [CustID],
                [CustClassID],
                [CustClassName],
                [AddrLine1],
                [AddrLine2],
                [City],
                [StateID],
                [CountryID],
                [PostalCode],
                [ContactName],
                [ContactTitle],
                [ContactFax],
                [ContactPhone],
                [ContactEmail],
                [PrintAck],
                [RequireAck],
                [Status],
                [BrandKey],
                [BrandID],
                [Brand],
                [BrandStatus],
                [ImportStatus])
             values (
                @CompanyID,
                @Key,
                @CustID,
                @CustClassID,
                @CustClassName,
                @AddrLine1,
                @AddrLine2,
                @City,
                @StateID,
                @CountryID,
                @PostalCode,
                @ContactName,
                @ContactTitle,
                @ContactFax,
                @ContactPhone,
                @ContactEmail,
                @PrintAck,
                @RequireAck,
                @Status,
                @BrandKey,
                @BrandID,
                @Brand,
                @BrandStatus,
                @ImportStatus
                )";
            for (int itemNum = 0; itemNum < data.Count; itemNum++)
            {
                Execute(sql, data[itemNum]);
            }
        }

        public int MigrateCustomersToRealTables(string guid)
        {
            int result;
            result = Query<int>("spARCustomerImport_Tylie",
                new
                {
                    oContinue = 0,
                    iCancel = 0,
                    iSessionKey = guid,
                    iCompanyID = 0, // Supported.Enterprise Company.
                    iRptOption = 0,
                    iPrintWarnings = 0,
                    iUseStageTable = 1,
                    oRecsProcessed = 0,
                    oFailedRecs = 0,
                    oTotalRecs = 0
                }, null, true, null, CommandType.StoredProcedure).SingleOrDefault();
            return result;
        }
    }
}
