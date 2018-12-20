using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using TylieSageApi.Common.Exceptions;
using TylieSageApi.Data.Entities.DataTransferObjects.Request;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;
using TylieSageApi.Data.Entities.Entities;

namespace TylieSageApi.Data
{
    public interface ICustomerRepository
    {
        IEnumerable<Customer> GetByCompanyId(string companyId);
        void AddCustomers(IList<Customer> data);
        void MigrateCustomersToRealTables(string guid);
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

        public void MigrateCustomersToRealTables(string guid)
        {
            DynamicParameters dynamicParameters = new DynamicParameters();
            string retValName = "_oRetVal";
            string storedProcName = "spARCustomerImport_Tylie";
            dynamicParameters.Add(retValName, dbType: DbType.Int32, direction: ParameterDirection.Output);
            Query<int>(storedProcName, dynamicParameters, null, true, null, CommandType.StoredProcedure).SingleOrDefault();
            int result = dynamicParameters.Get<int>(retValName);
            if (result != 1)
                throw new StoredProcedureException(storedProcName, result);
        }
    }
}
