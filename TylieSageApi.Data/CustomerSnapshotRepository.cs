﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;
using TylieSageApi.Data.Entities.Entities;

namespace TylieSageApi.Data
{
    public interface ICustomerRepository
    {
        IEnumerable<Customer> GetByCompanyId(string companyId);
    }

    public class CustomerRepository: BaseRepository, ICustomerRepository
    {
        public IEnumerable<Customer> GetByCompanyId(string companyId)
        {
            var parameters = new {CompanyId = companyId};
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
    }
}