using System;

namespace Accounting.DomainLogic.Exceptions
{
    public class AccountingException : Exception
    {
        public AccountingException(string message) :
            base(message)
        {
        }
    }
}
