using System;

namespace TylieSageApi.DomainLogic.Exceptions
{
    public class AccountingException : Exception
    {
        public string Title { get; set; }

        public AccountingException(string message) :
            base(message)
        {
        }

        public AccountingException(string title, string message) :
            base(message)
        {
            Title = title;
        }
    }
}
