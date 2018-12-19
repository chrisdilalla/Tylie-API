using System;

namespace TylieSageApi.DomainLogic.Utils
{
    public interface IUtils
    {
        string GetGuidString();
    }

    public class Utils: IUtils
    {
        public string GetGuidString()
        {
            Guid guid = Guid.NewGuid();
            return guid.ToString();
        }
    }
}
