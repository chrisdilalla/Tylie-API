namespace TylieSageApi.Web.Api.Utils
{
    public class Constants
    {
        public const string RouteWithCompanyAndDate =
            @"{companyID}/{lastUpdatedDate:regex(\d{4}-\d{2}-\d{2})}";
    }
}