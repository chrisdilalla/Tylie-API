using System.Web.Http;
using TylieSageApi.Web.Api.Infrastructure;

namespace TylieSageApi.Web.Api
{
    public static class WebApiConfig
    {
        public static void Register(HttpConfiguration config)
        {
            // Web API configuration and services
            GlobalConfiguration.Configuration.Filters.Add(new AppExceptionFilter());

            // Web API routes
            config.MapHttpAttributeRoutes(new CentralizedPrefixProvider("api/v1"));

            config.Routes.MapHttpRoute(
                name: "DefaultApi",
                routeTemplate: "api/{controller}/{id}",
                defaults: new { id = RouteParameter.Optional }
            );
        }
    }
}
