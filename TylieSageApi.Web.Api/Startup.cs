using Microsoft.Owin;
using Owin;
using TylieSageApi.Web.Api;

[assembly: OwinStartup(typeof(Startup))]

namespace TylieSageApi.Web.Api
{
    public partial class Startup
    {
        public void Configuration(IAppBuilder app)
        {
        }
    }
}
