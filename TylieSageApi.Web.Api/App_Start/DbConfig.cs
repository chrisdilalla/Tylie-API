using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Web;
using System.Web.Configuration;
using TylieSageApi.Data;

namespace TylieSageApi.Web.Api.App_Start
{
    public class DbConfig
    {
        public static void SetConfig()
        {
             BaseRepository.SetConnectionString(WebConfigurationManager.ConnectionStrings["main"].ConnectionString);
        }
    }
}