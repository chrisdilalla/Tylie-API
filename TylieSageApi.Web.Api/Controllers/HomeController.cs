using System.Web.Mvc;

namespace TylieSageApi.Web.Api.Controllers
{
    public class HomeController : Controller
    {
        public ActionResult Index()
        {
            ViewBag.Title = "Sage 500 - Tylie Integration API";
            return View();
        }
    }
}
