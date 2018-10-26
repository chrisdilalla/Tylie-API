using System.Web.Mvc;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TylieSageApi.Web.Api.Controllers;

namespace TylieSageApi.Tests.Controllers
{
    [TestClass]
    public class HomeControllerTest
    {
        [TestMethod]
        public void Index()
        {
            // Arrange
            HomeController controller = new HomeController();

            // Act
            ViewResult result = controller.Index() as ViewResult;

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual("Sage 500 - Tylie Integration API", result.ViewBag.Title);
        }
    }
}
