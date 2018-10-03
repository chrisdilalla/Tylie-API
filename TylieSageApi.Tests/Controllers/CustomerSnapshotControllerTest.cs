using Microsoft.VisualStudio.TestTools.UnitTesting;
using TylieSageApi.Data.Entities.DataTransferObjects.Request;
using TylieSageApi.Web.Api.Controllers;

namespace TylieSageApi.Tests.Controllers
{
    [TestClass]
    public class CustomerSnapshotControllerTest
    {
        [TestMethod]
        public void Post()
        {
            // Arrange
            CustomerSnapshotController controller = new CustomerSnapshotController();

            // Act
            controller.Post("test", new CustomerSnapshotRequestDto());

            // Assert
        }
    }
}
