using Accounting.Controllers;
using Accounting.Data.DataTransferObjects.Request;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Accounting.Tests.Controllers
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
