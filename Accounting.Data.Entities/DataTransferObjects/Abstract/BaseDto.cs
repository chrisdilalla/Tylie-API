using System.ComponentModel.DataAnnotations;

namespace Accounting.Data.DataTransferObjects.Abstract
{
    public abstract class BaseDto
    {
        [MaxLength(36)]
        public string TransitID { get; set; }
    }
}
