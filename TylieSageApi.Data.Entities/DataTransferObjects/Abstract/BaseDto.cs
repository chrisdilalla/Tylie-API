using System.ComponentModel.DataAnnotations;

namespace TylieSageApi.Data.Entities.DataTransferObjects.Abstract
{
    public abstract class BaseDto
    {
        [MaxLength(36)]
        public string TransitID { get; set; }
    }
}
