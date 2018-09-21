using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Accounting.Data.DataTransferObjects.Request
{
    public class SnapshotForCompanyAndDateRequestDto
    {
        [Required]
        [MaxLength(3)]
        public string CompanyID { get; set; }

        [Required]
        public DateTime LastUpdatedDate { get; set; }
    }
}
