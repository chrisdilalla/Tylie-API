using System.ComponentModel.DataAnnotations;
using System.Text.RegularExpressions;

namespace Accounting.Infrastructure
{
    public class AppDateFormatValidationAttribute : ValidationAttribute
    {
        private Regex appDateFormatRegex = new Regex("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]");

        protected override ValidationResult IsValid(object inputObj, ValidationContext validationContext)
        {
            string inputStr = (string)inputObj;
            if (appDateFormatRegex.IsMatch(inputStr))
                return ValidationResult.Success;
            else
                return new ValidationResult("Incorrect date format");
        }
    }
}