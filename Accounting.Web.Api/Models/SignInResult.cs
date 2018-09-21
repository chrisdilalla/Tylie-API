using System;
using Newtonsoft.Json;

namespace Accounting.Models
{
    public class SignInResult
    {
        [JsonProperty("access_token")]
        public string AccessToken { get; set; }

        [JsonProperty("expires_in")]
        public uint ExpiresIn { get; set; }

        [JsonProperty("userName")]
        public string UserName { get; set; }

        [JsonProperty(".issued")]
        public DateTimeOffset Issued { get; set; }

        [JsonProperty(".expires")]
        public DateTimeOffset Expires { get; set; }

        //not used
        [JsonProperty("token_type")]
        public string TokenType { get; set; }

    }
}