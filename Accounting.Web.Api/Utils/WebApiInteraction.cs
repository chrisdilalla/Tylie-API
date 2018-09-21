using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web;
using Newtonsoft.Json;

namespace Accounting.Utils
{
    public class WebApiInteraction
    {
        public async Task<T> AuthenticateAsync<T>(string userName, string password)
        {
            HttpContext currContext = HttpContext.Current;
            Uri hostUrl = currContext.Request.Url;
            string hostString = hostUrl.Scheme + Uri.SchemeDelimiter + hostUrl.Host + ":" + hostUrl.Port;
            using (var client = new HttpClient())
            {
                HttpResponseMessage result = null;
                FormUrlEncodedContent body = new FormUrlEncodedContent(new List<KeyValuePair<string, string>>
                {
                    new KeyValuePair<string, string>("grant_type", "password"),
                    new KeyValuePair<string, string>("userName", userName),
                    new KeyValuePair<string, string>("password", password)
                });
                string s = await body.ReadAsStringAsync();
                result = await client.PostAsync($"{hostString}/Token", body);

                string json = await result.Content.ReadAsStringAsync();
                if (result.IsSuccessStatusCode)
                {
                    return JsonConvert.DeserializeObject<T>(json);
                }
                return default(T);
            }
        }
    }
}