﻿using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace TylieSageApi.DomainLogic.Utils
{
    public class WebApiInteraction
    {
        public async Task<ApiPutPostResult> PostAsync<TIn, TOut>(string endpoint, TIn data, string authToken = null)
        {
            using (var client = new HttpClient())
            {
                if (!string.IsNullOrEmpty(authToken))
                {
                    //Add the authorization header
                    client.DefaultRequestHeaders.Authorization = AuthenticationHeaderValue.Parse("Bearer " + authToken);
                }

                client.DefaultRequestHeaders.Accept.Clear();
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                string inputJson = JsonConvert.SerializeObject(data);
                StringContent content = new StringContent(inputJson, Encoding.UTF8, "application/json");

                var result = await client.PostAsync(endpoint, content);
                ApiPutPostResult retval = new ApiPutPostResult(result);
                string responseContentString = await result.Content.ReadAsStringAsync();

                if (retval.IsSuccessStatusCode)
                {
                    if (typeof(TOut) != typeof(string))
                        retval.Object = JsonConvert.DeserializeObject<TOut>(responseContentString);
                }
                else
                {
                    retval.Object = responseContentString;
                }
                return retval;
            }
        }
    }
}