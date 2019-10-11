using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.Azure.ServiceBus;

namespace Contoso
{
    public static class AsyncProcessingWorkAcceptor
    {
        [FunctionName("AsyncProcessingWorkAcceptor")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,
            [Blob("data", FileAccess.Read, Connection = "StorageConnectionAppSetting")] CloudBlobContainer inputBlob,
            [ServiceBus("outqueue", Connection = "ServiceBusConnectionAppSetting")] IAsyncCollector<Message> OutMessage,
            ILogger log)
        {
            string reqid = Guid.NewGuid().ToString();
            
            string rqs = $"https://{Environment.GetEnvironmentVariable("WEBSITE_HOSTNAME")}/api/RequestStatus/{reqid}";

            using (var ms = new MemoryStream()) {
                await req.Body.CopyToAsync(ms);

                Message m = new Message(ms.ToArray());
                m.UserProperties["RequestGUID"] = reqid;
                m.UserProperties["RequestSubmittedAt"] = DateTime.Now;
                m.UserProperties["RequestStatusURL"] = rqs;
                
                await OutMessage.AddAsync(m);  
            }
            CloudBlockBlob cbb = inputBlob.GetBlockBlobReference($"{reqid}.blobdata");
            return (ActionResult) new AcceptedResult(rqs, $"Request Accepted for Processing{Environment.NewLine}ValetKey: {GenerateSASURIForBlob(cbb)}{Environment.NewLine}ProxyStatus: {rqs}");  
        }

        public static string GenerateSASURIForBlob(CloudBlockBlob blob)
        {
            SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy();
            sasConstraints.SharedAccessStartTime = DateTimeOffset.UtcNow.AddMinutes(-5);
            sasConstraints.SharedAccessExpiryTime = DateTimeOffset.UtcNow.AddMinutes(10);
            sasConstraints.Permissions = SharedAccessBlobPermissions.Read;

            //Generate the shared access signature on the blob, setting the constraints directly on the signature.
            string sasBlobToken = blob.GetSharedAccessSignature(sasConstraints);

            //Return the URI string for the container, including the SAS token.
            return blob.Uri + sasBlobToken;
        }
        
    }

    // To get to the original object, deserialize it with the correct class
    public class CustomerPOCO
    {
        public string id;
        public string customername;
    }
}
