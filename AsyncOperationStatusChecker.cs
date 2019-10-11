using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Linq;

namespace Contoso
{
    public static class AsyncOperationStatusChecker
    {
        [FunctionName("AsyncOperationStatusChecker")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "RequestStatus/{thisGUID}")] HttpRequest req,
            [Blob("data/{thisGuid}.blobdata", FileAccess.Read, Connection = "StorageConnectionAppSetting")] CloudBlockBlob inputBlob, string thisGUID,
            ILogger log)
        {

            OnCompleteEnum OnComplete = Enum.Parse<OnCompleteEnum>(req.Query["OnComplete"].FirstOrDefault() ?? "Redirect");
            OnPendingEnum OnPending = Enum.Parse<OnPendingEnum>(req.Query["OnPending"].FirstOrDefault() ?? "Accepted");

            log.LogInformation($"C# HTTP trigger function processed a request for status on {thisGUID} - OnComplete {OnComplete} - OnPending {OnPending}");

            // ** Check to see if the blob is present **
            if (await inputBlob.ExistsAsync())
            {
                // ** If it's present, depending on the value of the optional "OnComplete" parameter choose what to do. **
                // Default (OnComplete not present or set to "Redirect") is to return a 302 redirect with the location of a SAS for the document in the location field.   
                // If OnComplete is present and set to "Stream", the function should return the response inline.

                log.LogInformation($"Blob {thisGUID}.blobdata exists, hooray!");

                switch (OnComplete)
                {
                    case OnCompleteEnum.Redirect:
                        {
                            // Awesome, let's use the valet key pattern to offload the download via a SAS URI to blob storage
                            return (ActionResult)new RedirectResult(inputBlob.GenerateSASURI());
                        }

                    case OnCompleteEnum.Stream:
                        {
                            // If the action is set to return the file then lets download it and return it back
                            // ToDo: this operation is horrible for larger files, we should use a stream to minimize RAM usage.
                            return (ActionResult)new OkObjectResult(await inputBlob.DownloadTextAsync());
                        }

                    default:
                        {
                            throw new InvalidOperationException("How did we get here??");
                        }
                }
            }
            else
            {
                // ** If it's NOT present, then we need to back off, so depending on the value of the optional "OnPending" parameter choose what to do. **
                // Default (OnPending not present or set to "Accepted") is to return a 202 accepted with the location and Retry-After Header set to 5 seconds from now.
                // If OnPending is present and set to "Synchronous" then loop and keep retrying via an exponential backoff in the function until we time out.

                string rqs = $"http://{Environment.GetEnvironmentVariable("WEBSITE_HOSTNAME")}/api/RequestStatus/{thisGUID}";

                log.LogInformation($"Blob {thisGUID}.blob does not exist, still working ! result will be at {rqs}");


                switch (OnPending)
                {
                    case OnPendingEnum.Accepted:
                        {
                            // This SHOULD RETURN A 202 accepted 
                            return (ActionResult)new AcceptedResult() { Location = rqs };
                        }

                    case OnPendingEnum.Synchronous:
                        {
                            // This should back off and retry returning the data after a period of time timing out when the backoff period hits one minute

                            int backoff = 250;

                            while (!await inputBlob.ExistsAsync() && backoff < 64000)
                            {
                                log.LogInformation($"Synchronous mode {thisGUID}.blob - retrying in {backoff} ms");
                                backoff = backoff * 2;
                                await Task.Delay(backoff);

                            }

                            if (await inputBlob.ExistsAsync())
                            {
                                switch (OnComplete)
                                {
                                    case OnCompleteEnum.Redirect:
                                        {
                                            log.LogInformation($"Synchronous Redirect mode {thisGUID}.blob - completed after {backoff} ms");
                                            // Awesome, let's use the valet key pattern to offload the download via a SAS URI to blob storage
                                            return (ActionResult)new RedirectResult(inputBlob.GenerateSASURI());
                                        }

                                    case OnCompleteEnum.Stream:
                                        {
                                            log.LogInformation($"Synchronous Stream mode {thisGUID}.blob - completed after {backoff} ms");
                                            // If the action is set to return the file then lets download it and return it back
                                            // ToDo: this operation is horrible for larger files, we should use a stream to minimize RAM usage.
                                            return (ActionResult)new OkObjectResult(await inputBlob.DownloadTextAsync());
                                        }

                                    default:
                                        {
                                            throw new InvalidOperationException("How did we get here??");
                                        }
                                }
                            }
                            else
                            {
                                log.LogInformation($"Synchronous mode {thisGUID}.blob - NOT FOUND after timeout {backoff} ms");
                                return (ActionResult)new NotFoundResult();

                            }

                        }

                    default:
                        {
                            throw new InvalidOperationException("How did we get here??");
                        }
                }
            }
        }
    }

    public enum OnCompleteEnum {

        Redirect,
        Stream

    }

    public enum OnPendingEnum {

        Accepted,
        Synchronous

    }

}
