using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.Azure.Management.DataFactories.Runtime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Geocoding;
using CsvHelper;
using System.IO;

namespace GeoCodeCustomActivityNS
{
    class MyGeoCodeActivity : GeoCodeActivity<GeoCodeContext>
    {
        private struct LatLong
        {
            public double Lat;
            public double Long;
        }

        protected override GeoCodeContext PreExecute(IEnumerable<LinkedService> linkedServices,
            IEnumerable<Dataset> datasets, Activity activity, IActivityLogger logger)
        {
            // Process ADF artifacts up front as these objects are not serializable across app domain boundaries.
            Dataset dataset = datasets.First(ds => ds.Name == activity.Inputs.Single().Name);
            var blobProperties = (AzureBlobDataset)dataset.Properties.TypeProperties;
            LinkedService linkedService = linkedServices.First(ls => ls.Name == dataset.Properties.LinkedServiceName);
            var storageProperties = (AzureStorageLinkedService)linkedService.Properties.TypeProperties;

            // to get extended properties (for example: SliceStart)
            DotNetActivity dotNetActivity = (DotNetActivity)activity.TypeProperties;
           
            return new GeoCodeContext
            {
                ConnectionString = storageProperties.ConnectionString,
                FolderPath = blobProperties.FolderPath,
                FileName = blobProperties.FileName,
                OutputFolder = dotNetActivity.ExtendedProperties["OutputFolder"],
                MapsAPIKey = dotNetActivity.ExtendedProperties["MapsAPIKey"]
            };
        }

        public override IDictionary<string, string> Execute(GeoCodeContext context, IActivityLogger logger)
        {

            string connectionString = context.ConnectionString;

            logger.Write("Folder Path: {0}, FileName: {1}", context.FolderPath, context.FileName);

            string output = string.Empty; // for use later.

            CloudStorageAccount inputStorageAccount = CloudStorageAccount.Parse(context.ConnectionString);

            CloudBlobClient inputClient = inputStorageAccount.CreateCloudBlobClient();
            CloudBlobContainer inputContainer = inputClient.GetContainerReference(context.FolderPath);
            CloudBlockBlob blob = inputContainer.GetBlockBlobReference(context.FileName);

            logger.Write("Entering Calculate for blob {0}", blob.Uri.ToString());

            output = Calculate(blob, logger, context);

            logger.Write("Calculate complete");

            string datepart = DateTime.UtcNow.ToString().Substring(0, 10);
            string[] dateparts = datepart.Split('/');

            CloudBlobContainer outputContainer = inputClient.GetContainerReference(context.OutputFolder);
            CloudBlockBlob outputBlob = outputContainer.GetBlockBlobReference(dateparts[2] + "/" + dateparts[0] + "/" + dateparts[1] + "/" + context.FileName);
           
            logger.Write("Writing to the output blob: {0}", outputBlob.Uri);
            outputBlob.UploadText(output);
            logger.Write("Blob uploaded");

            return new Dictionary<string, string>();
        }

        private static string Calculate(CloudBlockBlob inputBlob, IActivityLogger logger, GeoCodeContext context)
        {
            string output = string.Empty;

            if ((inputBlob != null) && (inputBlob.Name.IndexOf("$$$.$$$") == -1))
            {
                string blobText = inputBlob.DownloadText();
                using (var stream = GenerateStreamFromString(blobText))
                {
                    using (CsvReader csv = new CsvReader(new StreamReader(stream), true))
                    {
                        while (csv.Read())
                        {
                            LatLong geocoderesult = GeoCodePostCode(csv.GetField(0) + " " + csv.GetField(1) + " " + csv.GetField(2) + " " + csv.GetField(3) + " " + csv.GetField(4), context.MapsAPIKey);
                            output += string.Format("{0}, {1}, {2}, {3}, {4}, {5}, {6}\r\n", csv.GetField(0), csv.GetField(1), csv.GetField(2), csv.GetField(3), csv.GetField(4), geocoderesult.Lat, geocoderesult.Long);
                        }
                    }
                }
            }
            return output;
        }


        public static MemoryStream GenerateStreamFromString(string value)
        {
            return new MemoryStream(Encoding.UTF8.GetBytes(value ?? ""));
        }

        private static LatLong GeoCodePostCode(string AddressToParse, string APIKey)
        {
            Geocoding.Microsoft.BingMapsGeocoder geocoder = new Geocoding.Microsoft.BingMapsGeocoder(APIKey);
            IEnumerable<Geocoding.Microsoft.BingAddress> addresses = geocoder.Geocode(AddressToParse);
            LatLong result = new LatLong();
            if (addresses != null && addresses.Any()) { 
                result.Lat = addresses.First().Coordinates.Latitude;
                result.Long = addresses.First().Coordinates.Longitude;
            } else
            {
                result.Lat = 0;
                result.Long = 0;
            }
            return result;
        }
    }
}
