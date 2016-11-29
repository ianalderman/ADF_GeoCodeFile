using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GeoCodeCustomActivityNS
{
    [Serializable]
    class GeoCodeContext
    {
        public string ConnectionString { get; set; }
        public string FolderPath { get; set; }
        public string FileName { get; set; }
        public string OutputFolder { get; set; }
        public string MapsAPIKey { get; set; }
    }
}
