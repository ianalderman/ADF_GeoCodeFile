using Microsoft.Azure.Management.DataFactories.Runtime;
using System.Collections.Generic;


namespace GeoCodeCustomActivityNS
{
    interface IGeoCodeActivity<TExecutionContext>
    {
        IDictionary<string, string> Execute(TExecutionContext context, IActivityLogger logger);
    }
}
