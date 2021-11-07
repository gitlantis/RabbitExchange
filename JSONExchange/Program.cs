using System;
using System.Data;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JSONExchange.Models;
using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Oracle.ManagedDataAccess.Client;
using Dapper;
using System.Collections.Generic;
using System.Linq;
using CommandDotNet;

namespace JSONExchange
{

    class Program
    {
        static int Main(string[] args)
        {
            return new AppRunner<RabbitTranCeiver>().Run(args);
        }
    }
}
