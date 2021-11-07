using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Authentication;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using CommandDotNet;
using Dapper;
using Dapper.Oracle;
using JSONExchange.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace JSONExchange
{
    [Command(Description = "RabbitMQ message receiver and transmitter service for RTSB exchange platform")]
    class RabbitTranCeiver
    {
        //connection settings
        static IConfigurationBuilder builder = new ConfigurationBuilder()
                       .SetBasePath(Directory.GetCurrentDirectory())
                       .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
        static IConfiguration config = builder.Build();

        static AutoResetEvent autoEvent = new AutoResetEvent(false);

        static RabbitMQSettings rmqSettings = new RabbitMQSettings();
        static ConnectionStrings connectionStrings = new ConnectionStrings();

        public RabbitTranCeiver()
        {
            config.GetSection("ConnectionStrings").Bind(connectionStrings);
            config.GetSection("RabbitMQSettings").Bind(rmqSettings);
        }

        [Command(Description = "data receiver from Rabbit")]
        public void Receive()
        {
            
            try
            {
                
                Connection conn = RabbitConnect();

                conn.model.BasicQos(0, 100, false);
                var consumer = new AsyncEventingBasicConsumer(conn.model);

                consumer.Received += async (eventModel, pars) =>
                {

                    var message = Encoding.UTF8.GetString(pars.Body.ToArray());

                    message = message.Replace("\n", " ");
                    message = message.Replace("\r", " ");
                    message = message.Replace("\t", " ");

                    string connString = connectionStrings.OracleDBConnection;
                    try
                    {                        
                        if (message.Length > 0)
                        {
                            int connection = 1;

                                var param = new DynamicParameters();
                                param.Add("@p_exchange", pars.Exchange);
                                param.Add("@p_queue", "");
                                param.Add("@p_key", pars.RoutingKey);
                                param.Add("@p_content", new OracleClobParameter(message));
                                param.Add("@p_out", direction: ParameterDirection.Output, dbType: DbType.Int32);

                            using (var dbConn = GetConnection(connString))
                            {

                                dbConn.Execute("exchange.rabbitmq_pkg.Set_Records", param, commandType: CommandType.StoredProcedure);
                                connection = param.Get<int>("@p_out");

                                Console.WriteLine("Message received from {0}:{1} \t{2}\t{3}", pars.Exchange, pars.RoutingKey, pars.DeliveryTag, DateTime.Now);
                                Console.WriteLine("Message length {0}", message.Length);
                                dbConn.Close();
                            }

                            if(connection == 0)
                            {
                                connString = connectionStrings.TestOracleDBConnection;
                                using (var dbConn = GetConnection(connString))
                                {
                                    dbConn.Execute("exchange.rabbitmq_pkg.Set_Records", param, commandType: CommandType.StoredProcedure);

                                    Console.WriteLine("Message received from {0}:{1} \t{2}\t{3}", pars.Exchange, pars.RoutingKey, pars.DeliveryTag, DateTime.Now);
                                    Console.WriteLine("Message length {0}", message.Length);
                                    dbConn.Close();
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message + " # " + pars.DeliveryTag);
                        Console.WriteLine("DB connection lost {0}", DateTime.Now);

                        using (var dbConn = GetConnection(connString))
                        {

                            var param = new DynamicParameters();

                            param.Add("@p_unique", "");
                            param.Add("@p_TableName", "json_input");
                            param.Add("@p_ProcedureName", "rabbitmq_pkg.set_records");
                            param.Add("@p_ProcessType", "INSERT");
                            param.Add("@p_Msg", e.Message + " # " + pars.DeliveryTag);
                            param.Add("@p_Content", new OracleClobParameter(message));


                            dbConn.Execute("exchange.errors_pkg.Add_Error", param, commandType: CommandType.StoredProcedure);
                            dbConn.Close();
                        }

                    }

                    conn.model.BasicAck(pars.DeliveryTag, false);

                    await Task.Yield();

                };

                foreach (var queue in RMQQueues.queues)
                {
                    conn.model.BasicConsume(queue: queue, autoAck: false, consumer);
                }

                Console.WriteLine("Rabbit is jumping...");
                Console.ReadLine();

                conn.model.Close();
                conn.connection.Abort();
                
            }

            catch (Exception e)
            {
                    Console.WriteLine(e.Message);
                    Console.WriteLine("Rabbit run out {0}", DateTime.Now);
            }

        }

        [Command(Description = "data sender from Exchange DB")]
        public void Send(string dbConnection) //[Option(LongName = "time", ShortName = "t", Description = "time delay in seconds for every DBQuery")]int timeDelay = 1
        {

            try
            {
                Connection conn = RabbitConnect();

                conn.model.ConfirmSelect();

                var records = new List<RMQMSG>();

                try
                {
                    RabbitPublisher(dbConnection, conn);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    Console.WriteLine("DB connection lost {0}", DateTime.Now);

                }

            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine("Rabbit run out {0}", DateTime.Now);
            }

        }

        private void RabbitPublisher(string connString, Connection conn){
            
            using (var dbConn = GetConnection(connString))
            {

                var dp = new OracleDynamicParameters();
                dp.Add("p_Result", dbType: OracleMappingType.RefCursor, direction: ParameterDirection.Output);

                var result = dbConn.Query<RMQMSG>("exchange.rabbitmq_pkg.Get_Records", dp, commandType: CommandType.StoredProcedure);

                IBasicProperties bp = conn.model.CreateBasicProperties();
                bp.ContentType = "application/json";
                bp.DeliveryMode = 2;

                foreach (var rec in result)
                {
                    try
                    {

                        Console.WriteLine("Sending : {0} to \t{1}:{2} \t{3}", rec.id.ToString(), rec.exchange, rec.routing_key, DateTime.Now);
                        byte[] messageBodyBytes = System.Text.Encoding.UTF8.GetBytes(rec.content);

                        conn.model.BasicPublish(exchange: rec.exchange, routingKey: rec.routing_key, bp, body: messageBodyBytes);

                        conn.model.WaitForConfirmsOrDie(TimeSpan.FromSeconds(10));


                        var param = new DynamicParameters();
                        param.Add("@p_Id", rec.id);
                        dbConn.Execute("exchange.rabbitmq_pkg.Set_Sent", param, commandType: CommandType.StoredProcedure);

                        Console.WriteLine("Sent sucessfully {0}\t{1}", rec.id.ToString(), DateTime.Now);

                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                        break;
                    }

                }
                dbConn.Close();
                conn.model.Close();
                conn.connection.Abort();
            }

        }

        [Command(Description = "data sender from and recieve data automatially")]
        public void Auto([Option(LongName = "time", ShortName = "t", Description = "time delay in seconds for every DBQuery")] int timeDelay = 1)
        {
            Task reciever = new Task(() => Receive());
            reciever.Start();

            while (true)
            {
                try
                {
                    //real
                    var result = 0;
                    var connection = connectionStrings.OracleDBConnection;
                    using (var dbConn = GetConnection(connection))
                    {
                        var dp = new OracleDynamicParameters();
                        dp.Add("p_Result", dbType: OracleMappingType.RefCursor, direction: ParameterDirection.Output);

                        result = dbConn.Query<int>("exchange.RabbitMQ_pkg.Output_count", dp, commandType: CommandType.StoredProcedure).FirstOrDefault();
                        dbConn.Close();

                        if (result > 0)
                        {
                            RunTask(connection);
                        }
                    }

                    //test
                    result = 0;
                    connection = connectionStrings.TestOracleDBConnection;
                    using (var dbConn = GetConnection(connection))
                    {
                        var dp = new OracleDynamicParameters();
                        dp.Add("p_Result", dbType: OracleMappingType.RefCursor, direction: ParameterDirection.Output);

                        result = dbConn.Query<int>("exchange.RabbitMQ_pkg.Output_count", dp, commandType: CommandType.StoredProcedure).FirstOrDefault();
                        dbConn.Close();
                        if (result > 0)
                        {
                            RunTask(connection);
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("DBConnectionLost");
                    Console.WriteLine(e.Message);
                }               

                Thread.Sleep(timeDelay * 1000);
            }
        }

        private void RunTask(string connection) {
            try
            {
                Task send = new Task(() => Send(connection));
                send.Start();
                send.Wait();
                send.Dispose();
            }
            catch (Exception e)
            {
                Console.WriteLine("Task error!");
                Console.WriteLine(e.Message);
            }
        }

        #region Tester coder is here
        //[Command(Description = "Test Rabbitmq receiver in msecs")]
        //private void TestSender([Option(LongName = "time", ShortName = "t", Description = "time delay")]int timeDelay)
        //{

        //    string[] exchange = { "broadcast", "common" };
        //    string[] bkeys = new string[] { "tpro.digest", "isugf.digest", "rqp.digest" };
        //    string[] ckeys = RMQQueueKeys.keys.ToArray();


        //    int i = 0;

        //    try
        //    {
        //        using (var model = RabbitConnect())
        //        {
        //            while (true)
        //            {
        //                try
        //                {


        //                    IBasicProperties bp = model.CreateBasicProperties();
        //                    bp.ContentType = "application/json";
        //                    //Dictionary<string, object> dict = new Dictionary<string, object>();
        //                    //dict.Add("guid", Guid.NewGuid().ToString());
        //                    //bp.Headers = dict;
        //                    bp.DeliveryMode = 2;
        //                    //bp.Expiration = "180000";

        //                    Random rnd = new Random();


        //                    int exIndex = rnd.Next(exchange.Length);
        //                    int keyIndex = rnd.Next(ckeys.Length);
        //                    int len = rnd.Next(1000, 8000);

        //                    string str = "{\"id\":" + i + ", \"Text\":\"" + RandomString(len) + "\"}";
        //                    //byte[] messageBodyBytes = System.Text.Encoding.UTF8.GetBytes(str);

        //                    //model.BasicPublish(exchange: "common", routingKey: ckeys[keyIndex], bp, body: messageBodyBytes);




        //                    List<OracleParameter> sParam = new List<OracleParameter>(){
        //                                new OracleParameter {
        //                                    ParameterName = "p_Result",
        //                                    OracleDbType = OracleDbType.RefCursor,
        //                                    Direction = ParameterDirection.Output
        //                                }
        //                            };


        //                    using (var dbConn = GetConnection())
        //                    {
        //                        var param = new DynamicParameters();
        //                        param.Add("@p_exchange", "common");
        //                        //param.Add("@p_queue", queue);
        //                        param.Add("@p_key", ckeys[keyIndex]);
        //                        param.Add("@p_content", str);

        //                        dbConn.Execute("exchange.rabbitmq_pkg.Test_Records", param, commandType: CommandType.StoredProcedure);
        //                    }




        //                    Console.WriteLine(str);

        //                }
        //                catch (Exception e)
        //                {

        //                    Console.WriteLine(e.Message);
        //                    Console.WriteLine("DB connection lost {0}", DateTime.Now);

        //                }
        //                i++;
        //                Thread.Sleep(timeDelay);
        //            }

        //        }
        //    }
        //    catch (Exception e)
        //    {

        //        Console.WriteLine(e.Message);
        //        Console.WriteLine("Rabbit run out {0}", DateTime.Now);
        //    }
        //}

        #endregion

        private static Connection RabbitConnect()
        {
            var factory = new ConnectionFactory
            {
                HostName = rmqSettings.host,
                Port = rmqSettings.port,
                UserName = rmqSettings.user,
                Password = rmqSettings.password,
                AutomaticRecoveryEnabled = true,
                DispatchConsumersAsync = true
            };

            Connection c = new Connection();
            c.connection = factory.CreateConnection();
            c.model = c.connection.CreateModel();

            return c;
        }

        private static OracleConnection GetConnection(string connection)
        {
            var conn = new OracleConnection(connection);
            if (conn.State == ConnectionState.Closed)
            {
                conn.Open();
            }
            return conn;
        }

        public static string RandomString(int length)
        {
            Random random = new Random();
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }

    }

    public class Connection
    {
        public IModel model { get; set; }
        public IConnection connection { get; set; }
    }

}

