using System;
using System.Collections.Generic;
using System.Text;

namespace JSONExchange.Models
{
    class RabbitMQSettings
    {
        public string host { get; set; }
        public int port { get; set; }
        public string user { get; set; }
        public string password { get; set; }
    }

    class RMQQueues
    {
        public static List<string> queues
        {
            get
            {
                List<string> value = new List<string>();

                value.Add("uzrtsb_in");
                value.Add("uzrtsb_digest");

                //value.Add("isugf_in");
                //value.Add("isugf_digest");

                //value.Add("rqp_in");
                //value.Add("rqp_digest");

                return value;
            }
        }
    }

    //class RMQQueueKeys
    //{
    //    public static List<string> keys
    //    {
    //        get
    //        {
    //            List<string> value = new List<string>();
    //            value.Add("tpro.request");
    //            value.Add("tpro.response");
    //            value.Add("tpro.digest");
    //            value.Add("isugf.request");
    //            value.Add("isugf.response");
    //            value.Add("isugf.digest");
    //            value.Add("rqp.request");
    //            value.Add("rqp.response");
    //            value.Add("rqp.digest");

    //            return value;
    //        }
    //    }
    //}

    class RMQMSG
    {
        public int id { get; set; }
        public string exchange { get; set; }
        public string queue { get; set; }
        public string routing_key { get; set; }
        public string content { get; set; }
    }

}
