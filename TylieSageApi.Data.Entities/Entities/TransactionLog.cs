﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TylieSageApi.Common;

namespace TylieSageApi.Data.Entities.Entities
{
    public class TransactionLog
    {
        public Guid TransitID { get; set; }
        public DateTime TimeStamp { get; set; }
        public int EventType { get; set; }
        public string EventDetails { get; set; }

        public TransactionLog()
        {
        }

        public TransactionLog(Guid transitID,
                            EventType eventType,
                            string eventDetails)
        {
            TransitID = transitID;
            TimeStamp = DateTime.UtcNow;
            EventType = (int)eventType;
            EventDetails = eventDetails;
        }

        public TransactionLog(string transitID,
            EventType eventType,
            string eventDetails) :
            this(Guid.Parse(transitID),
            eventType,
            eventDetails)
        {
        }
    }
}
