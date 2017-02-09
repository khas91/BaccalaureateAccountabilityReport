using Microsoft.SqlServer.Server;
using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.Text;

namespace Rand_Select
{
    [Serializable]
    [SqlUserDefinedAggregate(Format.UserDefined,
    IsNullIfEmpty = true,
    Name = "RAND_SELECT",
    IsInvariantToDuplicates = true,
    IsInvariantToOrder = true,
    IsInvariantToNulls = true,
    MaxByteSize = -1)]
    public class Rand_Select : IBinarySerialize
    {
        private List<string> values;

        public void Init()
        {
            values = new List<string>();
        }

        public void Accumulate(SqlString str)
        {
            if (!str.IsNull)
            {
                this.values.Add(str.Value);
            }
        }

        public void Merge(Rand_Select other)
        {
            this.values.AddRange(other.values);
        }

        public SqlString Terminate()
        {

            if (values.Count == 0)
            {
                return SqlString.Null;
            }

            Random rand = new Random();
            int index = rand.Next(0, values.Count);

            return values[index];
        }

        public void Read(BinaryReader r)
        {
            Init();

            int numValues = r.ReadInt32();

            for (int i = 0; i < numValues; i++)
            {
                values.Add(r.ReadString());
            }
        }

        public void Write(BinaryWriter w)
        {
            w.Write(values.Count);

            foreach (String value in values)
            {
                w.Write(value);
            }
        }
    }
}
