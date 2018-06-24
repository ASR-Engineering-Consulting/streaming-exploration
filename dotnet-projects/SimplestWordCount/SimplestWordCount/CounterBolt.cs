using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using Microsoft.SCP;
using Microsoft.SCP.Rpc.Generated;

namespace SimplestWordCount
{
    public class CounterBolt : ISCPBolt
    {
        private Context ctx;

        // a map to hold the words and their running totals
        private Dictionary<string, int> counts = new Dictionary<string, int>();

        public CounterBolt(Context ctx)
        {
            this.ctx = ctx;

            // declare that a 1-tuple (a word) will be processed
            Dictionary<string, List<Type>> inputSchema = new Dictionary<string, List<Type>>();
            inputSchema.Add(Constants.DEFAULT_STREAM_ID, new List<Type>() { typeof(string) });

            // identify that a 2-tuple (a specific word and its running total) will be emitted
            Dictionary<string, List<Type>> outputSchema = new Dictionary<string, List<Type>>();
            outputSchema.Add(Constants.DEFAULT_STREAM_ID, new List<Type>() { typeof(string), typeof(int) });

            this.ctx.DeclareComponentSchema(new ComponentStreamSchema(inputSchema, outputSchema));
        }

        public static CounterBolt Get(Context ctx, Dictionary<string, Object> parms)
        {
            return new CounterBolt(ctx);
        }

        public void Execute(SCPTuple tuple)
        {
            Context.Logger.Info("Execute enter");

            string word = tuple.GetString(0); // grab the first (and only field) from the tuple

            // figure out if the word has already been encountered or not
            int count = counts.ContainsKey(word) ? counts[word] : 0;
            count++;                // update counter
            counts[word] = count;   // update the map

            // log the running count for the word and emit those two fields into the output stream
            Context.Logger.Info("Emit: {0}, count: {1}", word, count);
            this.ctx.Emit(Constants.DEFAULT_STREAM_ID, new List<SCPTuple> { tuple }, new Values(word, count));

            Context.Logger.Info("Execute exit");
        }
    }
}