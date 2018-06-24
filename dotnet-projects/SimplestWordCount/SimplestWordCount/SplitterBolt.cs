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
    public class SplitterBolt : ISCPBolt
    {
        private Context ctx;

        public SplitterBolt(Context ctx)
        {
            this.ctx = ctx;

            // declare that a 1-tuple (a sentence) will be processed
            Dictionary<string, List<Type>> inputSchema = new Dictionary<string, List<Type>>();
            inputSchema.Add(Constants.DEFAULT_STREAM_ID, new List<Type>() { typeof(string) });

            // identify that a 1-tuple (a word) will be emitted
            Dictionary<string, List<Type>> outputSchema = new Dictionary<string, List<Type>>();
            outputSchema.Add(Constants.DEFAULT_STREAM_ID, new List<Type>() { typeof(string) });

            this.ctx.DeclareComponentSchema(new ComponentStreamSchema(inputSchema, outputSchema));
        }

        public static SplitterBolt Get(Context ctx, Dictionary<string, Object> parms)
        {
            return new SplitterBolt(ctx);
        }

        public void Execute(SCPTuple tuple)
        {
            Context.Logger.Info("Execute enter");

            string sentence = tuple.GetString(0); // grab the first (and only field) from the tuple
            // tokenize it into words
            foreach (string word in sentence.Split(' '))
            {
                // log each word and emit it into the output stream
                Context.Logger.Info("Emit: {0}", word);
                ctx.Emit(new Values(word));
            }

            Context.Logger.Info("Execute exit");
        }
    }
}