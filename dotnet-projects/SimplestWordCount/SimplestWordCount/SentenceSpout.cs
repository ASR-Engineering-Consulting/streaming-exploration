using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using Microsoft.SCP;
using Microsoft.SCP.Rpc.Generated;

namespace SimplestWordCount
{
    public class SentenceSpout : ISCPSpout
    {
        private Context ctx;
        private Random rand = new Random();
        string[] sentences = new string[] {
                                          "the cow jumped over the moon",
                                          "an apple a day keeps the doctor away",
                                          "four score and seven years ago",
                                          "snow white and the seven dwarfs",
                                          "i am at two with nature"};

        public SentenceSpout(Context ctx)
        {
            this.ctx = ctx;
            
            // identify that a 1-tuple of type string (a sentence) will be emitted
            Dictionary<string, List<Type>> outputSchema = new Dictionary<string, List<Type>>();
            outputSchema.Add(Constants.DEFAULT_STREAM_ID, new List<Type>() { typeof(string) });
            this.ctx.DeclareComponentSchema(new ComponentStreamSchema(null, outputSchema));
        }

        public static SentenceSpout Get(Context ctx, Dictionary<string, Object> parms)
        {
            return new SentenceSpout(ctx);
        }

        public void NextTuple(Dictionary<string, Object> parms)
        {
            Context.Logger.Info("NextTuple enter");

            // select one of the random sentences
            string sentence = sentences[rand.Next(0, sentences.Length - 1)];
            // log info message of selected sentence
            Context.Logger.Info("Emit: {0}", sentence);

            // raise this 1-tuple into the stream
            ctx.Emit(Constants.DEFAULT_STREAM_ID, new Values(sentence));

            Context.Logger.Info("NextTuple exit");
        }

        public void Ack(long seqId, Dictionary<string, Object> parms)
        {
            // nothing to do, never raising a message ID thus no reliable operations in play
        }

        public void Fail(long seqId, Dictionary<string, Object> parms)
        {
            // nothing to do, never raising a message ID thus no reliable operations in play
        }
    }
}