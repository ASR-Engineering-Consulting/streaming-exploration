using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SCP;
using Microsoft.SCP.Topology;

namespace SimplestWordCount
{
    [Active(true)]
    class Program : TopologyDescriptor
    {
        public ITopologyBuilder GetTopologyBuilder()
        {
            // use TopologyBuilder to define topology and define each spout/bolt one by one
            TopologyBuilder topologyBuilder = new TopologyBuilder("SimplestWordCount" + DateTime.Now.ToString("yyyyMMddHHmmss"));

            topologyBuilder.SetSpout(
                "sentenceSpout",
                SentenceSpout.Get,
                new Dictionary<string, List<string>>()
                {
                    // name the 1-tuple's single field as 'sentence'
                    {Constants.DEFAULT_STREAM_ID, new List<string>(){"sentence"}}
                },
                1);  // initial nbr of instances of this spout (same for bolts below)

            topologyBuilder.SetBolt(
                "splitterBolt",
                SplitterBolt.Get,
                new Dictionary<string, List<string>>()
                {
                    // name the 1-tuple's single field as 'word'
                    {Constants.DEFAULT_STREAM_ID, new List<string>(){"word"}}
                },
                // wire it up to the stream from the sentence generator
                2).shuffleGrouping("sentenceSpout");

            topologyBuilder.SetBolt(
                "counterBolt",
                CounterBolt.Get,
                new Dictionary<string, List<string>>()
                {
                    // name the 2-tuple's fields as 'word' and 'count'
                    {Constants.DEFAULT_STREAM_ID, new List<string>(){"word", "count"}}
                },
                // wire it up to the stream from the sentence splitting bolt
                3).fieldsGrouping("splitterBolt", new List<int>() { 0 }); 
                   // ^^^^^ make sure every instance of specific word goes to same bolt instance

            return topologyBuilder;
        }
    }
}

