package mapreduce

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string,       // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string,       // write the output here
	nMap int,             // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()

	// Read corresponding files, parse key-value, push them to a map
	type values []string
	kv := make(map[string]values)
	for i := 0; i < nMap; i++ {
		temp, err := ioutil.ReadFile(reduceName(jobName, i, reduceTaskNumber))
		CheckErr(err)
		dec := json.NewDecoder(strings.NewReader(string(temp)))
		for {
			var m KeyValue
			if err := dec.Decode(&m); err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
			}
			kv[m.Key] = append(kv[m.Key], m.Value)
		}
	}

	// Read through each key, apply ReduceF into the []value and write to outFile
	out, err := os.Create(outFile)
	CheckErr(err)
	enc := json.NewEncoder(out)

	for key, values := range kv {
		err = enc.Encode(KeyValue{key, reduceF(key, values)})
		CheckErr(err)
	}
	out.Close()
}
