package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
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
	//
	// Your code here (Part I).
	//

	fmt.Printf("%s %d\n", "Reduce Task", reduceTask)

	mappedKeyValues := make(map[string][]string) //creating map of strings having value as slice of strings

	for mapTask := 0; mapTask < nMap; mapTask++ {
		nReduceIntermediateFileName := reduceName(jobName, mapTask, reduceTask)
		intermediateFile, fileOpenError := os.Open(nReduceIntermediateFileName) //opening intermediate file
		defer intermediateFile.Close()
		if fileOpenError != nil {
			fmt.Print(fileOpenError)
		}
		decoder := json.NewDecoder(intermediateFile) //adding decoder to the intermediate file

		var kV KeyValue
		for {
			decodingError := decoder.Decode(&kV)
			if decodingError != nil {
				break
			}
			mappedKeyValues[kV.Key] = append(mappedKeyValues[kV.Key], kV.Value)
		}
	}

	keys := make([]string, 0, len(mappedKeyValues))
	for key := range mappedKeyValues {
		keys = append(keys, key) //creating slice of keys
	}

	sort.Strings(keys)

	output, createFileError := os.Create(outFile) //creating output file
	if createFileError != nil {
		fmt.Println(createFileError)
	}
	defer output.Close()
	enc := json.NewEncoder(output) // adding encoder to the output file

	for _, key := range keys {
		enc.Encode(KeyValue{key, reduceF(key, mappedKeyValues[key])}) //adding keyValues
	}

}
