package main

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"net"
	"math"
	"bytes"
	"sort"
	"bufio"
	"io"
	"time"
	"fmt"
)

type ByKey [][][]byte

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return bytes.Compare((a[i])[0], (a[j])[0]) == -1 }


type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	//fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	SERVER_HOST := ""
	SERVER_PORT := ""
	//fmt.Println("Got the following server configs:", scs.Servers[0].Port)
	numserv := len(scs.Servers)
	chanlist := make([]chan []byte, numserv)
	
	//loop over servers
	for index, element := range scs.Servers {
		currchan := make(chan []byte)
		chanlist[index] = currchan
		if element.ServerId == serverId {
			SERVER_HOST = element.Host
			SERVER_PORT = element.Port
			//fmt.Println(SERVER_HOST+":"+SERVER_PORT)
			server, err := net.Listen("tcp", SERVER_HOST+":"+SERVER_PORT)
			defer server.Close()
			//fmt.Println("Listening on ", SERVER_PORT)
			if err != nil {
					//fmt.Println("Error listening:", err.Error())
					os.Exit(1)
			}
			go recieveData(server, currchan)
		} else {
			//printout :="Establishing SEND connection with:" + element.Host+":"+element.Port
			//fmt.Println(printout)
			go sendData(element.Host+":"+element.Port, currchan, serverId)
		}
		//else dial up and send my info
	}
	
	//loop over inp
	go loopOverInputsAndSend(chanlist, numserv)
	
	
	//recieving data
	//whenever chanlist[serverId] is updated, add to persistent data struct to be sorted later
	datastruct := make([][][]byte,0)
	completedStreams := 0
	for{
		data := <- chanlist[serverId]
		
		entry_id := data[1]
		//fmt.Println(entry_id)
		byte2int := int(entry_id >> (8 - uint(math.Log2(float64(4)))))
		b2jh := serverId
		if byte2int != b2jh {
			fmt.Println("|kkk", serverId, ". ", byte2int,"|kkk", data)
			
		}
		
		//entry_id := curr_kvp[0]
		//byte2int := uint(entry_id >> (8 - uint(math.Log2(float64(numserv)))))
			
			
		if data[0] == 0 {
		//fmt.Println("recieved eos signal")
		completedStreams = completedStreams + 1
		} else {
			chunk := data[1:101]
			key := chunk[:10]
			value := chunk[10:]
				
			datastruct = append(datastruct, [][]byte{key,value})
			
			//fmt.Println("recieved data", data)
		}
		if completedStreams == numserv {
			//fmt.Println("FINISHED COLLECTING DATA")
			break
		}
	}
	sort.Sort(ByKey(datastruct))
	out, out_err := os.Create(os.Args[3])
    check(out_err)
	defer out.Close()
	for chunk_pos := 0; chunk_pos < len(datastruct); chunk_pos=chunk_pos+1 {
		entry := datastruct[chunk_pos]
		out.Write(entry[0])
		out.Write(entry[1])
	}
	


	/*
		Implement Distributed Sort
	*/
}
func loopOverInputsAndSend(chanlist []chan []byte, numserv int){
	//loop over all data, send stuff to appropriate channels
	f, err := os.Open(os.Args[2])
	defer f.Close()
    check(err)
	reader := bufio.NewReader(f)
	chunksize := 1
	//key_val := make([]byte, 100)
	for {
		key_val_try := make([]byte, 100*chunksize)
		n, err := reader.Read(key_val_try)
		if err != nil {
			if err == io.EOF {
				break
			}
		}
		key_val := key_val_try[0:n]
		for chunk_pos := 0; chunk_pos < len(key_val); chunk_pos=chunk_pos+100 {
			curr_kvp := key_val[chunk_pos:chunk_pos+100]
			
			entry_id := curr_kvp[0]
			byte2int := uint(entry_id >> (8 - uint(math.Log2(float64(numserv)))))
			
			data_to_send := make([]byte, 101)
			data_to_send[0]=1
			copy(data_to_send[1:],curr_kvp[:])
			//out := "putting data into channel "+strconv.Itoa(int(byte2int)) 
			//fmt.Println(out)
			//for _, n := range(data_to_send) {
			//	fmt.Printf("% 08b", n) // prints 00000000 11111101
			//}
			chanlist[byte2int] <- data_to_send
			//time.Sleep(20 * time.Millisecond)
		}
		//send done message to all endpoints once finished reading input file
		//fmt.Println("/n DONEEEEEE done reading input file")
	}
	for _, element := range chanlist {
			data_to_send := make([]byte, 101)
			//data_to_send[0]=0
			element <- data_to_send
		}
}
func recieveData(server net.Listener,ch chan []byte){
	for {
                connection, err := server.Accept()
                if err != nil {
                        //fmt.Println("Error accepting: ", err.Error())
                        os.Exit(1)
                }
                //fmt.Println("client connected")
                go processClient(connection, ch)
        }

}
func processClient(connection net.Conn, ch chan []byte) {
		defer connection.Close()
        for{
			buffer := make([]byte, 101)
			mLen, err := connection.Read(buffer)
			if err != nil {
					//fmt.Println("Error reading:", err.Error())
			}
			//fmt.Println("Received in processClient: ", buffer[:mLen])
			ch <- buffer[:mLen]
			if buffer[0] == 0 {
				//fmt.Println("eos in processClient")
				break
			}
			_, err = connection.Write([]byte("Thanks! Got your message:" + string(buffer[:mLen])))
			
		}
}

func sendData(url string, ch chan []byte, id int){
time.Sleep(5000 * time.Millisecond)
	//out2 := "trying to connect to " + url
	//fmt.Println(out2)
    connection, err := net.Dial("tcp", url)
	if err != nil {
		time.Sleep(250 * time.Millisecond)
		go sendData(url, ch, id)
        return 
    }
	defer connection.Close()
	//out := "successfully established send connection with: " + url
	//fmt.Println(out)
    ///send some data
	//message := "greetings from " + strconv.Itoa(id) + " to " + url
    //_, err = connection.Write([]byte(message))
	for {
		cur := <- ch
		if (len(cur) != 101) {
			fmt.Println("len not 101")
		}
		entry_id := cur[1]
		//fmt.Println(entry_id)
		byte2int := int(entry_id >> (8 - uint(math.Log2(float64(4)))))
		b2jh, _ := strconv.Atoi(url[len(url)-1:])
		if byte2int != b2jh {
			fmt.Println("|",url, ". ", url[len(url)-1:], ". ", byte2int,"|")
			
		}
		//fmt.Println("sending from ", id, " to", url)
		//for _, n := range(cur) {
		//	fmt.Printf("% 08b", n) // prints 00000000 11111101
		//}
		if cur[0] == 0 {
			//fmt.Println("terminataing")
			_, err = connection.Write(cur)
			break
		}
		_, err = connection.Write(cur)
		//buffer := make([]byte, 1024)
		//mLen, err := connection.Read(buffer)
		//if err != nil {
		//	fmt.Println("Error reading ",url, " retrying send:", err.Error())
		//	ch <- cur
		//}
		//fmt.Println("Received in sendData: ", string(buffer[:mLen]))
	}
    
    	
}

func check(e error) {
    if e != nil {
        panic(e)
    }
}