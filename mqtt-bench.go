package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const BASE_TOPIC string = "/mqtt-bench/benchmark"

var Debug bool = false
var mutex = &sync.Mutex{}

// 对于 Apollo，在订阅时保存 DefaultHandler 的处理结果。
var DefaultHandlerResults []*SubscribeResult

// 执行选项
type ExecOptions struct {
	Broker            string     // Broker URI
	Qos               byte       // QoS(0|1|2)
	Retain            bool       // Retain
	Topic             string     // Topic 路径
	Username          string     // 用户名
	Password          string     // 密码
	CertConfig        CertConfig // 证书定义
	ClientNum         int        // 并发客户端数
	Count             int        // 每个客户端的消息数
	MessageSize       int        // 一条消息的大小（字节）
	UseDefaultHandler bool       // 是否使用默认 MessageHandler 代替单独 Subscriber
	PreTime           int        // 执行之前的等待时间（毫秒）
	IntervalTime      int        // 每个消息的执行间隔时间（毫秒）
}

// 验证设置
type CertConfig interface{}

// 服务器身份验证设置
type ServerCertConfig struct {
	CertConfig
	ServerCertFile string // 服务器证书文件
}

// 客户端身份验证设置
type ClientCertConfig struct {
	CertConfig
	RootCAFile     string // 根证书文件
	ClientCertFile string // 客户端证书文件
	ClientKeyFile  string // 客户端公钥文件
}

// 为服务器证书生成TLS设置。
//   serverCertFile : 服务器证书文件
func CreateServerTlsConfig(serverCertFile string) *tls.Config {
	certpool := x509.NewCertPool()
	pem, err := ioutil.ReadFile(serverCertFile)
	if err == nil {
		certpool.AppendCertsFromPEM(pem)
	}

	return &tls.Config{
		RootCAs: certpool,
	}
}

// 为客户端证书生成 TLS 设置。
//   rootCAFile     : 根证书文件
//   clientCertFile : 客户端证书文件
//   clientKeyFile  : 客户端公钥文件
func CreateClientTlsConfig(rootCAFile string, clientCertFile string, clientKeyFile string) *tls.Config {
	certpool := x509.NewCertPool()
	rootCA, err := ioutil.ReadFile(rootCAFile)
	if err == nil {
		certpool.AppendCertsFromPEM(rootCA)
	}

	cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		panic(err)
	}
	cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		panic(err)
	}

	return &tls.Config{
		RootCAs:            certpool,
		ClientAuth:         tls.NoClientCert,
		ClientCAs:          nil,
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{cert},
	}
}

// 执行。
func Execute(exec func(clients []MQTT.Client, opts ExecOptions, param ...string) int, opts ExecOptions) {
	message := CreateFixedSizeMessage(opts.MessageSize)

	// 初始化数组
	DefaultHandlerResults = make([]*SubscribeResult, opts.ClientNum)

	clients := make([]MQTT.Client, opts.ClientNum)
	hasErr := false
	err_count := 0
	for i := 0; i < opts.ClientNum; i++ {
		client := Connect(i, opts)
		if client == nil {
			err_count += 1
			//hasErr = true
			//break
			// 持续打印 当前连接数 及 其中失败次数
			fmt.Printf("\n  %5d FAIL %3d\n", i, err_count)
			if err_count > 100 { // 累计 100 次错误后, 放弃 后续的连接尝试 (但不中止程序, 批量测试用)
				break
			}
		} else {
			//err_count -= 1
			fmt.Printf("\r  %5d OK", i)
		}
		clients[i] = client
	}
	fmt.Printf("\n  conn finish\n")

	// 如果存在连接错误，请断开连接的客户端的连接并结束该过程。
	if hasErr {
		for i := 0; i < len(clients); i++ {
			client := clients[i]
			if client != nil {
				Disconnect(client)
			}
		}
		return
	}

	// 等待一段时间以稳定下来。
	time.Sleep(time.Duration(opts.PreTime) * time.Millisecond)

	fmt.Printf("%s Start benchmark\n", time.Now())

	startTime := time.Now()
	totalCount := exec(clients, opts, message)
	endTime := time.Now()

	fmt.Printf("%s End benchmark\n", time.Now())

	// 由于断开连接需要时间，因此请异步处理。
	AsyncDisconnect(clients)

	// 输出处理结果。
	duration := (endTime.Sub(startTime)).Nanoseconds() / int64(1000000) // nanosecond -> millisecond
	throughput := float64(totalCount) / float64(duration) * 1000        // messages/sec
	fmt.Printf("\nResult : broker=%s, clients=%d, totalCount=%d, duration=%dms, throughput=%.2fmessages/sec\n",
		opts.Broker, opts.ClientNum, totalCount, duration, throughput)
}

// 对所有客户端执行发布处理。
// 返回发送的消息数（原则上是 客户端数 x 循环次数）。
func PublishAllClient(clients []MQTT.Client, opts ExecOptions, param ...string) int {
	message := param[0]

	wg := new(sync.WaitGroup)

	totalCount := 0
	for id := 0; id < len(clients); id++ { // 循环处理 所有 client
		wg.Add(1)

		client := clients[id]

		go func(clientId int) { // 各 client 在 go routine 里 并行处理
			defer wg.Done()

			for index := 0; index < opts.Count; index++ { // 发送 count 个消息 到 %d 主题
				topic := fmt.Sprintf(opts.Topic+"/%d", clientId)

				if Debug {
					fmt.Printf("Publish : id=%d, count=%d, topic=%s\n", clientId, index, topic)
				}
				Publish(client, topic, opts.Qos, opts.Retain, message)
				mutex.Lock()
				totalCount++
				mutex.Unlock()

				if opts.IntervalTime > 0 {
					time.Sleep(time.Duration(opts.IntervalTime) * time.Millisecond)
				}
			}
		}(id)
	}

	wg.Wait()

	return totalCount
}

// 发送消息。
func Publish(client MQTT.Client, topic string, qos byte, retain bool, message string) {
	token := client.Publish(topic, qos, retain, message)

	if token.Wait() && token.Error() != nil {
		fmt.Printf("Publish error: %s\n", token.Error())
	}
}

// 为所有客户端执行订阅过程。
// 等待指定的计数次数以接收消息（如果无法获取消息，则不会计数）。
// 在此过程中，在继续发布的同时执行“订阅”过程。
func SubscribeAllClient(clients []MQTT.Client, opts ExecOptions, param ...string) int {
	wg := new(sync.WaitGroup)

	results := make([]*SubscribeResult, len(clients))
	for id := 0; id < len(clients); id++ { // 所有 client 订阅 各自对应的 topic
		wg.Add(1)

		client := clients[id]
		topic := fmt.Sprintf(opts.Topic+"/%d", id)

		results[id] = Subscribe(client, topic, opts.Qos)

		// 使用 DefaultHandler 时，而不是用于订阅的单个Handler
		// 参考 DefaultHandler 的处理结果。
		if opts.UseDefaultHandler == true {
			results[id] = DefaultHandlerResults[id]
		}

		go func(clientId int) { // go routine 等待接收消息 达到数量
			defer wg.Done()

			var loop int = 0
			for results[clientId].Count < opts.Count { // 各 client 等待自己的 接收消息数
				loop++

				if Debug {
					fmt.Printf("Subscribe : id=%d, count=%d, topic=%s\n", clientId, results[clientId].Count, topic)
				}

				if opts.IntervalTime > 0 {
					time.Sleep(time.Duration(opts.IntervalTime) * time.Millisecond)
				} else {
					// 等待至少1000纳秒（0.001毫秒）以减少for语句的负载。
					time.Sleep(1000 * time.Nanosecond)
				}

				// 为了避免无限循环，在指定的Count达到100次时以错误终止。
				if loop >= opts.Count*100 {
					panic("Subscribe error : Not finished in the max count. It may not be received the message.")
				}
			}
		}(id)
	}

	wg.Wait()

	// 计算收到的消息数
	totalCount := 0
	for id := 0; id < len(results); id++ {
		totalCount += results[id].Count
	}

	return totalCount
}

// 订阅的处理结果
type SubscribeResult struct {
	Count int // 收到的消息数
}

// 接收消息。
//   返回 此 client 接收消息数 (的变量地址, 会在运行中 持续更新)
func Subscribe(client MQTT.Client, topic string, qos byte) *SubscribeResult {
	var result *SubscribeResult = &SubscribeResult{} // 给此 client 新建了 消息数变量
	result.Count = 0

	var handler MQTT.MessageHandler = func(client MQTT.Client, msg MQTT.Message) {
		result.Count++
		if Debug {
			fmt.Printf("Received message : topic=%s, message=%s\n", msg.Topic(), msg.Payload())
		}
	}

	token := client.Subscribe(topic, qos, handler)

	if token.Wait() && token.Error() != nil {
		fmt.Printf("Subscribe error: %s\n", token.Error())
	}

	return result
}

// 生成固定大小的消息。
func CreateFixedSizeMessage(size int) string {
	var buffer bytes.Buffer
	for i := 0; i < size; i++ {
		buffer.WriteString(strconv.Itoa(i % 10))
	}

	message := buffer.String()
	return message
}

// 连接到指定的 Broker 并返回其 MQTT 客户端。
// 如果连接失败，则返回 nil。
func Connect(id int, execOpts ExecOptions) MQTT.Client {

	// 如果ClientID在多个进程中重复，则在Broker端会成为问题，
	// 使用进程ID并分配ID。
	// mqttbench <进程ID的十六进制值>-<客户端的序号>
	pid := strconv.FormatInt(int64(os.Getpid()), 16)
	clientId := fmt.Sprintf("mqttbench%s-%d", pid, id)

	opts := MQTT.NewClientOptions()
	opts.AddBroker(execOpts.Broker)
	opts.SetClientID(clientId)

	if execOpts.Username != "" {
		opts.SetUsername(execOpts.Username)
	}
	if execOpts.Password != "" {
		opts.SetPassword(execOpts.Password)
	}

	// TLS设置
	certConfig := execOpts.CertConfig
	switch c := certConfig.(type) {
	case ServerCertConfig:
		tlsConfig := CreateServerTlsConfig(c.ServerCertFile)
		opts.SetTLSConfig(tlsConfig)
	case ClientCertConfig:
		tlsConfig := CreateClientTlsConfig(c.RootCAFile, c.ClientCertFile, c.ClientKeyFile)
		opts.SetTLSConfig(tlsConfig)
	default:
		// do nothing.
	}

	if execOpts.UseDefaultHandler == true {
		// 如果是 Apollo（使用1.7.1），除非指定了 DefaultPublishHandler，否则无法完成订阅。
		// 但是，请注意，即使指定了保留的消息，第一次也只会提取一次，在第二次访问之后将为空。
		var result *SubscribeResult = &SubscribeResult{}
		result.Count = 0

		var handler MQTT.MessageHandler = func(client MQTT.Client, msg MQTT.Message) { // 此 client 自己的 回调函数
			result.Count++
			if Debug {
				fmt.Printf("Received at defaultHandler : topic=%s, message=%s\n", msg.Topic(), msg.Payload())
			}
		}
		opts.SetDefaultPublishHandler(handler)

		DefaultHandlerResults[id] = result // 收集/记录 此 client 收到的消息数
	}

	client := MQTT.NewClient(opts)
	token := client.Connect()

	if token.Wait() && token.Error() != nil {
		fmt.Printf("Connected error: %s\n", token.Error())
		return nil
	}

	return client
}

// 异步断开与 Broker 的连接。
func AsyncDisconnect(clients []MQTT.Client) {
	wg := new(sync.WaitGroup)

	for _, client := range clients {
		wg.Add(1)
		go func(c MQTT.Client) {
			defer wg.Done()
			Disconnect(c)
		}(client)
	}

	wg.Wait()
}

// 与 Broker 断开连接。
func Disconnect(client MQTT.Client) {
	client.Disconnect(10)
}

// 检查文件是否存在。
// 如果文件存在，则返回 true，否则返回 false。
//   filePath：要检查是否存在的文件路径
func FileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return err == nil
}

func main() {
	broker := flag.String("broker", "tcp://{host}:{port}", "URI of MQTT broker (required)")
	action := flag.String("action", "p|pub or s|sub", "Publish or Subscribe or Subscribe(with publishing) (required)")
	qos := flag.Int("qos", 0, "MQTT QoS(0|1|2)")
	retain := flag.Bool("retain", false, "MQTT Retain")
	topic := flag.String("topic", BASE_TOPIC, "Base topic")
	username := flag.String("broker-username", "", "Username for connecting to the MQTT broker")
	password := flag.String("broker-password", "", "Password for connecting to the MQTT broker")
	tls := flag.String("tls", "", "TLS mode. 'server:certFile' or 'client:rootCAFile,clientCertFile,clientKeyFile'")
	clients := flag.Int("clients", 10, "Number of clients")
	count := flag.Int("count", 100, "Number of loops per client")
	size := flag.Int("size", 1024, "Message size per publish (byte)")
	useDefaultHandler := flag.Bool("support-unknown-received", false, "Using default messageHandler for a message that does not match any known subscriptions")
	preTime := flag.Int("pretime", 3000, "Pre wait time (ms)")
	intervalTime := flag.Int("intervaltime", 0, "Interval time per message (ms)")
	debug := flag.Bool("x", false, "Debug mode")

	flag.Parse()

	if len(os.Args) <= 1 {
		flag.Usage()
		return
	}

	// validate "broker"
	if broker == nil || *broker == "" || *broker == "tcp://{host}:{port}" {
		fmt.Printf("Invalid argument : -broker -> %s\n", *broker)
		return
	}

	// validate "action"
	var method string = ""
	if *action == "p" || *action == "pub" {
		method = "pub"
	} else if *action == "s" || *action == "sub" {
		method = "sub"
	}

	if method != "pub" && method != "sub" {
		fmt.Printf("Invalid argument : -action -> %s\n", *action)
		return
	}

	// parse TLS mode
	var certConfig CertConfig = nil
	if *tls == "" {
		// nil
	} else if strings.HasPrefix(*tls, "server:") {
		var strArray = strings.Split(*tls, "server:")
		serverCertFile := strings.TrimSpace(strArray[1])
		if FileExists(serverCertFile) == false {
			fmt.Printf("File is not found. : certFile -> %s\n", serverCertFile)
			return
		}

		certConfig = ServerCertConfig{
			ServerCertFile: serverCertFile}
	} else if strings.HasPrefix(*tls, "client:") {
		var strArray = strings.Split(*tls, "client:")
		var configArray = strings.Split(strArray[1], ",")
		rootCAFile := strings.TrimSpace(configArray[0])
		clientCertFile := strings.TrimSpace(configArray[1])
		clientKeyFile := strings.TrimSpace(configArray[2])
		if FileExists(rootCAFile) == false {
			fmt.Printf("File is not found. : rootCAFile -> %s\n", rootCAFile)
			return
		}
		if FileExists(clientCertFile) == false {
			fmt.Printf("File is not found. : clientCertFile -> %s\n", clientCertFile)
			return
		}
		if FileExists(clientKeyFile) == false {
			fmt.Printf("File is not found. : clientKeyFile -> %s\n", clientKeyFile)
			return
		}

		certConfig = ClientCertConfig{
			RootCAFile:     rootCAFile,
			ClientCertFile: clientCertFile,
			ClientKeyFile:  clientKeyFile}
	} else {
		// nil
	}

	execOpts := ExecOptions{}
	execOpts.Broker = *broker
	execOpts.Qos = byte(*qos)
	execOpts.Retain = *retain
	execOpts.Topic = *topic
	execOpts.Username = *username
	execOpts.Password = *password
	execOpts.CertConfig = certConfig
	execOpts.ClientNum = *clients
	execOpts.Count = *count
	execOpts.MessageSize = *size
	execOpts.UseDefaultHandler = *useDefaultHandler
	execOpts.PreTime = *preTime
	execOpts.IntervalTime = *intervalTime

	Debug = *debug

	switch method {
	case "pub":
		Execute(PublishAllClient, execOpts)
	case "sub":
		Execute(SubscribeAllClient, execOpts)
	}
}
