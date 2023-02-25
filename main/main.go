package main

import (
	"bufio"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"gopkg.in/yaml.v3"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

var ConfigYML Config
var MqttClient mqtt.Client

type Config struct {
	Mqtt struct {
		Host     string `yaml:"host"`
		Port     int16  `yaml:"port"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
	}
}

func init() {
	ReadConfigYaml()
	ConnectMqttServer()
}

func main() {
	fmt.Printf("Config: %+v\n", ConfigYML)

	fmt.Print("请输入操作\n" +
		"1 ==> 订阅 \n" +
		"2 ==> 发布 \n")
	var opt int
	if _, err := fmt.Scanf("%d\n", &opt); err != nil {
		fmt.Print("error")
	} else {
		fmt.Printf("fmt.Scanf scanned '%v'\n", opt)
	}

	switch opt {
	case 1:
		var topic, tag string
		fmt.Print("请输入订阅主题 TAG\n")
		if _, err := fmt.Scanf("%s %s\n", &topic, &tag); err != nil {
			fmt.Print("error")
		} else {
			if token := MqttClient.Subscribe(topic+"/"+tag, 0, func(client mqtt.Client, msg mqtt.Message) {
				fmt.Printf("收到消息：" + string(msg.Payload()) + "\n")
				WriteDate(string(msg.Payload()))
			}); token.Wait() && token.Error() != nil {
				fmt.Println(token.Error())
				os.Exit(1)
			}
		}
	case 2:
		fmt.Println()
		ReadDate()
	default:
		fmt.Println("error")
	}

	WaitSignal()
}

func ReadConfigYaml() {
	file, err := os.ReadFile("config.yaml")
	if err != nil {
		panic(err)
	}

	err = yaml.Unmarshal(file, &ConfigYML)

	if err != nil {
		panic(err)
	}

	fmt.Println("配置文件加载成功")
}

func ConnectMqttServer() {
	options := mqtt.NewClientOptions()
	options.AddBroker("tcp://" + ConfigYML.Mqtt.Host + ":" + strconv.Itoa(int(ConfigYML.Mqtt.Port))).SetAutoReconnect(true).SetUsername(ConfigYML.Mqtt.Username).SetPassword(ConfigYML.Mqtt.Password)

	// 创建MQTT客户端
	MqttClient = mqtt.NewClient(options)
	if token := MqttClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	fmt.Println("mqtt 连接成功")

}

func MqttSub(topic string, tag string) {
	// 订阅MQTT主题
	if token := MqttClient.Subscribe(topic+"/"+tag, 0, func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
	}); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}
}

func MqttPub(data string, topic string) {
	token := MqttClient.Publish(topic, 0, false, data)
	token.WaitTimeout(5 * time.Second)
	if token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}
}

func WaitSignal() {
	//等待信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	MqttClient.Disconnect(250)
}

func WriteDate(data string) {
	file, err := os.OpenFile("data-"+time.Now().Format("2006-01-02")+".txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("打开文件失败：", err)
		return
	}
	defer file.Close()

	if _, err := file.WriteString(data + "\n"); err != nil {
		panic(err)
	}
}

func ReadDate() {
	file, err := os.Open("data-2023-02-25.txt")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fmt.Println(scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		fmt.Println(err)
		return
	}
}
