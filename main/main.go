package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"gopkg.in/yaml.v3"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
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

type Gain struct {
	TagId       float32             `json:"tag_id"`
	BsAddr      float32             `json:"bs_addr"`
	RefreshRate float32             `json:"refresh_rate"`
	Timestamp   int64               `json:"timestamp"`
	TofSn       int                 `json:"tof_sn"`
	AllBsNum    int                 `json:"all_bs_num"`
	Rlist       map[string]RangeDTO `json:"rlist"`
}
type RangeDTO struct {
	BetweenAntenna int `json:"between_antenna"`
	Channel        int `json:"channel"`
	RangeBsAddr    int `json:"range_bs_addr"`
	RangeDist      int `json:"range_dist"`
	Rssi           int `json:"rssi"`
	TimeCalibrate  int `json:"time_calibrate"`
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
		fmt.Printf("您选择的序号为： '%v'\n", opt)
	}

	switch opt {
	case 1:
		var topic string
		fmt.Print("请输入订阅主题\n")
		if _, err := fmt.Scanf("%s\n", &topic); err != nil {
			fmt.Print("输入错误！！！！")
		} else {
			MqttSub(topic)
		}
	case 2:
		fmt.Println("请选择要发布的主题前缀：")
		var topicPrefix string
		if _, err := fmt.Scanf("%s\n", &topicPrefix); err != nil {
			fmt.Print("输入错误！！！！")
		} else {

			pwd, _ := os.Getwd()
			//获取文件或目录相关信息
			fileInfoList, err := os.ReadDir(pwd)
			if err != nil {
				log.Fatal(err)
			}
			var dataDir []string
			for i := range fileInfoList {
				if strings.Contains(fileInfoList[i].Name(), "mqtt-data") {
					dataDir = append(dataDir, fileInfoList[i].Name())
				}
			}
			// 交互式选择文件并读取内容
			fmt.Println("请选择要读取的目录：")
			for i, f := range dataDir {
				fmt.Printf("%d. %s\n", i+1, f)
			}
			var opt int
			fmt.Scan(&opt)
			if opt < 1 || opt > len(dataDir) {
				fmt.Println("输入的选项无效")
				os.Exit(1)
			}
			selectedDir := dataDir[opt-1]
			SwichFile(selectedDir, topicPrefix)
		}

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

func MqttSub(topic string) {
	// 订阅MQTT主题
	if token := MqttClient.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("收到消息：" + string(msg.Payload()) + "\n")
		WriteDate(string(msg.Payload()))
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

func SwichFile(dir, topicPrefix string) {
	swichDir := filepath.Join(".", dir)
	// 获取当前目录下的所有 txt 文件
	var txtFiles []string
	files, err := os.ReadDir(swichDir)
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		if !f.IsDir() && filepath.Ext(f.Name()) == ".txt" {
			txtFiles = append(txtFiles, f.Name())
		}
	}

	for index := 0; index < len(txtFiles); index++ {
		time.Sleep(time.Second)
		ReadDate(dir, txtFiles[index], topicPrefix)
		log.Println("=======================" + txtFiles[index] + "===========================")
		if index+1 >= len(txtFiles) {
			index = -1
			log.Println("####################### 即将重头开始 ###########################")
		}
	}

}

func WriteDate(data string) {
	dir := "mqtt-data-" + time.Now().Format("2006-01-02")
	os.Mkdir(dir, os.ModePerm)

	var gain Gain
	jsonErr := json.Unmarshal([]byte(data), &gain)
	if jsonErr != nil {
		panic(jsonErr)
	}

	writeFile := time.Unix(gain.Timestamp, 0).Format("2006-01-02-15-04-05")

	file, err := os.OpenFile("./"+dir+"/"+writeFile+".txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		fmt.Println("打开文件失败：", err)
		return
	}
	defer file.Close()

	if _, err := file.WriteString(data + "\n"); err != nil {
		panic(err)
	}
}

func ReadDate(fileDir, fileName, topicPrefix string) {
	pwd, _ := os.Getwd()
	file, err := os.Open(pwd + "/" + fileDir + "/" + fileName)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var gain Gain
		json.Unmarshal([]byte(scanner.Text()), &gain)
		gain.Timestamp = time.Now().Unix()
		marshal, _ := json.Marshal(gain)
		MqttPub(string(marshal), topicPrefix+"/"+fmt.Sprintf("%v", gain.TagId))
	}
	if err := scanner.Err(); err != nil {
		fmt.Println(err)
		return
	}
}
