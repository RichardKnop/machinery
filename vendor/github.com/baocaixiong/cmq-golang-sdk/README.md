# 腾讯CMQ Golang版本SDK

### 单独使用

```go
opt := &cmq.Options{
    Credential: &cmq.Credential{

    }
}

client := cmq.NewClient(
    cmq.Region("bj"),
    cmq.NetEnv("wan"),
    cmq.SetCredential(&cmq.Credential{
        SecretId:  "",
        SecretKey: "",
    })

input := models.NewSendMessageReq("queue name", ""msg body")
output := models.NewSendMessageResp()
err := client.Send(input, output)
if err != nil {
  log.Error(err)
}

fmt.Println(output)
```

### 配合machinery使用

[baocaixiong/machinery](https://github.com/baocaixiong/machinery)
