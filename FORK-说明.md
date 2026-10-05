# Gadgetbridge + 小米手环 11 + 健康数据推送

这是 [Gadgetbridge](https://codeberg.org/Freeyourgadget/Gadgetbridge) 的一个 fork，加了两件事：

```
① 支持小米手环 11
② 把健康数据通过 HTTP 推送到你指定的地址
```

**完整的搭建步骤（含手机端 auth key 获取、电脑端接收、Tailscale 内网穿透）：
→ [wechat-ai-companion / 手环接入指南](https://github.com/YuiBai468/wechat-ai-companion/blob/main/%E6%89%8B%E7%8E%AF%E6%8E%A5%E5%85%A5%E6%8C%87%E5%8D%97.md)**

---

## 一、小米手环 11 支持

Gadgetbridge 识别设备**靠蓝牙广播名字的正则**，不是产品 ID：

```java
// MiBand10Coordinator
protected Pattern getSupportedDeviceName() {
    return Pattern.compile("^Xiaomi Smart Band 10 [0-9A-F]{4}$");
}
```

手环 11 广播的是 `Xiaomi Smart Band 11 5B38` —— 同一个格式。

所以只是继承 + 换正则（`MiBand11Coordinator.java`，20 行）：

```java
public class MiBand11Coordinator extends MiBand10Coordinator {
    @Override
    public int getDeviceNameResource() { return R.string.devicetype_miband11; }

    @Override
    protected Pattern getSupportedDeviceName() {
        return Pattern.compile("^Xiaomi Smart Band 11 [0-9A-F]{4}$");
    }
}
```

同时在 `DeviceType.java` 注册：

```java
MIBAND11(MiBand11Coordinator.class),
```

---

## 二、健康数据推送

### ⚠️ 首先，一个必须知道的坑

**上游故意删掉了联网权限：**

```xml
<!-- Make sure we don't get the INTERNET permission even when a dependency requires it -->
<uses-permission android:name="android.permission.INTERNET" tools:node="remove" />
```

Gadgetbridge 主打"数据不出手机"，所以把 INTERNET 权限**主动移除**了。

**后果**：推送代码完全正确，但一个字节也发不出去 —— 而且**静默失败**：

```
HttpURLConnection 抛异常
  → HealthPush 捕获异常（防止崩溃）
  → 两边日志干干净净
  → 你会以为是网络/防火墙问题，排查半天
```

这个 fork 把它改回来了。**自己 fork 加推送功能的话，这一行必须先改。**

### 推了什么

| 类别 | 内容 | 频率 |
|---|---|---|
| **实时心率** | 心率、卡路里、距离、步数 | 每 5~10 秒 |
| **整批样本** | 一次同步的全部活动样本（含压力、血氧） | 每 30 秒抓取 |
| **睡眠结构** | 总时长、深睡、浅睡、REM、清醒、入睡/醒来时刻 | 睡眠数据到达时 |
| **日汇总** | 静息/最高/平均心率、压力均值、血氧均值、训练负荷 | 每 30 秒 |
| **手动测量** | 体温、血氧、压力等 | 测量时 |
| **运动状态** | 运动类型、开始/结束、实时数据 | 状态变化时 |

### 怎么配

```
Gadgetbridge → 设置 → Health push (HTTP)
  Push readings over HTTP   →  打开
  Endpoint URL              →  http://<电脑IP>:8765/hr
  Token                     →  可选
  Minimum interval          →  5~10 秒
```

### 收到什么

```json
{"metric":"hr","value":78,"ts":1770000000}

{"metric":"samples","value":1440,
 "text":"[{\"t\":...,\"hr\":78,\"st\":35,\"sp\":97,\"step\":120,...},...]"}

{"metric":"sleep_summary","value":380,
 "text":"total=380,deep=65,light=230,rem=85,awake=20,bed=...,wake=..."}

{"metric":"daily_summary","value":1,
 "text":"ts=...,hrRest=58,hrMax=142,stressAvg=32,spo2Avg=97,loadDay=64,..."}

{"metric":"workout","value":1,"text":"started:RUNNING"}
```

---

## 三、实现要点（想自己改的话）

### 为什么需要定时抓取

手环**只在实时流里推心率和步数**。压力、血氧、睡眠都是"存在手环上、等 App 来取"。

而 Gadgetbridge 默认**只在解锁手机时**才取一次。要实时就得自己在设备服务里加定时器：

```java
onFetchRecordedData(RecordedDataTypes.TYPE_ACTIVITY
        | RecordedDataTypes.TYPE_STRESS
        | RecordedDataTypes.TYPE_SPO2
        | RecordedDataTypes.TYPE_SLEEP);
```

### 为什么整批发而不是逐条发

一次同步几百上千条样本。原来的写法只发最后一条：

```java
// ❌ 把一整天的数据扔掉
HealthPush.pushSample(list.get(list.size() - 1), false);
```

改成整批发，一天约 70KB，一次同步一发，完全可以接受。

### 为什么重放历史数据不能用墙上时钟

批量重放是**瞬间跑完**的。如果用 `System.currentTimeMillis()` 计算"心率高了多久"，永远得到 0 秒 —— 一段真实 5 分钟的心率飙升会被判定为"没持续"。

**必须用样本自己的时间戳。**

### 为什么睡眠时机要用摘要里的唤醒时刻

睡眠数据什么时候到取决于什么时候同步，可能是几小时后。不加判断会出现**下午三点她说"你昨晚睡得不好"**。

用摘要里的 `wakeupTime` 卡一个窗口（默认 2 小时）。

---

## 四、许可

**AGPL-3.0** —— 继承自上游 Gadgetbridge。本 fork 的所有改动同样以 AGPL-3.0 发布。

如果你要在自己的服务里用它，注意 AGPL 的**网络服务条款**：通过网络提供服务也算分发，需要提供源码。
