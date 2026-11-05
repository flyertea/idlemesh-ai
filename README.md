# IdleMesh AI

把多台 Windows、macOS 和 Linux 电脑组成一个文本生成任务池。每台电脑用本机 CPU 和内存运行量化模型，通过网络领取任务。增加节点可以同时处理更多独立请求，执行中的节点离线后，其他持有相同模型的节点可以接手重算。

面向可信局域网或私有 VPN。Worker 主动连接协调器，无需开放 Worker 入站端口。单协调器可以快速开始，三协调器集群可以在其中一台掉线后继续处理已提交的任务。Worker 掉线时，其他拥有同模型的节点可以在租约到期后重算。单个模型需要装入单台电脑的可用内存。 [架构与实施路线](docs/architecture.md)

## 下载

下载并解压与操作系统、处理器对应的 ZIP。`amd64` 适用于 Intel 和 AMD 的 64 位处理器，`arm64` 适用于 Apple 芯片及其他 64 位 ARM 处理器。压缩包名例如 `idlemesh-v0.3.0-darwin-arm64.zip`。可以用发布页的 `SHA256SUMS` 核对下载。 [GitHub Releases](https://github.com/flyertea/idlemesh-ai/releases)

## 五分钟跑通

回显模式无需安装模型，适合先验证连接和任务调度。这个演练用 `--always-claim` 跳过资源检查，在电池供电的笔记本上也能跑通。以下三个步骤分别在三个终端运行，都从解压目录开始。

**终端 1 · 启动协调器**

```sh
./idlemesh coordinator
```

**终端 2 · 启动一个 Worker**

```sh
./idlemesh worker --id worker-a --engine echo --always-claim
```

**终端 3 · 提交并等待结果**

```sh
job=$(./idlemesh submit --engine echo --prompt '你好，IdleMesh！')
./idlemesh status "$job"
./idlemesh wait "$job"
```

在 Windows PowerShell 中，前两个终端分别运行 `.\idlemesh.exe coordinator` 和 `.\idlemesh.exe worker --id worker-a --engine echo --always-claim`。第三个终端运行下面的命令。

```powershell
$job = .\idlemesh.exe submit --engine echo --prompt '你好，IdleMesh！'
.\idlemesh.exe status $job
.\idlemesh.exe wait $job
```

`wait` 输出 `你好，IdleMesh！`。`status` 返回任务 JSON，状态值为 `queued`、`running`、`succeeded` 或 `failed`。协调器默认监听 `127.0.0.1:8787` 并在当前目录保存 `idlemesh.db`。重新启动时用同一工作目录或传入同一个 `--db` 路径，可继续查询和处理已保存的任务。

## 运行真实模型

在每台参与推理的电脑安装并启动 Ollama，下载同一个本地模型。这个示例的量化模型下载约 398 MB，运行内存还需容纳上下文和运行时。协调器和提交端可以独立部署。Ollama 对操作系统和处理器有自己的要求。 [macOS](https://docs.ollama.com/macos) · [Windows](https://docs.ollama.com/windows) · [Linux](https://docs.ollama.com/linux) · [Qwen2.5 0.5B 模型](https://ollama.com/library/qwen2.5:0.5b)

```sh
ollama pull qwen2.5:0.5b
./idlemesh worker --id cpu-a --model qwen2.5:0.5b --threads 2 --context 2048
```

另开终端提交实际推理请求。

```sh
job=$(./idlemesh submit --model qwen2.5:0.5b --max-tokens 64 --prompt '用两句话解释为什么多台电脑可以同时处理不同的任务。')
./idlemesh wait "$job"
```

Windows 使用 `.\idlemesh.exe` 并用 `$job =` 接收任务 ID。Worker 请求本机 Ollama 做 CPU 推理，任务结束后请求释放模型。`--threads` 限制推理线程数，`--context` 控制模型上下文大小。可以用 `ollama ps` 在模型运行时查看 `PROCESSOR` 栏。 [Ollama FAQ](https://docs.ollama.com/faq)

`submit --prompt-file prompt.txt` 从 UTF-8 文件读取提示词，`submit --prompt-file -` 从标准输入读取。两种形式都可以搭配 `--model` 或 `--engine echo`。单条提示词最大 32 KiB。

如果希望同时关闭本机 Ollama 的云功能，在 Ollama 进程的环境中设置 `OLLAMA_NO_CLOUD=1`，再重启 Ollama。 [Ollama 云功能设置](https://docs.ollama.com/faq#how-do-i-disable-ollamas-cloud-features)

## 只在有空闲资源时接单

从 v0.2 起，两种 Worker 默认都检查整台电脑的 CPU、可用内存和外部供电。切换到电池、资源不足或读不到指标时，Worker 暂停领取新任务，待符合条件一段时间后自动恢复。已领取的任务继续运行，完成后重新观察空闲状态。

| Worker 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `--idle-cpu-percent` | `25` | 整机 CPU 使用率上限，单位为百分比 |
| `--idle-min-memory-mib` | `2048` | 可用内存下限，单位为 MiB |
| `--idle-for` | `6s` | 满足条件后再接单所需的持续时间 |
| `--idle-check` | `2s` | 资源采样间隔 |

例如，给内存余量要求较高的节点设置至少 3 GiB 可用内存和 10 秒的稳定窗口。

```sh
./idlemesh worker --id cpu-a --model qwen2.5:0.5b --idle-min-memory-mib 3072 --idle-for 10s
```

任务长期处于 `queued` 时，先检查对应 Worker 的终端或标准错误日志。`paused claiming` 后会写明暂停原因，例如 CPU 繁忙、内存不足、电池供电或无法取得可靠读数。`resources ready` 表示节点恢复接单。阈值用于接单前判断，实际推理的 CPU 和内存需求还取决于模型、线程数及上下文。

专用计算节点或受控演练可用 `--always-claim` 恢复 v0.1 的接单行为，同时跳过 CPU、内存和供电检查。

## 让其他电脑加入

以下单协调器示例在一台常开的电脑上监听私有地址。把 `192.168.1.50` 和 `mesh.example.net` 换成你的协调器地址及名称，并使用客户端系统信任、且匹配该名称的 TLS 证书。只向参与设备开放协调器端口。协调器需要两个不同的随机令牌。在启动进程的终端设置环境变量，分别将客户端令牌交给提交端、将 Worker 令牌交给参与运算的电脑。

| 进程 | 需要设置的环境变量 |
| --- | --- |
| 协调器 | `IDLEMESH_CLIENT_TOKEN` 和 `IDLEMESH_WORKER_TOKEN` |
| Worker | `IDLEMESH_WORKER_TOKEN` |
| 提交、查询和等待结果 | `IDLEMESH_CLIENT_TOKEN` |

macOS/Linux 可用 `openssl rand -hex 32` 分别生成两个令牌，用 `export` 设置环境变量。PowerShell 用 `$env:变量名 = '对应令牌'`。令牌应通过私下渠道传递。

```sh
# 协调器主机
./idlemesh coordinator --listen 192.168.1.50:8787 --db idlemesh.db --tls-cert cert.pem --tls-key key.pem

# Worker 主机，每个节点使用不同的 ID
./idlemesh worker --server https://mesh.example.net:8787 --id cpu-b --model qwen2.5:0.5b --threads 2 --context 2048

# 客户端
job=$(./idlemesh submit --server https://mesh.example.net:8787 --model qwen2.5:0.5b --prompt '给我一个本地推理的应用例子。')
./idlemesh wait --server https://mesh.example.net:8787 "$job"
```

已经使用加密私有 VPN 的部署可以显式启用 `--allow-insecure-http`。协调器、Worker 和客户端都要传入该选项，两个角色的令牌仍然必需。提示词会发送到执行它的 Worker，任务数据存放在协调器数据库中，请按敏感数据保护数据库和备份。

## 部署三协调器集群

v0.3 支持三个固定的 Raft 投票节点。把它们放在三台独立且时钟同步的主机上，可在其中一台掉线后由其余两台选主并继续工作。失去多数派时，集群暂停提交、领取和查询，等多数派恢复后再处理。节点间采用双向 TLS。客户端和 Worker 用重复的 `--server` 指定同一集群的各个 API 地址，选主时可自动切换。 [部署步骤与故障演练](docs/ha-setup.md)

从单机切换到集群时，先让旧队列处理完并保留原 `--db` 文件，随后将客户端和 Worker 指向新集群。新集群使用独立的任务队列和任务 ID，旧任务可以通过原来的单机协调器查询。

## 手动验证掉线恢复

在同一电脑开多个终端也能验证调度。保持默认协调器运行，关闭之前的回显 Worker，然后按下面的顺序操作。

1. 启动慢 Worker。`./idlemesh worker --id slow --engine echo --echo-delay 2m --always-claim`
2. 提交任务。`job=$(./idlemesh submit --engine echo --prompt '掉线后完成')`
3. 用 `./idlemesh status "$job"` 确认任务进入 `running`，在慢 Worker 的终端按 Ctrl+C 停止它。
4. 启动替补。`./idlemesh worker --id spare --engine echo --always-claim`
5. 在提交终端运行 `./idlemesh wait "$job"`。默认租约为 15 秒，轮询和重新执行也需要时间。最后会输出 `掉线后完成`。用原任务 ID 查询状态，应看到 `state` 为 `succeeded`、`worker_id` 为 `spare`、`attempt` 为 `2`。

要验证协调器重启恢复，可先停止所有 Worker，提交一个回显任务，记住 ID。重启协调器时使用同一个 `--db` 路径，再用 `--always-claim` 启动回显 Worker。用原 ID 等待任务完成。

## 从源码构建

安装 Go 1.25 或更新版本，在仓库根目录运行以下命令。Windows 可将构建输出改为 `idlemesh.exe`。

```sh
go test ./...
go build -o idlemesh ./cmd/idlemesh
```

各命令的选项可通过 `idlemesh coordinator --help`、`idlemesh worker --help`、`idlemesh submit --help`、`idlemesh status --help` 和 `idlemesh wait --help` 查看。

欢迎通过 GitHub Issues 提交故障复现和跨平台实测结果。项目采用 [Apache 2.0 许可证](LICENSE)，模型及推理运行时分别适用各自的许可。
