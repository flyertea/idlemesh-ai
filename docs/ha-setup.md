# 部署三协调器集群

单协调器适合快速上手。需要在一台协调器停机后继续服务时，可把三个固定的 Raft 投票节点部署在三台常开主机上。两个互通的节点构成多数派。选举期间可能短暂停顿。以下演练先在一台电脑启动三个进程，验证选举、任务接续和幂等提交，再按实际地址在不同主机上部署。

## 本机演练

解压当前版本，把 `idlemesh` 二进制放在新的演练目录。以下演练使用默认的共享 Worker 令牌模式，证书命令使用 Bash 和 OpenSSL 3。Windows 可以在另一台可信主机签发证书和令牌，再使用后文的 PowerShell 命令启动。演练所有端口都绑定到 `127.0.0.1`。在开始前确认这些端口可用。

| 节点 ID | API 地址 | Raft 地址 | 状态目录 |
| --- | --- | --- | --- |
| `a` | `http://127.0.0.1:18781` | `127.0.0.1:18881` | `./state-a` |
| `b` | `http://127.0.0.1:18782` | `127.0.0.1:18882` | `./state-b` |
| `c` | `http://127.0.0.1:18783` | `127.0.0.1:18883` | `./state-c` |

### 生成专用 CA、节点证书和 API 令牌

在新目录运行一次。每个节点有自己的私钥和证书。证书的 IP SAN 匹配 `--raft-peer` 公布的地址，URI SAN 标识该节点，扩展用途同时允许 TLS 客户端和服务器认证。证书与私钥均为 PEM。 [OpenSSL 证书扩展说明](https://docs.openssl.org/3.0/man5/x509v3_config/)

```bash
umask 077
mkdir tls
openssl req -x509 -newkey rsa:3072 -noenc -sha256 -days 365 \
  -subj '/CN=IdleMesh Raft Lab CA' -keyout tls/ca.key -out tls/ca.pem \
  -addext 'basicConstraints=critical,CA:TRUE,pathlen:0' \
  -addext 'keyUsage=critical,keyCertSign,cRLSign'

for node in a b c; do
  openssl req -new -newkey rsa:2048 -noenc -sha256 -subj "/CN=IdleMesh Raft $node" \
    -keyout "tls/$node.key" -out "tls/$node.csr"
  cat > "tls/$node.ext" <<EXT
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=IP:127.0.0.1,URI:urn:idlemesh:raft:$node
EXT
  openssl x509 -req -in "tls/$node.csr" -CA tls/ca.pem -CAkey tls/ca.key \
    -CAcreateserial -out "tls/$node.pem" -days 90 -sha256 -extfile "tls/$node.ext"
done

openssl rand -hex 32 > tls/client.token
openssl rand -hex 32 > tls/worker.token
```

`tls/ca.pem` 是可分发的 CA 证书，`tls/ca.key` 是签发私钥。多机部署时将签发私钥留在离线签发机，每个节点只接收自己的证书和私钥以及 CA 证书。按角色私下传递令牌。两个令牌应不同，三个协调器都使用同一组。

### 启动三个协调器

打开三个终端，进入同一个演练目录。每个终端选用下面一行参数。首次启动一个全新的集群时，只有字典序最小的节点 `a` 使用 `--raft-bootstrap`。

```bash
# 终端 a，首次启动
node=a; api_port=18781; raft_port=18881; bootstrap=(--raft-bootstrap)
# 终端 b
node=b; api_port=18782; raft_port=18882; bootstrap=()
# 终端 c
node=c; api_port=18783; raft_port=18883; bootstrap=()
```

然后在各终端运行下面的命令。每台主机配置相同的三个 `--raft-peer`，其中包括本节点。重启 `a` 时改为 `bootstrap=()`，三个节点都应沿用原状态目录。

```bash
export IDLEMESH_CLIENT_TOKEN="$(cat tls/client.token)"
export IDLEMESH_WORKER_TOKEN="$(cat tls/worker.token)"
./idlemesh coordinator --listen "127.0.0.1:$api_port" \
  --raft-id "$node" --raft-listen "127.0.0.1:$raft_port" --raft-dir "./state-$node" \
  --raft-peer a=127.0.0.1:18881 --raft-peer b=127.0.0.1:18882 --raft-peer c=127.0.0.1:18883 \
  --raft-tls-cert "tls/$node.pem" --raft-tls-key "tls/$node.key" --raft-tls-ca tls/ca.pem \
  "${bootstrap[@]}"
```

### 确认领导者并提交任务

`/healthz` 用于确认进程仍在监听，三个运行中的节点都返回 `200`。`/readyz` 用客户端令牌访问。正常情况下只有能确认多数派的领导者返回 `200`，跟随节点和选举中的节点返回 `503`。在独立客户端终端运行以下命令，必要时等待几秒再查。

```bash
export IDLEMESH_CLIENT_TOKEN="$(cat tls/client.token)"
for port in 18781 18782 18783; do
  curl --silent --show-error --max-time 5 --output /dev/null --write-out "$port %{http_code}\n" \
    --header "Authorization: Bearer $IDLEMESH_CLIENT_TOKEN" "http://127.0.0.1:$port/readyz"
done
```

另开 Worker 终端运行回显节点。演练设置 20 秒回显延迟，以便在任务执行期间停止领导者。

```bash
export IDLEMESH_WORKER_TOKEN="$(cat tls/worker.token)"
./idlemesh worker --id lab-worker --engine echo --echo-delay 20s --always-claim \
  --server http://127.0.0.1:18781 --server http://127.0.0.1:18782 --server http://127.0.0.1:18783
```

回到客户端终端。用 `--request-id` 保存本次提交的幂等键。生成新键表示提交另一个任务。同一键必须搭配相同的引擎、模型、提示词和输出限制，否则会收到 `409`。

```bash
servers=(--server http://127.0.0.1:18781 --server http://127.0.0.1:18782 --server http://127.0.0.1:18783)
request_key=$(openssl rand -hex 16)
job=$(./idlemesh submit "${servers[@]}" --request-id "$request_key" --engine echo --prompt '集群故障切换成功')
./idlemesh status "${servers[@]}" "$job"
```

### 验证故障切换与多数派恢复

1. 任务进入 `running` 后，在 `/readyz` 返回 `200` 的协调器终端按 Ctrl+C。保留其余两个协调器和 Worker，运行 `./idlemesh wait "${servers[@]}" --timeout 2m "$job"`。命令应打印 `集群故障切换成功`。选举在租约内完成时 Worker 可继续原执行，否则按租约规则重试。
2. 在客户端重新运行 `./idlemesh submit "${servers[@]}" --request-id "$request_key" --engine echo --prompt '集群故障切换成功'`。结果应与原任务 ID 相同。脚本遇到网络错误、超时或 `503` 而无法确认首次提交结果时，可以这样重试。保存 1 到 128 个 ASCII 字母、数字、句点、下划线或连字符组成的键，直到取得任务 ID。
3. 在剩余两台中再停止一台。唯一运行的协调器仍能响应 `/healthz`，其 `/readyz` 会返回 `503`，客户端暂时无法提交或读取队列。然后用原目录和原配置重启任意一个停止的节点，启动参数省略 `--raft-bootstrap`。多数派恢复后，再次用原任务 ID 查询已提交的结果。

多地址客户端对一次调用最多尝试约 10 秒。单个地址的尝试上限约 1.5 秒，租约或命令的剩余时间较短时会提前结束。`wait` 在其自身的等待时间内继续查询。 [架构与租约行为](architecture.md#三协调器如何达成一致)

### PowerShell 对应命令

用可信签发机准备证书。演练电脑的 `tls` 目录只需接收 `ca.pem`、各节点的证书和私钥，以及两个令牌文件。以下展示节点 `a` 的首次启动。分别将 `$node`、`$apiPort`、`$raftPort` 改为表中值，并在启动 `b`、`c` 或重启 `a` 时设 `$bootstrap = @()`。所有命令从演练目录运行。

```powershell
$node = 'a'
$apiPort = 18781
$raftPort = 18881
$bootstrap = @('--raft-bootstrap')
$env:IDLEMESH_CLIENT_TOKEN = (Get-Content -Raw .\tls\client.token).Trim()
$env:IDLEMESH_WORKER_TOKEN = (Get-Content -Raw .\tls\worker.token).Trim()
$peers = @('--raft-peer', 'a=127.0.0.1:18881', '--raft-peer', 'b=127.0.0.1:18882', '--raft-peer', 'c=127.0.0.1:18883')
.\idlemesh.exe coordinator --listen "127.0.0.1:$apiPort" --raft-id $node --raft-listen "127.0.0.1:$raftPort" --raft-dir ".\state-$node" --raft-tls-cert ".\tls\$node.pem" --raft-tls-key ".\tls\$node.key" --raft-tls-ca .\tls\ca.pem @peers @bootstrap
```

Worker 和客户端终端各自加载对应令牌，并定义同一个 `$servers` 参数数组。

```powershell
$servers = @('--server', 'http://127.0.0.1:18781', '--server', 'http://127.0.0.1:18782', '--server', 'http://127.0.0.1:18783')
# Worker 终端
$env:IDLEMESH_WORKER_TOKEN = (Get-Content -Raw .\tls\worker.token).Trim()
.\idlemesh.exe worker --id lab-worker --engine echo --echo-delay 20s --always-claim @servers
```

```powershell
# 客户端终端。先在本终端定义相同的 $servers。
$env:IDLEMESH_CLIENT_TOKEN = (Get-Content -Raw .\tls\client.token).Trim()
foreach ($port in 18781, 18782, 18783) {
    curl.exe --silent --show-error --max-time 5 --output NUL --write-out "$port %{http_code}\n" --header "Authorization: Bearer $env:IDLEMESH_CLIENT_TOKEN" "http://127.0.0.1:$port/readyz"
}
$requestKey = [guid]::NewGuid().ToString('N')
$job = .\idlemesh.exe submit @servers --request-id $requestKey --engine echo --prompt '集群故障切换成功'
.\idlemesh.exe status @servers $job
.\idlemesh.exe wait @servers --timeout 2m $job
```

## 改为三台主机部署

部署使用独立的新集群目录。给三台协调器分配稳定的私网或 VPN 地址，例如 `mesh-a.example.net:9787`、`mesh-b.example.net:9787`、`mesh-c.example.net:9787` 作为 Raft 地址。三台使用相同的 ID 到地址映射。签发时将各自的 `IP:127.0.0.1` 改为对应的 `DNS:mesh-a.example.net` 等。若 peer 配置使用 IP，则使用对应的 IP SAN。Raft `--raft-listen` 绑定本机的私有接口，端口仅允许其他两个协调器访问。

为 API 单独配置证书和私钥，通过 `--listen`、`--tls-cert`、`--tls-key` 提供 HTTPS。例如把三个 API 地址设为 `https://mesh-a.example.net:8787`、`https://mesh-b.example.net:8787`、`https://mesh-c.example.net:8787`。API 证书应由客户端和 Worker 的操作系统信任，证书中的名称应匹配各自的 API 地址。Raft CA 只用于节点间认证。三个进程用相同的 `--lease`、`--max-attempts` 和角色令牌，并启用系统时间同步。Worker 和客户端各用三个 `--server` 指向同一集群的 API 地址。

每台主机都保留自己的 `--raft-dir`。将目录和备份按敏感数据保护。使用原名单维护时逐台重启并确认 `/readyz` 可以从集群中的某台获得 `200`。需要永久换机或从备份重建时，使用对应的维护流程。 [节点替换与灾难恢复](ha-operations.md)

从单协调器迁移时，先停止往旧队列提交任务，等待队列处理完并保留旧 `--db` 文件。新集群从独立的队列开始。需要查旧任务时，仍通过原数据库启动单机协调器并在原入口查询。

## 启用独立 Worker 凭据

三个协调器都须运行支持托管凭据的版本，并使用同一种凭据模式。托管模式与共享令牌模式的 Raft 节点彼此隔离。切换现有集群时，安排一次维护窗口，停止所有旧协调器后再启动新配置。

1. 为三个协调器配置相同的 `IDLEMESH_CLIENT_TOKEN` 和 `IDLEMESH_ADMIN_TOKEN`。两种令牌须不同。移除各协调器环境中的 `IDLEMESH_WORKER_TOKEN`。Bash 使用 `unset IDLEMESH_WORKER_TOKEN`，PowerShell 使用 `Remove-Item Env:IDLEMESH_WORKER_TOKEN`。
2. 在各协调器原启动命令加上 `--managed-workers`。沿用每个节点原有的 `--raft-dir`、ID、固定成员地址和证书。已有集群的启动命令省略 `--raft-bootstrap`。
3. 等待多数派恢复，用客户端令牌查询 `/readyz`。在管理员终端设置 `IDLEMESH_ADMIN_TOKEN`，为每个 Worker ID 分别发放令牌。Worker 使用新令牌重启，保留自己的 ID。原共享令牌在托管模式下失效。

用集群的三个 API 地址发放或撤销 `cpu-a`。三个协调器的 API 证书须受管理员终端信任。

```bash
servers=(--server https://mesh-a.example.net:8787 --server https://mesh-b.example.net:8787 --server https://mesh-c.example.net:8787)
./idlemesh worker-token issue --id cpu-a "${servers[@]}"
./idlemesh worker-token revoke --id cpu-a "${servers[@]}"
```

发放操作只在确认成功后输出一次令牌。发放因超时而未取得令牌时，可以再次为同一个 ID 发放，新的凭据会替代此前可能已生效的凭据。撤销成功后，旧 Worker 无法领取任务、续租或提交结果。多数派会保存撤销状态，领导者切换后也生效。 [凭据与角色说明](../README.md#为每台-worker-单独发放凭据)
