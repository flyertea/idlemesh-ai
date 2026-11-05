# 协调器替换、备份与灾难恢复

有两个原投票节点和各自的状态目录时，可以替换第三个节点，保留最新的已提交任务。原集群无法恢复多数派时，可以从已验证的备份重建，恢复范围由备份时点决定。

## 替换一台协调器

v0.5 每个集群支持一次成员替换。以下用本机演练中的 `a`、`b`、`c` 为例。`c` 需要永久退役，`a`、`b` 仍然健康。新节点使用 ID `d`、独立的 API 和 Raft 地址以及新的状态目录。三台主机部署时，换成实际的私网地址、端口和匹配的证书。 [三节点端口及证书配置](ha-setup.md#本机演练)

| 节点 | API | Raft | 状态目录 |
| --- | --- | --- | --- |
| 保留 `a` | `127.0.0.1:18781` | `127.0.0.1:18881` | 原 `./state-a` |
| 保留 `b` | `127.0.0.1:18782` | `127.0.0.1:18882` | 原 `./state-b` |
| 新节点 `d` | `127.0.0.1:18784` | `127.0.0.1:18884` | 新 `./state-d` |

1. 停止并隔离 `c`，切断旧主机的 Raft 和 API 流量。确认 `a`、`b` 仍可选出领导者，记录一个已完成任务的 ID 和完整输出，以及一个待处理任务的 ID。
2. 为 `d` 签发自己的证书和私钥。使用原集群的 Raft CA、`d` 地址的 DNS 或 IP SAN、URI SAN `urn:idlemesh:raft:d`，以及 `serverAuth` 和 `clientAuth` 两种用途。本机演练可以沿用签发节点证书的命令，只为 `d` 签发。 [证书签发示例](ha-setup.md#生成专用-ca节点证书和-api-令牌)
3. 安排维护窗口，停止 `a`、`b` 并保留各自的状态目录。三个进程使用目标名单 `a,b,d` 和同一个 `--raft-replace c=d`。启动时省略 `--raft-bootstrap`。只有 `d` 首次启动时使用空的状态目录，并额外传入 `--raft-join-replacement`。所有节点沿用当前凭据模式和对应的环境变量。
4. 等待三个节点各自记录 `Raft replacement committed locally: c -> d at index N`，确认三条日志的提交索引 `N` 相同，再把客户端和 Worker 的 `--server` 地址更新为 `a`、`b`、`d`。完成后查询原任务 ID 并核对完整输出。后续重启沿用原状态目录、目标名单和 `--raft-replace c=d`。替补节点后续重启省略 `--raft-join-replacement`。退役主机保持隔离。

下面以 `a` 为例。启动 `b`、`d` 时按表修改变量，其余参数相同。若集群已启用托管凭据，将 `mode=()` 改为 `mode=(--managed-workers)`。

```bash
node=a; api_port=18781; raft_port=18881; mode=()
join=()
# 仅在 d 的首次启动时执行下面这行
# join=(--raft-join-replacement)
./idlemesh coordinator --listen "127.0.0.1:$api_port" \
  --raft-id "$node" --raft-listen "127.0.0.1:$raft_port" --raft-dir "./state-$node" \
  --raft-peer a=127.0.0.1:18881 --raft-peer b=127.0.0.1:18882 --raft-peer d=127.0.0.1:18884 \
  --raft-replace c=d --raft-tls-cert "tls/$node.pem" --raft-tls-key "tls/$node.key" --raft-tls-ca tls/ca.pem "${join[@]}" \
  "${mode[@]}"
```

这次操作包含两个保留节点都停机的短暂窗口。重启后由原多数派处理移除和添加，新节点同步现有状态。若操作超时，先保留当前目录、目标名单和日志，排查节点连通性与证书身份。继续使用同一替换配置，旧名单只用于保留操作记录。

### 验证替换

仅在隔离演练环境中进行强制停止。可用 `kill -KILL "$RetiringPid"` 或 PowerShell 的 `Stop-Process -Id $RetiringPid -Force` 模拟崩溃。尝试用 `c` 的证书启动配置为 `d` 的新节点，应该在启动时被拒绝。用正确证书重新启动并等三个进程都记录提交日志后，再停止一个原节点。其余原节点与 `d` 应能组成多数派并提供服务。`/readyz` 可以在两个原节点已恢复多数派时提前返回 `200`。对比替换前后的任务 ID、完整输出和待处理任务。若使用托管凭据，还应验证有效凭据仍可接单，已撤销的凭据仍返回 `401`。

## 备份与恢复

### 导出备份

在需要提供备份的协调器启动命令加上 `--enable-backup`。集群部署在三个节点都开启，以便领导者切换后仍可备份。备份使用 `IDLEMESH_ADMIN_TOKEN`。托管凭据模式沿用已有的客户端和管理员令牌。共享令牌模式即使只监听本机，也要设置互不相同的 `IDLEMESH_CLIENT_TOKEN`、`IDLEMESH_WORKER_TOKEN` 和 `IDLEMESH_ADMIN_TOKEN`。

从管理员终端执行。单协调器可以只提供一个 `--server`。选择支持硬链接的本地文件系统，输出路径应尚不存在。客户端的 `--timeout` 默认 `10m`，服务端单次备份也有 10 分钟上限。

```sh
./idlemesh backup --output ./idlemesh-state.backup \
  --server http://127.0.0.1:18781 --server http://127.0.0.1:18782 --server http://127.0.0.1:18784
```

远程操作使用实际 HTTPS 地址。备份包含未完成任务、已完成结果、请求去重记录和 Worker 凭据摘要。文件可能包含明文提示词和结果，应私下存储和传输。下载和恢复会检查完整性，备份数据上限为 8 GiB。将 `--request-id`、验证任务 ID、完整输出和备份时间记在演练记录里。Raft 签发密钥、API 证书和进程环境中的令牌应另外管理。

### 恢复到单协调器

此命令离线运行，无需启动协调器或设置角色令牌。`--db` 必须是支持硬链接的本地文件系统上的新文件路径。

```sh
./idlemesh restore --backup ./idlemesh-state.backup --db ./recovered.db
```

验证时在隔离地址启动，使用原凭据模式及适用于恢复环境的角色令牌。下面以托管凭据模式为例。

```sh
./idlemesh coordinator --db ./recovered.db --listen 127.0.0.1:18790 --managed-workers
```

### 恢复到新的三节点集群

先隔离旧集群的 Raft 和 API 地址。用新的节点 ID、地址和 Raft CA 建立恢复集群，三个节点使用新目录、相同的新名单和原有的 Worker 凭据模式。首次引导只在字典序最小的投票节点上进行，同时传入 `--restore-backup`。另两个节点使用相同名单和各自的证书，启动时省略 `--raft-bootstrap` 与 `--restore-backup`。之后所有节点重启都省略这两个参数并沿用各自的恢复目录。新旧集群的 API 入口分开，开放恢复环境前为 API 配置新的角色令牌。

例如，在隔离演练中创建新的节点 `e`、`f`、`g`。按 [证书配置说明](ha-setup.md#生成专用-ca节点证书和-api-令牌) 在 `recovery-tls` 中签发新 CA 和对应证书。本机使用下列端口，三个节点均需绑定匹配的本地端口。下面以首个投票节点 `e` 和托管凭据模式为例。

| 节点 | API | Raft | 新目录 |
| --- | --- | --- | --- |
| `e` | `127.0.0.1:18791` | `127.0.0.1:18891` | `./recovery-e` |
| `f` | `127.0.0.1:18792` | `127.0.0.1:18892` | `./recovery-f` |
| `g` | `127.0.0.1:18793` | `127.0.0.1:18893` | `./recovery-g` |

```sh
./idlemesh coordinator --listen 127.0.0.1:18791 --managed-workers \
  --raft-id e --raft-listen 127.0.0.1:18891 --raft-dir ./recovery-e \
  --raft-peer e=127.0.0.1:18891 --raft-peer f=127.0.0.1:18892 --raft-peer g=127.0.0.1:18893 \
  --raft-tls-cert recovery-tls/e.pem --raft-tls-key recovery-tls/e.key --raft-tls-ca recovery-tls/ca.pem \
  --raft-bootstrap --restore-backup ./idlemesh-state.backup
```

首次从备份引导若中断，保留失败目录和日志。在新的空目录重新演练，确认三个恢复节点都使用同一组新配置。

### 验证状态再开放接入

- 在恢复后的 API 地址查询备份内已完成的任务，逐一比对原任务 ID 和完整输出。用相同 `--request-id` 及原提交参数重试，确认返回同一 ID。
- 恢复会清除备份中正在运行的租约。任务有剩余尝试次数时重新排队，用原 ID 等待完成。尝试次数用尽的任务标为失败，旧租约无法写回。
- 使用托管凭据时，先保持 Worker 入口隔离，按运营记录重新执行备份时点之后的撤销或换发。再验证有效凭据能接单，已撤销凭据被拒绝。
- 在演练中可于备份后额外提交一个任务。恢复后、提交任何新任务前，确认该 ID 无记录。备份之后发出的任务 ID 可能在恢复环境被后续任务重新使用。对照外部任务记录，确定要补交的请求和它们的新 ID，再切换客户端和 Worker。
- 在新建的测试目标试用截断的备份副本，确认恢复失败。对已存在的目标路径执行恢复也应被拒绝。保留原备份和旧状态，直到任务、凭据及新节点多数派均验证通过。

在 Windows PowerShell 中用 `.\idlemesh.exe` 运行相同命令。可使用 [PowerShell 参数数组示例](ha-setup.md#powershell-对应命令) 传入多个 `--server` 和 `--raft-peer`。
