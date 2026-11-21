# IPv4 UDP 组播转播网关

基于 FastAPI + asyncio 的 IPv4 UDP 组播转播网关。播出端（接收端）通过 HTTP API
按**单播地址 + 端口**订阅/退订/换台；**频道**由**组播地址 + 端口**唯一确定。
网关为每个频道维护一个组播入组，把收到的数据报逐个转发给该频道的所有订阅端。

## 语义保证（与需求逐条对应）

| 需求 | 实现 |
| --- | --- |
| 同频道共用一次入组 | 每个频道（`group:port`）只创建一个 socket、执行一次 `IP_ADD_MEMBERSHIP`；后续订阅者直接挂到该频道的订阅集合 |
| 收到的数据报转发给各订阅端 | 频道收到数据报后，用共享的发送 socket 向订阅集合快照逐份 `sendto` |
| 同端口的不同组分开 | 频道键是 `(group, port)` 二元组；每个频道 socket 单独绑定并入组，并显式设置 `IP_MULTICAST_ALL=0`（Linux 默认为 1，会把主机上其他 socket 入的组也投递进来，导致同端口频道串台——Python 未导出该常量，代码中按 Linux 值 49 设置） |
| 新频道收包就绪才换台 | 换台时先创建 socket、绑定、入组，再**在 `JOIN_TIMEOUT` 内等到目标频道的第一个数据报**（入组 + 等首包共用一份超时预算），之后才迁移订阅端；等首包期间不持有全局锁，旧频道转发和其它请求都不被阻塞 |
| 失败时继续转播原频道 | 新频道入组失败或 `JOIN_TIMEOUT` 内没有等到首包都会抛 `JoinError`（HTTP 502），此时旧订阅完全未动，旧频道继续转播；为目标频道临时建立的入组会被清理（已有其它订阅者/换台等待者的频道除外） |
| 换台/退订成功后停止发送旧频道 | 订阅集合的修改在事件循环内**同步完成**（两个 `await` 之间无让出），转发回调不会看到中间态；操作返回后该端不会再收到旧频道的任何新数据报 |
| 最后一端离开才退组 | 频道订阅集合变空**且没有等待首包的换台**时才 `IP_DROP_MEMBERSHIP` 并关闭 socket |
| 并发操作遵守同样规则 | 所有状态变更经同一把 `asyncio.Lock` 串行化；换台提交前会在锁内重新校验订阅未被并发改动（被改动则 409/404，不覆盖他人结果）；并发订阅同频道只入组一次、并发退订/换台不会重复退组或泄漏 |

一个播出端（`ip:port`）同一时刻只订阅一个频道；已订阅的端直接订阅别的频道会
返回 409，应使用换台接口。重复订阅同一频道是幂等的（返回 200）。

## 目录结构

```
app/
  main.py      FastAPI 应用、路由、异常映射、生命周期
  relay.py     转发引擎：RelayManager / Channel（入组、转发、换台、退组）
  schemas.py   请求/响应模型（IPv4、组播地址、端口校验）
tests/         pytest 端到端测试（真实 UDP 组播收发）
examples/
  send_multicast.py   发包示例：向组播频道发 UDP 数据报
  receive_unicast.py  收包示例：模拟播出端接收网关转发
Dockerfile / docker-compose.yml
```

## 启动

### Docker Compose（推荐）

组播流量无法穿过 Docker 的 NAT/端口映射，因此 compose 使用主机网络
（`network_mode: host`，仅 Linux）：

```bash
docker compose up --build
# API 监听在宿主机 8000 端口（可用 API_PORT 环境变量修改）
```

环境变量：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `MCAST_INTERFACE` | `0.0.0.0` | 组播入组的网卡地址；`0.0.0.0` 由内核按默认路由选择 |
| `JOIN_TIMEOUT` | `5` | 换台时就绪目标频道的总预算（秒）：入组 + 等待第一个数据报 |
| `API_PORT` | `8000` | API 监听端口 |

### 本地直接运行

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## API

统一前缀 `/api/v1`。错误码：400 系为请求问题，`409` 状态冲突，`404` 订阅不存在，
`422` 参数校验失败，`502` 目标频道就绪失败——无法入组或 `JOIN_TIMEOUT` 内未收到
首个数据报（换台时旧频道不受影响）。

### 订阅 `POST /api/v1/subscriptions`

```bash
curl -X POST http://127.0.0.1:8000/api/v1/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{"subscriber": {"ip": "127.0.0.1", "port": 9000},
       "channel": {"group": "239.10.0.1", "port": 5000}}'
```

新建返回 `201`，重复订阅返回 `200`（`already_subscribed: true`）。

### 退订 `DELETE /api/v1/subscriptions`

```bash
curl -X DELETE http://127.0.0.1:8000/api/v1/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{"subscriber": {"ip": "127.0.0.1", "port": 9000}}'
```

### 换台 `POST /api/v1/switch`

```bash
curl -X POST http://127.0.0.1:8000/api/v1/switch \
  -H 'Content-Type: application/json' \
  -d '{"subscriber": {"ip": "127.0.0.1", "port": 9000},
       "from_channel": {"group": "239.10.0.1", "port": 5000},
       "to_channel":   {"group": "239.10.0.2", "port": 5000}}'
```

`from_channel` 可选；提供时必须与当前频道一致，否则 `409`（可用于并发下的
乐观校验）。换到当前所在频道是幂等空操作（`changed: false`）。

**换台的就绪判定**：目标频道必须先在 `JOIN_TIMEOUT` 内收到第一个数据报，
接口才返回 `200` 并提交切换——期间旧频道照常转发，播出端不会断流。目标
频道已有订阅者且已在收包时立即提交；目标频道静默（超时无首包）或无法入组
时返回 `502`，原订阅和旧频道转发完全保留。因此调用方应为换台请求设置大于
`JOIN_TIMEOUT` 的 HTTP 超时。

### 观测

- `GET /api/v1/channels`：各频道的订阅端列表、收发计数、错误计数、就绪状态
  （`ready`：socket 已入组监听；`first_packet_received`：是否已收到首个数据报；
  `pending_switches`：等待该频道首包的换台数）
- `GET /api/v1/subscriptions`：订阅端 → 频道映射
- `GET /healthz`：健康检查
- `GET /docs`：FastAPI 自动生成的 OpenAPI 文档

## 本地发包 / 收包示例

终端 1 —— 启动收包端（模拟播出端）：

```bash
python3 examples/receive_unicast.py --ip 127.0.0.1 --port 9000
```

终端 2 —— 订阅并换台：

```bash
curl -X POST http://127.0.0.1:8000/api/v1/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{"subscriber": {"ip": "127.0.0.1", "port": 9000},
       "channel": {"group": "239.10.0.1", "port": 5000}}'
```

终端 3 —— 向频道发包：

```bash
python3 examples/send_multicast.py --group 239.10.0.1 --port 5000 --count 5
# 持续发送：--count 0 --interval 0.5；跨网卡时加 --interface <本机IP>
```

终端 1 会打印网关转发来的数据报。换台到 `239.10.0.2:5000` 后，只有发往新组的
报文会被转发；退订后立即停止。

## 验证情况

- `pytest`：**32 个用例全部通过**（连续 3 轮稳定），覆盖：订阅转发、幂等订阅、
  重复订阅冲突、两人共用一次入组、同端口不同组隔离、退订即停、最后一人退组、
  换台切流（持续流量下无旧频道报文）、**静默目标换台 502 且旧频道回滚保流**、
  **换台阻塞至目标频道首包到达才提交（等待期间旧频道照转）**、**并发换台到同一
  静默目标双双 502 且无频道泄漏**、**等待中的换台钉住目标频道（末订阅者退订不
  退组）**、**换台等待被取消后清理临时入组**、换台入组失败保旧频道（用占用目标
  端口制造真实入组失败）、换到已有频道共用入组、并发订阅/并发退订/并发换台/
  同源守卫、参数校验（非组播地址、非法端口、非法 IP）。
- 手工冒烟：uvicorn 启动后用 `examples/` 两个脚本 + curl 走通
  订阅 → 转发 → 换台（旧停新通）→ 退订（停止、频道清空）→ 404/422 错误码；
  另实测了静默目标 502 回滚、首包到达触发提交两个场景。
- 本环境无 Docker，`docker compose up --build` 未实际执行；Dockerfile 与
  compose 文件经过静态校验（YAML 解析、指令核对），镜像构建未验证。

## 已知限制 / 未完成事项

- 换台的就绪判定以"频道建成以来收到过至少一个数据报"为准：换到**曾经收过包、
  但当前流已停止**的既有频道会立即提交，不会重新等待新报文（新频道无此问题，
  必定等首包）。如需"必须等到换台后的新报文"，需另加策略（未实现）。
- 仅支持 IPv4；组播地址校验为 224.0.0.0/4，未进一步限制 224.0.0.0/24 等保留段。
- 单网卡入组（`MCAST_INTERFACE` 指定一个接口）；未做多接口入组。
- API 无认证鉴权，应只在受信网络中暴露；未做订阅数量/速率限制。
- 转发是单向的（网关 → 播出端），播出端回包不会送回组播源。
- Docker 镜像未在本环境构建验证（无 Docker 守护进程）。
