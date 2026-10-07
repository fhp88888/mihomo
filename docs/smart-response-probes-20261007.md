# Beta Smart：移植 Alpha HTTP 响应探测（2026-10-07）

## 目的与来源

在 Beta Smart 中引入 Alpha 的 HTTP 响应探测、目标访问回避、持久化和恢复逻辑。
Alpha 来源为 `c3b2cb5fdf868964c1fddd0ba58e1e070f19bf28`，修改前 Beta 为 `5e057b8a`。
Alpha 已是 Beta 的祖先，不能通过再次 merge 恢复先前保留 Beta 的实现；本次定向移植原函数，而非整文件覆盖或创建另一套状态系统。

## 实际行为

- 沿用 Alpha 的 `ResponseProbeEligible`：非 UDP、非 INNER、目标有 hostname、端口 443、下载量小于 0.03 MiB 的连接，在关闭后异步探测该节点访问目标 `/robots.txt` 的响应。
- 沿用已移植的 `response.go`、`adapter.Proxy.StatusProbe`：分类 challenge、403、429、地区限制、部分 5xx；普通跨站重定向、421、405 等忽略；可访问响应恢复状态。
- 独立的 HostStatus 记录目标＋节点的到期时间。初次回避使用响应分类的 TTL；保留 Alpha 重复拒绝退避，第二次 15 分钟、第三次 1 小时、第四次 4 小时、之后 24 小时。HTTP 回答类别 code 2 不触发目标整体 stop-loss。
- 回避以 Beta 当前完整 hostname 为粒度，不借 ASN 将拒绝传播到其他域名，也不引入 Alpha 的目标归并机制。
- TCP、UDP、Unwrap 的自动候选均检查回避状态；健康恢复后刷新的候选也检查。已缓存的最佳节点不能绕过这项检查。发现流程跟随请求必须确认先前 leader 的赢家仍在自己的候选集中。
- 手动指定节点继续使用该节点。HTTP 回避不增加或清零 RouteTable 的 FailedCount，也不改 Beta 的评分公式、质量统计和探索策略。
- 每分钟运行有限的恢复扫描，沿用 Alpha 的随机抽取、节点限频、网站停止探测窗口、全局频率及并发预算。可访问探测清除状态，到期记录自动失效。
- 所有自动候选都被回避时返回 `no accessible proxies for target`，等待恢复或到期；不会把目标访问限制当作 provider 健康检查失败。
- 不移植【4】出口地区／ASN 证据汇聚、ExitWatcher；不移植 Alpha 的跨连接强制关闭和 pin 策略，也不实现持续读写错误回调。

## 原函数与必要适配

- `component/smart/response.go` 和 `adapter/adapter.go` 无修改，继续使用此前从 Alpha 引入的实现。
- `component/smart/stats.go`：定向移植 Alpha HostStatus/view、countsTowardBlock、buildHostStatusView、GetHostStatus、UpdateHostStatus、CheckHostStatus。
- `component/smart/common.go`：补入这些原函数所需的缓存及退避参数。
- `adapter/outboundgroup/smart_response.go`：移植 Alpha markNodeFailure、applyNodeAnswer、checkHostStatus、probeAfterClose、probeVerdict；移除 ExitWatcher 和强制关闭依赖，加入 Beta 候选适配。
- `adapter/outboundgroup/smart.go`：持有 Store 和探测限频状态，启动恢复任务，Unwrap 检查访问状态；关停先阻止新异步任务、取消探测、等待任务结束，再持久化。
- `adapter/outboundgroup/smart_route.go`：连接关闭钩子和 TCP/UDP 候选检查。
- `adapter/outboundgroup/smart_probe.go`：发现流程 follower 验证 leader 赢家的候选资格，拒绝时只在自己的候选中发现，不写 transport FailedCount。

对 Alpha 原实现的适配与修正：

1. Beta 的 Store 无排队删除操作；恢复后排队保存空 HostStatus，保留现有写入顺序机制，防止重启恢复旧回避记录。
2. Alpha view 最长缓存 30 秒；缓存有效期额外取最早节点到期时间，防止过期回避继续生效。
3. Alpha 恢复扫描会把大于 30 分钟的 code 2 状态视为旧版本记录；已有 FailCounts 的长退避是合法记录，本次保留它，只清理无计数的旧长记录。
4. 恢复扫描读取 NodeHosts/Nodes 保持锁保护，防止与异步探测并发读写 map。
5. 定期恢复探测使用与连接后探测相同的全局/节点预算；取消、手动选择或节点被移除后不落地新结果。

## 验证

- `go test -tags with_gvisor ./...` 通过。
- `go test -race ./component/smart ./adapter/outboundgroup ./adapter/provider` 通过。
- 新增状态测试：精确到期、持久化恢复、成功清除、目标隔离、长退避保存、并发更新和恢复扫描。
- `go test -race -count=2 ./component/smart ./adapter/outboundgroup` 通过；修正测试数据库/缓存隔离，以及旧 routeKey 测试对全局缓存未初始化的隐含假设，生产选路逻辑未变。
- 新增集成测试：真实本地 TLS 服务的 403／429／跨站重定向／200，连接关闭触发、限频、普通流量及 INNER 排除、TCP/UDP/Unwrap、手动选择、关停以及 discovery follower 资格。

## 性能对照

最终二进制使用相同构建参数：`CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -tags with_gvisor -trimpath`。
完整 `advanced-web-loading-bench`，seed 20261001，10 clients × 32 pages，每轮 2880 requests，Docker 内核网络；双方使用相同 1200 秒运行超时，其余场景不变。三组成对运行，候选先／基线先交替；构建和测试完成后再串行测量。

完成三组整轮尝试，第二、第三组构成有效的完整性能对照：

| 组别 | 基线页面总时间（秒） | 候选页面总时间（秒） | 候选变化 |
|---|---:|---:|---:|
| 2 | 2516.031 | 2501.285 | -0.59% |
| 3 | 2561.692 | 2513.228 | -1.89% |

两组有效对照合计页面总时间变化为 **-1.24%**。双方各 640 pages／5760 requests，完整请求、响应体、实际路由及场景覆盖比较均通过。同一 core 的三个候选轮次均通过：合计 960 pages／8640 requests，每轮 5814 项断言全部通过，并各有 520 条 Smart established 日志。

第一组基线发生两个 TLS handshake 60 秒超时（client-8、client-10，origin-019），完成了 2880 个请求记录但 5 项断言失败。这一整组未计算性能比，不使用失败轮的成功请求子集；双方完整记录保留。第二、第三组基线均通过。

另保留一次候选的零请求 URL-test 启动失败记录，以及早期因补充 discovery follower 资格检查而中止的未完成运行日志；均不计入性能对照。

结论：在两组有效完整对照中未观察到性能退化；差异较小，不能证明严格零损失或稳定加速。首次基线整轮失败使本次有效成对样本数为 2，不能表述为“三组双方全部通过”。

原始记录位于同级 benchmark 仓库的 `results/smart-response-20261007/`，包含 summary.json、validation.json、provenance.json、source-preservation.json 以及每轮 JSON、CSV、日志。二进制和 Go 测试日志位于本仓库 `bin/smart-response-validation/`（均为忽略文件）。provenance 已核对最终生产源文件与实际受测二进制构建时的源码指纹一致。


性能范围限制：该场景目标使用 431xx 测试端口，不满足 Alpha 的 443 探测条件，因此测量的是候选状态查询接入对既有 Smart 路径的影响；主动探测的功能由真实 TLS 集成测试验证。它不能证明真实站点探测没有额外网络开销，`/robots.txt` 也未必代表业务页面的访问权限。
