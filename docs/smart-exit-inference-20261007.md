# Alpha 出口地区/ASN 推断接入 Beta Smart

## 范围和来源

基线为 Beta `489175f2`（已接入 Alpha HTTP 响应探测【3】）；来源为 Alpha `c3b2cb5fdf868964c1fddd0ba58e1e070f19bf28` 的 `component/smart/exit.go`，以及 Alpha Store 的节点状态读写辅助方法。

Alpha 已在 Beta 的合并历史中，因此这次使用定向源码移植；没有重复 cherry-pick 整个上游提交。出口模块的 47 个 Alpha 函数中，38 个函数体完全相同，8 个作适配，1 个 Alpha 固定节点缓存清理函数省略。增加 Close 和 Active 两个方法。逐函数核对结果保存在 benchmark 的 source-preservation.json。

## 行为

- 按完整目标 hostname 聚合，避免扩散到同域其他服务或同一目标 ASN 的其他域名。
- 聚类依据是节点实际出口国家/地区，以及启用 prefer-asn 且 ASN 初始化成功时的**出口 ASN**。这与 Beta 的**目标 ASN**路由键不同。
- 同一出口 IP 的哈希只计一次；原始 IP 不保存。保留 Alpha 的缺失哈希时回退节点名行为，此时独立出口的判断较弱。
- 同一类至少两个失败出口，加上另一类独立出口访问同目标的 2xx 成功对照，才形成怀疑。证据确认窗口为 10 分钟；没有成功对照的全站故障不形成地区/ASN 限制。
- 只汇聚地区不可用、普通 403、源站错误；429、挑战页和其他拒绝不作为地区/ASN 证据。成功对照仅来自 HTTP 主动探测的 2xx，普通流量不伪造 HTTP 成功。
- 出口信息有效期为 6 小时，查询失败后间隔 15 分钟重试。出口探测最多全局 2 个并发，超时 8 秒，与 HTTP 探测共享全局频率预算。
- HTTPS 连接建立后异步学习实际使用节点的出口；HTTP 证据到达时也可触发学习。未知出口的证据暂存，信息返回后再汇聚；不会阻塞业务连接等待出口查询。
- 在【3】单节点 HTTP 限制之后筛选候选，覆盖 Unwrap、TCP、UDP、最佳节点缓存、探索和发现 follower 的候选资格。Beta 评分、质量统计和探索机制保持原实现，地区推断不增加 FailedCount。
- 有正常健康候选时推迟可疑类；全体可用候选均被推迟时允许一个受租约约束的兜底节点。UDP 试探只选择支持 UDP 的节点；优先健康节点并保留 provider 健康恢复流程。
- 限制默认持续 30 分钟；到期进入最多 6 小时的半开恢复阶段，按类限制试探出口。8 秒租约约束的是节点身份，不是严格的一条连接；并发请求可能使用同一个试探节点。2xx 恢复清除该目标对应类的怀疑。
- 地区兜底不会绕过【3】仍然有效的单节点限制；这些节点须经已有 HTTP 后台恢复或状态过期重新获得资格。
- 手动选择不受候选推断影响。不关闭既有连接，不接入 Alpha 的固定节点策略。

## 必要适配

1. ExitWatcher 增加探测取消、准入关闭、等待退出和节点成员校验，避免配置关闭/节点移除后的迟到结果写入。
2. 半开状态允许过期后的试探参与正常候选选择，即使其他地区仍可用；修正限制期间已用过兜底租约后到期丢失恢复阶段的边界。
3. 未知出口暂存的相反结果只保留最新结果，避免旧成功对照在新失败之后重放，或者旧失败在恢复之后重放。
4. Store 节点状态移植到 Beta 不可变队列。增加每个 Store 的最新节点状态视图，保证队列已取出但尚未落盘时合并更新不丢字段；按组/配置/全量清理时同步清除。用阻塞数据库写入的并发测试复现并验证了这个适配点。

## 文件

生产改动：

- `component/smart/exit.go`：Alpha 完整出口模块及上述适配，取代原 `exit_probe.go` 的解析辅助代码。
- `component/smart/node_state.go`：Alpha 节点状态读写方法及 Beta 队列适配。
- `component/smart/stats.go`：NodeState 增加出口地区、ASN、IP 哈希、更新时间和失败时间字段。
- `component/smart/common.go`：Store 最新节点状态视图及清理接入。
- `adapter/outboundgroup/smart.go`：初始化 watcher、ASN 成功开关、关闭等待。
- `adapter/outboundgroup/smart_response.go`：HTTP 证据接入、候选筛选、有限兜底。
- `adapter/outboundgroup/smart_route.go`：HTTPS 连接建立后的异步出口学习入口。

测试：`component/smart/exit_test.go`、`adapter/outboundgroup/smart_exit_test.go`。

没有修改 Beta 路由表评分公式、ProbeCoordinator、provider、通用 adapter 的 HTTP/出口请求实现，也没有增加配置字段。此次公共组件改动局限于 Smart 包的 Store 和 NodeState，使用出口探测的仍是 Smart 分组。

## 已完成验证

- `go test -tags with_gvisor ./...`：通过。
- `go test -race -count=2 ./component/smart ./adapter/outboundgroup ./adapter/provider`：通过。
- 回归覆盖出口去重、独立成功对照、全站故障、证据过期、限制和半开恢复、出口 ASN 开关、重启持久化、队列落盘中的并发合并、清理、迟到结果、未知出口重放、相反证据时序、HTTP 封禁不被绕过、TCP/UDP 缓存选路、手动选择及不污染质量计数。

## 性能验证结果

2026-10-07 在用户确认另一项 benchmark 完成后，已执行三组串行、交替顺序的完整 advanced-web-loading-bench。对照为 Beta `489175f2` 与实现提交 `ff468268`，两版均使用 Smart；不是 Oracle/OPT 对照。

两版构建参数一致：`CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -tags with_gvisor -trimpath`。场景和种子 20261001 相同，两版超时均为 1200 秒；测量期间未运行本任务的编译或 Go 测试。

| 组 | 顺序 | 基线页面总耗时（秒） | 候选页面总耗时（秒） | 候选变化 |
| --- | --- | ---: | ---: | ---: |
| 1 | 候选→基线 | 2512.760 | 2793.541 | +11.17% |
| 2 | 基线→候选 | 2601.578 | 2503.074 | -3.79% |
| 3 | 候选→基线 | 2601.524 | 2511.149 | -3.47% |

三组合计，基线为 7715.862 秒，候选为 7807.765 秒，候选 **高 1.19%**。这些值是各页面耗时之和，不是并发运行的墙钟时间。

六轮全部成功；每轮覆盖 320 个页面、2,880 个请求和 5,814 条断言，总计 1,920 个页面、17,280 个请求、34,884 条通过的断言。完整 workload、请求/页面成员、响应/路由断言、环境、源码和二进制哈希、benchmark harness 哈希均已核对。三组结果全部保留，没有删除第一组明显变慢的结果，也没有只比较成功子集。

**结果方向不一致，不能确认性能无损，也不能据此判定存在稳定退化。** 第一组候选慢 11.17%，后两组分别快 3.79% 和 3.47%；平均差异为 +1.19%。

实际选路差异较大：第一组基线全部使用 SG 节点，候选 2,879/2,880 个请求使用 JP 节点；第二组两版均混用 JP/SG；第三组基线主要使用 JP，候选全部使用 SG。Smart 的运行时竞争探测会影响后续选路，固定 workload seed 并不保证两版选择相同出口。出口差异会影响页面耗时，这份数据不能独立量化代码开销，也没有证明这些选路差异由新增功能造成。详细计数保存在 route-usage.json。

此前唯一启动失败因另一项测试占用 43200 端口，发出请求数为 0；excluded-pre-final-port-conflict.json 保留了原始记录，排除于性能比较，且使用的不是最终候选二进制。恢复后的六轮没有失败或重试。

完整场景使用 431xx 目标端口，不触发 HTTPS 出口/HTTP 查询；它衡量该功能休眠时的 Smart 选路表现。实际触发与恢复由针对性测试验证，不能把此 benchmark 解释为已测量活跃出口查询对真实网站的性能影响。

二进制和测试日志：本仓库 `bin/smart-exit-validation/`。完整 JSON、CSV、逐轮日志、summary.json、validation.json、provenance.json、source-preservation.json、route-usage.json 和运行脚本：同级 `mihomo-benchmark/results/smart-exit-20261007/`（均为忽略文件）。
