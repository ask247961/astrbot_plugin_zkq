# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/lang/zh-CN/).

## [Unreleased]

### Added

- **扫码提取多账号会话**（扩展 ADR-0003 → ADR-0004）：
  - `/zkq 扫码提取` 开启提取会话，每轮提取一个账号；轮间对话门 `/zkq 继续提取` / `/zkq 结束提取`，3 分钟未回复自动结束
  - 会话期间 bot 主流程（顺序/随机/升级时间三种模式）全程让位；会话结束设备自动杀进程重启恢复挂机
  - 提取相关命令改为**仅私聊可用**（防群聊误触发破坏性操作）
- 二维码/村庄图改为**纯推送发图**（App 上传完立刻发，不再等 15 秒轮询）

### Fixed

- **WS 断连日志抛 `AttributeError: close_reason`**（服务端 `WebSocketResponse` 无该属性，每次断连刷一条 traceback）
- 轮询任务竞态（旧任务被新会话取代后不再误删新会话状态、不再向新会话发旧消息）；轮询到会话上限不再静默消失而是通知用户；村庄截图不再被旧二维码图顶掉

## [0.2.1] - 2026-08-04

### Changed

- 支持平台回退为 QQ 系（qq_official / qq_official_webhook / aiocqhttp）：
  其他平台（微信系/Telegram/Discord 等）的用户 ID 体系与 QQ 号不兼容，
  白名单、文件发送等能力无法通用，暂不开放

## [0.2.0] - 2026-08-03

### Added

- **WS 长连接通道**（通道 B'）：设备实时查询、按需日志/截图拉取；握手 HTTP 层鉴权（403 未授权 / 409 设备名冲突）；同设备重连替换；30s 心跳
- **查询协议**：`status / upgrade / logs / errors / ping / screenshot / logfile / clearlogs`，`requestId` 关联请求响应，10s 超时
- **日志能力**：
  - 设备日志改为按天滚动文件（`info-20260803.log`），保留天数由插件**统一下发**（零逐台配置）
  - 文本查看（跨天合并取尾）、按范围打包 zip 发送（行数/小时/天/全量）
  - 对话删除日志（`zkq_clear_logs`，仅私聊可用）
- **截图能力**：设备 PNG 无损原图 → zip 打包 → QQ 文件发送（不被二次压缩）
- **LLM 工具**：`zkq_status` / `zkq_server_status` / `zkq_logs_file` / `zkq_screenshot` / `zkq_clear_logs`，AI 对话直接调用；工具描述内置"直接执行不解释"行为约束
- **服务器状态查询**：CPU / 内存 / 磁盘 / 开机时长 / AstrBot 版本（psutil）
- **图片卡片回复**：`reply_style=image` 状态类回复渲染卡片图（PIL 本地渲染，无外部依赖）；`markdown` / `text` 可切换
- **设备改名自动迁移**：快照/WS 按 nonce 识别改名，自动清理旧设备名与旧连接
- **白名单策略**：私聊始终放行（仅主人能私聊），`allowed_qq` 只约束群聊
- **插件 logo**（复用 App 图标）

### Changed

- **WS 连接稳定性**：服务端关闭 permessage-deflate 压缩协商（`compress=False`），修复与 okhttp 客户端偶发的 `Received frame with non-zero reserved bits` 协议错误导致的断连
- **关键词动态化**：装饰器内置全量关键词，内部按配置 `keywords` 复核；未命中不消费消息
- **配置面板文案白话化**，去除内部技术细节
- 日志查询默认行数 200、单条回复上限 4000（QQ 平台上限内）

### Fixed

- 快照/日志/截图上传端点对非法请求返回明确错误而非 500
- 查询失败时输出诊断日志（连接状态/发送失败/超时原因）

## [0.1.0] - 2026-08-02

### Added

- 初始版本（阶段 A）：HTTP 快照接收 `POST /zkq_snapshot`（token 校验、设备名冲突检测、`recommended_interval` 下发、落盘持久化、7 天过期清理）
- `/zkq` 前缀命令：设备列表 / 状态 / 全部 / 帮助
- 高置信关键词被动回复（`现在跑哪个号`、`当前账号`、`升级还有多久`、`下次升级`）
- 群聊 `allowed_qq` 白名单
- 快照表持久化 `snapshots.json`（重启不丢）
