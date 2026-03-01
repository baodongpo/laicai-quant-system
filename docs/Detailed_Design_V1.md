# 来财 (LaiCai) V1.0 详细设计文档

## 1. 项目愿景与定位
**来财** 是一款为高级个人投资者设计的量化辅助系统。
- **定位**：纯侧路辅助，零自动化交易。
- **核心价值**：数据反幻觉（双路校验）、全历史 K 线离线化、趋势指标（RSI/BOLL）精准预警。

## 2. 系统架构设计

### 2.1 混合动力架构
- **Orchestrator (Node.js/TS)**: 负责任务编排、Discord 交互、AI 代理逻辑。
- **Data Service (Python)**: 负责数据抓取 (AkShare/Requests)、量化计算 (Pandas)、安全节拍控制。
- **Storage (SQLite)**: 本地化 K 线库，实现数据主权。

### 2.2 数据中心逻辑 (Module 1)
- **实时通道**：腾讯财经 HTTPS + QVeris AI 注入（针对 Watchlist 核心股）。
- **历史通道**：AkShare + 广州 10808 代理（针对全量 K 线拖库）。
- **同步引擎**：
    - **双向指针**：`forward_cursor` (向后补齐) 与 `backward_cursor` (往前深挖)。
    - **容灾机制**：带 PID 锁的 `global_locks` 与心跳检测（Heartbeat），支持 Core 掉后的自动现场恢复。

## 3. 数据库 Schema (SQLite)

### 3.1 `stocks_registry` (注册表)
存储股票配置（开关、买卖区间）。

### 3.2 `sync_state` (状态机)
管理 `daily`/`60m`/`15m` 维度的同步进度、PID 锁和心跳。

### 3.3 `daily_candles` (K 线库)
存储前复权历史数据（Date, O, H, L, C, V, Amount, Turnover）。

## 4. 目录结构与规范

```text
laicai/
├── docs/               # 详细设计、API 文档、架构图
├── core/               # Node.js 主逻辑 (TypeScript)
├── data-svc/           # Python 数据服务
│   ├── scripts/        # 独立运行的同步/计算脚本
│   └── lib/            # 公用数据抓取/逻辑组件
├── config/             # 配置文件 (*.template.json)
├── data/               # SQLite 数据库、日志文件 (Git 忽略)
├── assets/             # 静态资源、示意图
└── .learnings/         # AI 学习与进化记录
```

### 4.1 代码规范
- **Python**: 遵循 PEP 8，所有数据接口必须包含 `provider` 和 `timestamp` 元数据。
- **TypeScript**: 强类型定义，所有跨进程调用必须有完善的异常处理。
- **AI 友好**: 关键函数必须包含 `Docstring`，解释逻辑意图而非仅仅是代码说明。

## 5. 版本管理策略 (Git)
- **数据与敏感信息隔离**：数据库文件 (`.db`)、包含私密代理信息的 `config.json` 严禁入库。
- **模板化配置**：入库 `watchlist.template.json`，用户本地拷贝为 `watchlist.json`。

---
*更新日期: 2026-03-01*
*负责人: Bobo (波波) & Blade*
