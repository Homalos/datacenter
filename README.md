# Homalos 期货数据中心

## 🚀 快速启动

### 方式一：Web 控制面板（推荐）⭐

```bash
# 1. 激活虚拟环境并启动 Web 服务
.venv\Scripts\activate
python start_web.py

# 2. 访问 Web 控制面板
http://localhost:8000/dashboard

# 3. 在 Web 界面中点击"启动数据中心"按钮
```

**优势**：
- ✅ 支持 Web 界面启动/停止
- ✅ 实时查看日志流
- ✅ 监控系统健康状态
- ✅ 更灵活的控制方式

### 方式二：直接启动（已废弃）

```bash
# ⚠️ 已废弃，仅供参考
# python start_datacenter.py.deprecated
```

**说明**：推荐使用 Web 控制面板方式启动。旧的 `start_datacenter.py` 已重命名为 `.deprecated` 后缀，保留作为备份。

---

## 📋 实施路线图

### 第1-2周：核心模块实现
- [ ] K线合成器（BarGenerator）
- [ ] 合约管理器（ContractManager）
- [ ] 数据存储优化（批量写入）

### 第3周：系统管理模块
- [ ] 数据中心启动器（DataCenterStarter）
- [ ] 闹钟调度器（AlarmScheduler）

### 第4周：监控和API扩展
- [ ] 监控指标采集
- [ ] 告警机制
- [ ] API接口扩展

### 第5-6周：优化和测试
- [ ] 性能优化
- [ ] 压力测试
- [ ] 文档完善

## 🏗️ 系统架构概览

```
┌─────────────────────────────────────────────────────────┐
│                    数据中心启动器                          │
│   进程管理 | 配置加载 | 模块依赖 | 信号处理                │
└─────────────────────────────────────────────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
┌───────▼──────┐  ┌────────▼────────┐  ┌──────▼───────┐
│  事件总线     │  │  闹钟调度器      │  │  合约管理器   │
│  EventBus    │  │  AlarmScheduler │  │ ContractMgr  │
└───────┬──────┘  └────────┬────────┘  └──────┬───────┘
        │                  │                  │
        └──────────────────┼──────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
┌───────▼──────┐  ┌────────▼────────┐  ┌──────▼───────┐
│  行情网关     │  │  K线合成器       │  │  数据存储     │
│MarketGateway │  │  BarGenerator   │  │DataStorage   │
└──────────────┘  └─────────────────┘  └──────────────┘
                           │
                  ┌────────▼────────┐
                  │   Web服务        │
                  │  FastAPI Server │
                  └─────────────────┘
```

## 📊 核心功能

### ✅ 已实现
- [x] 事件总线（EventBus）
- [x] 行情网关（MarketGateway）
- [x] 数据存储（DataStorage）
- [x] Web API服务（FastAPI）

### 🚧 开发中
- [ ] K线合成器（BarGenerator）
- [ ] 合约管理器（ContractManager）
- [ ] 数据中心启动器（DataCenterStarter）
- [ ] 闹钟调度器（AlarmScheduler）

### 📅 计划中
- [ ] 监控指标采集
- [ ] 告警机制
- [ ] WebSocket实时推送
- [ ] 更多数据源支持

## 📁 配置文件说明

| 文件 | 说明 |
|-----|------|
| `config/data_center.yaml` | 数据中心主配置 |
| `config/brokers.yaml` | CTP服务器配置 |
| `config/instrument_exchange.json` | 所有合约（800+个，全部自动订阅） |
| `config/log_config.yaml` | 日志配置 |
| `config/extra.dev.yaml` | 开发环境配置 |
| `config/extra.prod.yaml` | 生产环境配置 |

## 🔧 技术栈

- **语言**: Python 3.13+
- **事件驱动**: 自研EventBus（多队列、高性能）
- **数据格式**: Parquet（列式存储、高压缩比）
- **Web框架**: FastAPI（异步、高性能）
- **日志**: Python logging
- **配置**: YAML + JSON
- **CTP接口**: 自封装C++ API

## 📈 性能指标

- **Tick处理**: 1000条/秒+
- **事件延迟**: <100ms
- **数据零丢失**: ✅
- **7x24小时运行**: ✅

## 🤝 贡献指南

欢迎贡献代码、文档或提出建议！

1. Fork项目
2. 创建特性分支
3. 提交代码
4. 创建Pull Request

详见 [开发指南.md](./开发指南.md)

## 📞 联系方式

- **GitHub**: https://github.com/homalos
- **Issues**: 报告bug或提出建议
- **Discussion**: 讨论技术问题

---

**版本**: v0.1.0  
**更新时间**: 2025-10-27  
**维护者**: Homalos Team

