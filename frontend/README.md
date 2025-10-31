# Homalos 数据中心 - Web 控制面板

基于 Vue 3 + TypeScript + Vite + Pinia + Naive UI 的现代化 Web 控制面板。

## 🚀 技术栈

- **Vue 3.4+** - 渐进式 JavaScript 框架（Composition API）
- **TypeScript 5.0+** - 类型安全的 JavaScript 超集
- **Vite 5.0+** - 下一代前端构建工具
- **Pinia 2.0+** - Vue 官方状态管理库
- **Naive UI 2.38+** - 优雅的 Vue 3 组件库（支持暗黑模式）
- **Axios** - HTTP 客户端

## 📦 安装依赖

```bash
cd frontend
npm install
```

## 🛠️ 开发模式

启动开发服务器（热重载）：

```bash
npm run dev
```

开发服务器将运行在 `http://localhost:5173`，API 请求会自动代理到后端 `http://127.0.0.1:8000`。

## 🏗️ 生产构建

构建生产版本：

```bash
npm run build
```

构建后的文件将输出到 `../static` 目录（FastAPI 静态文件目录）。

## 📂 项目结构

```
frontend/
├── src/
│   ├── api/                  # API 服务层
│   │   └── datacenter.ts     # 数据中心 API
│   ├── components/           # Vue 组件
│   │   ├── StatusCard.vue    # 系统状态卡片
│   │   ├── ModulesCard.vue   # 模块状态卡片
│   │   ├── LogsCard.vue      # 实时日志卡片
│   │   └── ThemeToggle.vue   # 主题切换按钮
│   ├── composables/          # 组合式函数
│   │   └── useLogs.ts        # 日志流管理
│   ├── stores/               # Pinia 状态管理
│   │   ├── datacenter.ts     # 数据中心状态
│   │   └── theme.ts          # 主题状态
│   ├── types/                # TypeScript 类型定义
│   │   └── index.ts          # 全局类型
│   ├── utils/                # 工具函数
│   │   └── format.ts         # 格式化函数
│   ├── App.vue               # 根组件
│   └── main.ts               # 应用入口
├── index.html                # HTML 模板
├── vite.config.ts            # Vite 配置
├── tsconfig.json             # TypeScript 配置
└── package.json              # 依赖管理
```

## ✨ 功能特性

- ✅ **实时状态监控** - 每 3 秒自动刷新数据中心状态
- ✅ **数据中心控制** - 启动/停止/重启数据中心服务
- ✅ **SSE 日志流** - 实时推送日志到 Web 界面
- ✅ **暗黑模式** - 支持浅色/暗黑主题切换（自动检测系统主题）
- ✅ **响应式布局** - 完美适配桌面、平板、手机
- ✅ **模块状态** - 实时显示各模块运行状态
- ✅ **自动滚动** - 日志自动滚动到底部（可切换）
- ✅ **类型安全** - TypeScript 全覆盖

## 🎯 开发指南

### 添加新功能

1. **API 服务** - 在 `src/api/datacenter.ts` 中添加 API 方法
2. **类型定义** - 在 `src/types/index.ts` 中添加接口类型
3. **状态管理** - 在 `src/stores/` 中添加或更新状态
4. **组件开发** - 在 `src/components/` 中创建 Vue 组件

### 代码规范

- 使用 **Composition API** 编写组件
- 使用 **TypeScript** 进行类型标注
- 使用 **Pinia** 进行状态管理
- 组件使用 **单文件组件 (SFC)** 格式

## 🔗 相关链接

- [Vue 3 官方文档](https://cn.vuejs.org/)
- [Vite 官方文档](https://cn.vitejs.dev/)
- [Pinia 官方文档](https://pinia.vuejs.org/zh/)
- [Naive UI 官方文档](https://www.naiveui.com/zh-CN/)
- [TypeScript 官方文档](https://www.typescriptlang.org/zh/)

## 📝 注意事项

1. **后端依赖** - 确保后端 FastAPI 服务已启动（默认 `http://127.0.0.1:8000`）
2. **Node.js 版本** - 推荐使用 Node.js 18+ 或 20+
3. **开发代理** - 开发模式下 API 会自动代理到后端
4. **生产构建** - 构建后的文件会直接输出到 `../static` 目录

## 🐛 故障排查

### 开发服务器无法启动

```bash
# 清理依赖重新安装
rm -rf node_modules package-lock.json
npm install
```

### API 请求失败

1. 检查后端服务是否启动
2. 检查 `vite.config.ts` 中的代理配置
3. 检查浏览器控制台是否有 CORS 错误

### 构建失败

```bash
# 检查 TypeScript 错误
npm run build
```

