<template>
  <n-config-provider :theme="theme" :theme-overrides="themeOverrides">
    <n-message-provider>
      <n-dialog-provider>
        <n-global-style />
        <div class="app-container" :class="{ 'app-dark': isDark }">
          <!-- Header -->
          <header class="app-header">
            <h1>Homalos 数据中心</h1>
            <p>期货行情数据采集与管理系统 - Web控制面板</p>
          </header>

          <!-- Main Content -->
          <main class="app-main">
            <n-grid :cols="2" :x-gap="20" :y-gap="20" responsive="screen" :collapsed-rows="1">
              <!-- 状态卡片 -->
              <n-gi>
                <StatusCard />
              </n-gi>

              <!-- 模块卡片 -->
              <n-gi>
                <ModulesCard />
              </n-gi>

              <!-- 日志卡片（占满整行） -->
              <n-gi :span="2">
                <LogsCard />
              </n-gi>
            </n-grid>
          </main>

          <!-- 主题切换按钮 -->
          <ThemeToggle />
        </div>
      </n-dialog-provider>
    </n-message-provider>
  </n-config-provider>
</template>

<script setup lang="ts">
import { computed, onMounted, watch } from 'vue'
import { storeToRefs } from 'pinia'
import { darkTheme, type GlobalThemeOverrides } from 'naive-ui'
import { useThemeStore } from './stores/theme'
import { useDatacenterStore } from './stores/datacenter'
import StatusCard from './components/StatusCard.vue'
import ModulesCard from './components/ModulesCard.vue'
import LogsCard from './components/LogsCard.vue'
import ThemeToggle from './components/ThemeToggle.vue'

const themeStore = useThemeStore()
const datacenterStore = useDatacenterStore()
const { isDark } = storeToRefs(themeStore)

// 主题配置
const theme = computed(() => isDark.value ? darkTheme : null)

// 监听主题变化，动态切换 body class
watch(isDark, (newValue) => {
  if (newValue) {
    document.body.classList.add('dark-body')
  } else {
    document.body.classList.remove('dark-body')
  }
}, { immediate: true })

const themeOverrides: GlobalThemeOverrides = {
  common: {
    primaryColor: '#3b82f6',
    primaryColorHover: '#2563eb',
    primaryColorPressed: '#1d4ed8',
    successColor: '#10b981',
    warningColor: '#f59e0b',
    errorColor: '#ef4444',
    infoColor: '#3b82f6'
  }
}

// 初始化
onMounted(() => {
  themeStore.initTheme()
  datacenterStore.fetchStatus()

  // 每3秒刷新一次状态
  setInterval(() => {
    datacenterStore.fetchStatus()
  }, 3000)
})
</script>

<style>
* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}

body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Microsoft YaHei', sans-serif;
  background: #f5f5f7;
  min-height: 100vh;
  transition: background-color 0.3s ease;
}

/* 暗黑模式下的 body 背景 */
body.dark-body {
  background: #0f172a;
}

.app-container {
  max-width: 1400px;
  margin: 0 auto;
  padding: 20px;
}

.app-header {
  text-align: center;
  margin-bottom: 30px;
}

.app-header h1 {
  font-size: clamp(1.5rem, 4vw, 2.5rem);
  margin-bottom: 10px;
  font-weight: 700;
  letter-spacing: -0.5px;
  color: #1d1d1f;
  transition: color 0.3s ease;
}

.app-header p {
  font-size: clamp(0.875rem, 2vw, 1.125rem);
  color: #6e6e73;
  font-weight: 400;
  transition: color 0.3s ease;
}

/* 暗黑模式下的标题和描述 */
.app-dark .app-header h1 {
  color: #f1f5f9;
}

.app-dark .app-header p {
  color: #cbd5e1;
}

/* 响应式 */
@media (max-width: 1024px) {
  .app-main :deep(.n-grid) {
    grid-template-columns: 1fr !important;
  }
}

@media (max-width: 768px) {
  .app-container {
    padding: 15px;
  }

  .app-header h1 {
    font-size: 2em;
  }

  .app-header p {
    font-size: 1em;
  }
}

@media (max-width: 640px) {
  .app-container {
    padding: 10px;
  }

  .app-header h1 {
    font-size: 1.8em;
  }
}

@media (max-width: 480px) {
  .app-container {
    padding: 8px;
  }

  .app-header {
    margin-bottom: 20px;
  }

  .app-header h1 {
    font-size: 1.5em;
    margin-bottom: 8px;
  }

  .app-header p {
    font-size: 0.9em;
  }
}
</style>

