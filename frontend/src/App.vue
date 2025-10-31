<template>
  <n-config-provider :theme="theme" :theme-overrides="themeOverrides">
    <n-message-provider>
      <n-dialog-provider>
        <n-global-style />
        <div class="app-root" :class="{ 'app-dark': isDark }">
          <!-- 导航栏 -->
          <NavBar />

          <!-- 主要内容 -->
          <main class="app-main">
            <div class="app-container">
              <router-view />
            </div>
          </main>

          <!-- 主题切换按钮 -->
          <ThemeToggle />
        </div>
      </n-dialog-provider>
    </n-message-provider>
  </n-config-provider>
</template>

<script setup lang="ts">
import { computed, watch } from 'vue'
import { storeToRefs } from 'pinia'
import { darkTheme, type GlobalThemeOverrides } from 'naive-ui'
import { useThemeStore } from './stores/theme'
import NavBar from './components/NavBar.vue'
import ThemeToggle from './components/ThemeToggle.vue'

const themeStore = useThemeStore()
const { isDark } = storeToRefs(themeStore)

// 初始化主题
themeStore.initTheme()

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

.app-root {
  min-height: 100vh;
  display: flex;
  flex-direction: column;
}

.app-main {
  flex: 1;
  padding: 20px 0;
}

.app-container {
  max-width: 1400px;
  margin: 0 auto;
  padding: 0 20px;
}

/* 响应式 */
@media (max-width: 768px) {
  .app-main {
    padding: 15px 0;
  }
  
  .app-container {
    padding: 0 15px;
  }
}

@media (max-width: 480px) {
  .app-main {
    padding: 10px 0;
  }
  
  .app-container {
    padding: 0 10px;
  }
}
</style>

