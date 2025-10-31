<template>
  <n-button
    circle
    size="large"
    type="primary"
    class="theme-toggle"
    @click="handleToggle"
    :title="isDark ? '切换到浅色模式' : '切换到暗黑模式'"
  >
    {{ themeIcon }}
  </n-button>
</template>

<script setup lang="ts">
import { storeToRefs } from 'pinia'
import { useThemeStore } from '@/stores/theme'
import { useMessage } from 'naive-ui'

const themeStore = useThemeStore()
const { isDark, themeIcon } = storeToRefs(themeStore)
const message = useMessage()

function handleToggle() {
  themeStore.toggleTheme()
  const text = isDark.value ? '已切换到暗黑模式 🌙' : '已切换到浅色模式 ☀️'
  message.success(text)
}
</script>

<style scoped>
.theme-toggle {
  position: fixed;
  bottom: 20px;
  right: 20px;
  width: 56px;
  height: 56px;
  font-size: 24px;
  z-index: 1000;
  box-shadow: 0 4px 12px rgba(59, 130, 246, 0.4);
  transition: transform 0.2s, box-shadow 0.2s;
}

.theme-toggle:hover {
  transform: translateY(-2px) scale(1.05);
  box-shadow: 0 6px 20px rgba(59, 130, 246, 0.5);
}

.theme-toggle:active {
  transform: translateY(0) scale(1);
}

@media (max-width: 640px) {
  .theme-toggle {
    width: 48px;
    height: 48px;
    bottom: 15px;
    right: 15px;
    font-size: 20px;
  }
}
</style>

