/**
 * 主题状态管理
 */
import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import type { ThemeMode } from '@/types'

export const useThemeStore = defineStore('theme', () => {
  // 状态
  const mode = ref<ThemeMode>('auto')
  const isDark = ref(false)

  // 计算属性
  const themeIcon = computed(() => isDark.value ? '☀️' : '🌙')

  // 初始化主题
  function initTheme() {
    const savedTheme = localStorage.getItem('theme') as ThemeMode | null

    if (savedTheme === 'dark') {
      mode.value = 'dark'
      isDark.value = true
    } else if (savedTheme === 'light') {
      mode.value = 'light'
      isDark.value = false
    } else {
      mode.value = 'auto'
      // 检测系统主题
      isDark.value = window.matchMedia('(prefers-color-scheme: dark)').matches
    }

    // 监听系统主题变化
    if (mode.value === 'auto') {
      const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)')
      mediaQuery.addEventListener('change', (e) => {
        if (mode.value === 'auto') {
          isDark.value = e.matches
        }
      })
    }
  }

  // 切换主题
  function toggleTheme() {
    if (isDark.value) {
      // 切换到浅色
      mode.value = 'light'
      isDark.value = false
      localStorage.setItem('theme', 'light')
    } else {
      // 切换到暗黑
      mode.value = 'dark'
      isDark.value = true
      localStorage.setItem('theme', 'dark')
    }
  }

  return {
    mode,
    isDark,
    themeIcon,
    initTheme,
    toggleTheme
  }
})

