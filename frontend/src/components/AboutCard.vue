<template>
  <n-card title="关于" hoverable :loading="loading">
    <n-space vertical :size="20">
      <!-- 项目信息 -->
      <div v-if="aboutInfo">
        <n-text strong style="font-size: 18px; display: block; margin-bottom: 10px">
          🚀 {{ aboutInfo.name }}
        </n-text>
        <n-text depth="3">
          {{ aboutInfo.description }}
        </n-text>
      </div>

      <n-divider style="margin: 10px 0" />

      <!-- 版本信息 -->
      <n-descriptions v-if="aboutInfo" label-placement="left" bordered :column="1" size="small">
        <n-descriptions-item label="版本">
          <n-tag type="info" size="small">v{{ aboutInfo.version }}</n-tag>
        </n-descriptions-item>
        <n-descriptions-item label="作者">
          {{ aboutInfo.author }}
        </n-descriptions-item>
        <n-descriptions-item label="版权">
          <n-text style="font-size: 12px">{{ aboutInfo.copyright }}</n-text>
        </n-descriptions-item>
        <n-descriptions-item v-if="aboutInfo.contact" label="联系方式">
          <n-button text tag="a" :href="aboutInfo.contact" target="_blank" type="primary" size="small">
            {{ aboutInfo.contact }}
          </n-button>
        </n-descriptions-item>
        <n-descriptions-item v-if="aboutInfo.user_guide" label="用户指南">
          <n-button text tag="a" :href="aboutInfo.user_guide" target="_blank" type="primary" size="small">
            查看指南
          </n-button>
        </n-descriptions-item>
        <n-descriptions-item label="时区">
          {{ aboutInfo.timezone }}
        </n-descriptions-item>
      </n-descriptions>

      <n-divider style="margin: 10px 0" />

      <!-- 技术栈 -->
      <div v-if="aboutInfo && aboutInfo.technology_stack.length > 0">
        <n-text strong style="display: block; margin-bottom: 10px">
          💻 技术栈
        </n-text>
        <n-space vertical :size="8">
          <n-text v-for="(tech, index) in aboutInfo.technology_stack" :key="index" depth="3">
            {{ tech }}
          </n-text>
        </n-space>
      </div>

      <n-divider style="margin: 10px 0" />

      <!-- 功能特性 -->
      <div>
        <n-text strong style="display: block; margin-bottom: 10px">
          ✨ 主要特性
        </n-text>
        <n-space vertical :size="8">
          <n-text depth="3">📊 实时行情数据采集与存储</n-text>
          <n-text depth="3">📈 Tick/K线数据管理</n-text>
          <n-text depth="3">🔄 多交易网关支持</n-text>
          <n-text depth="3">📝 实时日志流监控</n-text>
          <n-text depth="3">🎨 暗黑/浅色主题切换</n-text>
          <n-text depth="3">📱 响应式设计</n-text>
        </n-space>
      </div>

      <n-divider style="margin: 10px 0" />

      <!-- 系统信息 -->
      <div>
        <n-text strong style="display: block; margin-bottom: 10px">
          💻 系统信息
        </n-text>
        <n-space vertical :size="8">
          <div style="display: flex; justify-content: space-between">
            <n-text depth="3">后端地址</n-text>
            <n-text>{{ backendUrl }}</n-text>
          </div>
          <div style="display: flex; justify-content: space-between">
            <n-text depth="3">前端端口</n-text>
            <n-text>{{ frontendPort }}</n-text>
          </div>
          <div style="display: flex; justify-content: space-between">
            <n-text depth="3">当前主题</n-text>
            <n-tag :type="isDark ? 'warning' : 'info'" size="small">
              {{ isDark ? '暗黑模式' : '浅色模式' }}
            </n-tag>
          </div>
        </n-space>
      </div>

      <n-divider style="margin: 10px 0" />

      <!-- 链接 -->
      <div>
        <n-text strong style="display: block; margin-bottom: 10px">
          🔗 相关链接
        </n-text>
        <n-space>
          <n-button text tag="a" href="/docs" target="_blank" type="primary">
            <template #icon>
              <n-icon><BookIcon /></n-icon>
            </template>
            API 文档
          </n-button>
          <n-button text tag="a" href="/health" target="_blank" type="success">
            <template #icon>
              <n-icon><HeartIcon /></n-icon>
            </template>
            健康检查
          </n-button>
          <n-button text tag="a" href="/metrics/summary" target="_blank" type="info">
            <template #icon>
              <n-icon><StatsIcon /></n-icon>
            </template>
            监控指标
          </n-button>
        </n-space>
      </div>

      <!-- 版权信息 -->
      <n-divider style="margin: 10px 0" />
      <n-text v-if="aboutInfo" depth="3" style="text-align: center; display: block; font-size: 12px">
        {{ aboutInfo.copyright || '© 2025 Homalos 数据中心' }} | Powered by FastAPI & Vue 3
      </n-text>
    </n-space>
  </n-card>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { storeToRefs } from 'pinia'
import { useThemeStore } from '@/stores/theme'
import { useMessage } from 'naive-ui'
import axios from 'axios'
import type { AboutInfo } from '@/types'
import { BookOutline as BookIcon, HeartOutline as HeartIcon, StatsChartOutline as StatsIcon } from '@vicons/ionicons5'

const themeStore = useThemeStore()
const { isDark } = storeToRefs(themeStore)
const message = useMessage()

// 关于信息状态
const aboutInfo = ref<AboutInfo | null>(null)
const loading = ref(false)

// 获取关于信息
async function fetchAboutInfo() {
  loading.value = true
  try {
    const response = await axios.get('/about')
    if (response.data.success) {
      aboutInfo.value = response.data.data
    } else {
      message.error(response.data.message || '获取关于信息失败')
      // 使用默认值
      aboutInfo.value = response.data.data
    }
  } catch (error) {
    console.error('获取关于信息失败:', error)
    message.error('无法连接到服务器')
    // 使用默认值
    aboutInfo.value = {
      name: 'Homalos 数据中心',
      description: '期货行情数据采集与管理系统',
      version: '0.3.0',
      author: 'Homalos Team',
      copyright: 'Copyright © 2025 Homalos Team',
      contact: '',
      user_guide: '',
      timezone: 'Asia/Shanghai',
      technology_stack: [
        '后端：Python 3.13 + FastAPI',
        '前端：Vue 3 + Naive UI + Vite',
        '数据库：DuckDB'
      ],
      enable: true,
      debug: false
    }
  } finally {
    loading.value = false
  }
}

// 系统信息
const backendUrl = computed(() => {
  if (import.meta.env.DEV) {
    return 'http://127.0.0.1:8001'
  }
  return window.location.origin
})

const frontendPort = computed(() => {
  if (import.meta.env.DEV) {
    return '5173 (开发)'
  }
  return '8001 (生产)'
})

// 组件挂载时获取数据
onMounted(() => {
  fetchAboutInfo()
})
</script>

<style scoped>
/* 自定义样式可以在这里添加 */
</style>

