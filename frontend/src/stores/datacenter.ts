/**
 * 数据中心状态管理
 */
import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import { datacenterApi } from '@/api/datacenter'
import type { ServiceState, LogEntry } from '@/types'

export const useDatacenterStore = defineStore('datacenter', () => {
  // 状态
  const state = ref<ServiceState>({
    status: 'stopped',
    uptime_seconds: 0,
    modules: {}
  })
  const logs = ref<LogEntry[]>([])
  const loading = ref(false)
  const autoScroll = ref(true)

  // 计算属性
  const isRunning = computed(() => state.value.status === 'running')
  const isStarting = computed(() => state.value.status === 'starting')
  const isStopped = computed(() => state.value.status === 'stopped')

  const statusText = computed(() => {
    const map: Record<string, string> = {
      'stopped': '⚫ 未启动',
      'starting': '🟡 启动中',
      'running': '🟢 运行中',
      'stopping': '🟠 停止中',
      'error': '🔴 错误'
    }
    return map[state.value.status] || state.value.status
  })

  const uptimeText = computed(() => {
    const seconds = state.value.uptime_seconds
    if (!seconds) return '0秒'

    const days = Math.floor(seconds / 86400)
    const hours = Math.floor((seconds % 86400) / 3600)
    const minutes = Math.floor((seconds % 3600) / 60)
    const secs = seconds % 60

    let result = ''
    if (days > 0) result += `${days}天 `
    if (hours > 0) result += `${hours}小时 `
    if (minutes > 0) result += `${minutes}分 `
    result += `${secs}秒`

    return result
  })

  const startTimeText = computed(() => {
    if (!state.value.start_time) return '-'
    return new Date(state.value.start_time).toLocaleString('zh-CN')
  })

  // 方法
  async function fetchStatus() {
    try {
      const data = await datacenterApi.getStatus()
      state.value = data
    } catch (error) {
      console.error('获取状态失败:', error)
    }
  }

  async function start() {
    loading.value = true
    try {
      await datacenterApi.start()
      await fetchStatus()
      return { success: true, message: '数据中心启动命令已发送' }
    } catch (error: any) {
      return { success: false, message: error.response?.data?.message || '启动失败' }
    } finally {
      loading.value = false
    }
  }

  async function stop() {
    loading.value = true
    try {
      await datacenterApi.stop()
      await fetchStatus()
      return { success: true, message: '数据中心停止命令已发送' }
    } catch (error: any) {
      return { success: false, message: error.response?.data?.message || '停止失败' }
    } finally {
      loading.value = false
    }
  }

  async function restart() {
    loading.value = true
    try {
      await datacenterApi.restart()
      await fetchStatus()
      return { success: true, message: '数据中心重启命令已发送' }
    } catch (error: any) {
      return { success: false, message: error.response?.data?.message || '重启失败' }
    } finally {
      loading.value = false
    }
  }

  function addLog(log: LogEntry) {
    logs.value.push(log)
    // 限制日志数量
    if (logs.value.length > 500) {
      logs.value.shift()
    }
  }

  function clearLogs() {
    logs.value = []
  }

  function toggleAutoScroll() {
    autoScroll.value = !autoScroll.value
  }

  return {
    state,
    logs,
    loading,
    autoScroll,
    isRunning,
    isStarting,
    isStopped,
    statusText,
    uptimeText,
    startTimeText,
    fetchStatus,
    start,
    stop,
    restart,
    addLog,
    clearLogs,
    toggleAutoScroll
  }
})

