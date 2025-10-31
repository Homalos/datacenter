/**
 * 日志流管理 Composable
 */
import { ref, onMounted, onUnmounted } from 'vue'
import { datacenterApi } from '@/api/datacenter'
import { useDatacenterStore } from '@/stores/datacenter'
import type { LogEntry } from '@/types'

export function useLogs() {
  const datacenterStore = useDatacenterStore()
  const eventSource = ref<EventSource | null>(null)
  const connected = ref(false)

  // 连接日志流
  function connect() {
    if (eventSource.value) {
      eventSource.value.close()
    }

    console.log('🔌 正在连接日志流...')
    eventSource.value = datacenterApi.createLogStream()

    eventSource.value.onopen = () => {
      console.log('✅ 日志流连接成功')
      connected.value = true
    }

    eventSource.value.addEventListener('log', (event: MessageEvent) => {
      console.log('📝 收到日志事件:', event.data)
      try {
        if (!event.data || event.data.trim() === '') {
          console.warn('⚠️ 收到空日志数据')
          return
        }

        const log: LogEntry = JSON.parse(event.data)
        console.log('✅ 日志解析成功:', log)

        if (log && typeof log === 'object') {
          datacenterStore.addLog(log)
        } else {
          console.warn('⚠️ 日志对象格式无效:', log)
        }
      } catch (error) {
        console.error('❌ 日志解析失败:', error, '原始数据:', event.data)
      }
    })

    eventSource.value.addEventListener('ping', (event: MessageEvent) => {
      try {
        if (event.data) {
          const ping = JSON.parse(event.data)
          console.debug('💓 收到心跳:', ping)
        }
      } catch (error) {
        console.debug('⚠️ 心跳解析失败:', error)
      }
    })

    eventSource.value.onerror = (error) => {
      console.error('❌ 日志流连接错误:', error)
      connected.value = false
      console.log('🔄 将在5秒后重连...')
      setTimeout(connect, 5000)
    }
  }

  // 断开连接
  function disconnect() {
    if (eventSource.value) {
      eventSource.value.close()
      eventSource.value = null
      connected.value = false
      console.log('🔌 日志流已断开')
    }
  }

  // 组件挂载时连接
  onMounted(() => {
    // 添加初始日志
    datacenterStore.addLog({
      timestamp: new Date().toISOString(),
      level: 'INFO',
      message: 'Web控制面板已加载，等待连接数据中心...'
    })
    connect()
  })

  // 组件卸载时断开
  onUnmounted(() => {
    disconnect()
  })

  return {
    connected,
    connect,
    disconnect
  }
}

