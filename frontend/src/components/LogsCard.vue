<template>
  <n-card title="实时日志" hoverable>
    <!-- 日志控制按钮 -->
    <template #header-extra>
      <n-space>
        <n-button
          size="small"
          @click="handleClear"
          :disabled="logs.length === 0"
        >
          <template #icon>
            <n-icon><DeleteIcon /></n-icon>
          </template>
          清空日志
        </n-button>
        <n-button
          size="small"
          :type="autoScroll ? 'primary' : 'default'"
          @click="handleToggleAutoScroll"
        >
          <template #icon>
            <n-icon>
              <CheckmarkIcon v-if="autoScroll" />
              <CloseIcon v-else />
            </n-icon>
          </template>
          自动滚动
        </n-button>
      </n-space>
    </template>

    <!-- 日志内容 -->
    <div 
      ref="logsContainerRef" 
      class="logs-container"
      :class="{ 'logs-container-dark': isDark }"
    >
      <div
        v-for="(log, index) in logs"
        :key="index"
        class="log-entry"
      >
        <n-text depth="3" class="log-timestamp">
          {{ formatTimestamp(log.timestamp) }}
        </n-text>
        <n-tag
          :type="getLogType(log.level)"
          size="small"
          round
          strong
          class="log-level"
        >
          {{ log.level }}
        </n-tag>
        <n-text class="log-message">{{ log.message }}</n-text>
      </div>
    </div>
  </n-card>
</template>

<script setup lang="ts">
import { ref, watch, nextTick } from 'vue'
import { storeToRefs } from 'pinia'
import { useDatacenterStore } from '@/stores/datacenter'
import { useThemeStore } from '@/stores/theme'
import { useLogs } from '@/composables/useLogs'
import { formatTimestamp } from '@/utils/format'
import type { LogLevel } from '@/types'
import { TrashOutline as DeleteIcon, CheckmarkOutline as CheckmarkIcon, CloseOutline as CloseIcon } from '@vicons/ionicons5'

const datacenterStore = useDatacenterStore()
const themeStore = useThemeStore()
const { logs, autoScroll } = storeToRefs(datacenterStore)
const { isDark } = storeToRefs(themeStore)
const logsContainerRef = ref<HTMLElement | null>(null)

// 连接日志流
useLogs()

// 监听日志变化，自动滚动
watch(
  () => logs.value.length,
  async () => {
    if (autoScroll.value && logsContainerRef.value) {
      await nextTick()
      logsContainerRef.value.scrollTop = logsContainerRef.value.scrollHeight
    }
  }
)

function getLogType(level: LogLevel): 'default' | 'success' | 'warning' | 'error' | 'info' {
  const map: Record<LogLevel, 'default' | 'success' | 'warning' | 'error' | 'info'> = {
    'INFO': 'info',
    'WARNING': 'warning',
    'ERROR': 'error',
    'DEBUG': 'default'
  }
  return map[level] || 'default'
}

function handleClear() {
  datacenterStore.clearLogs()
}

function handleToggleAutoScroll() {
  datacenterStore.toggleAutoScroll()
}
</script>

<style scoped>
/* 浅色模式样式（默认） */
.logs-container {
  background: #f8fafc;
  color: #1e293b;
  padding: 16px;
  border-radius: 8px;
  font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
  font-size: 13px;
  max-height: 500px;
  overflow-y: auto;
  border: 1px solid #e2e8f0;
  box-shadow: inset 0 1px 3px rgba(0, 0, 0, 0.05);
  transition: all 0.3s ease;
}

/* 暗黑模式样式 */
.logs-container-dark {
  background: #1f2937 !important;
  color: #f3f4f6 !important;
  border-color: #374151 !important;
  box-shadow: inset 0 2px 8px rgba(0, 0, 0, 0.3) !important;
}

.log-entry {
  padding: 8px 0;
  border-bottom: 1px solid #e2e8f0;
  display: flex;
  align-items: center;
  gap: 10px;
  transition: border-color 0.3s ease;
}

/* 暗黑模式下的日志条目 */
.logs-container-dark .log-entry {
  border-bottom-color: #374151;
}

.log-entry:last-child {
  border-bottom: none;
}

.log-timestamp {
  font-size: 12px;
  flex-shrink: 0;
  color: #64748b;
  font-weight: 500;
  transition: color 0.3s ease;
}

/* 暗黑模式下的时间戳 */
.logs-container-dark .log-timestamp {
  color: #9ca3af !important;
}

.log-level {
  flex-shrink: 0;
}

.log-message {
  flex: 1;
  word-break: break-word;
  color: #334155;
  line-height: 1.6;
  transition: color 0.3s ease;
}

/* 暗黑模式下的日志消息 */
.logs-container-dark .log-message {
  color: #e5e7eb !important;
}

/* 滚动条样式 - 浅色模式 */
.logs-container::-webkit-scrollbar {
  width: 8px;
}

.logs-container::-webkit-scrollbar-track {
  background: #e5e7eb;
  border-radius: 4px;
}

.logs-container::-webkit-scrollbar-thumb {
  background: #9ca3af;
  border-radius: 4px;
}

.logs-container::-webkit-scrollbar-thumb:hover {
  background: #6b7280;
}

/* 滚动条样式 - 暗黑模式 */
.logs-container-dark::-webkit-scrollbar-track {
  background: #111827;
}

.logs-container-dark::-webkit-scrollbar-thumb {
  background: #4b5563;
}

.logs-container-dark::-webkit-scrollbar-thumb:hover {
  background: #6b7280;
}

@media (max-width: 768px) {
  .logs-container {
    max-height: 350px;
    font-size: 12px;
  }
}

@media (max-width: 480px) {
  .logs-container {
    max-height: 250px;
    font-size: 11px;
    padding: 12px;
  }
}
</style>

