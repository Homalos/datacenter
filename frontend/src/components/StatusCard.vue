<template>
  <n-card title="系统状态" hoverable>
    <n-space vertical :size="20">
      <!-- 当前状态 -->
      <div>
        <n-text strong>当前状态：</n-text>
        <n-tag
          :type="statusType"
          size="large"
          round
          strong
          style="margin-left: 10px"
        >
          {{ statusText }}
        </n-tag>
      </div>

      <!-- 信息网格 -->
      <n-grid :cols="2" :x-gap="15" :y-gap="15" responsive="screen">
        <n-gi>
          <n-card size="small" hoverable>
            <n-statistic label="启动时间">
              <template #default>
                <n-text style="font-size: 16px">{{ startTimeText }}</n-text>
              </template>
            </n-statistic>
          </n-card>
        </n-gi>
        <n-gi>
          <n-card size="small" hoverable>
            <n-statistic label="运行时长">
              <template #default>
                <n-text style="font-size: 16px">{{ uptimeText }}</n-text>
              </template>
            </n-statistic>
          </n-card>
        </n-gi>
      </n-grid>

      <!-- 控制按钮 -->
      <n-space wrap>
        <n-button
          type="success"
          :loading="loading"
          :disabled="isRunning || isStarting"
          @click="handleStart"
        >
          <template #icon>
            <n-icon><PlayIcon /></n-icon>
          </template>
          启动数据中心
        </n-button>
        <n-button
          type="error"
          :loading="loading"
          :disabled="!isRunning"
          @click="handleStop"
        >
          <template #icon>
            <n-icon><StopIcon /></n-icon>
          </template>
          停止数据中心
        </n-button>
        <n-button
          type="info"
          :loading="loading"
          @click="handleRestart"
        >
          <template #icon>
            <n-icon><RefreshIcon /></n-icon>
          </template>
          重启数据中心
        </n-button>
        <n-button
          type="default"
          @click="handleRefresh"
        >
          <template #icon>
            <n-icon><ReloadIcon /></n-icon>
          </template>
          刷新状态
        </n-button>
      </n-space>

      <!-- 错误提示 -->
      <n-alert
        v-if="state.error_message"
        type="error"
        :title="`错误: ${state.error_message}`"
        closable
      />

      <!-- 链接区域 -->
      <n-divider />
      <n-space>
        <n-button text tag="a" href="/docs" target="_blank" type="primary">
          📖 API 文档
        </n-button>
        <n-button text tag="a" href="/health" target="_blank" type="primary">
          ❤️ 健康检查
        </n-button>
        <n-button text tag="a" href="/metrics/summary" target="_blank" type="primary">
          📈 监控指标
        </n-button>
      </n-space>
    </n-space>
  </n-card>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { storeToRefs } from 'pinia'
import { useDatacenterStore } from '@/stores/datacenter'
import { useDialog, useMessage } from 'naive-ui'
import { PlayCircleOutline as PlayIcon, StopCircleOutline as StopIcon, RefreshOutline as RefreshIcon, ReloadOutline as ReloadIcon } from '@vicons/ionicons5'

const datacenterStore = useDatacenterStore()
const { state, loading, isRunning, isStarting, statusText, uptimeText, startTimeText } = storeToRefs(datacenterStore)
const dialog = useDialog()
const message = useMessage()

const statusType = computed(() => {
  const map: Record<string, 'default' | 'success' | 'warning' | 'error' | 'info'> = {
    'stopped': 'default',
    'starting': 'warning',
    'running': 'success',
    'stopping': 'warning',
    'error': 'error'
  }
  return map[state.value.status] || 'default'
})

async function handleStart() {
  const result = await datacenterStore.start()
  if (result.success) {
    message.success(result.message)
  } else {
    message.error(result.message)
  }
}

async function handleStop() {
  dialog.warning({
    title: '确认停止',
    content: '确定要停止数据中心吗？',
    positiveText: '确定',
    negativeText: '取消',
    onPositiveClick: async () => {
      const result = await datacenterStore.stop()
      if (result.success) {
        message.success(result.message)
      } else {
        message.error(result.message)
      }
    }
  })
}

async function handleRestart() {
  dialog.warning({
    title: '确认重启',
    content: '确定要重启数据中心吗？',
    positiveText: '确定',
    negativeText: '取消',
    onPositiveClick: async () => {
      const result = await datacenterStore.restart()
      if (result.success) {
        message.success(result.message)
      } else {
        message.error(result.message)
      }
    }
  })
}

function handleRefresh() {
  datacenterStore.fetchStatus()
  message.success('状态已刷新')
}
</script>

